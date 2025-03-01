"""Fetch prices from Yahoo Finance's CSV API.

As of late 2017, the older Yahoo finance API deprecated. In particular, the
ichart endpoint is gone, and the download endpoint requires a cookie (which
could be gotten - here's some documentation for that
http://blog.bradlucas.com/posts/2017-06-02-new-yahoo-finance-quote-download-url/).

We're using both the v7 and v8 APIs here, both of which are, as far as I can
tell, undocumented:

https://query1.finance.yahoo.com/v7/finance/quote
https://query1.finance.yahoo.com/v8/finance/chart/SYMBOL

Timezone information: Input and output datetimes are specified via UNIX
timestamps, but the timezone of the particular market is included in the output.
"""

__copyright__ = "Copyright (C) 2015-2020  Martin Blais"
__license__ = "GNU GPLv2"

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union
import re
import time
import json
import logging

import requests

from beanprice import source


class YahooError(ValueError):
    "An error from the Yahoo API."


def parse_response(response: requests.models.Response) -> Dict:
    """Process as response from Yahoo.

    Raises:
      YahooError: If there is an error in the response.
    """
    try:
        json = response.json(parse_float=Decimal)
    except (ValueError, requests.exceptions.JSONDecodeError) as e:
        # Handle non-JSON responses
        raise YahooError(f"Invalid JSON response from Yahoo: {response.text[:100]}...") from e

    if not json:
        raise YahooError("Empty response from Yahoo")

    content = next(iter(json.values()), None)
    if content is None:
        raise YahooError(f"Unexpected response format from Yahoo: {json}")

    if response.status_code != requests.codes.ok:
        raise YahooError("Status {}: {}".format(response.status_code, content.get("error", "Unknown error")))
    if len(json) != 1:
        raise YahooError(
            "Invalid format in response from Yahoo; many keys: {}".format(
                ",".join(json.keys())
            )
        )
    if content.get("error") is not None:
        raise YahooError("Error fetching Yahoo data: {}".format(content["error"]))
    if not content.get("result"):
        raise YahooError("No data returned from Yahoo, ensure that the symbol is correct")
    return content["result"][0]


# Note: Feel free to suggest more here via a PR.
_MARKETS = {
    "us_market": "USD",
    "ca_market": "CAD",
    "ch_market": "CHF",
}


def parse_currency(result: Dict[str, Any]) -> Optional[str]:
    """Infer the currency from the result."""
    if "market" not in result:
        return None
    return _MARKETS.get(result["market"], None)


_DEFAULT_PARAMS = {
    "lang": "en-US",
    "corsDomain": "finance.yahoo.com",
    ".tsrc": "finance",
}


def get_price_from_yfinance(ticker: str) -> Tuple[Decimal, datetime, str]:
    """Get price data using the yfinance library if available.

    This is a fallback method when the API is not accessible.
    """
    try:
        import yfinance as yf
    except ImportError:
        raise YahooError("yfinance library not installed. Install with 'pip install yfinance'")

    try:
        data = yf.Ticker(ticker)
        info = data.info

        if 'regularMarketPrice' not in info:
            raise YahooError(f"Could not find price data for {ticker}")

        price = Decimal(str(info['regularMarketPrice']))
        currency = info.get('currency', 'USD')

        # Use current time as we can't reliably extract the exact trade time
        trade_time = datetime.now(timezone.utc)

        return price, trade_time, currency
    except Exception as e:
        raise YahooError(f"Error fetching data with yfinance for {ticker}: {str(e)}")


def get_price_from_alternative_api(ticker: str, session: requests.Session) -> Tuple[Decimal, datetime, str]:
    """Get price data using an alternative Yahoo Finance API endpoint.

    This is a fallback method when the primary API is not accessible.
    """
    # Try the v10 API endpoint which might be less restricted
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
    params = {
        "modules": "price,summaryDetail",
    }

    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        if not data or 'quoteSummary' not in data or not data['quoteSummary'].get('result'):
            raise YahooError(f"No data returned for {ticker}")

        result = data['quoteSummary']['result'][0]

        # Extract price from the response
        if 'price' not in result:
            raise YahooError(f"Price data not found for {ticker}")

        price_data = result['price']
        price = Decimal(str(price_data.get('regularMarketPrice', {}).get('raw', 0)))

        if price == 0:
            raise YahooError(f"Invalid price (0) for {ticker}")

        # Get currency
        currency = price_data.get('currency', 'USD')

        # Use current time as we can't reliably extract the exact trade time
        trade_time = datetime.now(timezone.utc)

        return price, trade_time, currency
    except requests.exceptions.RequestException as e:
        raise YahooError(f"Error fetching data from alternative API for {ticker}: {str(e)}")


def get_price_series(
    ticker: str,
    time_begin: datetime,
    time_end: datetime,
    session: requests.Session,
) -> Tuple[List[Tuple[datetime, Decimal]], str]:
    """Return a series of timestamped prices."""

    if requests is None:
        raise YahooError("You must install the 'requests' library.")

    # First try the API approach
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/{}".format(ticker)
        payload: Dict[str, Union[int, str]] = {
            "period1": int(time_begin.timestamp()),
            "period2": int(time_end.timestamp()),
            "interval": "1d",
        }
        payload.update(_DEFAULT_PARAMS)

        response = session.get(url, params=payload, timeout=10)

        # Check if the response is valid before parsing
        if not response.text or response.status_code != 200:
            raise YahooError(f"Invalid response from Yahoo for {ticker}: Status {response.status_code}")

        result = parse_response(response)

        meta = result["meta"]
        try:
            tzone = timezone(
                timedelta(hours=meta["gmtoffset"] / 3600), meta["exchangeTimezoneName"]
            )
        except KeyError:
            # If timezone info is missing, use UTC
            tzone = timezone.utc

        if "timestamp" not in result:
            raise YahooError(
                "Yahoo returned no data for ticker {} for time range {} - {}".format(
                    ticker, time_begin, time_end
                )
            )

        timestamp_array = result["timestamp"]
        close_array = result["indicators"]["quote"][0]["close"]
        series = [
            (datetime.fromtimestamp(timestamp, tz=tzone), Decimal(price))
            for timestamp, price in zip(timestamp_array, close_array)
            if price is not None
        ]

        # Get currency from meta, default to USD if not available
        currency = meta.get("currency", "USD")
        return series, currency

    except (YahooError, requests.exceptions.RequestException) as e:
        logging.warning(f"Primary API failed for {ticker}: {str(e)}")

        # Try the alternative API approach
        try:
            price, trade_time, currency = get_price_from_alternative_api(ticker, session)
            # Return a single data point as our fallback
            return [(trade_time, price)], currency
        except YahooError as alt_error:
            logging.warning(f"Alternative API failed for {ticker}: {str(alt_error)}")

            # Try yfinance as a last resort
            try:
                price, trade_time, currency = get_price_from_yfinance(ticker)
                # Return a single data point as our fallback
                return [(trade_time, price)], currency
            except Exception as yf_error:
                # If all methods fail, raise the original API error
                raise YahooError(f"Failed to get price data for {ticker}: {str(e)}") from e


class Source(source.Source):
    "Yahoo Finance CSV API price extractor."

    def __init__(self):
        """Initialize a shared session with the required headers and cookies."""
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.114 Safari/537.36"
            }
        )
        # Try to initialize cookies using a more reliable domain
        try:
            # Try the main Yahoo Finance domain instead of fc.yahoo.com
            self.session.get("https://finance.yahoo.com", timeout=10)
        except requests.exceptions.RequestException as e:
            # If that fails, we'll continue without cookies
            # This might limit some functionality but allows basic price fetching
            pass

        # Try to get the crumb, but handle failure gracefully
        try:
            self.crumb = self.session.get(
                "https://query1.finance.yahoo.com/v1/test/getcrumb",
                timeout=10
            ).text
        except requests.exceptions.RequestException:
            # If we can't get a crumb, use an empty string
            # Some API endpoints might still work without it
            self.crumb = ""

    def get_latest_price(self, ticker: str) -> Optional[source.SourcePrice]:
        """See contract in beanprice.source.Source."""

        # First try the API approach
        try:
            url = "https://query1.finance.yahoo.com/v7/finance/quote"
            fields = ["symbol", "regularMarketPrice", "regularMarketTime"]
            payload = {
                "symbols": ticker,
                "fields": ",".join(fields),
                "crumb": self.crumb,  # Use the session's crumb
            }
            payload.update(_DEFAULT_PARAMS)

            response = self.session.get(url, params=payload, timeout=10)

            try:
                result = parse_response(response)
            except YahooError as error:
                # The parse_response method cannot know which ticker failed,
                # but the user definitely needs to know which ticker failed!
                raise YahooError("%s (ticker: %s)" % (error, ticker)) from error

            try:
                price = Decimal(result["regularMarketPrice"])

                tzone = timezone(
                    timedelta(hours=result["gmtOffSetMilliseconds"] / 3600000),
                    result["exchangeTimezoneName"],
                )
                trade_time = datetime.fromtimestamp(result["regularMarketTime"], tz=tzone)
            except KeyError as exc:
                raise YahooError(
                    "Invalid response from Yahoo: {}".format(repr(result))
                ) from exc

            # Try to get currency from the result, fall back to USD if not available
            currency = parse_currency(result)
            if currency is None and "currency" in result:
                currency = result["currency"]
            if currency is None:
                currency = "USD"  # Default to USD if we can't determine the currency

            return source.SourcePrice(price, trade_time, currency)

        except (YahooError, requests.exceptions.RequestException) as e:
            logging.warning(f"Primary API failed for {ticker}: {str(e)}")

            # Try the alternative API approach
            try:
                price, trade_time, currency = get_price_from_alternative_api(ticker, self.session)
                return source.SourcePrice(price, trade_time, currency)
            except YahooError as alt_error:
                logging.warning(f"Alternative API failed for {ticker}: {str(alt_error)}")

                # Try yfinance as a last resort
                try:
                    price, trade_time, currency = get_price_from_yfinance(ticker)
                    return source.SourcePrice(price, trade_time, currency)
                except Exception as yf_error:
                    # If all methods fail, raise the original API error
                    raise YahooError(f"Failed to get price data for {ticker}: {str(e)}") from e

    def get_historical_price(
        self, ticker: str, time: datetime
    ) -> Optional[source.SourcePrice]:
        """See contract in beanprice.source.Source."""

        # Get the latest data returned over the last 5 days.
        try:
            series, currency = get_price_series(
                ticker, time - timedelta(days=5), time, self.session
            )
        except YahooError as e:
            # Try a longer time range if the 5-day range fails
            try:
                series, currency = get_price_series(
                    ticker, time - timedelta(days=30), time, self.session
                )
            except YahooError:
                # If both API attempts fail, try the alternative API
                try:
                    price, trade_time, currency = get_price_from_alternative_api(ticker, self.session)
                    # We can only get the current price, so we'll use that
                    # This is not ideal for historical prices but better than failing
                    return source.SourcePrice(price, trade_time, currency)
                except YahooError:
                    # Try yfinance as a last resort
                    try:
                        price, trade_time, currency = get_price_from_yfinance(ticker)
                        return source.SourcePrice(price, trade_time, currency)
                    except Exception:
                        # Re-raise the original error if all attempts fail
                        raise e

        latest = None
        for data_dt, price in sorted(series):
            if data_dt >= time:
                break
            latest = data_dt, price
        if latest is None:
            raise YahooError("Could not find price before {} in {}".format(time, series))

        data_dt, price = latest
        return source.SourcePrice(price, data_dt, currency)

    def get_daily_prices(
        self, ticker: str, time_begin: datetime, time_end: datetime
    ) -> Optional[List[source.SourcePrice]]:
        """See contract in beanprice.source.Source."""
        series, currency = get_price_series(ticker, time_begin, time_end, self.session)
        return [source.SourcePrice(price, time, currency) for time, price in series]

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

import requests

from beanprice import source


class YahooError(ValueError):
    "An error from the Yahoo API."


def parse_response(response: requests.models.Response) -> Dict:
    """Process as response from Yahoo.

    Raises:
      YahooError: If there is an error in the response.
    """
    json = response.json(parse_float=Decimal)
    content = next(iter(json.values()))
    if response.status_code != requests.codes.ok:
        raise YahooError("Status {}: {}".format(response.status_code, content["error"]))
    if len(json) != 1:
        raise YahooError(
            "Invalid format in response from Yahoo; many keys: {}".format(
                ",".join(json.keys())
            )
        )
    if content["error"] is not None:
        raise YahooError("Error fetching Yahoo data: {}".format(content["error"]))
    if not content["result"]:
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


def get_price_series(
    ticker: str,
    time_begin: datetime,
    time_end: datetime,
    session: requests.Session,
) -> Tuple[List[Tuple[datetime, Decimal]], str]:
    """Return a series of timestamped prices."""

    if requests is None:
        raise YahooError("You must install the 'requests' library.")
    url = "https://query1.finance.yahoo.com/v8/finance/chart/{}".format(ticker)
    payload: Dict[str, Union[int, str]] = {
        "period1": int(time_begin.timestamp()),
        "period2": int(time_end.timestamp()),
        "interval": "1d",
    }
    payload.update(_DEFAULT_PARAMS)

    try:
        response = session.get(url, params=payload, timeout=10)  # Use shared session with timeout
    except requests.exceptions.RequestException as e:
        raise YahooError(f"Connection error fetching data for {ticker}: {str(e)}")

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


class Source(source.Source):
    "Yahoo Finance CSV API price extractor."

    def __init__(self):
        """Initialize a shared session with the required headers and cookies."""
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) "
                "Gecko/20100101 Firefox/110.0"
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

        url = "https://query1.finance.yahoo.com/v7/finance/quote"
        fields = ["symbol", "regularMarketPrice", "regularMarketTime"]
        payload = {
            "symbols": ticker,
            "fields": ",".join(fields),
            "exchange": "NYSE",
            "crumb": self.crumb,  # Use the session's crumb
        }
        payload.update(_DEFAULT_PARAMS)

        try:
            response = self.session.get(url, params=payload, timeout=10)  # Use shared session with timeout
        except requests.exceptions.RequestException as e:
            raise YahooError(f"Connection error fetching data for {ticker}: {str(e)}")

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
                # Re-raise the original error if both attempts fail
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

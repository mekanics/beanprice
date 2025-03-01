"""A source fetching cryptocurrency prices from Coinbase.

Valid tickers are in the form "XXX-YYY", such as "BTC-USD".

Here is the API documentation:
https://developers.coinbase.com/api/v2

For example:
https://api.coinbase.com/v2/prices/BTC-GBP/spot

Timezone information: Input and output datetimes are specified via UTC
timestamps.
"""

import datetime
from decimal import Decimal

import requests
from dateutil.tz import tz

from beanprice import source


class CoinbaseError(ValueError):
    "An error from the Coinbase API."


def fetch_quote(ticker, time=None):
    """Fetch a quote from Coinbase."""
    url = "https://api.coinbase.com/v2/prices/{}/spot".format(ticker.lower())
    options = {}
    if time is not None:
        options["date"] = time.astimezone(tz.tzutc()).date().isoformat()

    response = requests.get(url, options)
    if response.status_code != requests.codes.ok:
        raise CoinbaseError(
            "Invalid response ({}): {}".format(response.status_code, response.text)
        )
    result = response.json()

    # Check if data is a list or dictionary and handle accordingly
    if isinstance(result.get("data"), list):
        # If it's a list, find the entry that matches our ticker
        ticker_parts = ticker.lower().split('-')
        if len(ticker_parts) != 2:
            raise CoinbaseError(f"Invalid ticker format: {ticker}. Expected format: BASE-CURRENCY")

        base_currency = ticker_parts[0].upper()
        quote_currency = ticker_parts[1].upper()

        # Try to find the matching entry
        matching_entries = [
            item for item in result["data"]
            if item.get("base", "").upper() == base_currency and
               item.get("currency", "").upper() == quote_currency
        ]

        if matching_entries:
            data_item = matching_entries[0]
            price = Decimal(data_item.get("amount", 0))
            currency = data_item.get("currency", "")
        else:
            # If no exact match, take the first entry with the right quote currency
            quote_currency_entries = [
                item for item in result["data"]
                if item.get("currency", "").upper() == quote_currency
            ]

            if quote_currency_entries:
                data_item = quote_currency_entries[0]
                price = Decimal(data_item.get("amount", 0))
                currency = data_item.get("currency", "")
            else:
                # If still no match, just take the first entry
                if result["data"]:
                    data_item = result["data"][0]
                    price = Decimal(data_item.get("amount", 0))
                    currency = data_item.get("currency", "")
                else:
                    raise CoinbaseError(f"No price data found in response: {result}")
    else:
        # Original behavior for dictionary
        try:
            price = Decimal(result["data"]["amount"])
            currency = result["data"]["currency"]
        except (KeyError, TypeError) as e:
            raise CoinbaseError(f"Failed to parse response: {result}. Error: {e}")

    if time is None:
        time = datetime.datetime.now(tz.tzutc())

    return source.SourcePrice(price, time, currency)


class Source(source.Source):
    "Coinbase API price extractor."

    def get_latest_price(self, ticker):
        """See contract in beanprice.source.Source."""
        return fetch_quote(ticker)

    def get_historical_price(self, ticker, time):
        """See contract in beanprice.source.Source."""
        return fetch_quote(ticker, time)

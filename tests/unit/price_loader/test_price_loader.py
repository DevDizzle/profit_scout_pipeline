import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import datetime as real_datetime  # real datetime module

from price_loader import (
    get_tickers_from_metadata,
    process_price_data,
    update_prices_for_ticker,
)

# A little subclass so datetime.date.today() → 2023‑01‑02
class FakeDate(real_datetime.date):
    @classmethod
    def today(cls):
        return real_datetime.date(2023, 1, 2)


class TestPriceLoader(unittest.TestCase):

    @patch('price_loader.bigquery.Client')
    def test_get_tickers_from_metadata(self, mock_bq_client_class):
        """Test fetching tickers from BigQuery metadata table."""
        mock_bq_client = mock_bq_client_class.return_value
        mock_query_job = MagicMock()

        # Mock objects with a .Ticker attribute
        row1 = MagicMock()
        row1.Ticker = 'AAPL'
        row2 = MagicMock()
        row2.Ticker = 'GOOGL'
        mock_query_job.result.return_value = [row1, row2]
        mock_bq_client.query.return_value = mock_query_job

        tickers = get_tickers_from_metadata(mock_bq_client, 'mock_table_id')
        self.assertEqual(tickers, ['AAPL', 'GOOGL'])
        mock_bq_client.query.assert_called_once()

    def test_process_price_data(self):
        """Test cleaning & renaming of yfinance DataFrame."""
        sample = {
            'Date': ['2023-01-01', '2023-01-02'],
            'Open': [150.0, 152.0],
            'High': [155.0, 156.0],
            'Low': [149.0, 151.0],
            'Close': [153.0, 154.0],
            'Volume': [1000000, 1200000],
        }
        df = pd.DataFrame(sample)
        df['Date'] = pd.to_datetime(df['Date'])

        out = process_price_data(df, 'AAPL')
        self.assertListEqual(
            out.columns.tolist(),
            ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
        )
        self.assertEqual(out['ticker'].iat[0], 'AAPL')
        self.assertEqual(out['date'].iat[0], real_datetime.date(2023, 1, 1))

    @patch('price_loader.yf')
    @patch('price_loader.bq_client')
    @patch('price_loader.datetime.date', FakeDate)
    def test_update_prices_for_ticker_existing_data(self, mock_bq_client, mock_yf):
        """When BigQuery has a max_date, we fetch from max_date+1 to today+1."""
        # 1) Mock the BQ "MAX(date)" query to return 2023-01-01
        mock_query = MagicMock()
        row = MagicMock()
        row.max_date = real_datetime.date(2023, 1, 1)
        mock_query.result.return_value = [row]
        mock_bq_client.query.return_value = mock_query

        # 2) Mock yfinance
        mock_stock = MagicMock()
        mock_stock.history.return_value = pd.DataFrame({
            'Date': [pd.to_datetime('2023-01-02')],
            'Open': [152.0],
            'High': [156.0],
            'Low': [151.0],
            'Close': [154.0],
            'Volume': [1200000],
        })
        mock_yf.Ticker.return_value = mock_stock

        # 3) Mock the BQ load job
        mock_load = MagicMock()
        mock_load.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_load

        # Run it
        update_prices_for_ticker('AAPL', 'mock_table_id')

        # Assertions
        mock_bq_client.query.assert_called_once()
        mock_yf.Ticker.assert_called_once_with('AAPL')
        # start = max_date + 1 day → "2023-01-02"
        # end   = today + 1 day   → "2023-01-03"
        mock_stock.history.assert_called_once_with(
            start='2023-01-02',
            end='2023-01-03',
            auto_adjust=True
        )
        mock_bq_client.load_table_from_dataframe.assert_called_once()

    @patch('price_loader.yf')
    @patch('price_loader.bq_client')
    @patch('price_loader.datetime.date', FakeDate)
    def test_update_prices_for_ticker_no_existing_data(self, mock_bq_client, mock_yf):
        """When no BQ data exists, initial load spans ~10 years to today."""
        # 1) No max_date rows
        mock_query = MagicMock()
        mock_query.result.return_value = []
        mock_bq_client.query.return_value = mock_query

        # 2) Mock yfinance with two dates: 2013-01-01 and today (2023-01-02)
        mock_stock = MagicMock()
        mock_stock.history.return_value = pd.DataFrame({
            'Date': [pd.to_datetime('2013-01-01'),
                     pd.to_datetime('2023-01-02')],
            'Open': [50.0, 152.0],
            'High': [55.0, 156.0],
            'Low': [49.0, 151.0],
            'Close': [53.0, 154.0],
            'Volume': [1000000, 1200000],
        })
        mock_yf.Ticker.return_value = mock_stock

        # 3) Mock BQ load
        mock_load = MagicMock()
        mock_load.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_load

        update_prices_for_ticker('AAPL', 'mock_table_id')

        mock_bq_client.query.assert_called_once()
        mock_yf.Ticker.assert_called_once_with('AAPL')

        # initial start = today - ~10y → around 2013-01-01
        start_str = (real_datetime.date(2023, 1, 2) -
                     real_datetime.timedelta(days=10*365 + 2)
                    ).strftime('%Y-%m-%d')
        mock_stock.history.assert_called_once_with(
            start=start_str,
            end='2023-01-03',
            auto_adjust=True
        )
        mock_bq_client.load_table_from_dataframe.assert_called_once()


if __name__ == '__main__':
    unittest.main()

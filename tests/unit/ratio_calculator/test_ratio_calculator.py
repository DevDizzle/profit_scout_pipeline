import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from decimal import Decimal
import datetime as real_datetime

import ratio_calculator as rc

class TestCleanDataForJson(unittest.TestCase):
    def test_allows_only_numeric_and_skips_invalid(self):
        # Build a dict with various types
        data = {
            'good_float': 123.45,
            'good_decimal': Decimal('67.89'),
            'good_int': 42,
            'nan_float': float('nan'),
            'inf_float': float('inf'),
            'none_val': None,
            'string': 'hello',
            'date': real_datetime.date(2023,1,1),
            'timestamp': pd.Timestamp('2023-01-02'),
            'other': object()
        }
        cleaned = rc.clean_data_for_json(data, ticker='TICK', context='ctx')
        # Should include only good_float, good_decimal (as float), good_int
        self.assertIn('good_float', cleaned)
        self.assertAlmostEqual(cleaned['good_float'], 123.45)
        self.assertIn('good_decimal', cleaned)
        self.assertAlmostEqual(cleaned['good_decimal'], 67.89)
        self.assertIn('good_int', cleaned)
        self.assertEqual(cleaned['good_int'], 42)
        # Everything else should be skipped
        for key in ('nan_float','inf_float','none_val','string','date','timestamp','other'):
            self.assertNotIn(key, cleaned)

class TestAdjustRatioScale(unittest.TestCase):
    def test_in_range_returns_same(self):
        # debt_to_equity expected range (-3,10)
        out = rc.adjust_ratio_scale(2.5, 'debt_to_equity', 'T')
        self.assertEqual(out, 2.5)

    def test_out_of_range_kept(self):
        out = rc.adjust_ratio_scale(20.0, 'debt_to_equity', 'T')
        self.assertEqual(out, 20.0)

    def test_percentage_adjustment(self):
        # gross_margin expected (0,1). If we pass 50 (i.e. 5000%), it'll divide by 100 → 0.5
        out = rc.adjust_ratio_scale(50.0, 'gross_margin', 'T')
        self.assertAlmostEqual(out, 0.5)

    def test_non_numeric_input(self):
        self.assertIsNone(rc.adjust_ratio_scale("foo", 'roe', 'T'))

    def test_nan_and_inf(self):
        self.assertIsNone(rc.adjust_ratio_scale(float('nan'), 'roe', 'T'))
        self.assertIsNone(rc.adjust_ratio_scale(float('inf'), 'roe', 'T'))

class TestGetPriceOnOrBefore(unittest.TestCase):
    @patch('ratio_calculator.bq_client')
    def test_returns_price_when_found(self, mock_bq_client):
        # Mock a DataFrame with a price
        df = pd.DataFrame({'price':[123.0]})
        mock_job = MagicMock()
        mock_job.to_dataframe.return_value = df
        mock_bq_client.query.return_value = mock_job

        d = real_datetime.date(2023,1,15)
        price = rc.get_price_on_or_before('AAPL', d)
        self.assertEqual(price, 123.0)

        # Ensure query was called with the right parameters object
        mock_bq_client.query.assert_called_once()
    
    @patch('ratio_calculator.bq_client')
    def test_returns_none_if_empty(self, mock_bq_client):
        # Empty DataFrame
        df = pd.DataFrame({'price':[]})
        mock_job = MagicMock()
        mock_job.to_dataframe.return_value = df
        mock_bq_client.query.return_value = mock_job

        d = real_datetime.date(2023,1,15)
        price = rc.get_price_on_or_before('AAPL', d)
        self.assertIsNone(price)

    @patch('ratio_calculator.bq_client')
    def test_handles_api_error(self, mock_bq_client):
        # Simulate an API error from BigQuery
        from google.api_core import exceptions
        mock_bq_client.query.side_effect = exceptions.GoogleAPICallError("boom")

        with self.assertRaises(exceptions.GoogleAPICallError):
            rc.get_price_on_or_before('AAPL', real_datetime.date(2023,1,15))

if __name__ == '__main__':
    unittest.main()

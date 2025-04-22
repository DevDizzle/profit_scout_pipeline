import asyncio
import os
import tempfile
import unittest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from services.subscriber.src import subscriber

class TestSubscriberHelpers(unittest.TestCase):
    def test_create_snake_case_name_various(self):
        cases = {
            "Total Assets (USD)": "total_assets_usd",
            "  Net Income%  ": "net_income",
            "123 Revenue": "col_123_revenue",
            "select": "col_select",
            "": None,
            None: None,
        }
        for raw, want in cases.items():
            self.assertEqual(subscriber.create_snake_case_name(raw), want)

    def test_access_secret_version_success(self):
        fake_resp = MagicMock()
        fake_resp.payload.data.decode.return_value = "secret-xyz"
        client = MagicMock()
        client.access_secret_version.return_value = fake_resp

        with patch("services.subscriber.src.subscriber.secretmanager.SecretManagerServiceClient", lambda: client):
            val = subscriber.access_secret_version("proj", "sec", "ver")
        self.assertEqual(val, "secret-xyz")

    def test_access_secret_version_failure(self):
        client = MagicMock()
        client.access_secret_version.side_effect = Exception("boom")
        with patch("services.subscriber.src.subscriber.secretmanager.SecretManagerServiceClient", lambda: client):
            with self.assertRaises(Exception):
                subscriber.access_secret_version("p", "s", "v")

class TestProcessFinancialDF(unittest.TestCase):
    def test_empty_input(self):
        df = pd.DataFrame()
        out = subscriber.process_financial_df(df, "TICK", "income")
        self.assertTrue(out.empty)

    def test_basic_transformation(self):
        # build a small DataFrame keyed by date in the index
        idx = pd.to_datetime(["2021-01-01", "2021-04-01"], utc=True)
        raw = pd.DataFrame({
            "Net Income": [100, 200],
            "Total Assets": [1000, 1100],
        }, index=idx)
        res = subscriber.process_financial_df(raw, "ABC", "balance_sheet")
        # Check columns
        self.assertIn("period_end_date", res.columns)
        self.assertIn("net_income", res.columns)
        self.assertIn("total_assets", res.columns)
        # Check types
        self.assertTrue(all(isinstance(d, date) for d in res["period_end_date"]))
        self.assertTrue(all(isinstance(x, (int, float)) for x in res["net_income"]))
        # Check ticker column
        self.assertTrue(all(res["ticker"] == "ABC"))

class TestFetchFinancialStatements(unittest.IsolatedAsyncioTestCase):
    async def test_no_client(self):
        with self.assertRaises(RuntimeError):
            await subscriber.fetch_financial_statements_by_url(None, "url")

    async def test_filing_none(self):
        fake_api = MagicMock()
        # patch run_in_executor to return None for filing
        async def fake_run(coro, *args, **kwargs):
            return None
        with patch("services.subscriber.src.subscriber.asyncio.get_running_loop", lambda: MagicMock(run_in_executor=fake_run)):
            res = await subscriber.fetch_financial_statements_by_url(fake_api, "url")
        self.assertEqual(res, (None, None, None, None))

    async def test_filing_success(self):
        class FakeFiling:
            def __init__(self):
                self.period_of_report = "2021-01-01"
            def get_income_statement(self): return {"A": [1]}
            def get_balance_sheet(self): return {"B": [2]}
            def get_cash_flow_statement(self): return {"C": [3]}

        fake_api = MagicMock(from_url=lambda url: FakeFiling())
        # patch run_in_executor to call the callable immediately
        loop = asyncio.get_running_loop()
        async def fake_run(fn, *args):
            # if fn is a lambda: execute
            return fn()
        with patch.object(loop, "run_in_executor", fake_run):
            inc, bs, cf, por = await subscriber.fetch_financial_statements_by_url(fake_api, "url")
        self.assertEqual(por, "2021-01-01")
        self.assertIsInstance(inc, dict)
        self.assertIsInstance(bs, dict)
        self.assertIsInstance(cf, dict)

if __name__ == "__main__":
    unittest.main()

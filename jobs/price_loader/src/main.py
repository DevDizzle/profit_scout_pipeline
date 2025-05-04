# profit_scout_pipeline/services/price_loader/src/price_loader.py

import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import yfinance as yf
import time
import logging
import datetime

# --- Configuration (Uppercase, SCREAMING_SNAKE_CASE) ---
GCP_PROJECT_ID       = os.getenv('GCP_PROJECT_ID')
BQ_DATASET_ID        = os.getenv('BQ_DATASET_ID', 'profit_scout')
BQ_PRICE_TABLE_ID    = os.getenv('BQ_PRICES_TABLE_ID', 'price_data')
BQ_METADATA_TABLE_ID = os.getenv('BQ_METADATA_TABLE_ID', 'filing_metadata')

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.getLogger("yfinance").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Validate Configuration ---
essential_vars = [
    ('GCP_PROJECT_ID', GCP_PROJECT_ID),
    ('BQ_DATASET_ID', BQ_DATASET_ID),
    ('BQ_PRICE_TABLE_ID', BQ_PRICE_TABLE_ID),
    ('BQ_METADATA_TABLE_ID', BQ_METADATA_TABLE_ID),
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    raise RuntimeError(f"Missing essential environment variables: {', '.join(missing_vars)}")

# --- Construct full table IDs ---
PRICES_TABLE_FULL_ID   = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_PRICE_TABLE_ID}"
METADATA_TABLE_FULL_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_METADATA_TABLE_ID}"

# --- Initialize BigQuery Client ---
try:
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    logging.info(f"BigQuery client initialized for project '{GCP_PROJECT_ID}'.")
except Exception as e:
    raise RuntimeError(f"Failed to initialize BigQuery client: {e}")

# --- Helper Functions ---
def get_tickers_from_metadata(client, metadata_table_id):
    """
    Fetches the distinct list of tickers from the filing_metadata table.
    Returns a list of ticker symbols.
    """
    if not client:
        logging.error("BigQuery client not available for get_tickers_from_metadata.")
        return []

    query = f"""
        SELECT DISTINCT Ticker
        FROM `{metadata_table_id}`
        WHERE Ticker IS NOT NULL AND Ticker != ''
        ORDER BY Ticker
    """
    logging.info(f"Querying distinct tickers from {metadata_table_id}")

    try:
        query_job = client.query(query)
        results   = query_job.result(timeout=180)
        tickers   = [row.Ticker for row in results]
        logging.info(f"Found {len(tickers)} unique tickers in metadata table.")
        return tickers

    except NotFound:
        logging.error(f"Metadata table {metadata_table_id} not found.")
        return []
    except Exception as e:
        logging.error(f"Error querying tickers from metadata table {metadata_table_id}: {e}", exc_info=True)
        return []

def process_price_data(prices_df, ticker):
    """Cleans and standardizes the price DataFrame from yfinance."""
    if prices_df is None or prices_df.empty:
        return pd.DataFrame()

    try:
        prices_df = prices_df.reset_index()
        prices_df['ticker'] = ticker

        if 'Date' not in prices_df.columns:
            logging.error(
                f"'Date' column not found in yfinance output for {ticker}. "
                f"Columns: {prices_df.columns.tolist()}"
            )
            return pd.DataFrame()

        # strip timezone if present
        if pd.api.types.is_datetime64_any_dtype(prices_df['Date']):
            if prices_df['Date'].dt.tz is not None:
                prices_df['Date'] = prices_df['Date'].dt.tz_convert(None)

        expected_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        present_cols = ['ticker'] + [c for c in expected_cols if c in prices_df.columns]
        prices_df    = prices_df[present_cols].copy()

        rename_map = {
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'adj_close',
            'Volume': 'volume'
        }
        prices_df.rename(
            columns={k: v for k, v in rename_map.items() if k in present_cols},
            inplace=True
        )

        # ensure correct dtypes
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        if 'volume' in prices_df.columns:
            prices_df['volume'] = pd.to_numeric(prices_df['volume'], errors='coerce')

        for col in prices_df.columns.difference(['ticker', 'date']):
            prices_df[col] = pd.to_numeric(prices_df[col], errors='coerce')

        return prices_df

    except Exception as e:
        logging.error(f"Error processing yfinance data for {ticker}: {e}", exc_info=True)
        return pd.DataFrame()

# --- Incremental Update Function ---
def update_prices_for_ticker(ticker: str, prices_table_id: str):
    """
    Fetches stock prices for a single ticker from the last recorded date in BigQuery
    up to the current date and appends them.
    """
    logging.info(f"--- Starting Price Update for {ticker} ---")
    max_date = None

    # 1) Determine last date in BQ
    try:
        sql = f"""
            SELECT MAX(date) as max_date
            FROM `{prices_table_id}`
            WHERE ticker = @ticker
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        res = bq_client.query(sql, job_config=job_config).result(timeout=60)
        for row in res:
            max_date = row.max_date

        if max_date:
            if isinstance(max_date, datetime.datetime):
                max_date = max_date.date()
            logging.info(f"Max date for {ticker}: {max_date}")
        else:
            logging.info(f"No existing data for {ticker}, doing full load.")

    except NotFound:
        logging.warning(f"Table {prices_table_id} not found or no data for {ticker}.")
    except Exception as e:
        logging.error(f"Error fetching max date for {ticker}: {e}", exc_info=True)
        return

    # 2) Build date window
    if max_date:
        start_date = max_date + datetime.timedelta(days=1)
    else:
        start_date = datetime.date.today() - datetime.timedelta(days=10*365 + 2)
    end_date = datetime.date.today() + datetime.timedelta(days=1)

    if start_date >= end_date:
        logging.info(f"{ticker} is already up to date ({max_date}). Skipping.")
        return

    start_str = start_date.strftime('%Y-%m-%d')
    end_str   = end_date.strftime('%Y-%m-%d')

    # 3) Fetch via yfinance
    try:
        logging.info(f"Fetching yfinance for {ticker}: {start_str} → {end_str}")
        stock  = yf.Ticker(ticker)
        prices = stock.history(start=start_str, end=end_str, auto_adjust=True)

        if prices.empty:
            logging.info(f"No new data for {ticker} in {start_str}–{end_str}.")
            return

        logging.info(f"Fetched {len(prices)} rows for {ticker}.")
        new_df = process_price_data(prices, ticker)

    except Exception as e:
        logging.error(f"Error fetching yfinance data for {ticker}: {e}", exc_info=True)
        return

    # 4) Load into BigQuery with detailed error logging
    if not new_df.empty:
        table_ref = bigquery.Table(prices_table_id)
        schema = [
            bigquery.SchemaField('ticker', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('date',   'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('open',   'FLOAT',     mode='NULLABLE'),
            bigquery.SchemaField('high',   'FLOAT',     mode='NULLABLE'),
            bigquery.SchemaField('low',    'FLOAT',     mode='NULLABLE'),
            bigquery.SchemaField('adj_close', 'FLOAT',  mode='NULLABLE'),
            bigquery.SchemaField('volume', 'INTEGER',   mode='NULLABLE'),
        ]
        final_schema = [f for f in schema if f.name in new_df.columns]

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=final_schema
        )

        logging.info(f"Appending {len(new_df)} rows for {ticker}…")
        try:
            load_job = bq_client.load_table_from_dataframe(
                new_df, table_ref, job_config=job_config
            )
            load_job.result(timeout=300)  # allow up to 5 minutes
            logging.info(f"Appended {load_job.output_rows} rows for {ticker}.")
        except Exception as e:
            logging.error(f"BigQuery load_job.result() failed for {ticker}: {e}", exc_info=True)
            if hasattr(load_job, "errors") and load_job.errors:
                logging.error(f"Load job errors payload for {ticker}: {load_job.errors}")
            raise

    else:
        logging.info(f"No processed data for {ticker}, nothing to load.")

    logging.info(f"--- Finished Price Update for {ticker} ---")

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("--- Starting Daily Stock Price Update Job ---")
    tickers = get_tickers_from_metadata(bq_client, METADATA_TABLE_FULL_ID)

    if not tickers:
        logging.warning("No tickers retrieved; exiting.")
        exit(0)

    logging.info(f"Updating prices for {len(tickers)} tickers…")
    processed = 0
    total     = len(tickers)

    for i, ticker in enumerate(tickers, start=1):
        try:
            update_prices_for_ticker(ticker, PRICES_TABLE_FULL_ID)
            processed += 1
        except Exception:
            logging.error(f"Unhandled error for {ticker}, continuing.", exc_info=True)

        # simple rate‑limit
        time.sleep(0.75)

        if i % 50 == 0 or i == total:
            logging.info(f"Progress: {processed}/{total} tickers processed.")

    logging.info(f"--- Completed Update Job: {processed}/{total} tickers processed. ---")
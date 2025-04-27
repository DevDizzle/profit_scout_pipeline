# profit_scout_pipeline/services/price_loader/src/price_loader.py

import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import yfinance as yf
import time
import re
import logging
import datetime

# --- Configuration (Uppercase, SCREAMING_SNAKE_CASE) ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID', 'profit_scout')
BQ_PRICE_TABLE_ID = os.getenv('BQ_PRICES_TABLE_ID', 'price_data')
BQ_METADATA_TABLE_ID = os.getenv('BQ_METADATA_TABLE_ID', 'filing_metadata')

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
PRICES_TABLE_FULL_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_PRICE_TABLE_ID}"
METADATA_TABLE_FULL_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_METADATA_TABLE_ID}"

# --- Initialize BigQuery Client ---
bq_client = None
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
        results = query_job.result(timeout=180)
        tickers = [row.Ticker for row in results]
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
            logging.error(f"'Date' column not found in yfinance output for {ticker}. Columns: {prices_df.columns.tolist()}")
            return pd.DataFrame()

        if pd.api.types.is_datetime64_any_dtype(prices_df['Date']):
            if prices_df['Date'].dt.tz is not None:
                prices_df['Date'] = prices_df['Date'].dt.tz_convert(None)

        # Include only columns that match the table schema
        expected_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        present_cols = ['ticker'] + [col for col in expected_cols if col in prices_df.columns]
        prices_df = prices_df[present_cols].copy()

        rename_map = {
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'adj_close',  # Renamed to match table
            'Volume': 'volume'
        }
        prices_df.rename(columns={k: v for k, v in rename_map.items() if k in present_cols}, inplace=True)

        prices_df['date'] = pd.to_datetime(prices_df['date'])  # Keep as datetime for TIMESTAMP

        if 'volume' in prices_df.columns:
            prices_df['volume'] = pd.to_numeric(prices_df['volume'], errors='coerce')  # Keep as float to match INTEGER

        numeric_cols = prices_df.columns.difference(['ticker', 'date'])
        for col in numeric_cols:
            if col in prices_df:
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
    if not bq_client:
        logging.error(f"BigQuery client not initialized. Cannot update prices for {ticker}.")
        return

    logging.info(f"--- Starting Price Update for {ticker} ---")
    max_date = None

    try:
        query = f"""
            SELECT MAX(date) as max_date
            FROM `{prices_table_id}`
            WHERE ticker = @ticker
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result(timeout=60)
        for row in results:
            max_date = row.max_date

        if max_date:
            if isinstance(max_date, datetime.datetime):
                max_date = max_date.date()
            logging.info(f"Max date found for {ticker} in {prices_table_id}: {max_date}")
        else:
            logging.info(f"No existing price data found for {ticker} in {prices_table_id}. Will attempt initial load.")

    except NotFound:
        logging.warning(f"Table {prices_table_id} found, but no data for ticker {ticker}. Will attempt initial fetch.")
        max_date = None
    except Exception as e:
        logging.error(f"Error querying max date for {ticker} from {prices_table_id}: {e}. Aborting update for this ticker.")
        return

    if max_date:
        start_date = max_date + datetime.timedelta(days=1)
    else:
        start_date = datetime.date.today() - datetime.timedelta(days=10*365 + 2)

    end_date = datetime.date.today() + datetime.timedelta(days=1)

    if start_date >= end_date:
        logging.info(f"Data for {ticker} is already up to date (Max date: {max_date}).")
        return

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    try:
        logging.info(f"Fetching yfinance prices for {ticker} from {start_date_str} to {end_date_str}...")
        stock = yf.Ticker(ticker)
        prices = stock.history(start=start_date_str, end=end_date_str, auto_adjust=True)

        if prices.empty:
            logging.info(f"No new price data returned by yfinance for {ticker} in range {start_date_str} to {end_date_str}.")
            return

        logging.info(f"Fetched {len(prices)} new rows for {ticker}.")

        new_prices_df = process_price_data(prices, ticker)

        if not new_prices_df.empty:
            table_ref = bigquery.Table(prices_table_id)
            schema = [
                bigquery.SchemaField('ticker', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('date', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('open', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('high', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('low', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('adj_close', 'FLOAT', mode='NULLABLE'),
                bigquery.SchemaField('volume', 'INTEGER', mode='NULLABLE'),
            ]
            final_schema = [field for field in schema if field.name in new_prices_df.columns]

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=final_schema,
            )

            logging.info(f"Appending {len(new_prices_df)} new rows for {ticker} to {prices_table_id}...")
            try:
                load_job = bq_client.load_table_from_dataframe(new_prices_df, table_ref, job_config=job_config)
                load_job.result(timeout=180)
            except Exception as load_error:
                logging.error(f"BigQuery load failed for {ticker}: {load_error}", exc_info=True)
                raise

            if load_job.errors:
                logging.error(f"BigQuery append job failed for {ticker}: {load_job.errors}")
            else:
                logging.info(f"Successfully appended {load_job.output_rows} rows for {ticker}.")
        else:
            logging.info(f"DataFrame empty after processing for {ticker}, nothing to append.")

    except Exception as e:
        logging.error(f"Error during yfinance fetch or BQ load for {ticker}: {e}", exc_info=True)

    logging.info(f"--- Finished Price Update for {ticker} ---")

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("--- Starting Daily Stock Price Update Job ---")

    tickers = get_tickers_from_metadata(bq_client, METADATA_TABLE_FULL_ID)

    if not tickers:
        logging.warning("No tickers retrieved from metadata table. Price update job finishing.")
        exit(0)

    logging.info(f"Beginning price updates for {len(tickers)} tickers found in metadata...")
    processed_count = 0
    total_tickers = len(tickers)

    for i, ticker in enumerate(tickers):
        try:
            update_prices_for_ticker(ticker, PRICES_TABLE_FULL_ID)
            processed_count += 1
            time.sleep(0.75)
            if (i + 1) % 50 == 0 or (i + 1) == total_tickers:
                logging.info(f"Progress: Updated {processed_count}/{total_tickers} tickers...")
        except Exception as e:
            logging.error(f"Unexpected error processing ticker {ticker} in main loop: {e}", exc_info=True)

    logging.info(f"--- Completed Daily Stock Price Update Job. Processed {processed_count}/{total_tickers} tickers. ---")
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

# --- Configuration (using snake_case names) ---
gcp_project_id = os.getenv('gcp_project_id')
bq_dataset_id = os.getenv('bq_dataset_id', 'profit_scout') # Default dataset name
bq_price_table_id = os.getenv('bq_price_table_id', 'price_data') # Use TF name
bq_metadata_table_id = os.getenv('bq_metadata_table_id', 'filing_metadata') # Use TF name

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Reduce verbosity for cleaner logs
logging.getLogger("yfinance").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Validate Configuration (using snake_case names) ---
if not gcp_project_id:
    logging.critical("gcp_project_id environment variable is not set. Exiting.")
    exit(1)
if not bq_dataset_id:
    logging.critical("bq_dataset_id environment variable is not set. Exiting.")
    exit(1)
if not bq_price_table_id:
    logging.critical("bq_price_table_id environment variable is not set. Exiting.")
    exit(1)
if not bq_metadata_table_id:
    logging.critical("bq_metadata_table_id environment variable is not set. Exiting.")
    exit(1)

# --- Construct full table IDs (using snake_case variables) ---
prices_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_price_table_id}"
metadata_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_metadata_table_id}"


# --- Initialize BigQuery Client (using ADC) ---
bq_client = None
try:
    # Use snake_case variable
    bq_client = bigquery.Client(project=gcp_project_id)
    logging.info(f"BigQuery client initialized for project '{gcp_project_id}'.")
except Exception as e:
    logging.critical(f"Failed to initialize BigQuery client: {e}. Exiting.")
    exit(1) # Exit if BQ client fails, as it's essential

# --- Helper Functions ---

def get_tickers_from_metadata(client, metadata_table_id): # Pass full ID
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

        # Ensure Date is timezone naive before converting to date object
        if pd.api.types.is_datetime64_any_dtype(prices_df['Date']):
            if prices_df['Date'].dt.tz is not None:
                prices_df['Date'] = prices_df['Date'].dt.tz_convert(None)

        expected_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
        present_cols = ['ticker'] + [col for col in expected_cols if col in prices_df.columns]
        prices_df = prices_df[present_cols].copy()

        rename_map = {
            'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Volume': 'volume', 'Dividends': 'dividends',
            'Stock Splits': 'stock_splits'
        }
        prices_df.rename(columns={k: v for k, v in rename_map.items() if k in present_cols}, inplace=True)

        prices_df['date'] = pd.to_datetime(prices_df['date']).dt.date

        if 'volume' in prices_df.columns:
            prices_df['volume'] = pd.to_numeric(prices_df['volume'], errors='coerce').astype('Int64')

        # Convert numeric columns, coercing errors
        numeric_cols = prices_df.columns.difference(['ticker', 'date'])
        for col in numeric_cols:
             if col in prices_df: # Check if column exists after rename
                 prices_df[col] = pd.to_numeric(prices_df[col], errors='coerce')

        # Drop rows where essential numeric data might have become NaN if needed
        # prices_df.dropna(subset=['close'], inplace=True) # Example: remove rows without close price

        return prices_df
    except Exception as e:
        logging.error(f"Error processing yfinance data for {ticker}: {e}", exc_info=True)
        return pd.DataFrame()


# --- Incremental Update Function (Core Logic) ---
def update_prices_for_ticker(ticker: str, prices_table_id: str): # Pass full ID
    """
    Fetches stock prices for a single ticker from the last recorded date in BigQuery
    up to the current date and appends them.
    """
    if not bq_client:
        logging.error(f"BigQuery client not initialized. Cannot update prices for {ticker}.")
        return

    logging.info(f"--- Starting Price Update for {ticker} ---")
    max_date = None

    # 1. Query Max Date from BigQuery for this ticker
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
            max_date = row.max_date # datetime.date object or None

        if max_date:
            logging.info(f"Max date found for {ticker} in {prices_table_id}: {max_date}")
        else:
            logging.info(f"No existing price data found for {ticker} in {prices_table_id}. Will attempt initial load.")

    except NotFound:
        logging.warning(f"Table {prices_table_id} found, but no data for ticker {ticker}. Will attempt initial fetch.")
        max_date = None
    except Exception as e:
        logging.error(f"Error querying max date for {ticker} from {prices_table_id}: {e}. Aborting update for this ticker.")
        return

    # 2. Calculate Date Range for yfinance fetch
    if max_date:
        start_date = max_date + datetime.timedelta(days=1)
    else:
        # Initial load: ~10 years. Consider making this configurable.
        start_date = datetime.date.today() - datetime.timedelta(days=10*365 + 2)
        logging.info(f"Performing initial data fetch for {ticker} starting from {start_date}.")

    end_date = datetime.date.today() + datetime.timedelta(days=1)

    if start_date >= end_date:
        logging.info(f"Data for {ticker} is already up to date (Max date: {max_date}).")
        return

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # 3. Fetch Delta Data from yfinance
    try:
        logging.info(f"Fetching yfinance prices for {ticker} from {start_date_str} to {end_date_str}...")
        stock = yf.Ticker(ticker)
        prices = stock.history(start=start_date_str, end=end_date_str, auto_adjust=True) # Keep auto_adjust=True for simplicity

        if prices.empty:
            logging.info(f"No new price data returned by yfinance for {ticker} in range {start_date_str} to {end_date_str}.")
            return

        logging.info(f"Fetched {len(prices)} new rows for {ticker}.")

        # 4. Process Data
        new_prices_df = process_price_data(prices, ticker)

        # 5. Append to BigQuery
        if not new_prices_df.empty:
            table_ref = bq_client.table(prices_table_id) # Pass full ID

            # Define schema matching BQ table and processed DataFrame columns
            schema = [
                bigquery.SchemaField('ticker', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
                bigquery.SchemaField('open', 'FLOAT'),
                bigquery.SchemaField('high', 'FLOAT'),
                bigquery.SchemaField('low', 'FLOAT'),
                bigquery.SchemaField('close', 'FLOAT'),
                bigquery.SchemaField('volume', 'INTEGER'), # Match BQ type
                bigquery.SchemaField('dividends', 'FLOAT'),
                bigquery.SchemaField('stock_splits', 'FLOAT'),
            ]
            # Filter schema to only include columns actually present in the DataFrame
            final_schema = [field for field in schema if field.name in new_prices_df.columns]

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=final_schema, # Use filtered schema
            )

            logging.info(f"Appending {len(new_prices_df)} new rows for {ticker} to {prices_table_id}...")
            load_job = bq_client.load_table_from_dataframe(new_prices_df, table_ref, job_config=job_config)
            load_job.result(timeout=180)

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

    # Use snake_case variable for metadata table ID
    tickers = get_tickers_from_metadata(bq_client, metadata_table_full_id)

    if not tickers:
        logging.warning("No tickers retrieved from metadata table. Price update job finishing.")
        exit(0)

    logging.info(f"Beginning price updates for {len(tickers)} tickers found in metadata...")
    processed_count = 0
    total_tickers = len(tickers)

    # Loop through tickers and update prices
    for i, ticker in enumerate(tickers):
        try:
            # Pass the snake_case variable for prices table ID
            update_prices_for_ticker(ticker, prices_table_full_id)
            processed_count += 1
            # Add a small delay to be respectful to yfinance API
            time.sleep(0.75) # ~1.3 requests per second max
            # Log progress periodically
            if (i + 1) % 50 == 0 or (i + 1) == total_tickers:
                 logging.info(f"Progress: Updated {processed_count}/{total_tickers} tickers...")
        except Exception as e:
             # Catch potential errors in the main loop although inner func should handle most
             logging.error(f"Unexpected error processing ticker {ticker} in main loop: {e}", exc_info=True)

    logging.info(f"--- Completed Daily Stock Price Update Job. Processed {processed_count}/{total_tickers} tickers. ---")
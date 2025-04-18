import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import yfinance as yf
import time
import re
import logging
import datetime

# --- Configuration (Read from Environment Variables) ---
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BQ_DATASET_ID', 'profit_scout') # Default dataset name aligned with metadata table
PRICES_TABLE = os.getenv('BQ_PRICES_TABLE_ID', 'price_data') # Use specific env var, default to price_data
METADATA_TABLE = os.getenv('BQ_METADATA_TABLE_ID', 'filing_metadata') # Metadata table name

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Validate Configuration ---
if not PROJECT_ID:
    logging.critical("GCP_PROJECT_ID environment variable is not set. Exiting.")
    exit(1)
if not DATASET_ID:
    logging.critical("BQ_DATASET_ID environment variable is not set. Exiting.")
    exit(1)
if not PRICES_TABLE:
    logging.critical("BQ_PRICES_TABLE_ID environment variable is not set. Exiting.")
    exit(1)
if not METADATA_TABLE:
    logging.critical("BQ_METADATA_TABLE_ID environment variable is not set. Exiting.")
    exit(1)

# Construct full table IDs
prices_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{PRICES_TABLE}"
metadata_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE}"


# --- Initialize BigQuery Client (using ADC) ---
bq_client = None
try:
    bq_client = bigquery.Client(project=PROJECT_ID)
    logging.info(f"BigQuery client initialized for project '{PROJECT_ID}'.")
except Exception as e:
    logging.critical(f"Failed to initialize BigQuery client: {e}. Exiting.")
    exit(1) # Exit if BQ client fails, as it's essential

# --- Helper Functions ---

def get_tickers_from_metadata(bq_client, metadata_table_id_full):
    """
    Fetches the distinct list of tickers from the filing_metadata table.
    Returns a list of ticker symbols.
    """
    if not bq_client:
        logging.error("BigQuery client not available for get_tickers_from_metadata.")
        return []

    # Query to get distinct tickers from the specified metadata table
    query = f"""
        SELECT DISTINCT Ticker
        FROM `{metadata_table_id_full}`
        WHERE Ticker IS NOT NULL AND Ticker != ''
        ORDER BY Ticker
    """
    logging.info(f"Querying distinct tickers from {metadata_table_id_full}")

    try:
        query_job = bq_client.query(query)
        results = query_job.result(timeout=180) # Increased timeout for potentially large metadata tables
        tickers = [row.Ticker for row in results]
        logging.info(f"Found {len(tickers)} unique tickers in metadata table.")
        return tickers
    except NotFound:
         logging.error(f"Metadata table {metadata_table_id_full} not found.")
         return []
    except Exception as e:
        logging.error(f"Error querying tickers from metadata table {metadata_table_id_full}: {e}", exc_info=True)
        return []

def process_price_data(prices_df, ticker):
    """Cleans and standardizes the price DataFrame from yfinance."""
    if prices_df is None or prices_df.empty:
        return pd.DataFrame()

    prices_df = prices_df.reset_index()
    prices_df['ticker'] = ticker

    # Check for 'Date' column existence
    if 'Date' not in prices_df.columns:
        logging.error(f"'Date' column not found in yfinance output for {ticker}. Columns: {prices_df.columns.tolist()}")
        return pd.DataFrame()

    # Define expected columns from yfinance (check yfinance docs if names change)
    expected_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
    # Only select columns that are actually present in the dataframe
    present_cols = ['ticker'] + [col for col in expected_cols if col in prices_df.columns]
    prices_df = prices_df[present_cols].copy() # Use .copy() to avoid SettingWithCopyWarning

    # Rename columns to lowercase snake_case for BQ consistency
    rename_map = {
        'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low',
        'Close': 'close', 'Volume': 'volume', 'Dividends': 'dividends',
        'Stock Splits': 'stock_splits'
    }
    # Apply rename only for columns that were present
    prices_df.rename(columns={k: v for k, v in rename_map.items() if k in present_cols}, inplace=True)

    # Convert date column to date objects (yfinance might return datetime)
    prices_df['date'] = pd.to_datetime(prices_df['date']).dt.date

    # Ensure volume is integer (yfinance might return float)
    if 'volume' in prices_df.columns:
        prices_df['volume'] = prices_df['volume'].astype('Int64') # Use nullable Int

    return prices_df

# --- Incremental Update Function (Core Logic) ---
def update_prices_for_ticker(ticker: str, prices_bq_table_full_id: str):
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
        # Parameterized query for security and correctness
        query = f"""
            SELECT MAX(date) as max_date
            FROM `{prices_bq_table_full_id}`
            WHERE ticker = @ticker
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result(timeout=60)
        for row in results:
            max_date = row.max_date # Returns datetime.date object or None

        if max_date:
             logging.info(f"Max date found for {ticker} in {prices_bq_table_full_id}: {max_date}")
        else:
             logging.info(f"No existing price data found for {ticker} in {prices_bq_table_full_id}. Will attempt initial load.")

    except NotFound:
        # This is okay, means table exists but ticker has no data or table is empty
        logging.warning(f"Table {prices_bq_table_full_id} found, but no data for ticker {ticker}. Will attempt initial fetch.")
        max_date = None
    except Exception as e:
        # Log other BQ query errors but attempt to continue if possible? Or return?
        # Let's return for now, preventing fetch if BQ state is uncertain.
        logging.error(f"Error querying max date for {ticker} from {prices_bq_table_full_id}: {e}. Aborting update for this ticker.")
        return

    # 2. Calculate Date Range for yfinance fetch
    if max_date:
        start_date = max_date + datetime.timedelta(days=1)
    else:
        # Initial load: Fetch ~10 years of data. Adjust if needed.
        start_date = datetime.date.today() - datetime.timedelta(days=10*365 + 2) # Add buffer for weekends etc.
        logging.info(f"Performing initial data fetch for {ticker} starting from {start_date}.")

    # Fetch up to tomorrow (exclusive end date in yfinance) to include today's data if available
    end_date = datetime.date.today() + datetime.timedelta(days=1)

    # Check if data is already up-to-date
    if start_date >= end_date:
        logging.info(f"Data for {ticker} is already up to date (Max date: {max_date}).")
        return

    # Format dates for yfinance API (YYYY-MM-DD string)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # 3. Fetch Delta Data from yfinance
    try:
        logging.info(f"Fetching yfinance prices for {ticker} from {start_date_str} to {end_date_str}...")
        stock = yf.Ticker(ticker)
        # Use auto_adjust=False and fetch Adj Close if needed, or stick with True
        prices = stock.history(start=start_date_str, end=end_date_str, auto_adjust=True)

        if prices.empty:
            logging.info(f"No new price data returned by yfinance for {ticker} in range {start_date_str} to {end_date_str}.")
            return

        logging.info(f"Fetched {len(prices)} new rows for {ticker}.")

        # 4. Process Data
        new_prices_df = process_price_data(prices, ticker)

        # 5. Append to BigQuery
        if not new_prices_df.empty:
            table_ref = bq_client.table(prices_bq_table_full_id) # Get table reference

            # Define BQ schema explicitly for loading robustness
            # Ensure this matches your actual BQ table schema
            schema = [
                bigquery.SchemaField('ticker', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
                bigquery.SchemaField('open', 'FLOAT'),
                bigquery.SchemaField('high', 'FLOAT'),
                bigquery.SchemaField('low', 'FLOAT'),
                bigquery.SchemaField('close', 'FLOAT'),
                bigquery.SchemaField('volume', 'INTEGER'), # Or BIGNUMERIC if volume exceeds limits
                bigquery.SchemaField('dividends', 'FLOAT'),
                bigquery.SchemaField('stock_splits', 'FLOAT'),
            ]
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=schema,
                # Specify source format if loading from file, not needed for DataFrame
            )

            logging.info(f"Appending {len(new_prices_df)} new rows for {ticker} to {prices_bq_table_full_id}...")
            load_job = bq_client.load_table_from_dataframe(new_prices_df, table_ref, job_config=job_config)
            load_job.result(timeout=180) # Wait for job completion, increase timeout if needed

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

    # Fetch tickers to update from the metadata table
    tickers = get_tickers_from_metadata(bq_client, metadata_table_full_id)

    if not tickers:
        logging.warning("No tickers retrieved from metadata table. Price update job finishing.")
        exit(0) # Exit normally if no tickers found

    logging.info(f"Beginning price updates for {len(tickers)} tickers found in metadata...")
    processed_count = 0
    total_tickers = len(tickers)

    # Loop through tickers and update prices
    for i, ticker in enumerate(tickers):
        try:
            update_prices_for_ticker(ticker, prices_table_full_id) # Pass the full table ID
            processed_count += 1
            # Respectful delay to avoid hammering yfinance API
            time.sleep(0.75) # Adjust sleep time if needed
            # Log progress periodically
            if (i + 1) % 50 == 0:
                logging.info(f"Progress: Updated {processed_count}/{total_tickers} tickers...")
        except Exception as e:
             # Catch potential errors in the main loop although inner function should handle most
             logging.error(f"Unexpected error processing ticker {ticker} in main loop: {e}", exc_info=True)

    logging.info(f"--- Completed Daily Stock Price Update Job. Processed {processed_count}/{total_tickers} tickers. ---")
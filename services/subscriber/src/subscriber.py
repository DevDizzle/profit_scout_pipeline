# services/subscriber/src/subscriber.py

import asyncio
import base64
import json
import logging
import os
import re
import tempfile
from datetime import datetime, date, timezone # Added timezone

import pandas as pd
import requests
from google.cloud import bigquery, storage, secretmanager
from google.cloud.exceptions import NotFound
from sec_api import XbrlApi # Assuming this is the correct package/class
from tenacity import retry, wait_exponential, stop_after_attempt # Added tenacity imports

# --- Configuration (Read from Environment Variables) ---
# Environment Variable Names
ENV_PROJECT_ID = 'GCP_PROJECT_ID'
ENV_BQ_DATASET_ID = 'BQ_DATASET_ID'
ENV_METADATA_TABLE = 'BQ_METADATA_TABLE_ID' # e.g., filing_metadata
ENV_FINANCIALS_BS_TABLE = 'BQ_FINANCIALS_BS_TABLE_ID' # e.g., balance_sheet
ENV_FINANCIALS_IS_TABLE = 'BQ_FINANCIALS_IS_TABLE_ID' # e.g., income_statement
ENV_FINANCIALS_CF_TABLE = 'BQ_FINANCIALS_CF_TABLE_ID' # e.g., cash_flow
ENV_GCS_BUCKET_NAME = 'GCS_BUCKET_NAME'
ENV_GCS_PDF_FOLDER = 'GCS_PDF_FOLDER' # Specific folder for PDFs
ENV_SEC_API_SECRET_ID = 'SEC_API_SECRET_ID' # Secret name for sec-api.io key
ENV_SEC_API_SECRET_VERSION = 'SEC_API_SECRET_VERSION'

# Load Configuration
GCP_PROJECT_ID = os.getenv(ENV_PROJECT_ID)
BQ_DATASET_ID = os.getenv(ENV_BQ_DATASET_ID, 'profit_scout') # Default dataset
METADATA_TABLE = os.getenv(ENV_BQ_METADATA_TABLE)
FINANCIALS_BS_TABLE = os.getenv(ENV_FINANCIALS_BS_TABLE, 'balance_sheet')
FINANCIALS_IS_TABLE = os.getenv(ENV_FINANCIALS_IS_TABLE, 'income_statement')
FINANCIALS_CF_TABLE = os.getenv(ENV_FINANCIALS_CF_TABLE, 'cash_flow')
GCS_BUCKET_NAME = os.getenv(ENV_GCS_BUCKET_NAME)
GCS_PDF_FOLDER = os.getenv(ENV_GCS_PDF_FOLDER, 'SEC_Filings_Russell1000/') # Default PDF folder
SEC_API_SECRET_ID = os.getenv(ENV_SEC_API_SECRET_ID, 'sec-api-key')
SEC_API_SECRET_VERSION = os.getenv(ENV_SEC_API_SECRET_VERSION, 'latest')

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s [%(module)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Validate Essential Configuration ---
essential_vars = {
    ENV_PROJECT_ID: GCP_PROJECT_ID,
    ENV_BQ_DATASET_ID: BQ_DATASET_ID,
    ENV_METADATA_TABLE: METADATA_TABLE,
    ENV_GCS_BUCKET_NAME: GCS_BUCKET_NAME,
    ENV_SEC_API_SECRET_ID: SEC_API_SECRET_ID
}
missing_vars = [k for k, v in essential_vars.items() if not v]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}. Exiting.")
    exit(1)

# Construct full table IDs
metadata_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE}"
bs_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_BS_TABLE}"
is_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_IS_TABLE}"
cf_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_CF_TABLE}"

# Ensure GCS folder ends with /
GCS_PDF_FOLDER = GCS_PDF_FOLDER if GCS_PDF_FOLDER.endswith('/') else GCS_PDF_FOLDER + '/'

# --- Global Clients (Initialize in handler) ---
bq_client = None
gcs_client = None
xbrl_api_client = None
sec_api_key = None


# --- Helper Functions ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    # Consider making SecretManagerServiceClient global if reused frequently in high-throughput scenarios
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    logging.info(f"Successfully accessed secret: {secret_id}")
    return payload

def create_snake_case_name(raw_name):
    """Convert financial item names to snake_case."""
    if not raw_name: return None
    s = re.sub(r'\s*[:/\\(),%.]+\s*', '_', raw_name.strip())
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-zA-Z0-9_]', '', s)
    s = s.lower().strip('_')
    if not s: return None
    return 'col_' + s if s[0].isdigit() else s

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
async def fetch_financial_statements_by_url(xbrl_api_client, filing_url):
    """Fetch financial statements from a filing URL using sec-api.io."""
    if not xbrl_api_client: raise RuntimeError("XbrlApi client not initialized.")
    loop = asyncio.get_running_loop()
    try:
        # Assuming sec_api methods might be synchronous/blocking
        filing = await loop.run_in_executor(None, lambda: xbrl_api_client.from_url(filing_url))
        if not filing:
            logging.warning(f"Could not retrieve filing data from URL: {filing_url}")
            return None, None, None, None

        # Fetch statements - consider error handling for individual statement getters
        income_statement = await loop.run_in_executor(None, filing.get_income_statement)
        balance_sheet = await loop.run_in_executor(None, filing.get_balance_sheet)
        cash_flow_statement = await loop.run_in_executor(None, filing.get_cash_flow_statement)
        period_of_report = await loop.run_in_executor(None, getattr, filing, 'period_of_report', None)

        return income_statement, balance_sheet, cash_flow_statement, period_of_report
    except Exception as e:
        # Catch exceptions during API call or attribute access
        logging.error(f"Error fetching/parsing XBRL data for URL {filing_url}: {e}", exc_info=True)
        raise # Re-raise for retry decorator

def process_financial_df(df, ticker, statement_type):
    """Process raw financial statement data into a standardized DataFrame."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        logging.warning(f"No data to process for {ticker} - {statement_type}")
        return pd.DataFrame()
    try:
        df = df.T.copy() # Use .copy() to avoid modifying original if needed elsewhere
        date_col_name = df.index.name if df.index.name else 'period_end_date'
        df.index.name = date_col_name
        df.index = pd.to_datetime(df.index, errors='coerce', utc=True)
        df.dropna(subset=[date_col_name], inplace=True) # Drop rows where date conversion failed
        if df.empty: return pd.DataFrame()

        df.columns = [create_snake_case_name(col) for col in df.columns]
        df.dropna(axis=1, how='all', inplace=True)

        df['ticker'] = ticker
        df.reset_index(inplace=True) # Keep date_col_name

        numeric_cols = df.columns.difference(['ticker', date_col_name])
        for col in numeric_cols:
            # Coerce errors to NaN, which BQ handles as NULL for FLOAT/NUMERIC
            df[col] = pd.to_numeric(df[col], errors='coerce')

        logging.info(f"Processed {statement_type} for {ticker} - {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Error processing DataFrame for {ticker} - {statement_type}: {e}", exc_info=True)
        return pd.DataFrame()

# Modified: Accepts cleaned accession number for filename, sets NO GCS metadata
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
async def download_and_upload_pdf(
    ticker: str,
    accession_number_cleaned: str, # Expect hyphen-less AN for filename
    filing_url: str,
    sec_api_key: str,
    gcs_client,
    bucket_name: str,
    gcs_folder: str
):
    """
    Downloads PDF using sec-api.io helper URL, saves to GCS using
    ticker_accessionnumber(cleaned).pdf convention. Does NOT set GCS metadata.
    """
    if not gcs_client: raise RuntimeError("GCS client not initialized.")

    # Construct filename using the cleaned accession number
    base_filename = f"{ticker}_{accession_number_cleaned}"
    # Basic sanitize just in case ticker has odd chars, although less likely than AN
    sanitized_base = re.sub(r'[^\w\-]+', '_', base_filename)
    file_name = f"{sanitized_base}.pdf"
    blob_path = f"{gcs_folder}{file_name}" # Assumes gcs_folder ends with /

    loop = asyncio.get_running_loop()
    temp_file_path = None # Define outside try for finally block

    try:
        bucket = await loop.run_in_executor(None, gcs_client.get_bucket, bucket_name)
        blob = bucket.blob(blob_path)

        # Check if blob already exists
        blob_exists = await loop.run_in_executor(None, blob.exists)
        if blob_exists:
            logging.info(f"PDF already exists in GCS: gs://{bucket_name}/{blob_path}. Skipping download/upload.")
            return f"gs://{bucket_name}/{blob_path}" # Return existing path

        # Construct PDF download URL from sec-api.io helper
        pdf_api_url = f"https://api.sec-api.io/filing-reader?token={sec_api_key}&type=pdf&url={filing_url}"
        logging.info(f"Attempting PDF download for {ticker}_{accession_number_cleaned} from helper URL...")

        # Download PDF sync function
        def download_pdf_sync():
            local_path = None
            try:
                # Create a temporary file safely
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_f:
                    local_path = temp_f.name # Store path
                # Now download into the created temp file path
                with requests.Session() as session:
                    with session.get(pdf_api_url, stream=True, timeout=180) as r:
                        r.raise_for_status()
                        with open(local_path, 'wb') as f_out:
                            for chunk in r.iter_content(chunk_size=8192*4):
                                f_out.write(chunk)
                return local_path # Return path on success
            except requests.exceptions.RequestException as req_e:
                logging.error(f"HTTP error downloading PDF for {ticker} from {pdf_api_url}: {req_e}")
            except Exception as e:
                logging.error(f"Error during PDF download for {ticker}: {e}")
            # Cleanup temp file if download failed
            if local_path and os.path.exists(local_path):
                try: os.remove(local_path)
                except OSError: pass
            return None # Return None on failure

        temp_file_path = await loop.run_in_executor(None, download_pdf_sync)

        # Upload if download succeeded
        if temp_file_path:
            logging.info(f"Uploading {file_name} to GCS path: {blob_path}")
            # --- REMOVED GCS METADATA SETTING ---
            # blob.metadata = { ... } # Ensure this is removed/commented out
            await loop.run_in_executor(None, blob.upload_from_filename, temp_file_path, content_type='application/pdf')
            logging.info(f"Successfully uploaded PDF: gs://{bucket_name}/{blob_path}")
            return f"gs://{bucket_name}/{blob_path}" # Return GCS URI
        else:
            # Download failed, already logged
            raise IOError("PDF download failed.") # Raise error to signal failure

    except Exception as e:
        # Catch errors during GCS checks, upload, or re-raise download error
        logging.error(f"Error during PDF GCS operation for {ticker} / {accession_number_cleaned}: {e}", exc_info=True)
        raise # Re-raise for retry or main handler catch
    finally:
        # Ensure temporary file is always cleaned up
        if temp_file_path and os.path.exists(temp_file_path):
            try: await loop.run_in_executor(None, os.remove, temp_file_path)
            except Exception as e_clean: logging.warning(f"Error removing temp PDF {temp_file_path}: {e_clean}")

# Modified: Accepts cleaned accession number for insertion
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
async def insert_filing_metadata(
    bq_client,
    table_id_full: str,
    ticker: str,
    report_end_date: date | str | None, # Can accept date obj or string
    filed_date: str | None, # Expecting string from PubSub msg
    form_type: str,
    accession_number_cleaned: str, # Expect hyphen-less version
    link_to_filing_details: str
):
    """Inserts a single row into the filing_metadata BigQuery table."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    loop = asyncio.get_running_loop()

    def insert_meta_sync():
        try:
            # Prepare row data, converting types as needed for BQ schema
            row_to_insert = {
                "Ticker": ticker,
                "ReportEndDate": pd.to_datetime(report_end_date, errors='coerce').date() if report_end_date else None, # Convert to DATE
                "FiledDate": pd.to_datetime(filed_date, errors='coerce').date() if filed_date else None, # Convert to DATE
                "FormType": form_type,
                "AccessionNumber": accession_number_cleaned, # Insert CLEANED version
                "LinkToFilingDetails": link_to_filing_details,
                # Add timestamp if schema includes it
                # "created_at": datetime.now(timezone.utc)
            }
            # Ensure None for dates if conversion failed
            if isinstance(row_to_insert["ReportEndDate"], pd.Timestamp) and pd.isna(row_to_insert["ReportEndDate"]):
                row_to_insert["ReportEndDate"] = None
            if isinstance(row_to_insert["FiledDate"], pd.Timestamp) and pd.isna(row_to_insert["FiledDate"]):
                row_to_insert["FiledDate"] = None

            logging.info(f"Attempting to insert metadata row for {accession_number_cleaned} into {table_id_full}")
            errors = bq_client.insert_rows_json(table_id_full, [row_to_insert])
            if errors == []:
                logging.info(f"Successfully inserted metadata for {accession_number_cleaned}.")
            else:
                logging.error(f"Encountered errors inserting metadata for {accession_number_cleaned}: {errors}")
                # Raise exception to signal failure for retry
                raise bigquery.QueryJobError("Failed to insert metadata", errors=errors)
        except Exception as e:
            logging.error(f"Error during metadata BQ insert prep/call for {accession_number_cleaned}: {e}", exc_info=True)
            raise # Re-raise for retry

    await loop.run_in_executor(None, insert_meta_sync)

# Modified: Accepts cleaned accession number for appending
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
async def append_to_bigquery(
    df: pd.DataFrame,
    table_name: str, # Just table name, not full ID
    bq_client,
    project_id: str,
    dataset_id: str,
    accession_number_cleaned: str # Pass cleaned AN for logging/context
):
    """Appends financial DataFrame to BigQuery. Assumes df has cleaned AN."""
    if df is None or df.empty:
        logging.info(f"No data for {accession_number_cleaned} to append to {dataset_id}.{table_name}")
        return
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")

    table_ref_str = f"{project_id}.{dataset_id}.{table_name}"
    table_ref = bq_client.dataset(dataset_id).table(table_name)
    loop = asyncio.get_running_loop()
    # Use ticker from dataframe for context
    ticker = df['ticker'].iloc[0] if not df.empty else "UNKNOWN"

    def append_bq_sync():
        try:
            # Check table exists (optional, insert_rows/load_table will fail anyway)
            # bq_client.get_table(table_ref)

            # Convert date column to BQ DATE objects before loading
            date_col = 'period_end_date'
            if date_col in df.columns:
                 df[date_col] = pd.to_datetime(df[date_col]).dt.date
            if 'filing_date' in df.columns:
                 df['filing_date'] = pd.to_datetime(df['filing_date']).dt.date

            # Configure Load Job - schema usually inferred from DataFrame correctly
            # but can be specified if needed. BQ library generally handles NaN -> NULL.
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # schema=[...] # Optional: Define schema if auto-detect is unreliable
            )
            logging.info(f"Appending {len(df)} rows for {ticker}/{accession_number_cleaned} to {table_ref_str}...")
            load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            load_job.result(timeout=180) # Wait for completion

            if load_job.errors:
                logging.error(f"BQ append job failed for {ticker}/{accession_number_cleaned} to {table_name}: {load_job.errors}")
                raise bigquery.QueryJobError("Load job failed", errors=load_job.errors)
            else:
                logging.info(f"Successfully appended {load_job.output_rows} rows for {ticker}/{accession_number_cleaned} to {table_name}.")
        except NotFound:
             logging.error(f"BigQuery table {table_ref_str} not found. Cannot append data. Create table via Terraform.")
             raise # Fail if table doesn't exist
        except Exception as e:
            logging.error(f"Error appending data for {ticker}/{accession_number_cleaned} to {table_name}: {e}", exc_info=True)
            raise # Re-raise for retry

    await loop.run_in_executor(None, append_bq_sync)

# --- Main Handler ---
async def handle_filing_notification(event_data: dict):
    """
    Processes a single filing notification received from Pub/Sub.
    Orchestrates metadata insert, XBRL fetching, BQ loading, and PDF upload.
    Uses CLEANED (hyphen-less) accession number for filenames and BQ inserts.
    """
    global bq_client, gcs_client, xbrl_api_client, sec_api_key # Allow assignment

    # 1. Extract data & Clean Accession Number
    ticker = event_data.get('ticker')
    accession_number_original = event_data.get('accession_no') # Keep original if needed elsewhere
    form_type = event_data.get('form_type')
    filing_url = event_data.get('filing_url')
    filed_at_str = event_data.get('filed_at')

    if not all([ticker, accession_number_original, form_type, filing_url, filed_at_str]):
        logging.error(f"Incomplete Pub/Sub message received: {event_data}")
        raise ValueError("Incomplete Pub/Sub message.") # Fail processing

    # --- Clean Accession Number ---
    accession_number_cleaned = accession_number_original.replace('-', '')
    logging.info(f"--- Starting processing for {ticker} - {form_type} ({accession_number_original} / {accession_number_cleaned}) ---")

    try:
        # 2. Initialize Clients (Idempotent) & Fetch API Key
        if not sec_api_key: # Fetch only once per invocation potentially
             sec_api_key = access_secret_version(GCP_PROJECT_ID, SEC_API_SECRET_ID, SEC_API_SECRET_VERSION)
        if not bq_client: bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        if not gcs_client: gcs_client = storage.Client(project=GCP_PROJECT_ID)
        if not xbrl_api_client: xbrl_api_client = XbrlApi(api_key=sec_api_key)

        # 3. Fetch XBRL data
        income_data, balance_data, cashflow_data, period_of_report = await fetch_financial_statements_by_url(xbrl_api_client, filing_url)

        # 4. Insert Metadata (using CLEANED accession number)
        if period_of_report is not None:
            await insert_filing_metadata(
                bq_client=bq_client,
                table_id_full=metadata_table_full_id,
                ticker=ticker,
                report_end_date=period_of_report, # Pass date object or string
                filed_date=filed_at_str, # Pass date string
                form_type=form_type,
                accession_number_cleaned=accession_number_cleaned, # Use CLEANED
                link_to_filing_details=filing_url
            )
        else:
            logging.warning(f"Period of report not found for {accession_number_cleaned}. Skipping metadata insert.")

        # 5. Process Financial DataFrames
        income_df = process_financial_df(income_data, ticker, 'income_statement')
        balance_df = process_financial_df(balance_data, ticker, 'balance_sheet')
        cash_flow_df = process_financial_df(cashflow_data, ticker, 'cash_flow')

        # Add common identifiers (using CLEANED accession number for consistency)
        for df in [income_df, balance_df, cash_flow_df]:
            if not df.empty:
                df['accession_number'] = accession_number_cleaned # Use CLEANED
                df['filing_date'] = pd.to_datetime(filed_at_str).date() # Add as DATE object
                # Ensure date columns are suitable type (e.g., date objects)
                date_col = 'period_end_date'
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col]).dt.date

        # 6. Append Financial Data to BQ (using CLEANED accession number)
        # Table names mapping
        financial_tables = {
            FINANCIALS_IS_TABLE: income_df,
            FINANCIALS_BS_TABLE: balance_df,
            FINANCIALS_CF_TABLE: cash_flow_df
        }
        for table_name, df_to_append in financial_tables.items():
            await append_to_bigquery(
                df=df_to_append,
                table_name=table_name,
                bq_client=bq_client,
                project_id=GCP_PROJECT_ID,
                dataset_id=BQ_DATASET_ID,
                accession_number_cleaned=accession_number_cleaned # Pass cleaned for context
            )

        # 7. Download/Upload PDF (using CLEANED accession number for filename)
        pdf_gcs_path = await download_and_upload_pdf(
            ticker=ticker,
            accession_number_cleaned=accession_number_cleaned, # Use CLEANED
            filing_url=filing_url,
            sec_api_key=sec_api_key,
            gcs_client=gcs_client,
            bucket_name=GCS_BUCKET_NAME,
            gcs_folder=GCS_PDF_FOLDER # Use specific PDF folder
        )
        if pdf_gcs_path:
            logging.info(f"PDF processed for {ticker} ({accession_number_cleaned}). Path: {pdf_gcs_path}")
            # NOTE: No metadata is set on the uploaded PDF

        logging.info(f"--- Successfully finished processing for {ticker} - {form_type} ({accession_number_cleaned}) ---")

    except Exception as e:
        logging.error(f"--- Failed processing for {ticker} - {form_type} ({accession_number_original}) ---", exc_info=True)
        # Re-raise exception to signal failure to Pub/Sub/trigger for potential retry
        raise
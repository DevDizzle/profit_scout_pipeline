# services/subscriber/src/subscriber.py

import asyncio
import base64
import json
import logging
import os
import re
import tempfile
from datetime import date, datetime, timezone  # Added timezone

import pandas as pd
import requests
from google.cloud import bigquery, secretmanager, storage
from google.cloud.exceptions import NotFound
from sec_api import XbrlApi  # Assuming this is the correct package/class
from tenacity import retry, stop_after_attempt, wait_exponential  # Added tenacity imports

# --- Configuration (Read directly using snake_case names) ---
gcp_project_id = os.getenv('gcp_project_id')
bq_dataset_id = os.getenv('bq_dataset_id', 'profit_scout')  # Default dataset
bq_metadata_table_id = os.getenv('bq_metadata_table_id')
bq_bs_table_id = os.getenv('bq_bs_table_id', 'balance_sheet')
bq_is_table_id = os.getenv('bq_is_table_id', 'income_statement')
bq_cf_table_id = os.getenv('bq_cf_table_id', 'cash_flow')
gcs_bucket_name = os.getenv('gcs_bucket_name')
# Align default with .env.example and variables.tf
gcs_pdf_folder = os.getenv('gcs_pdf_folder', 'sec-pdf/')
sec_api_secret_name = os.getenv('sec_api_secret_name', 'sec-api-key') # Use TF name
sec_api_secret_version = os.getenv('sec_api_secret_version', 'latest') # Keep consistent name

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s', # Use filename
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Validate Essential Configuration (using snake_case names) ---
essential_vars = {
    'gcp_project_id': gcp_project_id,
    'bq_dataset_id': bq_dataset_id,
    'bq_metadata_table_id': bq_metadata_table_id,
    'gcs_bucket_name': gcs_bucket_name,
    'sec_api_secret_name': sec_api_secret_name
}
missing_vars = [k for k, v in essential_vars.items() if not v]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}. Exiting.")
    exit(1) # Use exit(1) for error exit

# --- Construct full table IDs (using snake_case variables) ---
# Check if required components exist before formatting
if not all([gcp_project_id, bq_dataset_id, bq_metadata_table_id]):
     logging.critical("Cannot construct metadata_table_full_id due to missing components.")
     exit(1)
metadata_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_metadata_table_id}"

if not all([gcp_project_id, bq_dataset_id, bq_bs_table_id]):
     logging.warning(f"Cannot construct bs_table_full_id due to missing components (using default: {bq_bs_table_id}).")
     # Decide if this is critical - perhaps allow defaults?
     # exit(1) # Uncomment if BS table is mandatory
bs_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_bs_table_id}" if all([gcp_project_id, bq_dataset_id, bq_bs_table_id]) else None

if not all([gcp_project_id, bq_dataset_id, bq_is_table_id]):
     logging.warning(f"Cannot construct is_table_full_id due to missing components (using default: {bq_is_table_id}).")
is_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_is_table_id}" if all([gcp_project_id, bq_dataset_id, bq_is_table_id]) else None

if not all([gcp_project_id, bq_dataset_id, bq_cf_table_id]):
     logging.warning(f"Cannot construct cf_table_full_id due to missing components (using default: {bq_cf_table_id}).")
cf_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_cf_table_id}" if all([gcp_project_id, bq_dataset_id, bq_cf_table_id]) else None

# Ensure GCS folder ends with /
gcs_pdf_folder = gcs_pdf_folder if gcs_pdf_folder.endswith('/') else gcs_pdf_folder + '/'

# --- Global Clients (Initialize in handler) ---
bq_client = None
gcs_client = None
xbrl_api_client = None
sec_api_key = None

# --- Helper Functions ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        logging.info(f"Successfully accessed secret: {secret_id}")
        return payload
    except Exception as e:
        logging.error(f"Failed to access secret '{secret_id}/{version_id}' in project '{project_id}': {e}", exc_info=True)
        raise # Reraise after logging

def create_snake_case_name(raw_name):
    """Convert financial item names to snake_case."""
    if not raw_name or not isinstance(raw_name, str): return None
    s = re.sub(r'\s*[:/\\(),%.]+\s*', '_', raw_name.strip())
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-zA-Z0-9_]', '', s)
    s = s.lower().strip('_')
    if not s: return None
    # Prepend col_ if starts with digit or is a reserved word (basic check)
    if s[0].isdigit() or s in ['select', 'from', 'where']: # Add more BQ keywords if needed
        return 'col_' + s
    return s

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
async def fetch_financial_statements_by_url(xbrl_api, filing_url):
    """Fetch financial statements from a filing URL using sec-api.io."""
    if not xbrl_api: raise RuntimeError("XbrlApi client not initialized.")
    loop = asyncio.get_running_loop()
    try:
        # Assuming sec_api methods might be synchronous/blocking
        logging.info(f"Fetching XBRL data from URL: {filing_url}")
        filing = await loop.run_in_executor(None, lambda: xbrl_api.from_url(filing_url))
        if not filing:
            logging.warning(f"Could not retrieve filing data from URL: {filing_url}")
            return None, None, None, None

        # Fetch statements - consider error handling for individual statement getters
        logging.info(f"Parsing financial statements for: {filing_url}")
        income_statement = await loop.run_in_executor(None, filing.get_income_statement)
        balance_sheet = await loop.run_in_executor(None, filing.get_balance_sheet)
        cash_flow_statement = await loop.run_in_executor(None, filing.get_cash_flow_statement)
        period_of_report = await loop.run_in_executor(None, getattr, filing, 'period_of_report', None)
        logging.info(f"Successfully parsed statements. Period of report: {period_of_report}")

        return income_statement, balance_sheet, cash_flow_statement, period_of_report
    except Exception as e:
        logging.error(f"Error fetching/parsing XBRL data for URL {filing_url}: {e}", exc_info=True)
        raise

def process_financial_df(df, ticker, statement_type):
    """Process raw financial statement data into a standardized DataFrame."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        logging.warning(f"No data to process for {ticker} - {statement_type}")
        return pd.DataFrame()
    try:
        df = df.T.copy()
        date_col_name = 'period_end_date' # Standardize name
        df.index.name = date_col_name
        df.index = pd.to_datetime(df.index, errors='coerce', utc=True) # Ensure timezone aware
        df.dropna(subset=[date_col_name], inplace=True)
        if df.empty: return pd.DataFrame()

        df.columns = [create_snake_case_name(col) for col in df.columns]
        df.dropna(axis=1, how='all', inplace=True) # Drop cols that became None

        df['ticker'] = ticker
        df.reset_index(inplace=True)

        numeric_cols = df.columns.difference(['ticker', date_col_name])
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Ensure date col is just date, not datetime
        df[date_col_name] = df[date_col_name].dt.date

        logging.info(f"Processed {statement_type} for {ticker} - {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Error processing DataFrame for {ticker} - {statement_type}: {e}", exc_info=True)
        return pd.DataFrame()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
async def download_and_upload_pdf(
    ticker: str,
    accession_number_cleaned: str,
    filing_url: str,
    api_key: str, # Pass key explicitly
    gcs_client_instance, # Pass client explicitly
    bucket_name: str,
    gcs_folder: str
):
    """Downloads PDF using sec-api.io helper URL, saves to GCS."""
    if not gcs_client_instance: raise RuntimeError("GCS client not initialized.")
    if not api_key: raise RuntimeError("SEC API Key not provided for PDF download.")

    base_filename = f"{ticker}_{accession_number_cleaned}"
    sanitized_base = re.sub(r'[^\w\-]+', '_', base_filename)
    file_name = f"{sanitized_base}.pdf"
    blob_path = f"{gcs_folder}{file_name}" # Assumes gcs_folder ends with /

    loop = asyncio.get_running_loop()
    temp_file_path = None

    try:
        bucket = await loop.run_in_executor(None, gcs_client_instance.get_bucket, bucket_name)
        blob = bucket.blob(blob_path)

        blob_exists = await loop.run_in_executor(None, blob.exists)
        if blob_exists:
            logging.info(f"PDF already exists in GCS: gs://{bucket_name}/{blob_path}. Skipping download/upload.")
            return f"gs://{bucket_name}/{blob_path}"

        pdf_api_url = f"https://api.sec-api.io/filing-reader?token={api_key}&type=pdf&url={filing_url}"
        logging.info(f"Attempting PDF download for {ticker}_{accession_number_cleaned}...")

        def download_pdf_sync():
            local_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_f:
                    local_path = temp_f.name
                with requests.Session() as session:
                    with session.get(pdf_api_url, stream=True, timeout=180) as r:
                        r.raise_for_status()
                        with open(local_path, 'wb') as f_out:
                            for chunk in r.iter_content(chunk_size=8192*4):
                                f_out.write(chunk)
                return local_path
            except requests.exceptions.RequestException as req_e:
                logging.error(f"HTTP error downloading PDF for {ticker}: {req_e}")
            except Exception as e:
                logging.error(f"Error during PDF download for {ticker}: {e}")
            if local_path and os.path.exists(local_path):
                try: os.remove(local_path)
                except OSError: pass
            return None

        temp_file_path = await loop.run_in_executor(None, download_pdf_sync)

        if temp_file_path:
            logging.info(f"Uploading {file_name} to GCS path: {blob_path}")
            await loop.run_in_executor(None, blob.upload_from_filename, temp_file_path, content_type='application/pdf')
            logging.info(f"Successfully uploaded PDF: gs://{bucket_name}/{blob_path}")
            return f"gs://{bucket_name}/{blob_path}"
        else:
            raise IOError("PDF download failed.")

    except Exception as e:
        logging.error(f"Error during PDF GCS operation for {ticker} / {accession_number_cleaned}: {e}", exc_info=True)
        raise
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try: await loop.run_in_executor(None, os.remove, temp_file_path)
            except Exception as e_clean: logging.warning(f"Error removing temp PDF {temp_file_path}: {e_clean}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
async def insert_filing_metadata(
    bq_client_instance, # Pass client explicitly
    table_id_full: str,
    ticker: str,
    report_end_date: date | str | None,
    filed_date: str | None,
    form_type: str,
    accession_number_cleaned: str,
    link_to_filing_details: str
):
    """Inserts a single row into the filing_metadata BigQuery table."""
    if not bq_client_instance: raise RuntimeError("BigQuery client not initialized.")
    if not table_id_full: raise ValueError("Metadata table ID is not configured.")

    loop = asyncio.get_running_loop()

    def insert_meta_sync():
        try:
            # Convert dates safely
            report_dt = pd.to_datetime(report_end_date, errors='coerce', utc=True).date() if report_end_date else None
            filed_dt = pd.to_datetime(filed_date, errors='coerce', utc=True).date() if filed_date else None

            row_to_insert = {
                "Ticker": ticker,
                "ReportEndDate": report_dt,
                "FiledDate": filed_dt,
                "FormType": form_type,
                "AccessionNumber": accession_number_cleaned,
                "LinkToFilingDetails": link_to_filing_details,
                "created_at": datetime.now(timezone.utc) # Add processing timestamp
            }

            logging.info(f"Attempting to insert metadata row for {accession_number_cleaned} into {table_id_full}")
            errors = bq_client_instance.insert_rows_json(table_id_full, [row_to_insert])
            if not errors:
                logging.info(f"Successfully inserted metadata for {accession_number_cleaned}.")
            else:
                logging.error(f"Encountered errors inserting metadata for {accession_number_cleaned}: {errors}")
                raise bigquery.QueryJobError("Failed to insert metadata", errors=errors)
        except Exception as e:
            logging.error(f"Error during metadata BQ insert prep/call for {accession_number_cleaned}: {e}", exc_info=True)
            raise

    await loop.run_in_executor(None, insert_meta_sync)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
async def append_to_bigquery(
    df: pd.DataFrame,
    table_name: str, # Just table name
    bq_client_instance, # Pass client explicitly
    project_id: str,
    dataset_id: str,
    accession_number_cleaned: str
):
    """Appends financial DataFrame to BigQuery."""
    if df is None or df.empty:
        logging.info(f"No data for {accession_number_cleaned} to append to {dataset_id}.{table_name}")
        return
    if not bq_client_instance: raise RuntimeError("BigQuery client not initialized.")
    if not all([project_id, dataset_id, table_name]):
        logging.error(f"Cannot append to BQ due to missing config: P:{project_id} D:{dataset_id} T:{table_name}")
        return # Or raise?

    table_ref_str = f"{project_id}.{dataset_id}.{table_name}"
    table_ref = bq_client_instance.dataset(dataset_id).table(table_name)
    loop = asyncio.get_running_loop()
    ticker = df['ticker'].iloc[0] if 'ticker' in df.columns and not df.empty else "UNKNOWN"

    def append_bq_sync():
        try:
            # Prepare dataframe (ensure date columns are correct type if not already)
            for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
                 df[col] = df[col].dt.date # Convert datetime to date objects for BQ DATE type

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # Autodetect should work if DFs are clean, otherwise specify schema
                # schema=[bigquery.SchemaField(...), ...]
            )
            logging.info(f"Appending {len(df)} rows for {ticker}/{accession_number_cleaned} to {table_ref_str}...")
            load_job = bq_client_instance.load_table_from_dataframe(df, table_ref, job_config=job_config)
            load_job.result(timeout=180)

            if load_job.errors:
                logging.error(f"BQ append job failed for {ticker}/{accession_number_cleaned} to {table_name}: {load_job.errors}")
                raise bigquery.QueryJobError("Load job failed", errors=load_job.errors)
            else:
                logging.info(f"Successfully appended {load_job.output_rows} rows for {ticker}/{accession_number_cleaned} to {table_name}.")
        except NotFound:
            logging.error(f"BigQuery table {table_ref_str} not found. Create table via Terraform.")
            raise
        except Exception as e:
            logging.error(f"Error appending data for {ticker}/{accession_number_cleaned} to {table_name}: {e}", exc_info=True)
            raise

    await loop.run_in_executor(None, append_bq_sync)

# --- Main Handler ---
async def handle_filing_notification(event_data: dict):
    """
    Processes a single filing notification received from Pub/Sub.
    Orchestrates metadata insert, XBRL fetching, BQ loading, and PDF upload.
    """
    global bq_client, gcs_client, xbrl_api_client, sec_api_key

    # 1. Extract data & Clean Accession Number
    ticker = event_data.get('ticker')
    accession_number_original = event_data.get('accession_no')
    form_type = event_data.get('form_type')
    filing_url = event_data.get('filing_url')
    filed_at_str = event_data.get('filed_at')

    if not all([ticker, accession_number_original, form_type, filing_url, filed_at_str]):
        logging.error(f"Incomplete Pub/Sub message received: {event_data}")
        raise ValueError("Incomplete Pub/Sub message.")

    accession_number_cleaned = accession_number_original.replace('-', '')
    log_prefix = f"{ticker} - {form_type} ({accession_number_cleaned})"
    logging.info(f"--- Starting processing for {log_prefix} ---")

    try:
        # 2. Initialize Clients & Fetch API Key (using snake_case vars)
        if not sec_api_key:
            sec_api_key = access_secret_version(
                project_id=gcp_project_id,
                secret_id=sec_api_secret_name, # Use TF consistent name
                version_id=sec_api_secret_version
            )
        if not bq_client: bq_client = bigquery.Client(project=gcp_project_id)
        if not gcs_client: gcs_client = storage.Client(project=gcp_project_id)
        if not xbrl_api_client: xbrl_api_client = XbrlApi(api_key=sec_api_key)

        # 3. Fetch XBRL data
        income_data, balance_data, cashflow_data, period_of_report = await fetch_financial_statements_by_url(xbrl_api_client, filing_url)

        # 4. Insert Metadata if data is available
        if period_of_report is not None and metadata_table_full_id:
            await insert_filing_metadata(
                bq_client_instance=bq_client,
                table_id_full=metadata_table_full_id,
                ticker=ticker,
                report_end_date=period_of_report,
                filed_date=filed_at_str,
                form_type=form_type,
                accession_number_cleaned=accession_number_cleaned,
                link_to_filing_details=filing_url
            )
        else:
            logging.warning(f"Period of report not found or metadata table not configured for {log_prefix}. Skipping metadata insert.")

        # 5. Process Financial DataFrames
        income_df = process_financial_df(income_data, ticker, 'income_statement')
        balance_df = process_financial_df(balance_data, ticker, 'balance_sheet')
        cash_flow_df = process_financial_df(cashflow_data, ticker, 'cash_flow')

        # Add common identifiers (use cleaned AN)
        common_data = {
            'accession_number': accession_number_cleaned,
            'filing_date': pd.to_datetime(filed_at_str, errors='coerce').date()
        }
        for df in [income_df, balance_df, cash_flow_df]:
            if not df.empty:
                for col, val in common_data.items():
                    df[col] = val
                 # Ensure date col is just date
                date_col = 'period_end_date'
                if date_col in df.columns:
                     df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date

        # 6. Append Financial Data to BQ if tables are configured
        financial_tables = {}
        if is_table_full_id: financial_tables[bq_is_table_id] = income_df
        if bs_table_full_id: financial_tables[bq_bs_table_id] = balance_df
        if cf_table_full_id: financial_tables[bq_cf_table_id] = cash_flow_df

        for table_name, df_to_append in financial_tables.items():
            await append_to_bigquery(
                df=df_to_append,
                table_name=table_name, # Pass table name only
                bq_client_instance=bq_client,
                project_id=gcp_project_id,
                dataset_id=bq_dataset_id,
                accession_number_cleaned=accession_number_cleaned
            )

        # 7. Download/Upload PDF if bucket/folder configured
        if gcs_bucket_name and gcs_pdf_folder:
            pdf_gcs_path = await download_and_upload_pdf(
                ticker=ticker,
                accession_number_cleaned=accession_number_cleaned,
                filing_url=filing_url,
                api_key=sec_api_key,
                gcs_client_instance=gcs_client,
                bucket_name=gcs_bucket_name,
                gcs_folder=gcs_pdf_folder
            )
            if pdf_gcs_path:
                logging.info(f"PDF processed for {log_prefix}. Path: {pdf_gcs_path}")
        else:
             logging.warning(f"GCS bucket/folder not configured. Skipping PDF processing for {log_prefix}.")

        logging.info(f"--- Successfully finished processing for {log_prefix} ---")

    except Exception as e:
        logging.error(f"--- Failed processing for {log_prefix} ({accession_number_original}) ---", exc_info=True)
        raise # Re-raise for Pub/Sub ACK failure / main handler
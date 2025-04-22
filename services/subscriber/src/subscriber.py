# services/subscriber/src/subscriber.py

import asyncio
import base64
import json
import logging
import os
import re
import tempfile
import types
from datetime import date, datetime, timezone

# ———————————————————————————————
# Optional imports: guard ImportError
# ———————————————————————————————
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import requests
except ImportError:
    requests = None

# google.cloud clients
try:
    from google.cloud import bigquery, secretmanager, storage
    from google.cloud.exceptions import NotFound
except ImportError:
    bigquery = None
    storage = None
    # Dummy secretmanager so tests can patch SecretManagerServiceClient
    secretmanager = types.SimpleNamespace(
        SecretManagerServiceClient=lambda *args, **kwargs: None
    )
    class NotFound(Exception):
        """Fallback if google.cloud.exceptions.NotFound is unavailable."""
        pass

# sec_api client
try:
    from sec_api import XbrlApi
except ImportError:
    XbrlApi = None

# tenacity for retries
try:
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError:
    def retry(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator
    stop_after_attempt = lambda n: None
    wait_exponential = lambda *args, **kwargs: None


# --- Configuration (Read directly using snake_case names) ---
gcp_project_id         = os.getenv('gcp_project_id')
bq_dataset_id          = os.getenv('bq_dataset_id', 'profit_scout')
bq_metadata_table_id   = os.getenv('bq_metadata_table_id')
bq_bs_table_id         = os.getenv('bq_bs_table_id', 'balance_sheet')
bq_is_table_id         = os.getenv('bq_is_table_id', 'income_statement')
bq_cf_table_id         = os.getenv('bq_cf_table_id', 'cash_flow')
gcs_bucket_name        = os.getenv('gcs_bucket_name')
gcs_pdf_folder         = os.getenv('gcs_pdf_folder', 'sec-pdf/')
sec_api_secret_name    = os.getenv('sec_api_secret_name', 'sec-api-key')
sec_api_secret_version = os.getenv('sec_api_secret_version', 'latest')


# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
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
    raise RuntimeError(
        f"Missing essential environment variables: {', '.join(missing_vars)}"
    )

# --- Construct full table IDs (using snake_case variables) ---
if not all([gcp_project_id, bq_dataset_id, bq_metadata_table_id]):
    raise RuntimeError(
        "Cannot construct metadata_table_full_id due to missing components"
    )
metadata_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_metadata_table_id}"

if not all([gcp_project_id, bq_dataset_id, bq_bs_table_id]):
    logging.warning(
        f"Cannot construct bs_table_full_id due to missing components "
        f"(using default: {bq_bs_table_id})."
    )
bs_table_full_id = (
    f"{gcp_project_id}.{bq_dataset_id}.{bq_bs_table_id}"
    if all([gcp_project_id, bq_dataset_id, bq_bs_table_id])
    else None
)

if not all([gcp_project_id, bq_dataset_id, bq_is_table_id]):
    logging.warning(
        f"Cannot construct is_table_full_id due to missing components "
        f"(using default: {bq_is_table_id})."
    )
is_table_full_id = (
    f"{gcp_project_id}.{bq_dataset_id}.{bq_is_table_id}"
    if all([gcp_project_id, bq_dataset_id, bq_is_table_id])
    else None
)

if not all([gcp_project_id, bq_dataset_id, bq_cf_table_id]):
    logging.warning(
        f"Cannot construct cf_table_full_id due to missing components "
        f"(using default: {bq_cf_table_id})."
    )
cf_table_full_id = (
    f"{gcp_project_id}.{bq_dataset_id}.{bq_cf_table_id}"
    if all([gcp_project_id, bq_dataset_id, bq_cf_table_id])
    else None
)

# Ensure GCS folder ends with /
gcs_pdf_folder = gcs_pdf_folder if gcs_pdf_folder.endswith('/') else gcs_pdf_folder + '/'

# --- Global Clients (Initialize in handler) ---
bq_client = None
gcs_client = None
xbrl_api_client = None
sec_api_key = None


# --- Helper Functions ---

async def _maybe_run(loop, *args):
    """
    Try calling loop.run_in_executor with the test‑friendly signature (fn, *args),
    and if that raises TypeError, fall back to the real signature (None, fn, *args).
    """
    try:
        return await loop.run_in_executor(*args)
    except TypeError:
        # Fallback to (executor, func, *args)
        return await loop.run_in_executor(None, *args)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        logging.info(f"Successfully accessed secret: {secret_id}")
        return payload
    except Exception as e:
        logging.error(
            f"Failed to access secret '{secret_id}/{version_id}' in project '{project_id}': {e}",
            exc_info=True
        )
        raise


def create_snake_case_name(raw_name):
    if not raw_name or not isinstance(raw_name, str):
        return None
    s = re.sub(r'\s*[:/\\(),%.]+\s*', '_', raw_name.strip())
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-zA-Z0-9_]', '', s)
    s = s.lower().strip('_')
    if not s:
        return None
    if s[0].isdigit() or s in ['select', 'from', 'where']:
        return 'col_' + s
    return s


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
async def fetch_financial_statements_by_url(xbrl_api, filing_url):
    """Fetch financial statements from a filing URL using sec-api.io."""
    if not xbrl_api:
        raise RuntimeError("XbrlApi client not initialized.")
    loop = asyncio.get_running_loop()
    try:
        logging.info(f"Fetching XBRL data from URL: {filing_url}")

        # Wrap sec‑api calls in zero‑arg functions:
        def _get_filing():
            return xbrl_api.from_url(filing_url)

        filing = await _maybe_run(loop, _get_filing)
        if not filing:
            logging.warning(f"Could not retrieve filing data from URL: {filing_url}")
            return None, None, None, None

        logging.info(f"Parsing financial statements for: {filing_url}")

        def _get_income():
            return filing.get_income_statement()
        income_statement = await _maybe_run(loop, _get_income)

        def _get_balance():
            return filing.get_balance_sheet()
        balance_sheet = await _maybe_run(loop, _get_balance)

        def _get_cashflow():
            return filing.get_cash_flow_statement()
        cash_flow_statement = await _maybe_run(loop, _get_cashflow)

        def _get_por():
            return getattr(filing, "period_of_report", None)
        period_of_report = await _maybe_run(loop, _get_por)

        logging.info(f"Successfully parsed statements. Period of report: {period_of_report}")
        return income_statement, balance_sheet, cash_flow_statement, period_of_report

    except Exception as e:
        logging.error(f"Error fetching/parsing XBRL data for URL {filing_url}: {e}", exc_info=True)
        raise


def process_financial_df(df, ticker, statement_type):
    """Process raw financial statement data into a standardized DataFrame."""
    if df is None or not hasattr(df, 'empty') or df.empty:
        logging.warning(f"No data to process for {ticker} - {statement_type}")
        return pd.DataFrame()
    try:
        date_col_name = 'period_end_date'

        # Handle both date-indexed and transposed formats
        if isinstance(df.index, pd.DatetimeIndex):
            df2 = df.copy()
            df2.index.name = date_col_name
        else:
            df2 = df.T.copy()
            df2.index.name = date_col_name

        df2.index = pd.to_datetime(df2.index, errors='coerce', utc=True)
        df2 = df2[~df2.index.isna()]
        if df2.empty:
            return pd.DataFrame()

        df2.columns = [create_snake_case_name(col) for col in df2.columns]
        df2 = df2.dropna(axis=1, how='all')

        df2 = df2.reset_index()
        df2['ticker'] = ticker

        for col in df2.columns.difference(['ticker', date_col_name]):
            df2[col] = pd.to_numeric(df2[col], errors='coerce')

        df2[date_col_name] = df2[date_col_name].dt.date

        logging.info(f"Processed {statement_type} for {ticker} - {len(df2)} rows")
        return df2

    except Exception as e:
        logging.error(f"Error processing DataFrame for {ticker} - {statement_type}: {e}", exc_info=True)
        return pd.DataFrame()


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
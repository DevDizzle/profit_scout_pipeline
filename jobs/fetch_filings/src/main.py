import os
import datetime
import time
import logging
import json
from dateutil.relativedelta import relativedelta

# --- GCP & Lib Imports (Guard against missing optional libs) ---
try:
    from google.cloud import bigquery, secretmanager
    from google.cloud.exceptions import NotFound
    import google.auth # Added for ADC credential errors
    GCP_LIBS_AVAILABLE = True
except ImportError:
    logging.error("Failed to import Google Cloud libraries. Ensure google-cloud-bigquery and google-cloud-secret-manager are installed.")
    GCP_LIBS_AVAILABLE = False

try:
    from sec_api import QueryApi
    SEC_API_LIB_AVAILABLE = True
except ImportError:
    logging.error("Failed to import sec_api. Ensure sec-api is installed.")
    SEC_API_LIB_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError:
    logging.warning("Tenacity library not found. Retries will not be available.")
    # Define dummy decorator if tenacity is missing
    def retry(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator
    stop_after_attempt = lambda n: None
    wait_exponential = lambda *args, **kwargs: None

# --- Configuration (from Environment Variables) ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID', 'profit_scout')
BQ_METADATA_TABLE_ID = os.getenv('BQ_METADATA_TABLE_ID', 'filing_metadata')
SEC_API_SECRET_ID = os.getenv('SEC_API_SECRET_ID', 'sec-api-key')
SEC_API_SECRET_VERSION = os.getenv('SEC_API_SECRET_VERSION', 'latest')
LOOKBACK_HOURS = int(os.getenv('LOOKBACK_HOURS', '24')) # How far back to query SEC API
FILING_TYPES = os.getenv('FILING_TYPES', '"10-K","10-Q"') # Comma-separated, quoted
TICKERS_TO_QUERY = os.getenv('TICKERS_TO_QUERY') # Optional: comma-separated list of tickers, otherwise fetches all

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Validate Configuration ---
essential_vars = [
    ('GCP_PROJECT_ID', GCP_PROJECT_ID),
    ('BQ_DATASET_ID', BQ_DATASET_ID),
    ('BQ_METADATA_TABLE_ID', BQ_METADATA_TABLE_ID),
    ('SEC_API_SECRET_ID', SEC_API_SECRET_ID),
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}")
    raise RuntimeError(f"Missing essential environment variables: {', '.join(missing_vars)}")

if not GCP_LIBS_AVAILABLE or not SEC_API_LIB_AVAILABLE:
     raise RuntimeError("Required libraries (GCP, sec-api) are not available.")

# --- Construct Full BQ Table ID ---
METADATA_TABLE_FULL_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_METADATA_TABLE_ID}"

# --- Global Clients (Initialize later) ---
bq_client = None
secret_client = None
query_api = None

# --- Helper Functions ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    global secret_client
    if not secret_client:
        logging.info("Initializing Secret Manager client...")
        secret_client = secretmanager.SecretManagerServiceClient()

    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = secret_client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8").strip()
        logging.info(f"Successfully accessed secret: {secret_id}")
        if not payload:
             raise ValueError(f"Secret {secret_id} version {version_id} exists but is empty.")
        return payload
    except google.api_core.exceptions.NotFound:
        logging.error(f"Secret '{secret_id}' or version '{version_id}' not found in project '{project_id}'.")
        raise
    except Exception as e:
        logging.error(f"Failed to access secret '{secret_id}/{version_id}' in project '{project_id}': {e}", exc_info=True)
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
def get_recent_sec_filings(api_client, lookback_hours, filing_types_str, tickers_list=None):
    """Queries the SEC API for recent filings."""
    if not api_client:
        raise RuntimeError("SEC QueryApi client not initialized.")

    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(hours=lookback_hours)
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S') # Use ISO format with time

    # Build the query string
    query_parts = [
        f'formType:({filing_types_str})', # e.g., "10-K","10-Q"
        f'filedAt:[{start_date_str} TO {end_date_str}]',
        'NOT formType:("10-K/A" OR "10-Q/A")' # Exclude amendments
    ]
    if tickers_list:
        ticker_query = " OR ".join([f'ticker:"{t.strip().upper()}"' for t in tickers_list])
        query_parts.append(f'({ticker_query})')

    final_query_string = " AND ".join(query_parts)
    logging.info(f"SEC API Query String: {final_query_string}")

    query = {
        "query": {"query_string": {"query": final_query_string}},
        "from": "0",
        "size": "200", # Adjust size limit as needed, consider pagination if expecting many
        "sort": [{"filedAt": {"order": "desc"}}]
    }

    try:
        response = api_client.get_filings(query)
        filings = response.get('filings', [])
        logging.info(f"SEC API returned {len(filings)} filings for the last {lookback_hours} hours.")
        return filings
    except Exception as e:
        logging.error(f"Error querying SEC API: {e}", exc_info=True)
        raise # Reraise to allow retry

def check_filings_exist_in_bq(bq_client_instance, table_id_full, accession_numbers):
    """Checks a list of accession numbers against BigQuery. Returns set of existing numbers."""
    if not bq_client_instance:
        raise RuntimeError("BigQuery client not initialized.")
    if not accession_numbers:
        return set()

    # Ensure accession numbers are clean (no dashes)
    clean_accession_numbers = [str(an).replace('-', '') for an in accession_numbers]

    query = f"""
        SELECT DISTINCT AccessionNumber
        FROM `{table_id_full}`
        WHERE AccessionNumber IN UNNEST(@acc_nos)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("acc_nos", "STRING", clean_accession_numbers)
        ]
    )
    try:
        query_job = bq_client_instance.query(query, job_config=job_config)
        results = query_job.result(timeout=120) # Adjust timeout if needed
        existing_numbers = {row.AccessionNumber for row in results}
        logging.info(f"Checked {len(accession_numbers)} accession numbers against BQ. Found {len(existing_numbers)} existing.")
        return existing_numbers
    except Exception as e:
        logging.error(f"Error checking BQ for existing filings: {e}", exc_info=True)
        raise # Reraise to signal failure

def insert_new_filings_to_bq(bq_client_instance, table_id_full, filings_to_insert):
    """Inserts new filing metadata rows into BigQuery."""
    if not bq_client_instance:
        raise RuntimeError("BigQuery client not initialized.")
    if not filings_to_insert:
        logging.info("No new filings to insert into BigQuery.")
        return 0

    rows_to_insert = []
    for filing in filings_to_insert:
        # Validate and format data for BQ schema
        # Schema: Ticker(S), ReportEndDate(D), FiledDate(D), FormType(S), AccessionNumber(S), LinkToFilingDetails(S)
        try:
            report_end_date = filing.get('periodOfReport')[:10] if filing.get('periodOfReport') else None
            filed_date = filing.get('filedAt')[:10] if filing.get('filedAt') else None
            accession_no_clean = filing.get('accessionNo', '').replace('-', '')

            if not all([filing.get('ticker'), report_end_date, filed_date, filing.get('formType'), accession_no_clean, filing.get('linkToFilingDetails')]):
                 logging.warning(f"Skipping insert due to missing data in filing: {filing.get('accessionNo', 'N/A')}")
                 continue

            rows_to_insert.append({
                "Ticker": filing.get('ticker'),
                "ReportEndDate": report_end_date,
                "FiledDate": filed_date,
                "FormType": filing.get('formType'),
                "AccessionNumber": accession_no_clean,
                "LinkToFilingDetails": filing.get('linkToFilingDetails')
            })
        except Exception as format_err:
            logging.warning(f"Error formatting row for filing {filing.get('accessionNo', 'N/A')}: {format_err}")
            continue

    if not rows_to_insert:
        logging.info("No valid rows formatted for BigQuery insertion.")
        return 0

    logging.info(f"Attempting to insert {len(rows_to_insert)} new filing metadata rows into {table_id_full}...")
    try:
        # Ensure table exists (optional, Terraform should handle this)
        # try:
        #     bq_client_instance.get_table(table_id_full)
        # except NotFound:
        #     logging.error(f"BigQuery table {table_id_full} not found!")
        #     raise

        errors = bq_client_instance.insert_rows_json(table_id_full, rows_to_insert)
        if not errors:
            logging.info(f"Successfully inserted {len(rows_to_insert)} rows.")
            return len(rows_to_insert)
        else:
            logging.error(f"Errors encountered during BigQuery insert: {errors}")
            # Decide how to handle partial failures if needed
            return 0 # Count only fully successful batches or implement partial count
    except Exception as e:
        logging.error(f"Failed to insert rows into BigQuery: {e}", exc_info=True)
        raise # Reraise to signal failure

# --- Main Function ---
def main():
    global bq_client, query_api
    start_time = time.time()
    logging.info(f"Starting fetch-new-filings job. Lookback: {LOOKBACK_HOURS} hours.")

    try:
        # Initialize BQ Client using ADC
        logging.info(f"Initializing BigQuery client for project {GCP_PROJECT_ID}...")
        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        logging.info("BigQuery client initialized.")

        # Get SEC API Key from Secret Manager
        api_key = access_secret_version(GCP_PROJECT_ID, SEC_API_SECRET_ID, SEC_API_SECRET_VERSION)
        query_api = QueryApi(api_key=api_key)
        logging.info("SEC Query API client initialized.")

        # Determine tickers to query
        tickers = None
        if TICKERS_TO_QUERY:
            tickers = [t.strip() for t in TICKERS_TO_QUERY.split(',')]
            logging.info(f"Querying for specific tickers: {tickers}")

        # Fetch recent filings from SEC API
        recent_filings = get_recent_sec_filings(query_api, LOOKBACK_HOURS, FILING_TYPES, tickers)

        if not recent_filings:
            logging.info("No recent filings found from SEC API.")
            return

        # Extract accession numbers to check against BQ
        accession_numbers_to_check = [f.get('accessionNo') for f in recent_filings if f.get('accessionNo')]
        accession_numbers_clean = [an.replace('-', '') for an in accession_numbers_to_check]

        # Check which filings already exist in BigQuery
        existing_accession_numbers = check_filings_exist_in_bq(bq_client, METADATA_TABLE_FULL_ID, accession_numbers_clean)

        # Filter out existing filings
        new_filings_to_process = []
        for filing in recent_filings:
            an_clean = filing.get('accessionNo', '').replace('-', '')
            if an_clean and an_clean not in existing_accession_numbers:
                new_filings_to_process.append(filing)

        logging.info(f"Identified {len(new_filings_to_process)} new filings to insert into BigQuery.")

        # Insert new filings into BigQuery
        inserted_count = insert_new_filings_to_bq(bq_client, METADATA_TABLE_FULL_ID, new_filings_to_process)

        # Prepare output for Cloud Workflow
        # Workflow needs Ticker, AccessionNumber (cleaned), LinkToFilingDetails for Job 2
        workflow_output = []
        if inserted_count > 0:
             # Use the filings that were successfully formatted for insertion
             for formatted_row in [r for f in new_filings_to_process for r in [ {
                 "Ticker": f.get('ticker'),
                 "AccessionNumber": f.get('accessionNo', '').replace('-', ''),
                 "LinkToFilingDetails": f.get('linkToFilingDetails')
                 }] if all(r.values())]: # Basic check
                 workflow_output.append(formatted_row)

        # Log output for Workflow (Workflows can parse structured logs)
        # Alternatively, write JSON to a specific GCS path
        if workflow_output:
            output_log = {
                "job_name": "fetch-new-filings",
                "status": "success",
                "new_filings_processed": inserted_count,
                "new_filing_details": workflow_output # Pass details needed for next step
            }
            print(json.dumps(output_log)) # Print JSON to stdout for Cloud Logging
            logging.info(f"Output details for {len(workflow_output)} new filings prepared for workflow.")
        else:
             logging.info("No new filings processed or output generated for workflow.")


    except google.auth.exceptions.DefaultCredentialsError as cred_err:
        logging.critical(f"GCP Credentials Error: Could not automatically find credentials. Ensure the Cloud Run Job is running with a Service Account that has necessary permissions. Error: {cred_err}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"An error occurred during the fetch-new-filings job: {e}", exc_info=True)
        # Optionally re-raise to mark the job run as failed
        raise
    finally:
        end_time = time.time()
        logging.info(f"fetch-new-filings job finished in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
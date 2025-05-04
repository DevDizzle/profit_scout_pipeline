# main.py - Production Version for download-pdf job

import os
import time
import logging
import tempfile
import json
import sys # Added for explicit exit

# --- GCP & Lib Imports ---
try:
    from google.cloud import storage, secretmanager, bigquery
    import google.auth
    import google.api_core.exceptions # For specific exception handling
    GCP_LIBS_AVAILABLE = True
except ImportError:
    logging.error("Failed to import Google Cloud libraries. Ensure google-cloud-storage, google-cloud-secret-manager, and google-cloud-bigquery are installed.")
    GCP_LIBS_AVAILABLE = False

try:
    import requests
    REQUESTS_LIB_AVAILABLE = True
except ImportError:
    logging.error("Failed to import requests. Ensure requests is installed.")
    REQUESTS_LIB_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    logging.warning("Tenacity library not found. Retries will not be available.")
    # Define dummy decorator if tenacity is missing
    def retry(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator
    stop_after_attempt = lambda n: None
    wait_exponential = lambda *args, **kwargs: None
    retry_if_exception_type = lambda *args, **kwargs: None

# --- Configuration (from Environment Variables) ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_PDF_FOLDER = os.getenv('GCS_PDF_FOLDER', 'sec-pdf/').rstrip('/') + '/'
SEC_API_SECRET_ID = os.getenv('SEC_API_SECRET_ID', 'sec-api-key')
SEC_API_SECRET_VERSION = os.getenv('SEC_API_SECRET_VERSION', 'latest')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID', 'profit_scout')
BQ_METADATA_TABLE_ID = os.getenv('BQ_METADATA_TABLE_ID', 'filing_metadata')
# Optional: Filter FormType (e.g., '10-K', '10-Q') or leave empty for all
FORM_TYPE_FILTER = os.getenv('FORM_TYPE_FILTER')
# Optional: Limit the number of filings processed per run (useful for testing/throttling)
# Set to 0 or leave empty for no limit
MAX_FILINGS_TO_PROCESS = int(os.getenv('MAX_FILINGS_TO_PROCESS', '0'))

# --- Logging Setup ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Validate Configuration & Inputs ---
essential_vars = [
    ('GCP_PROJECT_ID', GCP_PROJECT_ID),
    ('GCS_BUCKET_NAME', GCS_BUCKET_NAME),
    ('GCS_PDF_FOLDER', GCS_PDF_FOLDER),
    ('SEC_API_SECRET_ID', SEC_API_SECRET_ID),
    ('BQ_DATASET_ID', BQ_DATASET_ID),
    ('BQ_METADATA_TABLE_ID', BQ_METADATA_TABLE_ID),
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}")
    raise RuntimeError(f"Missing essential environment variables: {', '.join(missing_vars)}")

if not GCP_LIBS_AVAILABLE or not REQUESTS_LIB_AVAILABLE:
     raise RuntimeError("Required libraries (GCP, requests, bigquery) are not available.")

# --- Global Clients (Initialize later) ---
gcs_client = None
secret_client = None
bq_client = None
sec_api_key = None

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
        logging.error(f"Failed to access secret '{secret_id}/{version_id}' in project '{project_id}': {e}", exc_info=False) # Less verbose on retry
        raise

def check_gcs_blob_exists(bucket, blob_name):
    """Checks if a specific blob exists in the GCS bucket."""
    try:
        blob = bucket.blob(blob_name)
        exists = blob.exists()
        return exists
    except Exception as e:
        logging.error(f"Error checking GCS existence for gs://{bucket.name}/{blob_name}: {e}", exc_info=True)
        # Default to assuming it doesn't exist on error to allow download attempt maybe?
        # Or re-raise/return specific error? Let's assume download attempt is okay.
        return False

RETRYABLE_REQUEST_EXCEPTIONS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)
def should_retry_requests_error(exception):
    """Return True if we should retry the Requests exception based on type or status code."""
    if isinstance(exception, requests.exceptions.HTTPError):
        # Retry on server errors (5xx) and rate limiting (429)
        return exception.response.status_code >= 500 or exception.response.status_code == 429
    return isinstance(exception, (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError))

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=30),
    retry=retry_if_exception_type(should_retry_requests_error),
    reraise=True
)
def download_pdf_from_sec_api(api_key, filing_url, local_temp_path, log_prefix=""):
    """Downloads PDF from sec-api filing reader endpoint with retries."""
    log_url = filing_url[:80] + '...' if len(filing_url) > 80 else filing_url
    pdf_api_url = f"https://api.sec-api.io/filing-reader?token={api_key}&type=pdf&url={filing_url}"
    logging.info(f"[{log_prefix}] Attempting download for filing: {log_url}")

    try:
        with requests.get(pdf_api_url, stream=True, timeout=180, allow_redirects=True) as r:
            r.raise_for_status() # Raise HTTPError for bad responses (4XX or 5XX)

            content_type = r.headers.get('content-type', '').lower()
            if 'application/pdf' not in content_type:
                 logging.warning(f"[{log_prefix}] Expected PDF content type, but got '{content_type}' for {log_url}. Download will proceed.")

            with open(local_temp_path, 'wb') as f:
                bytes_downloaded = 0
                for chunk in r.iter_content(chunk_size=16384): # Slightly larger chunk size
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

        if os.path.exists(local_temp_path) and bytes_downloaded > 100: # Basic check for non-empty file
             logging.info(f"[{log_prefix}] Successfully downloaded PDF to {local_temp_path} ({bytes_downloaded / 1024:.1f} KB)")
        elif os.path.exists(local_temp_path):
             logging.error(f"[{log_prefix}] PDF download resulted in suspiciously small file ({bytes_downloaded} bytes) at {local_temp_path} for {log_url}")
             raise IOError(f"[{log_prefix}] PDF download resulted in suspiciously small file ({bytes_downloaded} bytes) for {log_url}")
        else:
             # This case means the download loop likely didn't run or finished instantly
             logging.error(f"[{log_prefix}] PDF download failed, temporary file not created or empty at {local_temp_path} for {log_url}")
             raise IOError(f"[{log_prefix}] PDF download failed, temporary file not created or empty for {log_url}")

    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
        response_text = http_err.response.text[:500] # Log first 500 chars
        logging.error(
            f"[{log_prefix}] HTTP Error {status_code} encountered for {log_url} after retries. "
            f"Response: {response_text}",
            exc_info=False # Keep log cleaner for HTTP errors after retry
        )
        raise
    except requests.exceptions.RequestException as req_err:
        # Handle other request errors (Timeout, ConnectionError, etc.) AFTER retries
        logging.error(f"[{log_prefix}] Download Request Error for {log_url} after retries: {req_err}", exc_info=False)
        raise # Reraise
    except Exception as e:
        # Catch other potential errors (like IOError, file system issues)
        logging.error(f"[{log_prefix}] Unexpected Download/Save Error for {log_url}: {e}", exc_info=True)
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def upload_to_gcs(bucket, local_path, blob_path, log_prefix=""):
    """Uploads a local file to GCS with retries."""
    gcs_uri = f"gs://{bucket.name}/{blob_path}"
    logging.info(f"[{log_prefix}] Uploading {local_path} to {gcs_uri}")
    blob = bucket.blob(blob_path)
    try:
        # Optional: Set content type explicitly if needed
        # blob.upload_from_filename(local_path, content_type='application/pdf')
        blob.upload_from_filename(local_path)
        logging.info(f"[{log_prefix}] Successfully uploaded to {gcs_uri}")
    except Exception as e:
        logging.error(f"[{log_prefix}] Failed to upload {local_path} to GCS {gcs_uri}: {e}", exc_info=False) # Less verbose on retry
        raise # Reraise for tenacity


# --- Main Execution Logic ---
def main():
    global gcs_client, bq_client, sec_api_key
    start_time = time.time()
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    total_rows_checked = 0
    exit_code = 0 # Assume success unless errors occur

    logging.info("--- Starting Batch PDF Download Job ---")

    try:
        # Initialize Clients
        logging.info(f"Initializing GCS client for project {GCP_PROJECT_ID}...")
        gcs_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = gcs_client.bucket(GCS_BUCKET_NAME)
        logging.info(f"GCS client initialized for bucket '{GCS_BUCKET_NAME}'.")

        logging.info(f"Initializing BigQuery client for project {GCP_PROJECT_ID}...")
        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        logging.info("BigQuery client initialized.")

        # Get SEC API Key
        sec_api_key = access_secret_version(GCP_PROJECT_ID, SEC_API_SECRET_ID, SEC_API_SECRET_VERSION)

        # --- BigQuery Query ---
        table_full_id = f"`{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_METADATA_TABLE_ID}`"
        query_parts = [f"SELECT Ticker, AccessionNumber, LinkToFilingDetails FROM {table_full_id}"]

        # Add optional filtering - IMPORTANT: Ensure filters don't select rows already processed if possible
        # Ideally, add a status column to BQ table later to track PDF download status.
        # For now, filtering might just limit scope if table is large.
        filter_conditions = []
        if FORM_TYPE_FILTER:
            # Basic check for common types, adjust quoting/escaping if needed
            valid_form_types = [ft.strip() for ft in FORM_TYPE_FILTER.split(',') if ft.strip()]
            if valid_form_types:
                formatted_types = ', '.join([f"'{ft}'" for ft in valid_form_types])
                filter_conditions.append(f"FormType IN ({formatted_types})")

        # Add other potential filters, e.g., date range
        # filter_conditions.append("FiledDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)")

        if filter_conditions:
            query_parts.append("WHERE " + " AND ".join(filter_conditions))

        # Optional ordering and limit
        query_parts.append("ORDER BY FiledDate DESC, Ticker") # Process recent first
        if MAX_FILINGS_TO_PROCESS > 0:
             query_parts.append(f"LIMIT {MAX_FILINGS_TO_PROCESS}")

        bq_query = "\n".join(query_parts)
        logging.info(f"Executing BigQuery query:\n{bq_query}")

        query_job = bq_client.query(bq_query)
        results = query_job.result() # Waits for job to complete
        total_rows_in_batch = results.total_rows # Note: Might be None if LIMIT wasn't used accurately by BQ backend sometimes
        logging.info(f"Query returned {total_rows_in_batch if total_rows_in_batch is not None else 'unknown number of'} filings to check.")

        # --- Process Loop ---
        row_iterator = iter(results) # Get iterator
        while True:
             # Enforce MAX_FILINGS_TO_PROCESS limit reliably if BQ LIMIT didn't or wasn't used
             if MAX_FILINGS_TO_PROCESS > 0 and total_rows_checked >= MAX_FILINGS_TO_PROCESS:
                 logging.info(f"Reached processing limit ({MAX_FILINGS_TO_PROCESS}). Stopping.")
                 break

             try:
                 row = next(row_iterator)
                 total_rows_checked += 1
             except StopIteration:
                 break # End of results
             except Exception as iter_err:
                 logging.error(f"Error fetching next row from BigQuery results: {iter_err}", exc_info=True)
                 failed_count += 1
                 continue # Try to proceed if possible? Or break? Let's continue.

             ticker = row.Ticker
             accession_number = row.AccessionNumber # Assumes this is the cleaned version needed
             filing_url = row.LinkToFilingDetails
             log_prefix = f"{ticker}_{accession_number}" # Unique identifier for logging this item

             # Basic validation of required fields from BQ
             if not all([ticker, accession_number, filing_url]):
                 logging.warning(f"[{log_prefix}] Skipping row {total_rows_checked} due to missing data retrieved from BigQuery.")
                 failed_count += 1
                 continue

             # Defensive check in case accession number has hyphens
             # Consider cleaning this *before* inserting into BigQuery ideally
             cleaned_accession_number = accession_number.replace('-', '')
             if accession_number != cleaned_accession_number:
                  logging.warning(f"[{log_prefix}] AccessionNumber '{accession_number}' contained hyphens. Using cleaned version: '{cleaned_accession_number}'. Update source data if possible.")
                  accession_number = cleaned_accession_number # Use cleaned version for filenames/logs
                  log_prefix = f"{ticker}_{accession_number}" # Update log prefix too

             logging.info(f"--- Processing {log_prefix} (Row {total_rows_checked}) ---")

             try:
                 pdf_filename = f"{ticker}_{accession_number}.pdf"
                 gcs_relative_path = os.path.join(GCS_PDF_FOLDER, pdf_filename)
                 gcs_full_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_relative_path}"

                 logging.info(f"[{log_prefix}] Checking existence: {gcs_full_uri}")
                 pdf_exists = check_gcs_blob_exists(bucket, gcs_relative_path)

                 if pdf_exists:
                     logging.info(f"[{log_prefix}] PDF already exists. Skipping download.")
                     skipped_count += 1
                 else:
                     logging.info(f"[{log_prefix}] PDF does not exist. Attempting download and upload.")
                     local_temp_file = None # Ensure defined before try/finally
                     try:
                         # Create a temporary file for download
                         with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_f:
                             local_temp_file = temp_f.name

                         # Download
                         download_pdf_from_sec_api(sec_api_key, filing_url, local_temp_file, log_prefix)

                         # Upload
                         upload_to_gcs(bucket, local_temp_file, gcs_relative_path, log_prefix)

                         logging.info(f"[{log_prefix}] Successfully processed.")
                         processed_count += 1

                     except Exception as download_upload_err:
                          # Errors during download/upload already logged in helper functions
                          logging.error(f"[{log_prefix}] Failed during download/upload process: {download_upload_err}", exc_info=False) # Less verbose stack trace here
                          failed_count += 1
                     finally:
                         # Cleanup temp file regardless of success/failure
                         if local_temp_file and os.path.exists(local_temp_file):
                             try:
                                 os.remove(local_temp_file)
                                 logging.debug(f"[{log_prefix}] Cleaned up temp file: {local_temp_file}")
                             except OSError as rm_err:
                                 logging.warning(f"[{log_prefix}] Could not remove temp file {local_temp_file} during cleanup: {rm_err}")

             except Exception as processing_err:
                 # Catch errors during path construction or other logic for this row
                 logging.error(f"[{log_prefix}] Failed processing row {total_rows_checked} due to unexpected error: {processing_err}", exc_info=True)
                 failed_count += 1
             # --- End of loop for one row ---

    except google.auth.exceptions.DefaultCredentialsError as cred_err:
        logging.critical(f"GCP Credentials Error: {cred_err}", exc_info=True)
        failed_count += 1
    except google.api_core.exceptions.Forbidden as forbidden_err:
        # Catch specific 403 errors, especially during BQ query
        logging.critical(f"Permission Denied Error: {forbidden_err}. Check Service Account IAM roles (e.g., BigQuery Data Viewer/Job User on project/dataset).", exc_info=False)
        failed_count += 1
    except Exception as e:
        # Catch-all for other critical errors during setup or BQ query
        logging.critical(f"An critical error occurred during the main job execution: {e}", exc_info=True)
        failed_count += 1
    finally:
        end_time = time.time()
        logging.info(f"--- Batch PDF Download Job Finished ---")
        logging.info(f"Duration: {end_time - start_time:.2f} seconds")
        logging.info(f"Filings Checked: {total_rows_checked}")
        logging.info(f"Filings Processed (Downloaded & Uploaded): {processed_count}")
        logging.info(f"Filings Skipped (Already Existed): {skipped_count}")
        logging.info(f"Filings Failed: {failed_count}")

        # Output summary JSON
        summary_status = "completed_with_errors" if failed_count > 0 else "completed"
        print(json.dumps({
            "job_name": "download-filing-pdf-batch",
            "status": summary_status,
            "processed": processed_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "total_checked": total_rows_checked
        }))

        # Set exit code based on failures
        if failed_count > 0:
            exit_code = 1 # Indicate failure

    # Explicitly exit with code to ensure Cloud Run job status reflects outcome
    sys.exit(exit_code)


if __name__ == "__main__":
    # Before running main, ensure necessary libraries are installed
    if not GCP_LIBS_AVAILABLE or not REQUESTS_LIB_AVAILABLE:
         logging.critical("Exiting due to missing required libraries.")
         sys.exit(1) # Exit immediately if libs missing
    else:
        main()
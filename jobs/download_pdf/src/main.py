import os
import time
import logging
import tempfile
import json

# --- GCP & Lib Imports ---
try:
    from google.cloud import storage, secretmanager
    import google.auth
    GCP_LIBS_AVAILABLE = True
except ImportError:
    logging.error("Failed to import Google Cloud libraries. Ensure google-cloud-storage and google-cloud-secret-manager are installed.")
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
    retry_if_exception_type = lambda *args, **kwargs: None # Dummy for type hint

# --- Configuration (from Environment Variables) ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_PDF_FOLDER = os.getenv('GCS_PDF_FOLDER', 'sec-pdf/').rstrip('/') + '/'
SEC_API_SECRET_ID = os.getenv('SEC_API_SECRET_ID', 'sec-api-key')
SEC_API_SECRET_VERSION = os.getenv('SEC_API_SECRET_VERSION', 'latest')

# --- Input Parameters (Expected from Workflow/Trigger) ---
# These will likely be passed as environment variables or arguments by the Workflow
INPUT_TICKER = os.getenv('INPUT_TICKER')
INPUT_ACCESSION_NUMBER = os.getenv('INPUT_ACCESSION_NUMBER') # Expect cleaned (no hyphens)
INPUT_FILING_URL = os.getenv('INPUT_FILING_URL') # This is the LinkToFilingDetails

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Validate Configuration & Inputs ---
essential_vars = [
    ('GCP_PROJECT_ID', GCP_PROJECT_ID),
    ('GCS_BUCKET_NAME', GCS_BUCKET_NAME),
    ('GCS_PDF_FOLDER', GCS_PDF_FOLDER),
    ('SEC_API_SECRET_ID', SEC_API_SECRET_ID),
    ('INPUT_TICKER', INPUT_TICKER),
    ('INPUT_ACCESSION_NUMBER', INPUT_ACCESSION_NUMBER),
    ('INPUT_FILING_URL', INPUT_FILING_URL),
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing essential environment variables or input parameters: {', '.join(missing_vars)}")
    raise RuntimeError(f"Missing essential environment variables or input parameters: {', '.join(missing_vars)}")

if not GCP_LIBS_AVAILABLE or not REQUESTS_LIB_AVAILABLE:
     raise RuntimeError("Required libraries (GCP, requests) are not available.")

# --- Global Clients (Initialize later) ---
gcs_client = None
secret_client = None
sec_api_key = None # Fetched from Secret Manager

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

def check_gcs_blob_exists(bucket, blob_name):
    """Checks if a specific blob exists in the GCS bucket."""
    try:
        blob = bucket.blob(blob_name)
        exists = blob.exists()
        logging.info(f"Checking existence for gs://{bucket.name}/{blob_name}: {'Exists' if exists else 'Does not exist'}")
        return exists
    except Exception as e:
        logging.error(f"Error checking GCS existence for {blob_name}: {e}", exc_info=True)
        # Default to assuming it doesn't exist on error to allow download attempt
        return False

RETRYABLE_REQUEST_EXCEPTIONS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.HTTPError # Retry on 5xx errors specifically
)

def should_retry_requests_error(exception):
    """Return True if we should retry the Requests exception."""
    if isinstance(exception, requests.exceptions.HTTPError):
        # Only retry on server-side errors (5xx)
        return exception.response.status_code >= 500
    # Retry on Timeout, ConnectionError, ChunkedEncodingError
    return isinstance(exception, RETRYABLE_REQUEST_EXCEPTIONS)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=30),
    retry=retry_if_exception_type(should_retry_requests_error), # Use custom retry condition
    reraise=True
)
def download_pdf_from_sec_api(api_key, filing_url, local_temp_path):
    """Downloads PDF from sec-api filing reader endpoint with retries."""
    pdf_api_url = f"https://api.sec-api.io/filing-reader?token={api_key}&type=pdf&url={filing_url}"
    logging.info(f"Attempting download from: {pdf_api_url[:100]}... (URL truncated)") # Avoid logging full URL with key

    try:
        with requests.get(pdf_api_url, stream=True, timeout=180, allow_redirects=True) as r:
            r.raise_for_status() # Raise HTTPError for bad responses (4XX or 5XX) - retry handles 5xx

            content_type = r.headers.get('content-type', '').lower()
            if 'application/pdf' not in content_type:
                logging.warning(f"Expected PDF content type, but got '{content_type}'. Download will proceed.")

            with open(local_temp_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192*2): # Read in slightly larger chunks
                    f.write(chunk)

        if os.path.exists(local_temp_path) and os.path.getsize(local_temp_path) > 100: # Basic check for non-empty file
             logging.info(f"Successfully downloaded PDF to {local_temp_path} ({os.path.getsize(local_temp_path) / 1024:.1f} KB)")
        else:
            # This case might happen if the stream ends prematurely but no error raised
            raise IOError(f"PDF download resulted in a missing or suspiciously small file (<100 bytes) at {local_temp_path}")

    except requests.exceptions.RequestException as req_err:
        # Handle non-HTTP errors covered by should_retry_requests_error
        logging.error(f"Download Request Error: {req_err}", exc_info=True)
        raise # Reraise for tenacity
    except Exception as e:
         # Catch other potential errors (like IOError above)
         logging.error(f"Unexpected Download/Save Error: {e}", exc_info=True)
         raise # Reraise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def upload_to_gcs(bucket, local_path, blob_path):
    """Uploads a local file to GCS with retries."""
    logging.info(f"Uploading {local_path} to gs://{bucket.name}/{blob_path}")
    blob = bucket.blob(blob_path)
    try:
        blob.upload_from_filename(local_path)
        logging.info(f"Successfully uploaded to gs://{bucket.name}/{blob_path}")
    except Exception as e:
        logging.error(f"Failed to upload {local_path} to GCS: {e}", exc_info=True)
        raise # Reraise for tenacity

# --- Main Execution Logic ---
def main():
    global gcs_client, sec_api_key
    start_time = time.time()
    status = "started"
    gcs_output_path = None
    local_temp_file = None
    log_prefix = f"{INPUT_TICKER} - {INPUT_ACCESSION_NUMBER}"
    logging.info(f"--- Starting PDF Download Job for {log_prefix} ---")
    logging.info(f"Input Filing URL: {INPUT_FILING_URL}")

    try:
        # Initialize GCS Client using ADC
        logging.info(f"Initializing GCS client for project {GCP_PROJECT_ID}...")
        gcs_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = gcs_client.bucket(GCS_BUCKET_NAME)
        logging.info(f"GCS client initialized for bucket '{GCS_BUCKET_NAME}'.")

        # Get SEC API Key from Secret Manager
        sec_api_key = access_secret_version(GCP_PROJECT_ID, SEC_API_SECRET_ID, SEC_API_SECRET_VERSION)

        # Define GCS path and check existence
        # Using Ticker_AccessionNumber.pdf format consistent with previous steps
        pdf_filename = f"{INPUT_TICKER}_{INPUT_ACCESSION_NUMBER}.pdf"
        gcs_output_path = os.path.join(GCS_PDF_FOLDER, pdf_filename)

        if check_gcs_blob_exists(bucket, gcs_output_path):
            logging.info(f"PDF already exists in GCS: gs://{GCS_BUCKET_NAME}/{gcs_output_path}. Skipping download.")
            status = "skipped_exists"
        else:
            # Download PDF to a temporary file
            # Use tempfile for secure temporary file creation/cleanup
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_f:
                local_temp_file = temp_f.name

            logging.info(f"Downloading PDF to temporary file: {local_temp_file}")
            download_pdf_from_sec_api(sec_api_key, INPUT_FILING_URL, local_temp_file)
            status = "downloaded"

            # Upload the downloaded file to GCS
            upload_to_gcs(bucket, local_temp_file, gcs_output_path)
            status = "uploaded"

        # --- Output for Workflow ---
        # The main output is the GCS path. We can log this or
        # Cloud Run Jobs v2 allows writing structured output. For simplicity, logging.
        output_data = {
            "job_name": "download-filing-pdf",
            "ticker": INPUT_TICKER,
            "accession_number": INPUT_ACCESSION_NUMBER,
            "status": status,
            "output_gcs_path": f"gs://{GCS_BUCKET_NAME}/{gcs_output_path}" if gcs_output_path else None
        }
        print(json.dumps(output_data)) # Log output as JSON

    except google.auth.exceptions.DefaultCredentialsError as cred_err:
        logging.critical(f"GCP Credentials Error: Could not automatically find credentials. Ensure the Cloud Run Job is running with a Service Account that has necessary permissions. Error: {cred_err}", exc_info=True)
        status = "error_auth"
        raise
    except Exception as e:
        logging.error(f"An error occurred during the download-pdf job for {log_prefix}: {e}", exc_info=True)
        status = "error_runtime"
        # Log failure output
        print(json.dumps({ "job_name": "download-filing-pdf", "status": status, "ticker": INPUT_TICKER, "accession_number": INPUT_ACCESSION_NUMBER, "error": str(e) }))
        raise # Re-raise to mark job execution as failed
    finally:
        # Cleanup local temp file if it still exists
        if local_temp_file and os.path.exists(local_temp_file):
            try:
                os.remove(local_temp_file)
                logging.info(f"Cleaned up temporary file: {local_temp_file}")
            except OSError as rm_err:
                logging.warning(f"Could not remove temp file {local_temp_file} during cleanup: {rm_err}")

        end_time = time.time()
        logging.info(f"--- PDF Download Job for {log_prefix} finished in {end_time - start_time:.2f} seconds. Final Status: {status} ---")

if __name__ == "__main__":
    main()
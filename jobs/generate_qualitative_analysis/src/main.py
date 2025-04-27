import os
import time
import logging
import tempfile
import json
import re
import asyncio # For waiting on Gemini file processing

# --- GCP & Lib Imports ---
try:
    from google.cloud import storage, secretmanager
    import google.auth
    import pandas as pd # For metadata CSV
    GCP_LIBS_AVAILABLE = True
except ImportError:
    logging.error("Failed to import Google Cloud or pandas libraries.")
    GCP_LIBS_AVAILABLE = False

try:
    import google.generativeai as genai
    # from google.generativeai.types import File # If needed for type hinting
    GEMINI_LIB_AVAILABLE = True
except ImportError:
    logging.error("Failed to import google-generativeai. Ensure it's installed.")
    GEMINI_LIB_AVAILABLE = False

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
# Input PDF Prefix (used only for constructing potential input paths if needed)
# GCS_PDF_PREFIX = os.getenv('GCS_PDF_FOLDER', 'sec-pdf/').rstrip('/') + '/'
# Output Prefixes
GCS_QUALITATIVE_TXT_PREFIX = os.getenv('GCS_ANALYSIS_TXT_PREFIX', 'Qualitative_Analysis_TXT_R1000/').rstrip('/') + '/'
GCS_METADATA_CSV_PREFIX = os.getenv('GCS_METADATA_CSV_PREFIX', 'Qualitative_Metadata_R1000/').rstrip('/') + '/'
# Secrets & Model
GEMINI_API_SECRET_ID = os.getenv('GEMINI_API_KEY_SECRET_ID', 'gemini-api-key')
GEMINI_API_KEY_SECRET_VERSION = os.getenv('GEMINI_API_KEY_SECRET_VERSION', 'latest')
GEMINI_MODEL_NAME = os.getenv('GEMINI_MODEL_NAME', 'gemini-1.5-flash') # Ensure this model supports File API
# Gemini Config
GEMINI_TEMPERATURE = float(os.getenv('GEMINI_TEMPERATURE', '0.2'))
GEMINI_MAX_TOKENS = int(os.getenv('GEMINI_MAX_TOKENS', '8192'))
GEMINI_REQ_TIMEOUT = int(os.getenv('GEMINI_REQ_TIMEOUT', '300')) # Seconds

# --- Input Parameters (Expected from Workflow/Trigger) ---
INPUT_PDF_GCS_PATH = os.getenv('INPUT_PDF_GCS_PATH') # e.g., gs://bucket/sec-pdf/TICKER_ACCNO.pdf
INPUT_TICKER = os.getenv('INPUT_TICKER')
INPUT_ACCESSION_NUMBER = os.getenv('INPUT_ACCESSION_NUMBER') # Cleaned
INPUT_FILING_DATE = os.getenv('INPUT_FILING_DATE') # YYYY-MM-DD string
INPUT_FORM_TYPE = os.getenv('INPUT_FORM_TYPE') # e.g., 10-K or 10-Q

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
    ('GCS_QUALITATIVE_TXT_PREFIX', GCS_QUALITATIVE_TXT_PREFIX),
    ('GCS_METADATA_CSV_PREFIX', GCS_METADATA_CSV_PREFIX),
    ('GEMINI_API_SECRET_ID', GEMINI_API_SECRET_ID),
    ('GEMINI_MODEL_NAME', GEMINI_MODEL_NAME),
    ('INPUT_PDF_GCS_PATH', INPUT_PDF_GCS_PATH),
    ('INPUT_TICKER', INPUT_TICKER),
    ('INPUT_ACCESSION_NUMBER', INPUT_ACCESSION_NUMBER),
    ('INPUT_FILING_DATE', INPUT_FILING_DATE),
    ('INPUT_FORM_TYPE', INPUT_FORM_TYPE),
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing essential environment variables or input parameters: {', '.join(missing_vars)}")
    raise RuntimeError(f"Missing essential environment variables or input parameters: {', '.join(missing_vars)}")

if not GCP_LIBS_AVAILABLE or not GEMINI_LIB_AVAILABLE:
     raise RuntimeError("Required libraries (GCP, google-generativeai, pandas) are not available.")

# --- Global Clients (Initialize later) ---
gcs_client = None
secret_client = None
gemini_api_key = None
gemini_model = None

# --- Analysis Prompt (Copied from notebook script Part 4) ---
# Ensure this is the prompt you want to use
qualitative_analysis_prompt = """
You are an expert financial analyst reviewing an SEC filing (10-K or 10-Q).
Your task is to extract key financial performance indicators AND synthesize them into a qualitative assessment based *strictly* on the information presented within this document.
Follow these steps:

1.  **Extract Key Financial Data & Trends**:
    * Identify primary figures (Revenue, Operating Income, Net Income, EPS, Operating Cash Flow, Key Debt metric, RMR/ARR if applicable).
    * For each, state the value for the current period and the prior year period.
    * **Calculate and state the Year-over-Year percentage change.**
    * **If possible, state if this YoY change represents an acceleration or deceleration compared to the previous year's YoY change.** (e.g., "Revenue growth accelerated to 15% YoY from 5% prior YoY").
2.  **Identify Key Qualitative Signals from the Filing**:
    * Based *only* on the data and discussion *in the filing*, identify the **Top 2-3 Qualitative Strengths**. Link to specific data/metrics from Step 1 where applicable (e.g., "Strong Revenue Growth (+15% YoY)"). Be concise.
    * Identify the **Top 2-3 Qualitative Weaknesses/Concerns**. Link to specific data/metrics from Step 1 where applicable (e.g., "Margin Pressure (Operating Margin down 200bps YoY)"). Be concise.
3.  **Synthesize Overall Qualitative Assessment**:
    * Provide a brief summary paragraph synthesizing the key changes, strengths, and weaknesses identified above.
    * Conclude this paragraph by stating whether the overall qualitative picture *presented solely within this document* appears predominantly **Positive**, **Negative**, or **Mixed**.
    * **Also assess the overall management tone conveyed in the filing (e.g., Confident, Optimistic, Cautious, Neutral, Defensive) based only on the language used.**
4.  **Summarize Investment Implications (from Filing)**:
    * Briefly list the key factors (derived from the points above) from *this specific filing* most relevant for investment. Focus on *changes* and *outlook*.
    * **Do NOT provide a recommendation.**
5.  **Extract Key Guidance & Outlook (from Filing)**:
    * Summarize any **explicit financial guidance** (e.g., revenue/EPS ranges for next Q/Year) mentioned *within this document*. State "None provided" if applicable.
    * Summarize management's qualitative outlook regarding future business prospects or key strategic initiatives *mentioned within this document*. State "None provided" if applicable.

**Output Format**:
Your output should be clearly structured using bullet points for steps 1, 2, 4, and 5, and a paragraph for step 3. Ensure the analysis remains objective and grounded *exclusively* in the provided SEC filing text.
Avoid external information or real-time market data. Ensure the full analysis is included without truncation. Be concise in each bullet point.

CRITICAL INSTRUCTION: Your response MUST contain ONLY the requested analysis sections (Key Data/Trends, Qualitative Signals, Qualitative Assessment, Investment Implications, Guidance/Outlook).
Do NOT include any introductory sentences, concluding remarks, warnings, or disclaimers stating that this is not financial advice or that the analysis is based only on the provided text.
Output *only* the structured analysis itself.
"""

# --- Helper Functions ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    global secret_client
    if not secret_client:
        logging.info("Initializing Secret Manager client...")
        secret_client = secretmanager.SecretManagerServiceClient()
    # ... (rest of function is same as before) ...
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
        return False # Assume doesn't exist on error

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def download_from_gcs(bucket, blob_name, local_path):
    """Downloads a file from GCS."""
    logging.info(f"Downloading gs://{bucket.name}/{blob_name} to {local_path}")
    blob = bucket.blob(blob_name)
    try:
        blob.download_to_filename(local_path)
        if not os.path.exists(local_path) or os.path.getsize(local_path) < 100:
             raise IOError(f"Downloaded file {local_path} missing or appears empty.")
        logging.info(f"Successfully downloaded GCS file ({os.path.getsize(local_path)/1024:.1f} KB).")
    except Exception as e:
        logging.error(f"Failed to download gs://{bucket.name}/{blob_name}: {e}", exc_info=True)
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=20), reraise=True)
async def upload_to_gemini_async(local_path, display_name):
    """Uploads local file to Gemini File API, waits until ACTIVE."""
    if not GEMINI_LIB_AVAILABLE: return None # Should not happen if validation passed
    if not gemini_model: raise RuntimeError("Gemini client not initialized.")

    logging.info(f"Uploading {display_name} to Gemini File API...")
    uploaded_file = None
    loop = asyncio.get_running_loop()
    try:
        # Run synchronous genai.upload_file in executor
        uploaded_file = await loop.run_in_executor(None, genai.upload_file, local_path, display_name=display_name)
        logging.info(f"Initial upload request sent ({uploaded_file.name}). Waiting for ACTIVE state...")

        attempts = 0
        max_attempts = 60 # Wait up to 2 minutes (60 * 2 sec)
        while uploaded_file.state.name != "ACTIVE" and attempts < max_attempts:
            await asyncio.sleep(2)
            # Run synchronous genai.get_file in executor
            uploaded_file = await loop.run_in_executor(None, genai.get_file, uploaded_file.name)
            attempts += 1
            logging.debug(f"File {uploaded_file.name} state: {uploaded_file.state.name}. Attempt {attempts}/{max_attempts}")
            if uploaded_file.state.name == "FAILED":
                raise google.api_core.exceptions.GoogleAPICallError(f"Gemini file processing failed: {uploaded_file.name}. State: {uploaded_file.state}")

        if uploaded_file.state.name != "ACTIVE":
            raise TimeoutError(f"Gemini file {uploaded_file.name} did not become ACTIVE after {attempts * 2} seconds.")

        logging.info(f"File {uploaded_file.name} is ACTIVE.")
        return uploaded_file
    except Exception as e:
        logging.error(f"Error during Gemini upload/processing for {display_name}: {e}", exc_info=True)
        # Attempt cleanup if upload started but failed processing
        if uploaded_file and uploaded_file.name:
            logging.warning(f"Attempting to delete potentially failed Gemini upload: {uploaded_file.name}")
            await delete_gemini_file_async(uploaded_file.name) # Needs to be async
        raise # Re-raise the original error

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=10, max=60), reraise=True)
async def generate_content_async(model_instance, prompt_list):
    """Generates content using Gemini model with retry."""
    if not model_instance: raise RuntimeError("Gemini model not initialized.")

    logging.info("Generating content via Gemini...")
    request_options = {"timeout": GEMINI_REQ_TIMEOUT}
    loop = asyncio.get_running_loop()
    response_text = "Error: Content generation failed unexpectedly."
    try:
        # Run synchronous generate_content in executor
        response = await loop.run_in_executor(
            None,
            model_instance.generate_content,
            prompt_list,
            request_options=request_options
        )
        logging.info("Received response from Gemini.")

        # --- Robust response handling (from notebook script) ---
        if hasattr(response, 'text') and response.text:
            response_text = response.text
            logging.info("Content generation successful (from text).")
        elif hasattr(response, 'parts') and response.parts:
            response_text = "".join(part.text for part in response.parts if hasattr(part, 'text'))
            if response_text:
                logging.info("Content generation successful (from parts).")
            else:
                logging.warning(f"Gemini returned parts but no text content. Parts: {response.parts}")
                response_text = "Error: Content generation returned empty parts."
        elif hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
            reason = response.prompt_feedback.block_reason_message or response.prompt_feedback.block_reason
            logging.error(f"Content generation blocked. Reason: {reason}")
            response_text = f"Error: Content generation blocked ({reason})"
        else:
            logging.warning(f"Gemini returned no text/parts or block reason. Candidates: {getattr(response, 'candidates', 'N/A')}")
            response_text = "Error: Content generation returned unknown empty response."
        # --- End robust handling ---

        # Raise error if generation failed or was blocked to trigger retry or mark failure
        if response_text.startswith("Error:"):
             raise google.api_core.exceptions.GoogleAPICallError(response_text)

        return response_text

    except google.api_core.exceptions.GoogleAPICallError as api_error:
        logging.error(f"Gemini API Call Error: {api_error}", exc_info=True)
        raise # Reraise for tenacity
    except Exception as e:
        logging.error(f"Unexpected Error during Gemini content generation: {e}", exc_info=True)
        raise # Reraise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
async def delete_gemini_file_async(file_name):
    """Requests deletion of a file from Gemini File API with retry."""
    if not GEMINI_LIB_AVAILABLE: return
    if not file_name:
        logging.debug("No Gemini file name provided to delete.")
        return

    logging.info(f"Requesting deletion of Gemini file: {file_name}")
    loop = asyncio.get_running_loop()
    try:
        # Run synchronous delete_file in executor
        await loop.run_in_executor(None, genai.delete_file, file_name)
        logging.info(f"Successfully requested deletion of Gemini file: {file_name}")
    except Exception as e:
        # Log warning but don't fail the whole job if cleanup fails
        logging.warning(f"Failed to delete Gemini file {file_name} (will continue): {e}")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def upload_to_gcs(bucket, local_path, blob_path, content_type='text/plain'):
    """Uploads a local file to GCS with retries."""
    logging.info(f"Uploading {local_path} to gs://{bucket.name}/{blob_path}")
    blob = bucket.blob(blob_path)
    try:
        blob.upload_from_filename(local_path, content_type=content_type)
        logging.info(f"Successfully uploaded to gs://{bucket.name}/{blob_path}")
    except Exception as e:
        logging.error(f"Failed to upload {local_path} to GCS: {e}", exc_info=True)
        raise # Reraise for tenacity

# --- Main Execution Logic ---
async def main():
    global gcs_client, gemini_api_key, gemini_model
    start_time = time.time()
    status = "started"
    analysis_txt_gcs_uri = None
    metadata_csv_gcs_uri = None
    local_pdf_path = None
    local_txt_path = None
    local_csv_path = None
    uploaded_gemini_file = None
    log_prefix = f"{INPUT_TICKER} - {INPUT_ACCESSION_NUMBER}"
    logging.info(f"--- Starting Qualitative Analysis Job for {log_prefix} ---")
    logging.info(f"Input PDF GCS Path: {INPUT_PDF_GCS_PATH}")

    # Derive output paths
    base_filename = f"{INPUT_TICKER}_{INPUT_FILING_DATE}_{INPUT_FORM_TYPE}_{INPUT_ACCESSION_NUMBER}" # Reconstruct base name
    txt_filename = f"{base_filename}_analysis.txt"
    csv_filename = f"{base_filename}_metadata.csv"
    gcs_txt_path = os.path.join(GCS_QUALITATIVE_TXT_PREFIX, txt_filename)
    gcs_csv_path = os.path.join(GCS_METADATA_CSV_PREFIX, csv_filename)

    temp_dir = tempfile.mkdtemp() # Create a temporary directory for this run

    try:
        # Initialize GCS Client using ADC
        logging.info(f"Initializing GCS client for project {GCP_PROJECT_ID}...")
        gcs_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = gcs_client.bucket(GCS_BUCKET_NAME)
        logging.info(f"GCS client initialized for bucket '{GCS_BUCKET_NAME}'.")

        # Check if output TXT already exists
        if check_gcs_blob_exists(bucket, gcs_txt_path):
            logging.info(f"Analysis TXT already exists: gs://{GCS_BUCKET_NAME}/{gcs_txt_path}. Skipping.")
            status = "skipped_exists"
            analysis_txt_gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_txt_path}" # Still report existing path
        else:
            # Get Gemini API Key from Secret Manager
            gemini_api_key = access_secret_version(GCP_PROJECT_ID, GEMINI_API_SECRET_ID, GEMINI_API_KEY_SECRET_VERSION)
            genai.configure(api_key=gemini_api_key)
            gemini_model = genai.GenerativeModel(
                GEMINI_MODEL_NAME,
                generation_config={"temperature": GEMINI_TEMPERATURE, "max_output_tokens": GEMINI_MAX_TOKENS}
            )
            logging.info(f"Gemini client initialized with model {GEMINI_MODEL_NAME}.")

            # Download input PDF from GCS
            if not INPUT_PDF_GCS_PATH.startswith(f"gs://{GCS_BUCKET_NAME}/"):
                raise ValueError(f"Input PDF path {INPUT_PDF_GCS_PATH} does not match bucket {GCS_BUCKET_NAME}")
            input_blob_name = INPUT_PDF_GCS_PATH[len(f"gs://{GCS_BUCKET_NAME}/"):]
            local_pdf_path = os.path.join(temp_dir, os.path.basename(input_blob_name))
            download_from_gcs(bucket, input_blob_name, local_pdf_path)

            # Upload PDF to Gemini File API
            display_name = os.path.basename(local_pdf_path)
            uploaded_gemini_file = await upload_to_gemini_async(local_pdf_path, display_name)
            status = "gemini_uploaded"

            # Generate Analysis
            analysis_text = await generate_content_async(gemini_model, [qualitative_analysis_prompt, uploaded_gemini_file])
            status = "analysis_generated"

            # Save Analysis TXT locally
            local_txt_path = os.path.join(temp_dir, txt_filename)
            with open(local_txt_path, 'w', encoding='utf-8') as f_txt:
                f_txt.write(analysis_text)
            logging.info(f"Analysis text saved locally to {local_txt_path}")

            # Upload Analysis TXT to GCS
            upload_to_gcs(bucket, local_txt_path, gcs_txt_path, content_type='text/plain')
            analysis_txt_gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_txt_path}"
            status = "analysis_uploaded"

        # --- Always create and upload metadata CSV ---
        processing_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        metadata_dict = {
            'Ticker': [INPUT_TICKER],
            'FormType': [INPUT_FORM_TYPE],
            'FilingDate': [INPUT_FILING_DATE],
            'AccessionNumber': [INPUT_ACCESSION_NUMBER],
            'SourcePDF_GCS_Path': [INPUT_PDF_GCS_PATH],
            'AnalysisTXT_GCS_Path': [analysis_txt_gcs_uri], # Use variable which might be from check_exists
            'ProcessingTimestampUTC': [processing_timestamp],
            'ProcessingStatus': [status], # Reflects the final status of this run
            'ModelUsed': [GEMINI_MODEL_NAME if status not in ['started', 'skipped_exists'] else 'N/A']
        }
        df_meta = pd.DataFrame(metadata_dict)
        local_csv_path = os.path.join(temp_dir, csv_filename)
        df_meta.to_csv(local_csv_path, index=False, encoding='utf-8')
        logging.info(f"Metadata CSV saved locally to {local_csv_path}")

        # Upload Metadata CSV to GCS
        upload_to_gcs(bucket, local_csv_path, gcs_csv_path, content_type='text/csv')
        metadata_csv_gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_csv_path}"
        logging.info(f"Metadata CSV uploaded to {metadata_csv_gcs_uri}")
        # --- End Metadata CSV Handling ---

        # Final status determination
        if status not in ['skipped_exists', 'analysis_uploaded']:
             # If it wasn't skipped and didn't complete upload, mark as error
             if not status.startswith("error_"): status = "error_incomplete"
        elif status == 'analysis_uploaded':
            status = 'success' # Mark explicit success only if analysis was generated and uploaded

        # --- Output for Workflow ---
        output_data = {
            "job_name": "generate-qualitative-analysis",
            "ticker": INPUT_TICKER,
            "accession_number": INPUT_ACCESSION_NUMBER,
            "status": status,
            "input_pdf_gcs_path": INPUT_PDF_GCS_PATH,
            "output_analysis_txt_gcs_path": analysis_txt_gcs_uri,
            "output_metadata_csv_gcs_path": metadata_csv_gcs_uri
        }
        print(json.dumps(output_data)) # Log output as JSON

    except google.auth.exceptions.DefaultCredentialsError as cred_err:
        logging.critical(f"GCP Credentials Error: {cred_err}", exc_info=True)
        status = "error_auth"
        print(json.dumps({ "job_name": "generate-qualitative-analysis", "status": status, "error": str(cred_err) }))
        raise
    except Exception as e:
        logging.error(f"An error occurred during the qualitative-analysis job for {log_prefix}: {e}", exc_info=True)
        status = "error_runtime"
        print(json.dumps({ "job_name": "generate-qualitative-analysis", "status": status, "error": str(e) }))
        raise
    finally:
        # Cleanup Gemini File if it was created
        if uploaded_gemini_file and uploaded_gemini_file.name:
            await delete_gemini_file_async(uploaded_gemini_file.name) # Needs await

        # Cleanup local files/directory
        logging.debug(f"Cleaning up temporary directory: {temp_dir}")
        for local_path in [local_pdf_path, local_txt_path, local_csv_path]:
             if local_path and os.path.exists(local_path):
                 try: os.remove(local_path)
                 except OSError as rm_err: logging.warning(f"Could not remove temp file {local_path}: {rm_err}")
        if os.path.exists(temp_dir):
             try: os.rmdir(temp_dir)
             except OSError as rmdir_err: logging.warning(f"Could not remove temp dir {temp_dir}: {rmdir_err}")

        end_time = time.time()
        logging.info(f"--- Qualitative Analysis Job for {log_prefix} finished in {end_time - start_time:.2f} seconds. Final Status: {status} ---")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
import os
import time
import logging
import tempfile
import json
import re
from datetime import datetime

# --- GCP & Lib Imports ---
try:
    from google.cloud import storage
    import google.auth
    GCP_LIBS_AVAILABLE = True
except ImportError:
    logging.error("Failed to import Google Cloud libraries. Ensure google-cloud-storage is installed.")
    GCP_LIBS_AVAILABLE = False

try:
    # Note: Use the Vertex AI integration
    import google.generativeai as genai
    from google.generativeai.types import GenerationConfig, Tool # Use types from main library
    from google.ai.generativelanguage import GoogleSearchRetrieval # Import specific tool part
    GEMINI_LIB_AVAILABLE = True
except ImportError:
    logging.error("Failed to import google-generativeai. Ensure google-generativeai[vertexai] is installed.")
    GEMINI_LIB_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    from google.api_core import exceptions as google_exceptions # For retry
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
    google_exceptions = None # Indicate missing exceptions for retry

# --- Configuration (from Environment Variables) ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_REGION = os.getenv('GCP_REGION', 'us-central1') # Needed for Vertex AI init
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
# Use the same output prefix as the old news_summarizer for consistency
GCS_OUTPUT_PREFIX = os.getenv('GCS_NEWS_SUMMARY_PREFIX', 'RiskAssessments/').rstrip('/') + '/'
# Model (ensure this is a Vertex AI model ID supporting Function Calling/Tools)
VERTEX_MODEL_NAME = os.getenv('GEMINI_MODEL_NAME', 'gemini-1.5-flash-001') # Use the same env var name for simplicity

# --- Input Parameters (Expected from Workflow/Trigger) ---
INPUT_TICKER = os.getenv('INPUT_TICKER')
INPUT_FILING_DATE = os.getenv('INPUT_FILING_DATE') # YYYY-MM-DD string
# Optional Company Name - use Ticker if not provided
INPUT_COMPANY_NAME = os.getenv('INPUT_COMPANY_NAME', INPUT_TICKER)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Validate Configuration & Inputs ---
essential_vars = [
    ('GCP_PROJECT_ID', GCP_PROJECT_ID),
    ('GCP_REGION', GCP_REGION),
    ('GCS_BUCKET_NAME', GCS_BUCKET_NAME),
    ('GCS_OUTPUT_PREFIX', GCS_OUTPUT_PREFIX),
    ('VERTEX_MODEL_NAME', VERTEX_MODEL_NAME),
    ('INPUT_TICKER', INPUT_TICKER),
    ('INPUT_FILING_DATE', INPUT_FILING_DATE),
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing essential environment variables or input parameters: {', '.join(missing_vars)}")
    raise RuntimeError(f"Missing essential environment variables or input parameters: {', '.join(missing_vars)}")

if not GCP_LIBS_AVAILABLE or not GEMINI_LIB_AVAILABLE:
     raise RuntimeError("Required libraries (GCP, google-generativeai) are not available.")

# --- Global Clients (Initialize later) ---
gcs_client = None
vertex_client = None # Use Vertex AI client

# --- Assessment Prompt (Copied from notebook script Part 5) ---
# Ensure this is the prompt you want to use
risk_assessment_prompt_template = """
Analyze {company_name} ({ticker}) using Google Search results from the past {lookback_days} days for stock prediction input. Be concise and data-focused. Output *only* the requested sections using the specified format.

**1. Key Data Points/Events (Max 3):**
    * [Bullet point summarizing a critical financial metric, guidance, or event]
    * [Bullet point summarizing another critical metric/event]
    * [Bullet point summarizing a third critical metric/event]

**2. Overall Sentiment (Choose one: Positive, Negative, Neutral):**
    [Sentiment Category]

**3. Primary 30-Day Expectation (1 sentence):**
    [State the most likely directional bias or lack thereof for the next 30 days based *only* on the search results]

**4. Key Potential Positive Catalysts (Next 30 days, max 2 points):**
    * [Potential factor 1, e.g., "Upcoming product launch announcement"]
    * [Potential factor 2, e.g., "Positive industry trend confirmation"]

**5. Key Potential Negative Risks (Next 30 days, max 2 points):**
    * [Potential risk 1, e.g., "Increased competitive pressure from X"]
    * [Potential risk 2, e.g., "Regulatory scrutiny announcement"]
"""

# --- Helper Functions ---
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

# Define exceptions suitable for retry with Vertex AI client
RETRYABLE_VERTEX_EXCEPTIONS = ()
if google_exceptions:
    RETRYABLE_VERTEX_EXCEPTIONS = (
        google_exceptions.Aborted,
        google_exceptions.DeadlineExceeded,
        google_exceptions.InternalServerError,
        google_exceptions.ResourceExhausted,
        google_exceptions.ServiceUnavailable,
        google_exceptions.Unknown,
        google.auth.exceptions.TransportError # For potential transient network issues
    )

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=30),
    retry=retry_if_exception_type(RETRYABLE_VERTEX_EXCEPTIONS),
    reraise=True
)
def generate_vertex_content_with_search(model_instance, prompt_text):
    """Generates content using Vertex AI Gemini with Search tool."""
    if not model_instance: raise RuntimeError("Vertex AI Gemini model instance not initialized.")

    logging.info("Generating content via Vertex AI Gemini + Search...")
    # Define the Google Search tool
    # Note: Using GoogleSearchRetrieval directly
    search_tool = Tool(google_search_retrieval=GoogleSearchRetrieval(disable_attribution=False))

    # Configure generation - low temperature for factual summary
    config = GenerationConfig(temperature=0.1)

    try:
        # Call the model using the specific method if available, or generate_content
        # Assuming generate_content works with tools for the vertexai client
        response = model_instance.generate_content(
            prompt_text,
            generation_config=config,
            tools=[search_tool]
        )
        logging.info("Received response from Vertex AI Gemini.")

        # --- Robust response handling (similar to previous job) ---
        if hasattr(response, 'text') and response.text:
            response_text = response.text
            logging.info("Content generation successful (from text).")
        elif hasattr(response, 'parts') and response.parts:
            response_text = "".join(part.text for part in response.parts if hasattr(part, 'text'))
            if response_text:
                logging.info("Content generation successful (from parts).")
            else:
                logging.warning(f"Vertex AI returned parts but no text content. Parts: {response.parts}")
                response_text = "Error: Content generation returned empty parts."
        elif hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
            reason = response.prompt_feedback.block_reason_message or response.prompt_feedback.block_reason
            logging.error(f"Content generation blocked. Reason: {reason}")
            response_text = f"Error: Content generation blocked ({reason})"
        else:
            # Check for function calls if expecting them (not in this prompt)
            # Check candidates if available
            candidates_text = f"Candidates: {response.candidates}" if hasattr(response, 'candidates') else "Candidates attribute not found."
            logging.warning(f"Vertex AI returned no text/parts or block reason. {candidates_text}")
            response_text = "Error: Content generation returned unknown empty response."
        # --- End robust handling ---

        # Raise error if generation failed or was blocked to trigger retry or mark failure
        if response_text.startswith("Error:"):
             # Use a retryable exception type if possible
             raise google_exceptions.GoogleAPICallError(response_text) if google_exceptions else RuntimeError(response_text)

        return response_text

    except google_exceptions.GoogleAPICallError as api_error:
        logging.error(f"Vertex AI API Call Error: {api_error}", exc_info=True)
        raise # Reraise for tenacity
    except Exception as e:
        logging.error(f"Unexpected Error during Vertex AI content generation: {e}", exc_info=True)
        raise # Reraise

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
def main():
    global gcs_client, vertex_client
    start_time = time.time()
    status = "started"
    assessment_txt_gcs_uri = None
    local_temp_path = None
    log_prefix = f"{INPUT_TICKER} - {INPUT_FILING_DATE}"
    logging.info(f"--- Starting Headline Assessment Job for {log_prefix} ---")

    # Construct output filename based on Ticker and FilingDate
    # Format date as YYYYMMDD for filename consistency
    try:
        filing_date_dt = datetime.datetime.strptime(INPUT_FILING_DATE, '%Y-%m-%d')
        date_str_for_filename = filing_date_dt.strftime('%Y%m%d')
    except ValueError:
        logging.error(f"Invalid INPUT_FILING_DATE format: {INPUT_FILING_DATE}. Using original string.")
        date_str_for_filename = INPUT_FILING_DATE.replace('-','') # Fallback

    assessment_filename = f"RiskAssessment_{INPUT_TICKER}_{date_str_for_filename}.txt"
    gcs_output_path = os.path.join(GCS_OUTPUT_PREFIX, assessment_filename)

    temp_dir = tempfile.mkdtemp() # Create a temporary directory for this run

    try:
        # Initialize GCS Client using ADC
        logging.info(f"Initializing GCS client for project {GCP_PROJECT_ID}...")
        gcs_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = gcs_client.bucket(GCS_BUCKET_NAME)
        logging.info(f"GCS client initialized for bucket '{GCS_BUCKET_NAME}'.")

        # Initialize Vertex AI Client using ADC
        # Use google.generativeai library configured for Vertex
        logging.info(f"Initializing Vertex AI Gemini client (Project: {GCP_PROJECT_ID}, Location: {GCP_REGION})...")
        # No explicit client object needed like 'vertexai.init()', library handles it via ADC
        # We just instantiate the model later.
        vertex_model = genai.GenerativeModel(f'projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/endpoints/{VERTEX_MODEL_NAME}') # Use specific endpoint format if needed, else just model name might work
        # Or more simply if library handles resolution:
        # vertex_model = genai.GenerativeModel(VERTEX_MODEL_NAME)

        logging.info(f"Vertex AI Gemini client configured for model {VERTEX_MODEL_NAME}.")

        # Check if output TXT already exists
        if check_gcs_blob_exists(bucket, gcs_output_path):
            logging.info(f"Assessment TXT already exists: gs://{GCS_BUCKET_NAME}/{gcs_output_path}. Skipping.")
            status = "skipped_exists"
            assessment_txt_gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_output_path}"
        else:
            # Generate Assessment
            # Use a lookback period (e.g., 14 or 30 days from filing date)
            lookback_days = 14 # Or make configurable via env var
            prompt = risk_assessment_prompt_template.format(
                company_name=INPUT_COMPANY_NAME,
                ticker=INPUT_TICKER,
                lookback_days=lookback_days
            )
            assessment_text = generate_vertex_content_with_search(vertex_model, prompt)
            status = "assessment_generated"

            # Save Assessment TXT locally
            local_temp_path = os.path.join(temp_dir, assessment_filename)
            with open(local_temp_path, 'w', encoding='utf-8') as f_txt:
                f_txt.write(assessment_text)
            logging.info(f"Assessment text saved locally to {local_temp_path}")

            # Upload Assessment TXT to GCS
            upload_to_gcs(bucket, local_temp_path, gcs_output_path, content_type='text/plain')
            assessment_txt_gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_output_path}"
            status = "assessment_uploaded"

        # --- Output for Workflow ---
        output_data = {
            "job_name": "generate-headline-assessment",
            "ticker": INPUT_TICKER,
            "filing_date": INPUT_FILING_DATE,
            "status": status,
            "output_assessment_txt_gcs_path": assessment_txt_gcs_uri
        }
        print(json.dumps(output_data)) # Log output as JSON

    except google.auth.exceptions.DefaultCredentialsError as cred_err:
        logging.critical(f"GCP Credentials Error: {cred_err}", exc_info=True)
        status = "error_auth"
        print(json.dumps({ "job_name": "generate-headline-assessment", "status": status, "error": str(cred_err) }))
        raise
    except Exception as e:
        logging.error(f"An error occurred during the headline-assessment job for {log_prefix}: {e}", exc_info=True)
        status = "error_runtime"
        print(json.dumps({ "job_name": "generate-headline-assessment", "status": status, "error": str(e) }))
        raise
    finally:
        # Cleanup local files/directory
        logging.debug(f"Cleaning up temporary directory: {temp_dir}")
        if local_temp_path and os.path.exists(local_temp_path):
             try: os.remove(local_temp_path)
             except OSError as rm_err: logging.warning(f"Could not remove temp file {local_temp_path}: {rm_err}")
        if os.path.exists(temp_dir):
             try: os.rmdir(temp_dir)
             except OSError as rmdir_err: logging.warning(f"Could not remove temp dir {temp_dir}: {rmdir_err}")

        end_time = time.time()
        logging.info(f"--- Headline Assessment Job for {log_prefix} finished in {end_time - start_time:.2f} seconds. Final Status: {status} ---")

if __name__ == "__main__":
    main()
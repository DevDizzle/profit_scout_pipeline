# profit_scout_pipeline/services/news_summarizer/src/news_summarizer.py

import os
import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from google import auth as google_auth
# Ensure google.generativeai is installed
try:
    import google.generativeai as genai
    # Check if Tools are available (may depend on version)
    from google.generativeai.types import GenerateContentResponse, GenerationConfig, Tool, FunctionLibrary, Part
    # Updated way to specify GoogleSearch tool
    from google.ai.generativelanguage import Tool as LmTool, GoogleSearchRetrieval
    genai_installed = True
except ImportError:
    genai_installed = False
    logging.warning("google-generativeai library not found or outdated. News summarization will be skipped.")

import json
import logging
from datetime import datetime, timedelta, date, timezone
import time
import re
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential
from google.api_core import exceptions
import asyncio
import tempfile

# --- Configuration (using snake_case names) ---
gcp_project_id = os.getenv('gcp_project_id')
gcp_region = os.getenv('gcp_region', 'us-central1') # Use TF name 'gcp_region'
bq_dataset_id = os.getenv('bq_dataset_id', 'profit_scout')
bq_metadata_table_id = os.getenv('bq_metadata_table_id')
gcs_bucket_name = os.getenv('gcs_bucket_name')
# Align defaults with TF/env
gcs_news_summary_prefix = os.getenv('gcs_news_summary_prefix', 'headline-analysis/')
gcs_pdf_folder = os.getenv('gcs_pdf_folder', 'sec-pdf/') # Use TF name 'gcs_pdf_folder'
gemini_model_name = os.getenv('gemini_model_name', 'gemini-2.0-flash-latest') # Align default with env

# --- Constants ---
LOOKBACK_DAYS = 30

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s", # Use filename
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Suppress overly verbose logs from underlying libraries
for logger_name in ["google.api_core", "google.auth", "google.cloud", "urllib3", "requests"]:
     logging.getLogger(logger_name).setLevel(logging.WARNING)

# --- Validate Essential Configuration (using snake_case names) ---
essential_vars = {
    'gcp_project_id': gcp_project_id,
    'gcp_region': gcp_region, # Added region as it's used for Vertex AI client
    'bq_dataset_id': bq_dataset_id,
    'bq_metadata_table_id': bq_metadata_table_id,
    'gcs_bucket_name': gcs_bucket_name,
}
# Conditionally add model name check if library is installed
if genai_installed:
    essential_vars['gemini_model_name'] = gemini_model_name

missing_vars = [k for k, v in essential_vars.items() if not v]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}. Exiting.")
    exit(1)

# Construct full table IDs / prefixes (using snake_case variables)
metadata_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_metadata_table_id}"
gcs_news_summary_prefix = gcs_news_summary_prefix if gcs_news_summary_prefix.endswith('/') else gcs_news_summary_prefix + '/'
gcs_pdf_folder = gcs_pdf_folder if gcs_pdf_folder.endswith('/') else gcs_pdf_folder + '/'

# --- Global Clients ---
bq_client = None
storage_client = None
genai_client = None # Gemini client instance

# --- Helper Functions ---

def initialize_clients():
    """Initializes global clients needed for the function."""
    global bq_client, storage_client, genai_client
    if bq_client and storage_client and (genai_client or not genai_installed):
         return True # Already initialized or genai not needed/installed

    all_initialized = True
    try:
        if not bq_client:
            bq_client = bigquery.Client(project=gcp_project_id)
            logging.info("BigQuery client initialized.")
        if not storage_client:
            storage_client = storage.Client(project=gcp_project_id)
            logging.info("GCS client initialized.")
        if not genai_client and genai_installed:
             # Configure the Gemini client (Vertex AI SDK initialization)
            genai.init(project=gcp_project_id, location=gcp_region)
            genai_client = genai # Store the configured module itself or a specific client if available
            logging.info(f"Vertex AI Gemini SDK initialized for project {gcp_project_id} in {gcp_region}.")

    except Exception as e:
        logging.error(f"Failed to initialize clients: {e}", exc_info=True)
        # Reset any partially initialized clients
        bq_client = None; storage_client = None; genai_client = None
        all_initialized = False

    # Final check
    if bq_client and storage_client and (genai_client or not genai_installed):
        return True
    else:
        logging.error("Client initialization incomplete.")
        return False


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def get_filing_date_from_metadata(accession_number_cleaned: str) -> date | None:
    """Queries BigQuery metadata table for the filing date."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    logging.info(f"Querying filing date for cleaned AN: {accession_number_cleaned} from {metadata_table_full_id}")
    query = f"""
    SELECT FiledDate
    FROM `{metadata_table_full_id}`
    WHERE AccessionNumber = @accession_number
    ORDER BY FiledDate DESC -- Get latest if duplicates somehow exist
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("accession_number", "STRING", accession_number_cleaned),
        ]
    )
    try:
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result(timeout=60))
        if results:
            filed_date = results[0].FiledDate
            if isinstance(filed_date, date):
                logging.info(f"Found FiledDate: {filed_date} for {accession_number_cleaned}")
                return filed_date
            else:
                # Attempt conversion if it's a string/datetime
                try:
                    return pd.to_datetime(filed_date).date()
                except Exception:
                    logging.error(f"Could not convert found FiledDate ({type(filed_date)}: {filed_date}) to date object for {accession_number_cleaned}")
                    return None
        else:
            logging.warning(f"FiledDate not found in metadata for AccessionNumber: {accession_number_cleaned}")
            return None
    except Exception as e:
        logging.error(f"Error querying FiledDate for {accession_number_cleaned}: {e}", exc_info=True)
        raise

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=5, max=60), reraise=True)
def generate_news_summary(company_name: str, ticker: str, lookback_days: int) -> str:
    """Generates news summary using Vertex AI Gemini with Google Search."""
    if not genai_installed: return "google-generativeai library not installed."
    if not genai_client: raise RuntimeError("Vertex AI Gemini client not initialized.")

    prompt = f"""
    Analyze recent news and events for {company_name} ({ticker}) using Google Search results from the past {lookback_days} days. Focus on information relevant to potential stock movement or company health. Be concise and data-focused. Output *only* the requested sections using the specified format.

    **1. Key News/Events (Max 3):**
          * [Bullet point summarizing a key news item, announcement, or market event]
          * [...]
          * [...]

    **2. Overall Sentiment from News (Choose one: Positive, Negative, Neutral):**
          [Sentiment Category based on search results]

    **3. Potential Near-Term Impact (1 sentence):**
          [Summarize the likely near-term impact or key takeaway from recent news]

    **4. Key Catalysts Mentioned (Max 2 points):**
          * [Potential factor 1 based on search results/news]
          * [...]

    **5. Key Risks/Concerns Mentioned (Max 2 points):**
          * [Potential risk 1 based on search results/news]
          * [...]
    """
    try:
        logging.info(f"Generating news assessment for {ticker} (Lookback: {lookback_days} days) using model {gemini_model_name}...")
        # Initialize the generative model using the configured name
        model = genai.GenerativeModel(gemini_model_name) # Use genai.init() configured client

        # Define the Google Search tool using the specific type expected by the SDK
        search_tool = LmTool(
            google_search_retrieval=GoogleSearchRetrieval(disable_attribution=False) # Keep attribution by default
        )

        # Generate content configuration
        config = GenerationConfig(
            temperature=0.1,
            # Add other config as needed: max_output_tokens, top_p, top_k
        )

        # Call generate_content with the prompt and tools
        response = model.generate_content(
            prompt,
            generation_config=config,
            tools=[search_tool]
        )

        logging.info(f"Successfully received Gemini response for {ticker}.")
        if response and hasattr(response, 'text') and response.text:
            return response.text
        else:
            logging.warning(f"Received empty or non-text response from Vertex AI for {ticker}. Response: {response}")
            # Check for specific finish reasons if available
            try:
                finish_reason = response.candidates[0].finish_reason if response.candidates else "UNKNOWN"
                logging.warning(f"Gemini Finish Reason for {ticker}: {finish_reason}")
                if finish_reason == 'SAFETY':
                     return "Content generation blocked due to safety settings."
            except Exception: pass # Ignore errors inspecting response details
            return "Received empty/invalid response from AI model"

    except exceptions.GoogleAPICallError as api_error:
        logging.error(f"Vertex AI API error generating content for {ticker}: {api_error}")
        raise # Reraise for retry mechanism
    except Exception as e:
        logging.error(f"Error generating content for {ticker}: {e}", exc_info=True)
        # Don't return the raw exception string to GCS, provide a generic error
        return f"Error during generation. Check logs for details."


def save_text_to_gcs_sync(bucket, gcs_path: str, text_content: str):
    """Synchronously saves text content to GCS, ensuring temp file cleanup."""
    temp_txt_path = None
    try:
        # Use tempfile for secure temporary file creation
        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', suffix=".txt") as temp_txt:
            temp_txt_path = temp_txt.name
            temp_txt.write(text_content)

        logging.info(f"Uploading text news summary to GCS: gs://{bucket.name}/{gcs_path}")
        txt_blob = bucket.blob(gcs_path)
        # Set metadata
        output_metadata = {
            'processingTimestampUTC': datetime.now(timezone.utc).isoformat(),
            'contentType': 'text/plain' # Explicitly set content type
        }
        txt_blob.metadata = output_metadata
        # Upload from the closed temporary file path
        txt_blob.upload_from_filename(temp_txt_path, content_type='text/plain')
        logging.info(f"Successfully uploaded text to gs://{bucket.name}/{gcs_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload text to gs://{bucket.name}/{gcs_path}: {e}", exc_info=True)
        return False
    finally:
        # Ensure temporary file is always cleaned up
        if temp_txt_path and os.path.exists(temp_txt_path):
            try: os.remove(temp_txt_path)
            except Exception as e_clean: logging.warning(f"Error removing temp TXT {temp_txt_path}: {e_clean}")

def check_gcs_blob_exists(bucket, blob_name):
    """Checks if a specific blob exists in the GCS bucket."""
    try:
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Error checking GCS existence for {blob_name}: {e}")
        return False # Assume doesn't exist or error occurred

# --- Main Event Handler Logic ---

async def process_news_summary_event(gcs_pdf_path: str, ticker: str, accession_number_cleaned: str):
    """Processes a GCS event related to a PDF to generate a news summary."""
    global storage_client, bq_client, genai_client # Allow access to global clients

    # Ensure clients are initialized (idempotent)
    if not initialize_clients():
        raise RuntimeError("Clients not initialized before processing.")

    logging.info(f"--- Processing News Summary for {ticker} ({accession_number_cleaned}) triggered by PDF: {gcs_pdf_path} ---")

    # 1. Construct Target Output Path
    base_filename = f"{ticker}_{accession_number_cleaned}"
    sanitized_base = re.sub(r'[^\w\-]+', '_', base_filename)
    # Align output filename convention if needed (e.g., always .txt)
    output_filename = f"{sanitized_base}.txt"
    destination_blob_name = f"{gcs_news_summary_prefix}{output_filename}"

    # 2. Check if output already exists (Idempotency)
    gcs_bucket = storage_client.bucket(gcs_bucket_name)
    loop = asyncio.get_running_loop()
    output_exists = await loop.run_in_executor(None, check_gcs_blob_exists, gcs_bucket, destination_blob_name)

    if output_exists:
        logging.info(f"Skipping {ticker} ({accession_number_cleaned}): Output {destination_blob_name} already exists.")
        return # Success, already processed

    # 3. Get Filing Date from Metadata Table
    filed_date = await loop.run_in_executor(None, get_filing_date_from_metadata, accession_number_cleaned)
    if not filed_date:
        # Changed to warning - maybe news summary is still valuable without exact date?
        logging.warning(f"[{ticker}] Could not retrieve FiledDate for {accession_number_cleaned}. Proceeding without it.")
        # Decide if you want to return or proceed without the date context
        # return # Uncomment to stop if date is mandatory

    # 4. Generate News Summary/Assessment
    company_name = ticker # Use ticker as company name for simplicity, enhance if needed
    lookback = LOOKBACK_DAYS # Use constant
    summary_text = await loop.run_in_executor(None, generate_news_summary, company_name, ticker, lookback)

    # Check for errors indicated by the returned text
    if summary_text.startswith("Error") or summary_text == "Received empty/invalid response from AI model" or summary_text == "Content generation blocked due to safety settings.":
        logging.error(f"[{ticker}] Failed to generate news summary for {accession_number_cleaned}: {summary_text}")
        # Decide how to handle: raise error to signal failure, or log and exit?
        raise RuntimeError(f"News summary generation failed: {summary_text}")

    # 5. Save Summary to GCS
    save_success = await loop.run_in_executor(None, save_text_to_gcs_sync, gcs_bucket, destination_blob_name, summary_text)

    if not save_success:
        logging.error(f"[{ticker}] Failed to save news summary to GCS for {accession_number_cleaned}")
        raise IOError(f"Failed to save news summary to GCS for {accession_number_cleaned}")

    logging.info(f"--- Successfully finished News Summary for {ticker} ({accession_number_cleaned}) ---")

# --- Entry Point (Example for GCS Trigger) ---
async def handle_gcs_event(event_data: dict):
    """
    Entry point for handling GCS notification events.
    Parses event data and triggers the news summary processing.
    """
    # Initialize clients early
    if not initialize_clients():
        logging.critical("Failed to initialize necessary clients. Aborting function.")
        # Depending on the trigger context, decide how to signal fatal error
        # For Cloud Functions/Run, raising an exception might be appropriate
        raise RuntimeError("Client initialization failed")

    try:
        # Parse GCS event data
        bucket_name = event_data.get('bucket')
        gcs_path = event_data.get('name')
        logging.info(f"News Summarizer received GCS event for: gs://{bucket_name}/{gcs_path}")

        # Basic validation of the event
        if not gcs_path or not bucket_name:
             logging.warning("Event missing 'bucket' or 'name' field. Ignoring.")
             return # Not a valid GCS event we can process
        if bucket_name != gcs_bucket_name:
             logging.warning(f"Event for wrong bucket ({bucket_name} vs {gcs_bucket_name}). Ignoring.")
             return
        # Ensure it's triggered by the PDF folder specified, not its own output or other folders
        if not gcs_path.startswith(gcs_pdf_folder) or not gcs_path.lower().endswith('.pdf'):
            logging.info(f"Ignoring event for non-PDF file or wrong prefix (Expected: {gcs_pdf_folder}): {gcs_path}")
            return

        # Parse Ticker and Accession Number from the filename
        ticker = None
        accession_number_cleaned = None
        try:
            # Extract filename from the full path
            filename = os.path.basename(gcs_path)
            # Remove .pdf extension and split by the last underscore
            base_name = filename.rsplit('.', 1)[0]
            parts = base_name.split('_') # Simple split assumes TICKER_ACCESSION format
            if len(parts) >= 2: # Need at least two parts
                ticker = parts[0]
                accession_number_original = parts[1] # Assume second part is AN
                accession_number_cleaned = accession_number_original.replace('-', '') # Clean it
                logging.info(f"Parsed from filename: Ticker={ticker}, Cleaned AN={accession_number_cleaned}")
            else:
                 raise ValueError("Filename pattern 'TICKER_ACCESSIONNUMBER.pdf' not matched")
        except Exception as parse_err:
            logging.error(f"Failed to parse Ticker/AN from filename '{gcs_path}': {parse_err}. Cannot process.")
            # Acknowledge the event but don't proceed
            return # Or raise an error if parsing failure should cause retry?

        # Trigger the core processing logic
        await process_news_summary_event(gcs_path, ticker, accession_number_cleaned)

        logging.info(f"Successfully processed news summary event for {gcs_path}")

    except Exception as e:
        # Catch all exceptions from processing to prevent silent failures
        logging.error(f"Error processing news summary GCS event for {event_data.get('name', 'N/A')}: {e}", exc_info=True)
        # Re-raise the exception to signal failure to the trigger (e.g., Cloud Functions/Run)
        raise

# Example local execution (for testing purposes)
# async def local_test():
#     test_event = {
#         "bucket": "your-gcs-bucket-name", # Replace with your bucket name
#         "name": "sec-pdf/AAPL_000032019323000106.pdf" # Example PDF path
#     }
#     # Ensure required Env Vars are set for local run
#     os.environ['gcp_project_id'] = 'your-project-id'
#     os.environ['gcp_region'] = 'us-central1'
#     os.environ['bq_dataset_id'] = 'profit_scout'
#     os.environ['bq_metadata_table_id'] = 'filing_metadata'
#     os.environ['gcs_bucket_name'] = test_event['bucket']
#     os.environ['gcs_pdf_folder'] = 'sec-pdf/'
#     os.environ['gcs_news_summary_prefix'] = 'headline-analysis/'
#     os.environ['gemini_model_name'] = 'gemini-1.5-flash-001' # Or your preferred model

#     try:
#         await handle_gcs_event(test_event)
#     except Exception as e:
#         print(f"Local test failed: {e}")

# if __name__ == "__main__":
#     # Make sure ADC is configured for local runs (gcloud auth application-default login)
#     asyncio.run(local_test())
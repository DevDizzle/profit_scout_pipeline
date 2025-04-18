# services/pdf_summarizer/src/pdf_summarizer.py

import os
# import pandas as pd # No longer needed
import numpy as np # Keep for checking np.isnan/inf if used in helpers
from google.cloud import storage, secretmanager
# from google.cloud.exceptions import NotFound # Not directly used here
import google.generativeai as genai
import json
import logging
# from tqdm import tqdm # Removed unused import
from datetime import datetime, timedelta, date, timezone # Added timezone
import time
import re
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential
from google.api_core import exceptions
# from decimal import Decimal # Removed unused import
import concurrent.futures
import threading
import tempfile
import asyncio


# --- Configuration ---
# Environment Variable Names
ENV_PROJECT_ID = 'GCP_PROJECT_ID'
ENV_GCS_BUCKET_NAME = 'GCS_BUCKET_NAME'
ENV_GEMINI_KEY_SECRET_ID = 'GEMINI_API_KEY_SECRET_ID'
ENV_GEMINI_KEY_SECRET_VERSION = 'GEMINI_API_KEY_SECRET_VERSION'
ENV_GEMINI_MODEL_NAME = 'GEMINI_MODEL_NAME'
ENV_GCS_ANALYSIS_TXT_PREFIX = 'GCS_ANALYSIS_TXT_PREFIX'
ENV_GCS_PDF_PREFIX = 'GCS_PDF_PREFIX' # Added: Prefix where source PDFs are stored

# Load Configuration from Environment Variables
PROJECT_ID = os.getenv(ENV_PROJECT_ID)
GCS_BUCKET_NAME = os.getenv(ENV_GCS_BUCKET_NAME)
GEMINI_SECRET_ID = os.getenv(ENV_GEMINI_KEY_SECRET_ID)
GEMINI_SECRET_VERSION = os.getenv(ENV_GEMINI_KEY_SECRET_VERSION, 'latest')
GEMINI_MODEL_NAME = os.getenv(ENV_GEMINI_MODEL_NAME, 'gemini-2.0-flash-001')
GCS_ANALYSIS_TXT_PREFIX = os.getenv(ENV_GCS_ANALYSIS_TXT_PREFIX, 'SECAnalysis_R1000/')
GCS_PDF_PREFIX = os.getenv(ENV_GCS_PDF_PREFIX, 'SEC_Filings_Russell1000/') # Match subscriber's output folder

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] [%(module)s:%(lineno)d] - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Reduce verbosity of Google libraries
logging.getLogger("google.api_core").setLevel(logging.WARNING)
logging.getLogger("google.auth").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Validate Essential Configuration ---
essential_vars = {
    ENV_PROJECT_ID: PROJECT_ID,
    ENV_GCS_BUCKET_NAME: GCS_BUCKET_NAME,
    ENV_GEMINI_KEY_SECRET_ID: GEMINI_SECRET_ID,
    ENV_GEMINI_MODEL_NAME: GEMINI_MODEL_NAME # Model name is essential now
}
missing_vars = [k for k, v in essential_vars.items() if not v]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}. Exiting.")
    exit(1) # Exit if essential config is missing

# Ensure prefixes end with /
GCS_ANALYSIS_TXT_PREFIX = GCS_ANALYSIS_TXT_PREFIX if GCS_ANALYSIS_TXT_PREFIX.endswith('/') else GCS_ANALYSIS_TXT_PREFIX + '/'
GCS_PDF_PREFIX = GCS_PDF_PREFIX if GCS_PDF_PREFIX.endswith('/') else GCS_PDF_PREFIX + '/'


# --- Global Clients & Key (Initialize Safely in handler) ---
storage_client = None
gemini_model = None
gemini_api_key = None # Store fetched key globally within invocation lifespan


# --- Helper Functions ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    logging.info(f"Successfully accessed secret: {secret_id}")
    return payload

def initialize_clients():
    """Initializes GCP and Gemini clients safely. Returns True if successful."""
    global storage_client, gemini_model, gemini_api_key
    # Don't re-initialize if already done within this invocation
    if storage_client and gemini_model: return True

    try:
        if not storage_client:
            storage_client = storage.Client(project=PROJECT_ID)
            logging.info("GCS client initialized using ADC.")

        if not gemini_api_key:
            gemini_api_key = access_secret_version(PROJECT_ID, GEMINI_SECRET_ID, GEMINI_SECRET_VERSION)

        if gemini_api_key and not gemini_model:
            genai.configure(api_key=gemini_api_key)
            gemini_model = genai.GenerativeModel(
                GEMINI_MODEL_NAME,
                generation_config={"temperature": 0.2, "max_output_tokens": 8192} # Match earlier config
            )
            logging.info(f"Gemini client initialized with model {GEMINI_MODEL_NAME}.")

        # Final check
        if storage_client and gemini_model:
             return True
        else:
             # Log specific missing client if possible
             missing = []
             if not storage_client: missing.append("Storage Client")
             if not gemini_model: missing.append("Gemini Model")
             logging.error(f"Client initialization incomplete. Missing: {', '.join(missing)}")
             return False

    except Exception as e:
        logging.error(f"Failed to initialize clients: {e}", exc_info=True)
        storage_client = None; gemini_model = None; gemini_api_key = None # Reset on error
        return False

# --- Analysis Prompt ---
qualitative_analysis_prompt = """
You are an expert financial analyst reviewing an SEC filing (10-K or 10-Q). Your task is to extract key financial performance indicators AND synthesize them into a qualitative assessment based *strictly* on the information presented within this document.

Follow these steps:

1.  **Extract Key Financial Data & Trends**: Identify primary figures (Revenue, Operating Income, Net Income, EPS, Operating Cash Flow, Key Debt metric, RMR/ARR if applicable). For each, state the value for the current period and the prior year period. Calculate and state the Year-over-Year percentage change. If possible, state if this YoY change represents an acceleration or deceleration compared to the previous year's YoY change.
2.  **Identify Key Qualitative Signals from the Filing**: Based *only* on the data and discussion *in the filing*, identify the **Top 2-3 Qualitative Strengths**. Link to specific data/metrics from Step 1 where applicable. Identify the **Top 2-3 Qualitative Weaknesses/Concerns**. Link to specific data/metrics from Step 1 where applicable.
3.  **Synthesize Overall Qualitative Assessment**: Provide a brief summary paragraph synthesizing the key changes, strengths, and weaknesses identified above. Conclude this paragraph by stating whether the overall qualitative picture *presented solely within this document* appears predominantly **Positive**, **Negative**, or **Mixed**. Also assess the overall management tone conveyed in the filing (e.g., Confident, Optimistic, Cautious, Neutral, Defensive) based only on the language used.
4.  **Summarize Investment Implications (from Filing)**: Briefly list the key factors (derived from the points above) from *this specific filing* most relevant for investment. Focus on *changes* and *outlook*.
5.  **Extract Key Guidance & Outlook (from Filing)**: Summarize any **explicit financial guidance** (e.g., revenue/EPS ranges for next Q/Year) mentioned *within this document*. State "None provided" if applicable. Summarize management's qualitative outlook regarding future business prospects or key strategic initiatives *mentioned within this document*. State "None provided" if applicable.

**Output Format**:
Your output should be clearly structured using bullet points for steps 1, 2, 4, and 5, and a paragraph for step 3. Ensure the analysis remains objective and grounded *exclusively* in the provided SEC filing text. Avoid external information or real-time market data. Ensure the full analysis is included without truncation. Be concise in each bullet point.
"""

# --- Async Helper Functions ---

async def upload_to_gemini_async(local_path, display_name):
    """Uploads local file to Gemini File API, waits until ACTIVE."""
    if not gemini_model: raise RuntimeError("Gemini client not initialized.")
    logging.info(f"Uploading {display_name} to Gemini...")
    loop = asyncio.get_running_loop()
    uploaded_file = None
    try:
        # Initial upload call
        uploaded_file = await loop.run_in_executor(None, genai.upload_file, local_path, display_name)
        logging.info(f"Initial upload request sent for {display_name} ({uploaded_file.name}). Waiting for ACTIVE state...")

        # Polling loop
        attempts = 0
        max_attempts = 30 # Wait up to 60 seconds (30 * 2s sleep)
        while uploaded_file.state.name != "ACTIVE" and attempts < max_attempts:
            logging.debug(f"File {uploaded_file.name} state: {uploaded_file.state.name}. Checking again in 2 seconds...")
            await asyncio.sleep(2)
            uploaded_file = await loop.run_in_executor(None, genai.get_file, uploaded_file.name)
            attempts += 1
            if uploaded_file.state.name == "FAILED":
                raise Exception(f"Gemini file processing failed: {uploaded_file.name}. State: {uploaded_file.state}")

        if uploaded_file.state.name != "ACTIVE":
             raise TimeoutError(f"Gemini file {uploaded_file.name} did not become ACTIVE after {attempts * 2} seconds.")

        logging.info(f"File {uploaded_file.name} is ACTIVE.")
        return uploaded_file
    except Exception as e:
        # Attempt cleanup if upload started but failed processing
        if uploaded_file and uploaded_file.name:
             logging.warning(f"Attempting to delete potentially failed Gemini upload: {uploaded_file.name}")
             await delete_gemini_file_async(uploaded_file.name) # Fire and forget cleanup
        logging.error(f"Error during Gemini upload/processing for {display_name}: {e}", exc_info=True)
        raise

async def generate_content_async(model, prompt_list):
    """Generates content using Gemini, handling potential errors."""
    if not model: raise RuntimeError("Gemini model not initialized.")
    logging.info(f"Generating content from Gemini model {model.model_name}...")
    loop = asyncio.get_running_loop()
    response_text = "Content generation failed." # Default error text
    try:
        # Increased timeout for potentially long analysis
        response = await loop.run_in_executor(None, model.generate_content, prompt_list, request_options={"timeout": 300}) # 5 min timeout

        if hasattr(response, 'text') and response.text:
            response_text = response.text
            logging.info("Content generation successful.")
        elif response.prompt_feedback and response.prompt_feedback.block_reason:
            reason = response.prompt_feedback.block_reason
            logging.error(f"Content generation blocked. Reason: {reason}")
            response_text = f"Content generation blocked: {reason}" # Specific block reason
        else: # No text and no block reason? Log details.
            logging.warning(f"Gemini returned no text content. Parts: {getattr(response, 'parts', 'N/A')}. Candidates: {getattr(response, 'candidates', 'N/A')}")
            response_text = "Content generation returned empty."
    except Exception as e:
        logging.error(f"Error during Gemini content generation: {e}", exc_info=True)
        response_text = f"Content generation failed: {e}" # Include error in output for context
    return response_text

async def delete_gemini_file_async(file_name):
    """Requests deletion of a file from Gemini File API."""
    if not file_name: logging.debug("No Gemini file name provided to delete."); return
    # No need to check gemini_model, delete_file is a static method on genai
    logging.info(f"Requesting deletion of Gemini file: {file_name}")
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, genai.delete_file, file_name)
        logging.info(f"Successfully requested deletion of Gemini file: {file_name}")
    except Exception as e:
        # Log warning, but don't fail the whole process if cleanup fails
        logging.warning(f"Failed to delete Gemini file {file_name}: {e}")

async def save_text_to_gcs(bucket, gcs_path: str, text_content: str, metadata: dict):
    """Saves text content to GCS, creating a temp file first."""
    loop = asyncio.get_running_loop()
    temp_txt_path = None
    try:
        # Write to temp file first
        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', suffix=".txt") as temp_txt:
            temp_txt_path = temp_txt.name
            temp_txt.write(text_content)

        logging.info(f"Uploading text analysis to GCS: gs://{bucket.name}/{gcs_path}")
        txt_blob = bucket.blob(gcs_path)
        # Set metadata on the output blob
        txt_blob.metadata = metadata
        # Upload from temp file
        await loop.run_in_executor(None, txt_blob.upload_from_filename, temp_txt_path, content_type='text/plain')
        logging.info(f"Successfully uploaded text to gs://{bucket.name}/{gcs_path}")
        return True # Indicate success
    except Exception as e:
        logging.error(f"Failed to upload text to gs://{bucket.name}/{gcs_path}: {e}", exc_info=True)
        return False # Indicate failure
    finally:
        # Cleanup temp file
        if temp_txt_path and os.path.exists(temp_txt_path):
            try: await loop.run_in_executor(None, os.remove, temp_txt_path); logging.debug(f"Removed temporary TXT: {temp_txt_path}")
            except Exception as e: logging.warning(f"Error removing temp TXT {temp_txt_path}: {e}")


# --- Main Processing Function ---

async def generate_sec_analysis(gcs_blob_path: str, ticker: str, accession_no: str):
    """
    Downloads PDF from GCS, generates Qualitative Analysis via Gemini,
    and saves the result to a GCS text file. Relies on Ticker/Accession from filename.
    """
    global storage_client, gemini_model # Use globally initialized clients for this invocation

    if not storage_client or not gemini_model:
        logging.error("Clients not initialized. Cannot process SEC analysis.")
        # Optionally raise an exception to signal failure for retry
        raise RuntimeError("Clients not initialized")

    logging.info(f"--- Starting SEC Analysis for Ticker: {ticker}, Filing: {accession_no} ---")
    logging.info(f"Processing PDF from GCS path: gs://{GCS_BUCKET_NAME}/{gcs_blob_path}")

    local_pdf_path = None
    uploaded_gemini_file = None
    uploaded_file_name = None
    analysis_success = False
    gcs_analysis_path = None # Define outside try for logging
    loop = asyncio.get_running_loop()

    try:
        # 1. Download PDF from GCS
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_blob_path)
        # Check existence just in case trigger fired on a deleted object?
        blob_exists = await loop.run_in_executor(None, blob.exists)
        if not blob_exists: raise FileNotFoundError(f"PDF blob not found at trigger time: {gcs_blob_path}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
            local_pdf_path = temp_pdf.name
            logging.info(f"Downloading PDF to temporary file: {local_pdf_path}")
            await loop.run_in_executor(None, blob.download_to_filename, local_pdf_path)

        # 2. Upload PDF to Gemini
        # Use only ticker and accession number for display name
        display_name = f"{ticker}_{accession_no}_SEC_Filing.pdf"
        uploaded_gemini_file = await upload_to_gemini_async(local_pdf_path, display_name)
        uploaded_file_name = uploaded_gemini_file.name # Store name for cleanup

        # 3. Generate Qualitative Analysis
        logging.info(f"Generating qualitative analysis for {ticker} ({accession_no})...")
        qualitative_analysis_text = await generate_content_async(
            gemini_model, [qualitative_analysis_prompt, uploaded_gemini_file]
        )

        # 4. Prepare Metadata & Filename for Output Text File
        output_metadata = {
            'ticker': ticker,
            'accessionNumber': accession_no,
            'originalPDFPath': f"gs://{GCS_BUCKET_NAME}/{gcs_blob_path}",
            'processingTimestampUTC': datetime.now(timezone.utc).isoformat()
        }
        # Use only ticker and accession number for output filename base
        base_filename = f"{ticker}_{accession_no}"
        base_filename = re.sub(r'[^\w\-.]+', '_', base_filename) # Sanitize
        gcs_analysis_path = f"{GCS_ANALYSIS_TXT_PREFIX}{base_filename}.txt" # Changed to remove QualitativeAnalysis_

        # 5. Save Qualitative Analysis Text to GCS
        analysis_success = await save_text_to_gcs(
            bucket, gcs_analysis_path, qualitative_analysis_text, output_metadata
        )
        if not analysis_success:
             # If saving failed, maybe raise exception to signal failure for retry?
             raise IOError(f"Failed to save analysis text to GCS for {accession_no}")


    except Exception as e:
        logging.error(f"Error during SEC analysis processing for {ticker} ({accession_no}): {e}", exc_info=True)
        analysis_success = False
        # Re-raise the exception so the trigger mechanism (Eventarc/Cloud Func) knows it failed
        raise
    finally:
        # 6. Cleanup
        if uploaded_file_name:
            await delete_gemini_file_async(uploaded_file_name)
        if local_pdf_path and os.path.exists(local_pdf_path):
            try:
                # Use run_in_executor for os.remove as it can block on NFS etc.
                await loop.run_in_executor(None, os.remove, local_pdf_path)
                logging.info(f"Removed temporary PDF: {local_pdf_path}")
            except Exception as e:
                logging.warning(f"Error removing temp PDF {local_pdf_path}: {e}")

        logging.info(f"--- Finished SEC Analysis for Ticker: {ticker}, Filing: {accession_no} ---")
        if gcs_analysis_path: # Log output path only if it was determined
             logging.info(f"Qualitative Analysis Saved: {analysis_success} to gs://{GCS_BUCKET_NAME}/{gcs_analysis_path}")
        else:
             logging.info(f"Qualitative Analysis Save Status: {analysis_success}")


# --- Entry Point (Example for Cloud Function/Eventarc targeting Cloud Run) ---
# This function acts as the trigger handler
async def handle_gcs_event(event_data: dict):
    """
    Trigger handler function (e.g., called by Flask route in main.py).
    Parses event data, extracts filename, parses IDs, calls analysis function.
    """
    if not initialize_clients():
        logging.critical("Failed to initialize necessary clients. Aborting function.")
        # Raise exception to signal failure to the trigger (e.g., return 500 from Flask)
        raise RuntimeError("Client initialization failed")

    try:
        bucket_name = event_data.get('bucket')
        gcs_path = event_data.get('name')
        logging.info(f"Received GCS event for: gs://{bucket_name}/{gcs_path}")

        # --- Basic Event Validation ---
        if not gcs_path or not bucket_name or bucket_name != GCS_BUCKET_NAME:
            logging.warning(f"Event missing data or for wrong bucket ({bucket_name} vs {GCS_BUCKET_NAME}). Ignoring.")
            return # Success (ignore event)

        # Check if file is in the expected PDF input path and is a PDF
        if not gcs_path.startswith(GCS_PDF_PREFIX) or not gcs_path.lower().endswith('.pdf'):
            logging.info(f"Ignoring event for non-matching path ({gcs_path} vs prefix {GCS_PDF_PREFIX}) or non-PDF file.")
            return # Success (ignore event)
        # --- End Validation ---

        # --- Parse Filename for Ticker and Accession Number ---
        ticker = None
        accession_no = None
        try:
            filename = os.path.basename(gcs_path)
            # Assumes format: TICKER_ACCESSIONNUMBER.pdf
            parts = filename.rsplit('.', 1)[0].split('_', 1)
            if len(parts) == 2:
                ticker = parts[0]
                accession_no = parts[1]
                logging.info(f"Parsed from filename: Ticker={ticker}, AccessionNo={accession_no}")
            else:
                raise ValueError(f"Filename '{filename}' did not match expected TICKER_ACCESSIONNUMBER.pdf format")
        except Exception as parse_err:
            logging.error(f"Failed to parse ticker/accession number from filename '{gcs_path}': {parse_err}. Cannot process.")
            # Depending on requirements, might dead-letter or just log and return success
            return # Ignore event if filename is wrong format

        if not ticker or not accession_no: # Should be caught by exception, but double check
            logging.error(f"Ticker or AccessionNumber could not be determined for {gcs_path}. Cannot process.")
            return # Ignore event
        # --- End Parsing ---

        # Call the main analysis function
        await generate_sec_analysis(
            gcs_blob_path=gcs_path,
            ticker=ticker,
            accession_no=accession_no
            # Removed filing_date and form_type arguments
        )

        # If generate_sec_analysis completes without error, we assume success
        logging.info(f"Successfully processed event for {gcs_path}")

    except Exception as e:
        # Log error from generate_sec_analysis or this handler
        logging.error(f"Error processing GCS event for {event_data.get('name', 'N/A')}: {e}", exc_info=True)
        # Re-raise the exception to signal failure to the trigger mechanism for potential retry
        raise
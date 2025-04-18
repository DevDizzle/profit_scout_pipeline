# services/pdf_summarizer/src/pdf_summarizer.py

import os
# import pandas as pd # No longer needed
import numpy as np # Keep for checking np.isnan/inf if used in helpers
from google.cloud import storage, secretmanager
# from google.cloud.exceptions import NotFound # Not directly used here
# Ensure google.generativeai is installed
try:
    import google.generativeai as genai
    genai_installed = True
except ImportError:
    genai_installed = False
    logging.warning("google-generativeai library not found. PDF summarization cannot use Gemini directly.")

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

# --- Configuration (using snake_case names) ---
gcp_project_id = os.getenv('gcp_project_id')
gcs_bucket_name = os.getenv('gcs_bucket_name')
gemini_api_secret_name = os.getenv('gemini_api_secret_name') # Use TF/env name
gemini_api_key_secret_version = os.getenv('gemini_api_key_secret_version', 'latest') # Align with .env example
gemini_model_name = os.getenv('gemini_model_name', 'gemini-2.0-flash-latest') # Align default with .env example
gcs_analysis_txt_prefix = os.getenv('gcs_analysis_txt_prefix', 'headline-analysis/') # Align default with TF/env
gcs_pdf_folder = os.getenv('gcs_pdf_folder', 'sec-pdf/') # Align default with TF/env

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] [%(filename)s:%(lineno)d] - %(message)s", # Use filename
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Reduce verbosity of Google libraries
for logger_name in ["google.api_core", "google.auth", "urllib3", "requests"]:
     logging.getLogger(logger_name).setLevel(logging.WARNING)

# --- Validate Essential Configuration (using snake_case names) ---
essential_vars = {
    'gcp_project_id': gcp_project_id,
    'gcs_bucket_name': gcs_bucket_name,
    'gemini_api_secret_name': gemini_api_secret_name,
}
# Conditionally add model name check if library is installed
if genai_installed:
    essential_vars['gemini_model_name'] = gemini_model_name

missing_vars = [k for k, v in essential_vars.items() if not v]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}. Exiting.")
    exit(1) # Exit if essential config is missing

# Ensure prefixes end with /
gcs_analysis_txt_prefix = gcs_analysis_txt_prefix if gcs_analysis_txt_prefix.endswith('/') else gcs_analysis_txt_prefix + '/'
gcs_pdf_folder = gcs_pdf_folder if gcs_pdf_folder.endswith('/') else gcs_pdf_folder + '/'

# --- Global Clients & Key (Initialize Safely in handler) ---
storage_client = None
gemini_model = None
gemini_api_key = None # Store fetched key globally within invocation lifespan

# --- Helper Functions ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    # Use secret_id which is the name (e.g., gemini_api_secret_name)
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        logging.info(f"Successfully accessed secret: {secret_id}")
        return payload
    except Exception as e:
        logging.error(f"Failed to access secret '{secret_id}/{version_id}' in project '{project_id}': {e}", exc_info=True)
        raise

def initialize_clients():
    """Initializes GCP and Gemini clients safely. Returns True if successful."""
    global storage_client, gemini_model, gemini_api_key

    # Check if already initialized (idempotent)
    clients_needed = [storage_client]
    if genai_installed: clients_needed.append(gemini_model)
    if all(clients_needed): return True

    all_initialized = True
    try:
        if not storage_client:
            storage_client = storage.Client(project=gcp_project_id)
            logging.info("GCS client initialized.")

        # Initialize Gemini only if library is present
        if genai_installed:
            if not gemini_api_key:
                # Use snake_case variable name for the secret name
                gemini_api_key = access_secret_version(
                    gcp_project_id,
                    gemini_api_secret_name,
                    gemini_api_key_secret_version
                )

            if gemini_api_key and not gemini_model:
                genai.configure(api_key=gemini_api_key)
                gemini_model = genai.GenerativeModel(
                    gemini_model_name, # Use snake_case variable
                    generation_config={"temperature": 0.2, "max_output_tokens": 8192}
                )
                logging.info(f"Gemini client initialized with model {gemini_model_name}.")

    except Exception as e:
        logging.error(f"Failed to initialize clients: {e}", exc_info=True)
        storage_client = None; gemini_model = None; gemini_api_key = None # Reset on error
        all_initialized = False

    # Final check
    if storage_client and (gemini_model or not genai_installed):
         return True
    else:
        missing = []
        if not storage_client: missing.append("Storage Client")
        if genai_installed and not gemini_model: missing.append("Gemini Model")
        logging.error(f"Client initialization incomplete. Missing: {', '.join(missing)}")
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
    if not genai_installed: return None # Cannot upload if library not installed
    if not gemini_model: raise RuntimeError("Gemini client not initialized.") # Model instance needed? No, genai.upload_file
    logging.info(f"Uploading {display_name} to Gemini...")
    loop = asyncio.get_running_loop()
    uploaded_file = None
    try:
        uploaded_file = await loop.run_in_executor(None, genai.upload_file, local_path, display_name=display_name)
        logging.info(f"Initial upload request sent for {display_name} ({uploaded_file.name}). Waiting for ACTIVE state...")

        attempts = 0
        max_attempts = 30 # Wait up to 60 seconds
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
        if uploaded_file and uploaded_file.name:
            logging.warning(f"Attempting to delete potentially failed Gemini upload: {uploaded_file.name}")
            await delete_gemini_file_async(uploaded_file.name)
        logging.error(f"Error during Gemini upload/processing for {display_name}: {e}", exc_info=True)
        raise

async def generate_content_async(model_instance, prompt_list): # Pass model instance
    """Generates content using Gemini, handling potential errors."""
    if not genai_installed: return "Gemini library not available."
    if not model_instance: raise RuntimeError("Gemini model instance not initialized.")
    logging.info(f"Generating content from Gemini model {model_instance.model_name}...")
    loop = asyncio.get_running_loop()
    response_text = "Content generation failed." # Default error text
    try:
        # Call generate_content on the model instance
        response = await loop.run_in_executor(
             None,
             model_instance.generate_content,
             prompt_list,
             request_options={"timeout": 300} # 5 min timeout
        )

        # Process response (handle potential variations in response structure)
        if hasattr(response, 'text') and response.text:
            response_text = response.text
            logging.info("Content generation successful.")
        elif hasattr(response, 'parts') and response.parts:
             # Combine text from parts if available
             response_text = "".join(part.text for part in response.parts if hasattr(part, 'text'))
             if response_text:
                 logging.info("Content generation successful (from parts).")
             else:
                 logging.warning(f"Gemini returned parts but no text content. Parts: {response.parts}")
                 response_text = "Content generation returned empty parts."
        elif hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
            reason = response.prompt_feedback.block_reason
            logging.error(f"Content generation blocked. Reason: {reason}")
            response_text = f"Content generation blocked: {reason}"
        else:
            logging.warning(f"Gemini returned no text/parts or block reason. Candidates: {getattr(response, 'candidates', 'N/A')}")
            response_text = "Content generation returned unknown empty response."

    except exceptions.GoogleAPICallError as api_error:
         logging.error(f"Gemini API Call Error: {api_error}", exc_info=True)
         response_text = f"Content generation failed: API Error ({api_error.code})" # Include status code
    except Exception as e:
        logging.error(f"Error during Gemini content generation: {e}", exc_info=True)
        response_text = f"Content generation failed: {type(e).__name__}" # Type of error

    return response_text

async def delete_gemini_file_async(file_name):
    """Requests deletion of a file from Gemini File API."""
    if not genai_installed: return
    if not file_name: logging.debug("No Gemini file name provided to delete."); return
    logging.info(f"Requesting deletion of Gemini file: {file_name}")
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, genai.delete_file, file_name)
        logging.info(f"Successfully requested deletion of Gemini file: {file_name}")
    except Exception as e:
        logging.warning(f"Failed to delete Gemini file {file_name}: {e}") # Non-critical

async def save_text_to_gcs(bucket, gcs_path: str, text_content: str, metadata: dict):
    """Saves text content to GCS, creating a temp file first."""
    loop = asyncio.get_running_loop()
    temp_txt_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', suffix=".txt") as temp_txt:
            temp_txt_path = temp_txt.name
            temp_txt.write(text_content)

        logging.info(f"Uploading text analysis to GCS: gs://{bucket.name}/{gcs_path}")
        txt_blob = bucket.blob(gcs_path)
        txt_blob.metadata = metadata # Set metadata before upload
        await loop.run_in_executor(None, txt_blob.upload_from_filename, temp_txt_path, content_type='text/plain')
        logging.info(f"Successfully uploaded text to gs://{bucket.name}/{gcs_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload text to gs://{bucket.name}/{gcs_path}: {e}", exc_info=True)
        return False
    finally:
        if temp_txt_path and os.path.exists(temp_txt_path):
            try: await loop.run_in_executor(None, os.remove, temp_txt_path); logging.debug(f"Removed temporary TXT: {temp_txt_path}")
            except Exception as e: logging.warning(f"Error removing temp TXT {temp_txt_path}: {e}")

# --- Main Processing Function ---

async def generate_sec_analysis(gcs_blob_path: str, ticker: str, accession_no: str):
    """
    Downloads PDF from GCS, generates Qualitative Analysis via Gemini,
    and saves the result to a GCS text file.
    """
    global storage_client, gemini_model # Use globally initialized clients

    if not genai_installed:
         logging.error("Cannot generate SEC analysis: google-generativeai library not installed.")
         raise ImportError("google-generativeai library is required for this function.")

    if not storage_client or not gemini_model:
        logging.error("Clients not initialized. Cannot process SEC analysis.")
        raise RuntimeError("Clients not initialized")

    logging.info(f"--- Starting SEC Analysis for Ticker: {ticker}, Filing: {accession_no} ---")
    logging.info(f"Processing PDF from GCS path: gs://{gcs_bucket_name}/{gcs_blob_path}")

    local_pdf_path = None
    uploaded_gemini_file = None
    uploaded_file_name = None
    analysis_success = False
    gcs_analysis_path = None
    loop = asyncio.get_running_loop()

    try:
        # 1. Download PDF from GCS
        bucket = storage_client.bucket(gcs_bucket_name) # Use snake_case var
        blob = bucket.blob(gcs_blob_path)
        blob_exists = await loop.run_in_executor(None, blob.exists)
        if not blob_exists: raise FileNotFoundError(f"PDF blob not found: {gcs_blob_path}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
            local_pdf_path = temp_pdf.name
            logging.info(f"Downloading PDF to temporary file: {local_pdf_path}")
            await loop.run_in_executor(None, blob.download_to_filename, local_pdf_path)

        # 2. Upload PDF to Gemini
        display_name = f"{ticker}_{accession_no}_SEC_Filing.pdf"
        uploaded_gemini_file = await upload_to_gemini_async(local_pdf_path, display_name)
        if not uploaded_gemini_file: raise RuntimeError("Gemini file upload failed.") # Check return
        uploaded_file_name = uploaded_gemini_file.name

        # 3. Generate Qualitative Analysis
        logging.info(f"Generating qualitative analysis for {ticker} ({accession_no})...")
        qualitative_analysis_text = await generate_content_async(
            gemini_model, [qualitative_analysis_prompt, uploaded_gemini_file] # Pass model instance
        )
        # Check for failure indicators from generate_content
        if qualitative_analysis_text.startswith("Content generation failed") \
           or qualitative_analysis_text.startswith("Content generation blocked") \
           or qualitative_analysis_text == "Content generation returned empty." \
           or qualitative_analysis_text == "Content generation returned unknown empty response.":
             raise RuntimeError(f"Gemini analysis failed: {qualitative_analysis_text}")


        # 4. Prepare Metadata & Filename for Output Text File
        output_metadata = {
            'ticker': ticker,
            'accessionNumber': accession_no,
            'originalPDFPath': f"gs://{gcs_bucket_name}/{gcs_blob_path}", # Use snake_case var
            'processingTimestampUTC': datetime.now(timezone.utc).isoformat(),
            'modelUsed': gemini_model_name # Add model name used
        }
        base_filename = f"{ticker}_{accession_no}"
        base_filename = re.sub(r'[^\w\-.]+', '_', base_filename)
        # Use snake_case prefix variable
        gcs_analysis_path = f"{gcs_analysis_txt_prefix}{base_filename}.txt"

        # 5. Save Qualitative Analysis Text to GCS
        analysis_success = await save_text_to_gcs(
            bucket, gcs_analysis_path, qualitative_analysis_text, output_metadata
        )
        if not analysis_success:
            raise IOError(f"Failed to save analysis text to GCS for {accession_no}")

    except Exception as e:
        logging.error(f"Error during SEC analysis processing for {ticker} ({accession_no}): {e}", exc_info=True)
        analysis_success = False
        raise # Re-raise for trigger mechanism
    finally:
        # 6. Cleanup
        if uploaded_file_name:
            await delete_gemini_file_async(uploaded_file_name)
        if local_pdf_path and os.path.exists(local_pdf_path):
            try:
                await loop.run_in_executor(None, os.remove, local_pdf_path)
                logging.info(f"Removed temporary PDF: {local_pdf_path}")
            except Exception as e:
                logging.warning(f"Error removing temp PDF {local_pdf_path}: {e}")

        log_suffix = f"gs://{gcs_bucket_name}/{gcs_analysis_path}" if gcs_analysis_path else "N/A"
        logging.info(f"--- Finished SEC Analysis for Ticker: {ticker}, Filing: {accession_no}. Success: {analysis_success}. Output: {log_suffix} ---")


# --- Entry Point Handler ---
async def handle_gcs_event(event_data: dict):
    """
    Trigger handler function (e.g., called by Flask route in main.py).
    Parses event data, extracts filename, parses IDs, calls analysis function.
    """
    if not initialize_clients():
        logging.critical("Failed to initialize necessary clients. Aborting function.")
        raise RuntimeError("Client initialization failed")

    try:
        bucket_name = event_data.get('bucket')
        gcs_path = event_data.get('name')
        logging.info(f"PDF Summarizer received GCS event for: gs://{bucket_name}/{gcs_path}")

        # --- Basic Event Validation ---
        if not gcs_path or not bucket_name:
            logging.warning("Event missing 'bucket' or 'name' field. Ignoring.")
            return
        # Use snake_case var
        if bucket_name != gcs_bucket_name:
            logging.warning(f"Event for wrong bucket ({bucket_name} vs {gcs_bucket_name}). Ignoring.")
            return
        # Use snake_case var
        if not gcs_path.startswith(gcs_pdf_folder) or not gcs_path.lower().endswith('.pdf'):
            logging.info(f"Ignoring event for non-matching path ({gcs_path} vs prefix {gcs_pdf_folder}) or non-PDF file.")
            return
        # --- End Validation ---

        # --- Parse Filename ---
        ticker = None
        accession_no = None
        try:
            filename = os.path.basename(gcs_path)
            parts = filename.rsplit('.', 1)[0].split('_', 1)
            if len(parts) == 2:
                ticker = parts[0]
                accession_no = parts[1] # Keep original format from filename for consistency if needed
                logging.info(f"Parsed from filename: Ticker={ticker}, AccessionNo={accession_no}")
            else:
                raise ValueError(f"Filename '{filename}' did not match expected TICKER_ACCESSIONNUMBER.pdf format")
        except Exception as parse_err:
            logging.error(f"Failed to parse ticker/accession number from filename '{gcs_path}': {parse_err}. Cannot process.")
            return # Ignore event

        if not ticker or not accession_no:
             logging.error(f"Ticker or AccessionNumber could not be determined for {gcs_path}. Cannot process.")
             return # Ignore event
        # --- End Parsing ---

        # Call the main analysis function
        await generate_sec_analysis(
            gcs_blob_path=gcs_path,
            ticker=ticker,
            accession_no=accession_no
        )
        logging.info(f"Successfully processed PDF analysis event for {gcs_path}")

    except Exception as e:
        logging.error(f"Error processing PDF analysis GCS event for {event_data.get('name', 'N/A')}: {e}", exc_info=True)
        raise # Re-raise for trigger mechanism

# Example local execution (for testing)
# async def local_test():
#     # ... (setup similar to news_summarizer local_test) ...
# if __name__ == "__main__":
#     # ... (setup similar to news_summarizer __main__) ...
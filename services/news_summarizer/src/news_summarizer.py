import os
import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from google import auth as google_auth
import google.generativeai as genai
from google.generativeai.types import GenerateContentConfig, Tool, GoogleSearch
import json
import logging
from datetime import datetime, timedelta, date, timezone
import time
import re
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential
from google.api_core import exceptions
import asyncio
import tempfile

# --- Configuration ---
# Environment Variable Names
ENV_PROJECT_ID = 'GCP_PROJECT_ID'
ENV_LOCATION = 'GCP_REGION'
ENV_BQ_DATASET_ID = 'BQ_DATASET_ID'
ENV_BQ_METADATA_TABLE = 'BQ_METADATA_TABLE_ID'
ENV_GCS_BUCKET_NAME = 'GCS_BUCKET_NAME'
ENV_GCS_NEWS_SUMMARY_PREFIX = 'GCS_NEWS_SUMMARY_PREFIX'
ENV_GCS_PDF_PREFIX = 'GCS_PDF_PREFIX'
ENV_GEMINI_MODEL_NAME = 'GEMINI_MODEL_NAME'

# Load Configuration
PROJECT_ID = os.getenv(ENV_PROJECT_ID)
LOCATION = os.getenv(ENV_LOCATION, 'us-central1')
DATASET_ID = os.getenv(ENV_BQ_DATASET_ID, 'profit_scout')
METADATA_TABLE = os.getenv(ENV_BQ_METADATA_TABLE)
GCS_BUCKET_NAME = os.getenv(ENV_GCS_BUCKET_NAME)
GCS_NEWS_SUMMARY_PREFIX = os.getenv(ENV_GCS_NEWS_SUMMARY_PREFIX, 'NewsSummaries_R1000/')
GCS_PDF_PREFIX = os.getenv(ENV_GCS_PDF_PREFIX, 'SEC_Filings_Russell1000/')
GEMINI_MODEL_NAME = os.getenv(ENV_GEMINI_MODEL_NAME, 'gemini-2.0-flash-001')

# --- Constants ---
LOOKBACK_DAYS = 30

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s [%(module)s:%(lineno)d] - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("google.api_core").setLevel(logging.WARNING)
logging.getLogger("google.auth").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Validate Essential Configuration ---
essential_vars = {
    ENV_PROJECT_ID: PROJECT_ID,
    ENV_LOCATION: LOCATION,
    ENV_BQ_DATASET_ID: DATASET_ID,
    ENV_BQ_METADATA_TABLE: METADATA_TABLE,
    ENV_GCS_BUCKET_NAME: GCS_BUCKET_NAME,
}
missing_vars = [k for k, v in essential_vars.items() if not v]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}. Exiting.")
    exit(1)

# Construct full table IDs / prefixes
metadata_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE}"
GCS_NEWS_SUMMARY_PREFIX = GCS_NEWS_SUMMARY_PREFIX if GCS_NEWS_SUMMARY_PREFIX.endswith('/') else GCS_NEWS_SUMMARY_PREFIX + '/'
GCS_PDF_PREFIX = GCS_PDF_PREFIX if GCS_PDF_PREFIX.endswith('/') else GCS_PDF_PREFIX + '/'

# --- Global Clients ---
bq_client = None
storage_client = None
genai_client = None

# --- Helper Functions ---

def initialize_clients():
    global bq_client, storage_client, genai_client
    if bq_client and storage_client and genai_client: return True

    try:
        if not bq_client:
            bq_client = bigquery.Client(project=PROJECT_ID)
            logging.info("BigQuery client initialized using ADC.")
        if not storage_client:
            storage_client = storage.Client(project=PROJECT_ID)
            logging.info("GCS client initialized using ADC.")
        if not genai_client:
            vtx_credentials, _ = google_auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            genai_client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION, credentials=vtx_credentials)
            logging.info(f"Vertex AI Gemini client initialized for project {PROJECT_ID} in {LOCATION}.")

        if bq_client and storage_client and genai_client: return True
        else: logging.error("Client initialization incomplete."); return False

    except Exception as e:
        logging.error(f"Failed to initialize clients: {e}", exc_info=True)
        bq_client = None; storage_client = None; genai_client = None
        return False

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def get_filing_date_from_metadata(accession_number_cleaned: str) -> date | None:
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    logging.info(f"Querying filing date for cleaned AN: {accession_number_cleaned} from {metadata_table_full_id}")
    query = f"""
    SELECT FiledDate
    FROM `{metadata_table_full_id}`
    WHERE AccessionNumber = @accession_number
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
                try: return pd.to_datetime(filed_date).date()
                except: logging.error(f"Could not convert found FiledDate ({filed_date}) to date object for {accession_number_cleaned}"); return None
        else:
            logging.warning(f"FiledDate not found in metadata for AccessionNumber: {accession_number_cleaned}")
            return None
    except Exception as e:
        logging.error(f"Error querying FiledDate for {accession_number_cleaned}: {e}", exc_info=True)
        raise

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=5, max=60), reraise=True)
def generate_news_summary(company_name: str, ticker: str, lookback_days: int) -> str:
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
        logging.debug(f"Generating news assessment for {ticker} (Lookback: {lookback_days} days)...")
        response = genai_client.generate_content(
            model=GEMINI_MODEL_NAME,
            contents=prompt,
            generation_config=GenerateContentConfig(
                tools=[Tool(google_search=GoogleSearch())],
                temperature=0.1
            )
        )
        logging.debug(f"Successfully received response for {ticker}.")
        if response and hasattr(response, 'text') and response.text:
            return response.text
        else:
            logging.warning(f"Received empty response from Vertex AI for {ticker}. Details: {response}")
            return "Received empty response"
    except exceptions.GoogleAPICallError as api_error:
        logging.error(f"Vertex AI API error generating content for {ticker}: {api_error}")
        raise
    except Exception as e:
        logging.error(f"Error generating content for {ticker}: {e}", exc_info=True)
        return f"Error during generation: {e}"

def save_text_to_gcs_sync(bucket, gcs_path: str, text_content: str):
    temp_txt_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', suffix=".txt") as temp_txt:
            temp_txt_path = temp_txt.name
            temp_txt.write(text_content)
        logging.info(f"Uploading text news summary sync to GCS: gs://{bucket.name}/{gcs_path}")
        txt_blob = bucket.blob(gcs_path)
        output_metadata = {
            'processingTimestampUTC': datetime.now(timezone.utc).isoformat()
        }
        txt_blob.metadata = output_metadata
        txt_blob.upload_from_filename(temp_txt_path, content_type='text/plain')
        logging.info(f"Successfully uploaded text sync to gs://{bucket.name}/{gcs_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload text sync to gs://{bucket.name}/{gcs_path}: {e}", exc_info=True)
        return False
    finally:
        if temp_txt_path and os.path.exists(temp_txt_path):
            try: os.remove(temp_txt_path)
            except Exception as e_clean: logging.warning(f"Error removing temp TXT sync {temp_txt_path}: {e_clean}")

def check_gcs_blob_exists(bucket, blob_name):
    try:
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Error checking GCS existence for {blob_name}: {e}")
        return False

# --- Main Event Handler Logic ---

async def process_news_summary_event(gcs_path: str, ticker: str, accession_number_cleaned: str):
    global storage_client, bq_client, genai_client

    if not all([storage_client, bq_client, genai_client]):
        raise RuntimeError("Clients not initialized before processing.")

    logging.info(f"--- Processing News Summary for {ticker} ({accession_number_cleaned}) ---")

    # 1. Construct Target Output Path
    base_filename = f"{ticker}_{accession_number_cleaned}"
    sanitized_base = re.sub(r'[^\w\-]+', '_', base_filename)
    output_filename = f"{sanitized_base}.txt" # Changed to remove NewsSummary_
    destination_blob_name = f"{GCS_NEWS_SUMMARY_PREFIX}{output_filename}"

    # 2. Check if output already exists (Idempotency)
    gcs_bucket = storage_client.bucket(GCS_BUCKET_NAME)
    loop = asyncio.get_running_loop()
    output_exists = await loop.run_in_executor(None, check_gcs_blob_exists, gcs_bucket, destination_blob_name)

    if output_exists:
        logging.info(f"Skipping {ticker} ({accession_number_cleaned}): Output {destination_blob_name} already exists.")
        return

    # 3. Get Filing Date from Metadata Table
    filed_date = await loop.run_in_executor(None, get_filing_date_from_metadata, accession_number_cleaned)
    if not filed_date:
        logging.error(f"[{ticker}] Cannot proceed without FiledDate for {accession_number_cleaned}.")
        return

    # 4. Generate News Summary/Assessment
    company_name = ticker
    summary_text = await loop.run_in_executor(None, generate_news_summary, company_name, ticker, LOOKBACK_DAYS)

    if summary_text.startswith("Error:"):
        logging.error(f"[{ticker}] Failed to generate news summary for {accession_number_cleaned}: {summary_text}")
        raise RuntimeError(f"News summary generation failed: {summary_text}")

    # 5. Save Summary to GCS
    save_success = await loop.run_in_executor(None, save_text_to_gcs_sync, gcs_bucket, destination_blob_name, summary_text)

    if not save_success:
        logging.error(f"[{ticker}] Failed to save news summary to GCS for {accession_number_cleaned}")
        raise IOError(f"Failed to save news summary to GCS for {accession_number_cleaned}")

    logging.info(f"--- Successfully finished News Summary for {ticker} ({accession_number_cleaned}) ---")

# --- Entry Point ---
async def handle_gcs_event(event_data: dict):
    if not initialize_clients():
        logging.critical("Failed to initialize necessary clients. Aborting function.")
        raise RuntimeError("Client initialization failed")

    try:
        bucket_name = event_data.get('bucket')
        gcs_path = event_data.get('name')
        logging.info(f"News Summarizer received GCS event for: gs://{bucket_name}/{gcs_path}")

        if not gcs_path or not bucket_name or bucket_name != GCS_BUCKET_NAME:
            logging.warning(f"Event missing data or for wrong bucket ({bucket_name} vs {GCS_BUCKET_NAME}). Ignoring.")
            return
        if not gcs_path.startswith(GCS_PDF_PREFIX) or not gcs_path.lower().endswith('.pdf'):
            logging.info(f"Ignoring event for non-PDF file or wrong prefix: {gcs_path}")
            return

        ticker = None
        accession_number_cleaned = None
        try:
            filename = os.path.basename(gcs_path)
            parts = filename.rsplit('.', 1)[0].split('_', 1)
            if len(parts) == 2:
                ticker = parts[0]
                accession_number_original = parts[1]
                accession_number_cleaned = accession_number_original.replace('-', '')
                logging.info(f"Parsed from filename: Ticker={ticker}, Cleaned AN={accession_number_cleaned}")
            else: raise ValueError("Filename pattern mismatch")
        except Exception as parse_err:
            logging.error(f"Failed to parse Ticker/AN from filename '{gcs_path}': {parse_err}. Cannot process.")
            return

        await process_news_summary_event(gcs_path, ticker, accession_number_cleaned)

        logging.info(f"Successfully processed news summary event for {gcs_path}")

    except Exception as e:
        logging.error(f"Error processing news summary GCS event for {event_data.get('name', 'N/A')}: {e}", exc_info=True)
        raise
# services/news_summarizer/src/news_summarizer.py

import os
import json
import logging
import re
import tempfile
import asyncio
import types
from datetime import datetime, date, timezone

# ———————————————————————————————
# Optional / guarded imports for GCS & BQ clients
# ———————————————————————————————
try:
    # Import each submodule directly to avoid partial google.cloud namespace issues
    import google.cloud.storage as storage
    import google.cloud.bigquery as bigquery
    from google.cloud.exceptions import NotFound
except ImportError:
    # Dummy modules exposing the minimal API your code/tests require
    storage = types.SimpleNamespace(
        Client=lambda *args, **kwargs: None
    )
    bigquery = types.SimpleNamespace(
        Client=lambda *args, **kwargs: None,
        QueryJobConfig=lambda *args, **kwargs: None,
        ScalarQueryParameter=lambda *args, **kwargs: None
    )
    NotFound = Exception

# generative AI
try:
    import google.generativeai as genai
    from google.generativeai.types import GenerationConfig
    from google.ai.generativelanguage import Tool as LmTool, GoogleSearchRetrieval
    genai_installed = True
except ImportError:
    genai_installed = False
    logging.warning("google-generativeai not found; skipping news summarization.")

from tenacity import retry, stop_after_attempt, wait_exponential
from google.api_core import exceptions as google_exceptions

# ———————————————————————————————
# Configuration
# ———————————————————————————————
gcp_project_id          = os.getenv('gcp_project_id')
gcp_region              = os.getenv('gcp_region', 'us-central1')
bq_dataset_id           = os.getenv('bq_dataset_id', 'profit_scout')
bq_metadata_table_id    = os.getenv('bq_metadata_table_id')
gcs_bucket_name         = os.getenv('gcs_bucket_name')
gcs_pdf_folder          = os.getenv('gcs_pdf_folder', 'sec-pdf/')
gcs_news_summary_prefix = os.getenv('gcs_news_summary_prefix', 'headline-analysis/')
gemini_model_name       = os.getenv('gemini_model_name', 'gemini-2.0-flash-latest')

LOOKBACK_DAYS = 30

# ———————————————————————————————
# Logging setup (quiet 3rd‑party noise)
# ———————————————————————————————
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s",
)
for logger_name in ["google.api_core", "google.auth", "google.cloud", "urllib3", "requests"]:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

# ———————————————————————————————
# Validate essential env vars
# ———————————————————————————————
essential = {
    'gcp_project_id': gcp_project_id,
    'gcp_region': gcp_region,
    'bq_dataset_id': bq_dataset_id,
    'bq_metadata_table_id': bq_metadata_table_id,
    'gcs_bucket_name': gcs_bucket_name,
}
if genai_installed:
    essential['gemini_model_name'] = gemini_model_name

missing = [k for k, v in essential.items() if not v]
if missing:
    logging.critical(f"Missing essential environment variables: {', '.join(missing)}. Exiting.")
    exit(1)

# ———————————————————————————————
# Construct IDs & prefixes
# ———————————————————————————————
metadata_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_metadata_table_id}"
if not gcs_news_summary_prefix.endswith('/'):
    gcs_news_summary_prefix += '/'
if not gcs_pdf_folder.endswith('/'):
    gcs_pdf_folder += '/'

# ———————————————————————————————
# Global clients
# ———————————————————————————————
bq_client      = None
storage_client = None
genai_client   = None

# ———————————————————————————————
# Helpers
# ———————————————————————————————
def initialize_clients():
    """Idempotently initialize global clients."""
    global bq_client, storage_client, genai_client

    if bq_client and storage_client and (genai_client or not genai_installed):
        return True

    success = True
    try:
        if not bq_client:
            bq_client = bigquery.Client(project=gcp_project_id)
            logging.info("BigQuery client initialized.")
        if not storage_client:
            storage_client = storage.Client(project=gcp_project_id)
            logging.info("GCS client initialized.")
        if genai_installed and not genai_client:
            genai.init(project=gcp_project_id, location=gcp_region)
            genai_client = genai
            logging.info("Gemini SDK initialized.")
    except Exception as e:
        logging.error(f"Failed to initialize clients: {e}", exc_info=True)
        bq_client = storage_client = genai_client = None
        success = False

    return bool(bq_client and storage_client and (genai_client or not genai_installed))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def get_filing_date_from_metadata(accession_number_cleaned: str) -> date | None:
    """Look up the FiledDate for a given AccessionNumber in BigQuery."""
    if not bq_client:
        raise RuntimeError("BigQuery client not initialized.")

    query = f"""
      SELECT FiledDate
      FROM `{metadata_table_full_id}`
      WHERE AccessionNumber = @accession_number
      ORDER BY FiledDate DESC
      LIMIT 1
    """
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("accession_number", "STRING", accession_number_cleaned),
        ]
    )
    job = bq_client.query(query, job_cfg)
    rows = list(job.result(timeout=60))
    if not rows:
        logging.warning(f"No metadata row for {accession_number_cleaned}")
        return None

    filed = rows[0].FiledDate
    if isinstance(filed, date):
        return filed
    try:
        return pd.to_datetime(filed).date()
    except Exception:
        logging.error(f"Cannot parse FiledDate {filed}", exc_info=True)
        return None


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=5, max=60), reraise=True)
def generate_news_summary(company_name: str, ticker: str, lookback_days: int) -> str:
    """Generates news summary using Vertex AI Gemini with Google Search."""
    if not genai_installed:
        return "google-generativeai library not installed."
    if not genai_client:
        raise RuntimeError("Vertex AI Gemini client not initialized.")

    prompt = f"""
    Analyze recent news and events for {company_name} ({ticker}) using Google Search results from the past {lookback_days} days...
    """
    try:
        model = genai.GenerativeModel(gemini_model_name)
        search_tool = LmTool(google_search_retrieval=GoogleSearchRetrieval(disable_attribution=False))
        config = GenerationConfig(temperature=0.1)
        response = model.generate_content(prompt, generation_config=config, tools=[search_tool])

        if hasattr(response, 'text') and response.text:
            return response.text
        else:
            return "Received empty/invalid response from AI model"
    except google_exceptions.GoogleAPICallError as api_error:
        logging.error(f"Vertex AI API error generating content for {ticker}: {api_error}")
        raise
    except Exception as e:
        logging.error(f"Error generating content for {ticker}: {e}", exc_info=True)
        return "Error during generation. Check logs for details."


def save_text_to_gcs_sync(bucket, gcs_path: str, text_content: str) -> bool:
    """Write out a .txt and upload it to GCS."""
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as f:
            temp_path = f.name
            f.write(text_content)

        blob = bucket.blob(gcs_path)
        blob.metadata = {
            'processingTimestampUTC': datetime.now(timezone.utc).isoformat(),
            'contentType': 'text/plain',
        }
        blob.upload_from_filename(temp_path, content_type='text/plain')
        logging.info(f"Uploaded text to gs://{bucket.name}/{gcs_path}")
        return True

    except Exception as e:
        logging.error(f"Failed to upload text to gs://{bucket.name}/{gcs_path}: {e}", exc_info=True)
        return False

    finally:
        if temp_path and os.path.exists(temp_path):
            try: os.remove(temp_path)
            except: pass


def check_gcs_blob_exists(bucket, blob_name):
    """Checks if a specific blob exists in the GCS bucket."""
    try:
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Error checking GCS existence for {blob_name}: {e}")
        return False


# ———————————————————————————————
# Main Event Handler Logic
# ———————————————————————————————
async def process_news_summary_event(gcs_pdf_path: str, ticker: str, accession_number_cleaned: str):
    global storage_client, bq_client, genai_client

    if not initialize_clients():
        raise RuntimeError("Clients not initialized before processing.")

    logging.info(f"--- Processing {ticker}/{accession_number_cleaned} for {gcs_pdf_path} ---")

    # build output path
    base = f"{ticker}_{accession_number_cleaned}"
    sanitized = re.sub(r"[^\w\-]+", "_", base)
    destination = f"{gcs_news_summary_prefix}{sanitized}.txt"

    bucket = storage_client.bucket(gcs_bucket_name)
    loop = asyncio.get_running_loop()

    # skip if already exists
    if await loop.run_in_executor(None, check_gcs_blob_exists, bucket, destination):
        logging.info(f"Skipping, summary already exists: {destination}")
        return

    # fetch filed date (optional)
    filed_date = await loop.run_in_executor(None, get_filing_date_from_metadata, accession_number_cleaned)
    if not filed_date:
        logging.warning(f"Could not retrieve FiledDate for {accession_number_cleaned}")

    # generate summary
    summary = await loop.run_in_executor(None, generate_news_summary, ticker, ticker, LOOKBACK_DAYS)
    if summary.startswith("Error") or summary in {
        "Received empty/invalid response from AI model",
        "Content generation blocked due to safety settings."
    }:
        logging.error(f"Summary generation failed: {summary}")
        raise RuntimeError(f"News summary generation failed: {summary}")

    # upload
    if not await loop.run_in_executor(None, save_text_to_gcs_sync, bucket, destination, summary):
        raise IOError(f"Failed to save news summary to {destination}")

    logging.info(f"Finished news summary for {ticker}/{accession_number_cleaned}")

# ———————————————————————————————
# Entry point for GCS-triggered events
# ———————————————————————————————
async def handle_gcs_event(event_data: dict):
    if not initialize_clients():
        logging.critical("Client init failed; aborting.")
        raise RuntimeError("Client initialization failed.")

    bucket = event_data.get('bucket')
    name   = event_data.get('name')

    # basic validation
    if bucket != gcs_bucket_name or not name or not name.startswith(gcs_pdf_folder) or not name.lower().endswith('.pdf'):
        logging.info(f"Ignoring event for {bucket}/{name}")
        return

    # parse ticker & accession from filename
    try:
        filename = os.path.basename(name)
        base, _  = filename.rsplit('.', 1)
        parts    = base.split('_', 1)
        ticker, raw_an = parts
        accession_number_cleaned = raw_an.replace('-', '')
    except Exception as e:
        logging.error(f"Filename parse failed for {name}: {e}")
        return

    await process_news_summary_event(name, ticker, accession_number_cleaned)
    logging.info(f"GCS event processed: {bucket}/{name}")

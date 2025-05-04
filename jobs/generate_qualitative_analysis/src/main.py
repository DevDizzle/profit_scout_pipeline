#!/usr/bin/env python3
# main.py — Batch SEC filing qualitative analysis via Gemini File API

import os
import time
import logging
import tempfile
import asyncio
import warnings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from google.cloud import storage, secretmanager
from google import genai
from google.genai.types import GenerateContentConfig, HttpOptions, FileData, Part
from google.api_core import exceptions as core_exceptions

# ——— Suppress Pydantic warning about built-in any ———
warnings.filterwarnings(
    "ignore",
    r".*<built-in function any> is not a Python type.*",
    category=UserWarning,
    module="pydantic._internal._generate_schema"
)

# ——— Configuration ———
PROJECT_ID              = os.getenv('GCP_PROJECT_ID', 'profit-scout-456416')
GCS_BUCKET_NAME         = os.getenv('GCS_BUCKET_NAME', 'profit-scout')
GCS_PDF_PREFIX          = os.getenv('GCS_PDF_PREFIX', 'sec-pdf/').rstrip('/') + '/'
GCS_ANALYSIS_TXT_PREFIX = os.getenv('GCS_ANALYSIS_TXT_PREFIX', 'sec-analysis/').rstrip('/') + '/'
GCS_TEMP_PREFIX         = os.getenv('GCS_TEMP_PREFIX', 'sec-pdf/tmp/').rstrip('/') + '/'

SECRET_ID   = os.getenv('GEMINI_API_SECRET_ID', 'gemini-api-key')
SECRET_VER  = os.getenv('GEMINI_API_KEY_SECRET_VERSION', 'latest')
MODEL_NAME  = os.getenv('GEMINI_MODEL_NAME', 'gemini-1.5-flash-001')
TEMPERATURE = float(os.getenv('GEMINI_TEMPERATURE', '0.2'))
MAX_TOKENS  = int(os.getenv('GEMINI_MAX_TOKENS', '8192'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '2'))

# ——— Logging ———
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ——— Prompt ———
QUAL_PROMPT = """
You are a top-tier financial analyst tasked with analyzing an SEC filing (10-K or 10-Q) to extract critical performance data and insights. Your output serves two goals:  
(1) A financial analysis bot assessing company health, and  
(2) An ML model predicting stock price appreciation over the next 90 days.  

Keep it concise, metric-driven, and ML-ready with standardized formats.  
Base all insights strictly on the content of the SEC filing.

---

**Instructions**:

1. **Key Metrics & Trends**  
   * List: Revenue, Op Income, Net Income, EPS, Op Cash Flow, Debt (e.g., Total Debt).  
   * Format: “[Metric]: [Current]; [YoY %]; [Accel/Decel or N/A].”  
   * Example: “Revenue: $100M; +11.1%; Accel from +5%.”

2. **Strengths & Risks**  
   * 2–3 Strengths, 2–3 Risks, tied to metrics.  
   * Format: “[Signal]: [Metric].”  
   * Example: “Strong Growth: Revenue +11.1%.”

3. **Performance Snapshot**  
   * 3-sentence summary of trends and tone.  
   * End with: “Sentiment: [Pos/Neg/Mix]; Tone: [e.g., Confident, Cautious, Uncertain].”

4. **Investment Signals**  
   * 2–3 metric-based factors investors should know.  
   * Format: “[Factor]: [Metric].”  
   * Example: “Growth Driver: Revenue +11.1%.”

5. **Guidance & Catalysts**  
   * Guidance: e.g., “Revenue $110M–$120M” or “None.”  
   * Outlook: e.g., “Bullish, new product” or “None.”  
   * End with: “Outlook: [Bull/Bear/Neu]; Catalyst: [e.g., Product, Macro, None].”

---

**CRITICAL INSTRUCTIONS**:

- **Output only the analysis.**
- **Do not include introductions, disclaimers, or speculative content.**
- **Use only the SEC filing — no outside data or assumptions.**
- **Keep content concise, structured, and predictive.**
- **Follow the exact section format and tags for machine-readability.**

---

**Output Example**:

* **Key Metrics & Trends**:  
  * Revenue: $100M; +11.1%; Accel from +5%  
  * Net Income: -$10M; -50%; N/A  

* **Strengths & Risks**:  
  * Strong Growth: Revenue +11.1%  
  * Profit Risk: Net Income -$10M  

* **Performance Snapshot**:  
  Revenue grew 11.1% with acceleration, but losses widened. Management is optimistic about new products. Outlook is mixed.  
  Sentiment: Mix; Tone: Optimistic  

* **Investment Signals**:  
  * Growth Driver: Revenue +11.1%  
  * Loss Concern: Net Income -$10M  

* **Guidance & Catalysts**:  
  * Guidance: None  
  * Outlook: Bullish, new product  
  * Outlook: Bull; Catalyst: Product
"""

# ——— Helper Functions ———
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def get_secret(secret_id, version="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version}"
    return client.access_secret_version(request={"name": name}).payload.data.decode().strip()

def extract_info_from_filename(gcs_path):
    """
    Split on the first underscore: TICKER_ACCESSION.pdf
    """
    filename = os.path.basename(gcs_path)
    name, _ = os.path.splitext(filename)
    if "_" not in name:
        logging.warning(f"Cannot parse ticker/accession from filename: {filename}")
        return None, None, None, None
    ticker, accession = name.split("_", 1)
    return ticker.upper(), None, None, accession

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def upload_to_gcs(bucket, local_path, gcs_path):
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket.name}/{gcs_path}"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def delete_from_gcs(bucket, gcs_path):
    blob = bucket.blob(gcs_path)
    if blob.exists():
        blob.delete()

RETRYABLE_EXCEPTIONS = (
    core_exceptions.GoogleAPICallError,
    core_exceptions.RetryError,
    core_exceptions.ServerError,
    core_exceptions.ServiceUnavailable,
    ConnectionAbortedError
)

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(min=4, max=20),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    reraise=True
)
@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(min=4, max=20),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    reraise=True
)
async def upload_to_gemini(client, local_pdf):
    # Upload, get back a File object
    file_obj = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: client.files.upload(file=local_pdf)
    )
    logging.info(f"Uploaded {local_pdf} → {file_obj.uri}")
    return file_obj, None

async def generate_analysis(client, file_obj):
    """
    Generate qualitative analysis for a PDF uploaded to Gemini File API.
    :param client: genai.Client instance
    :param file_obj: FileData object returned by client.files.upload(...)
    :returns: str (the generated analysis text)
    """
    # Configure model parameters
    config = GenerateContentConfig(
        temperature=TEMPERATURE,
        max_output_tokens=MAX_TOKENS
    )

    # Include the uploaded file object, then your prompt
    contents = [
        file_obj,
        Part.from_text(text=QUAL_PROMPT)  
    ]

    # Run the multimodal generation call on a background thread
    response = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: client.models.generate_content(
            model=MODEL_NAME,
            contents=contents,
            config=config
        )
    )

    # Extract result text
    if response.text:
        return response.text
    if hasattr(response, "parts"):
        return "".join(p.text for p in response.parts if hasattr(p, "text"))
    raise RuntimeError("Empty response from Gemini")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def download_pdf(bucket, blob_name, local_path):
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)
    if not os.path.exists(local_path) or os.path.getsize(local_path) < 100:
        raise IOError("Downloaded PDF missing or appears empty.")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def upload_txt(bucket, local_path, blob_path):
    bucket.blob(blob_path).upload_from_filename(local_path)

async def process_pdf(client, bucket, blob_name, temp_dir):
    basename = os.path.basename(blob_name)
    ticker, _, _, accession_no = extract_info_from_filename(blob_name)
    if not ticker or not accession_no:
        return 'skipped_parse'

    key = f"{ticker}_{accession_no}"
    txt_blob_path = f"{GCS_ANALYSIS_TXT_PREFIX}{key}.txt"
    if bucket.blob(txt_blob_path).exists():
        return 'skipped_existing'

    logging.info(f"Processing {key}")
    status = 'error_unexpected'
    local_pdf = os.path.join(temp_dir, basename)
    local_txt = None
    temp_gcs_path = None

    try:
        download_pdf(bucket, blob_name, local_pdf)
        file_obj, temp_gcs_path = await upload_to_gemini(client, local_pdf)
        logging.info(f"[{ticker}] Gemini File URI: {file_obj.uri}")

        analysis = await generate_analysis(client, file_obj)
        status = 'success'

        local_txt = os.path.join(temp_dir, f"{key}.txt")
        with open(local_txt, 'w', encoding='utf-8') as f:
            f.write(analysis)
        upload_txt(bucket, local_txt, txt_blob_path)
        logging.info(f"[{ticker}] Saved analysis to: gs://{GCS_BUCKET_NAME}/{txt_blob_path}")

    except Exception as e:
        logging.error(f"[{ticker}] Failed processing {key}: {e}")
        if isinstance(e, core_exceptions.BadRequest):
            status = 'error_generate_bad_pdf'
        elif isinstance(e, (core_exceptions.GoogleAPICallError, TimeoutError)):
            status = 'error_generate'
        else:
            status = 'error_worker'

    finally:
        if temp_gcs_path:
            try:
                delete_from_gcs(bucket, temp_gcs_path)
            except Exception:
                pass
        for path in [local_pdf, local_txt]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

    return status

async def main():
    start = time.time()
    results_summary = {
        'success': 0, 'error_generate': 0, 'error_generate_bad_pdf': 0,
        'error_worker': 0, 'skipped_parse': 0, 'skipped_existing': 0
    }
    try:
        import google.genai
        logging.info(f"Google GenAI SDK version: {google.genai.__version__}")
        logging.info(f"Client methods: {dir(genai.Client)}")
        api_key = get_secret(SECRET_ID, SECRET_VER)
        client = genai.Client(api_key=api_key, http_options=HttpOptions(api_version='v1beta'))
        gcs_client = storage.Client(project=PROJECT_ID)
        bucket = gcs_client.bucket(GCS_BUCKET_NAME)

        blobs = [
            b.name
            for b in bucket.list_blobs(prefix=GCS_PDF_PREFIX)
            if b.name.lower().endswith('.pdf')
        ]
        logging.info(f"Found {len(blobs)} PDFs to process")

        temp_dir = tempfile.mkdtemp()
        sem = asyncio.Semaphore(MAX_WORKERS)

        async def sem_task(blob_name):
            async with sem:
                status = await process_pdf(client, bucket, blob_name, temp_dir)
                results_summary[status] += 1

        await asyncio.gather(*(sem_task(b) for b in blobs))

        try:
            os.rmdir(temp_dir)
        except OSError:
            pass

        total_errors = sum(v for k, v in results_summary.items() if 'error' in k)
        total_skipped = results_summary['skipped_parse'] + results_summary['skipped_existing']
        logging.info(f"""
Processing Summary:
  Total PDFs:           {len(blobs)}
  Skipped parses:       {results_summary['skipped_parse']}
  Already exists:       {results_summary['skipped_existing']}
  Successes:            {results_summary['success']}
  Errors:               {total_errors}
Elapsed time:          {time.time() - start:.1f}s
""")

    except Exception as fatal:
        logging.critical(f"Fatal error in batch execution: {fatal}", exc_info=True)
        raise

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception:
        logging.critical("Fatal error in batch execution", exc_info=True)
        exit(1)
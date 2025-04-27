import os
import logging
import json
import re
import threading
import time
import concurrent.futures
from datetime import datetime, timedelta, date
from decimal import Decimal

import pandas as pd
import numpy as np
from tqdm import tqdm # Keep tqdm if you run it interactively, less useful in logs

# --- GCP & Lib Imports ---
try:
    from google.cloud import bigquery, secretmanager
    from google.api_core import exceptions
    import google.auth
    GCP_LIBS_AVAILABLE = True
except ImportError:
    logging.error("Failed to import Google Cloud libraries.")
    GCP_LIBS_AVAILABLE = False

try:
    import google.generativeai as genai
    GEMINI_LIB_AVAILABLE = True
except ImportError:
    # If Gemini isn't used directly but via Vertex endpoint, this might be ok
    logging.warning("google.generativeai library not found. Ensure correct client is used if needed.")
    GEMINI_LIB_AVAILABLE = False # Assume direct usage based on original code

try:
    from tenacity import retry, stop_after_attempt, wait_fixed
except ImportError:
    logging.warning("Tenacity library not found. Retries will not be available.")
    # Define dummy decorator if tenacity is missing
    def retry(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator
    stop_after_attempt = lambda n: None
    wait_fixed = lambda *args, **kwargs: None


# ─── Configuration ───────────────────────────────────────────────────────────
PROJECT_ID            = os.environ.get("GCP_PROJECT_ID") # Use .get for safety
BQ_DATASET            = os.environ.get("BQ_DATASET_ID")
BS_TABLE              = os.environ.get("BQ_FINANCIALS_BS_TABLE_ID")
IS_TABLE              = os.environ.get("BQ_FINANCIALS_IS_TABLE_ID")
CF_TABLE              = os.environ.get("BQ_FINANCIALS_CF_TABLE_ID")
# Support both env var names: BQ_PRICE_TABLE_ID and BQ_PRICES_TABLE_ID
PRICE_TABLE = os.environ.get("BQ_PRICE_TABLE_ID", os.environ.get("BQ_PRICES_TABLE_ID", "price_data")) # Default if neither set
METADATA_TABLE        = os.environ.get("BQ_METADATA_TABLE_ID")
RATIOS_TABLE          = os.environ.get("BQ_RATIOS_TABLE_ID")
GEMINI_SECRET_NAME    = os.environ.get("GEMINI_API_KEY_SECRET_ID")
GEMINI_SECRET_VERSION = os.environ.get("GEMINI_API_KEY_SECRET_VERSION", "latest")
# Default to a known model supporting JSON if GEMINI_MODEL_NAME isn't set
GEMINI_MODEL_NAME     = os.environ.get("GEMINI_MODEL_NAME", "gemini-2.0-flash-latest") 
MAX_WORKERS           = int(os.environ.get("MAX_WORKERS", "8"))

RATIOS = [
    "debt_to_equity", "fcf_yield", "current_ratio", "roe",
    "gross_margin", "operating_margin", "quick_ratio", "eps",
    "eps_change", "revenue_growth", "price_trend_ratio"
]
# ────────────────────────────────────────────────────────────────────────────────


# ─── Logging Setup ───────────────────────────────────────────────────────────
# Set level based on an env var, defaulting to INFO for production jobs
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] [%(module)s:%(lineno)d] - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Silence noisy libraries unless debugging
if LOG_LEVEL != "DEBUG":
    for logger_name in ["google.api_core", "google.auth", "urllib3", "google.cloud.bigquery", "google.cloud.secretmanager"]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
logging.info(f"Logging initialized at {LOG_LEVEL} level.")
# ────────────────────────────────────────────────────────────────────────────────

# --- Validate Configuration ---
essential_vars = [
    ('PROJECT_ID', PROJECT_ID),
    ('BQ_DATASET', BQ_DATASET),
    ('BS_TABLE', BS_TABLE),
    ('IS_TABLE', IS_TABLE),
    ('CF_TABLE', CF_TABLE),
    ('PRICE_TABLE', PRICE_TABLE),
    ('METADATA_TABLE', METADATA_TABLE),
    ('RATIOS_TABLE', RATIOS_TABLE),
    ('GEMINI_SECRET_NAME', GEMINI_SECRET_NAME), # Need the secret name
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}")
    raise RuntimeError(f"Missing essential environment variables: {', '.join(missing_vars)}")

if not GCP_LIBS_AVAILABLE or not GEMINI_LIB_AVAILABLE: # Check if Gemini lib needed is available
     raise RuntimeError("Required libraries (GCP, google-generativeai) are not available.")

# --- Global Clients (Initialize later) ---
BQ_CLIENT = None
SECRET_CLIENT = None
GEMINI_MODEL = None

# ─── Secret Manager Helper ────────────────────────────────────────────────────
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def get_secret(secret_id: str, version: str = "latest") -> str:
    global SECRET_CLIENT
    if not SECRET_CLIENT:
        logging.info("Initializing Secret Manager client...")
        SECRET_CLIENT = secretmanager.SecretManagerServiceClient()

    ver = version # Use provided version or default
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{ver}"
    logging.debug(f"Accessing secret: {name}")
    try:
        response = SECRET_CLIENT.access_secret_version(request={"name": name})
        secret_value = response.payload.data.decode("utf-8").strip()
        logging.info(f"Successfully retrieved secret: {secret_id}")
        if not secret_value:
             raise ValueError(f"Secret {secret_id} version {ver} exists but is empty.")
        return secret_value
    except google.api_core.exceptions.NotFound:
        logging.error(f"Secret '{secret_id}' or version '{ver}' not found in project '{PROJECT_ID}'.")
        raise
    except Exception as e:
        logging.error(f"Failed to retrieve secret {secret_id}: {e}", exc_info=True)
        raise
# ────────────────────────────────────────────────────────────────────────────────

# --- Initialize BQ and Gemini Clients ---
def initialize_clients():
    global BQ_CLIENT, GEMINI_MODEL
    if BQ_CLIENT and GEMINI_MODEL:
        logging.debug("Clients already initialized.")
        return True

    try:
        if not BQ_CLIENT:
            logging.info(f"Initializing BigQuery client for project: {PROJECT_ID}")
            BQ_CLIENT = bigquery.Client(project=PROJECT_ID)
            logging.info("BigQuery client initialized.")

        if not GEMINI_MODEL:
            logging.info("Fetching Gemini API Key...")
            api_key = get_secret(GEMINI_SECRET_NAME, GEMINI_SECRET_VERSION)
            genai.configure(api_key=api_key)
            logging.info(f"Initializing Gemini model: {GEMINI_MODEL_NAME}")
            GEMINI_MODEL = genai.GenerativeModel(
                model_name=GEMINI_MODEL_NAME,
                generation_config=genai.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=2048,
                    response_mime_type="application/json" # Request JSON output
                )
            )
            logging.info(f"Initialized Gemini model {GEMINI_MODEL_NAME}")

        return True

    except google.auth.exceptions.DefaultCredentialsError as cred_err:
        logging.critical(f"GCP Credentials Error: Could not automatically find credentials. Ensure the Cloud Run Job is running with a Service Account that has necessary permissions. Error: {cred_err}", exc_info=True)
        raise
    except Exception as e:
        logging.critical(f"Failed to initialize BQ client or Gemini: {e}", exc_info=True)
        raise

# ─── Fetch Unprocessed Filings ─────────────────────────────────────────────────
# (get_unprocessed_filings function remains the same as in ratio_calculator (3).txt)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def get_unprocessed_filings() -> list[dict]:
    logging.info("Fetching unprocessed filings...")
    if not BQ_CLIENT: raise RuntimeError("BQ Client not initialized in get_unprocessed_filings")
    # Qualify Ticker to avoid ambiguity when joining
    sql = f"""
    SELECT
      m.Ticker AS Ticker,
      m.ReportEndDate,
      m.FiledDate,
      m.AccessionNumber
    FROM `{PROJECT_ID}.{BQ_DATASET}.{METADATA_TABLE}` m
    LEFT JOIN `{PROJECT_ID}.{BQ_DATASET}.{RATIOS_TABLE}` r
      ON m.AccessionNumber = r.accession_number
    WHERE r.accession_number IS NULL
      AND m.Ticker IS NOT NULL
      AND m.ReportEndDate IS NOT NULL
    ORDER BY m.FiledDate DESC
    """
    logging.debug(f"Executing SQL for unprocessed filings:\n{sql}")
    try:
        # Use default credentials (ADC)
        df = BQ_CLIENT.query(sql).to_dataframe(create_bqstorage_client=False)
        logging.info(f"Found {len(df)} unprocessed filings.")
        if not df.empty:
            # Convert dates safely
            df["ReportEndDate"] = pd.to_datetime(df["ReportEndDate"], errors='coerce').dt.date
            df["FiledDate"] = pd.to_datetime(df["FiledDate"], errors='coerce').dt.date
            df.dropna(subset=["ReportEndDate", "FiledDate"], inplace=True) # Drop rows where date conversion failed
            logging.debug("Converted dates for unprocessed filings.")
        return df.to_dict('records')
    except Exception as e:
        logging.error(f"Error fetching unprocessed filings: {e}", exc_info=True)
        raise
# ────────────────────────────────────────────────────────────────────────────────

# ─── Price Lookup ──────────────────────────────────────────────────────────────
# (get_price_on_or_before function remains the same as in ratio_calculator (3).txt)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def get_price_on_or_before(ticker: str, target_date: date) -> float | None:
    if not BQ_CLIENT: raise RuntimeError("BQ Client not initialized in get_price_on_or_before")
    logging.debug(f"[{ticker}] Looking for price on or before {target_date}")
    td = target_date.strftime('%Y-%m-%d')
    # Look back 7 days max - adjust if price data might lag more
    sd = (target_date - timedelta(days=7)).strftime('%Y-%m-%d')
    query = f"""
    SELECT adj_close
    FROM `{PROJECT_ID}.{BQ_DATASET}.{PRICE_TABLE}`
    WHERE ticker = @ticker_param
      AND DATE(date) <= DATE(@target_date_param)
      AND DATE(date) >= DATE(@start_date_param) -- Optimization: limit date range scan
    ORDER BY DATE(date) DESC
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker_param", "STRING", ticker),
            bigquery.ScalarQueryParameter("target_date_param", "DATE", td),
            bigquery.ScalarQueryParameter("start_date_param", "DATE", sd),
        ]
    )
    logging.debug(f"[{ticker}] Price lookup query with params: Ticker={ticker}, Target={td}, Start={sd}")
    try:
        query_job = BQ_CLIENT.query(query, job_config=job_config)
        df = query_job.to_dataframe(create_bqstorage_client=False)
        if not df.empty and pd.notna(df.adj_close.iloc[0]):
            price = float(df.adj_close.iloc[0])
            logging.debug(f"[{ticker}] Found price {price} for date <= {target_date}")
            return price
        else:
            logging.warning(f"[{ticker}] No price found for date <= {target_date} (looked back to {sd}). Query returned {len(df)} rows.")
            return None
    except Exception as e:
        logging.error(f"[{ticker}] Error fetching price for date <= {target_date}: {e}", exc_info=True)
        raise
# ────────────────────────────────────────────────────────────────────────────────

# --- Fetch BigQuery Data ---
# (fetch_bigquery_data function remains the same as in ratio_calculator (3).txt)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
def fetch_bigquery_data(ticker: str, report_end_date, prior_period: bool = False) -> dict | None:
    """
    Fetches financial statement rows joined with price for 'ticker' and 'report_end_date'.
    """
    if not BQ_CLIENT: raise RuntimeError("BQ Client not initialized in fetch_bigquery_data")
    red = pd.to_datetime(report_end_date).date()
    logging.debug(f"[{ticker}] Fetching BQ data for report end date: {red}, prior_period={prior_period}")

    def run_query(date_cond: str, cte_limit: str, outer_limit: str, query_desc: str) -> pd.DataFrame:
        logging.debug(f"[{ticker}] Running BQ query: {query_desc}")
        sql = f"""
        WITH RankedData AS (
          SELECT
            bs.*,
            inc.* EXCEPT(ticker, period_end_date),
            cf.* EXCEPT(ticker, period_end_date),
            p.adj_close        AS price_adj_close,
            bs.period_end_date AS bq_report_end_date -- Keep original date for logging
          FROM `{PROJECT_ID}.{BQ_DATASET}.{BS_TABLE}` bs
          LEFT JOIN `{PROJECT_ID}.{BQ_DATASET}.{IS_TABLE}` inc
            ON bs.ticker = inc.ticker AND bs.period_end_date = inc.period_end_date
          LEFT JOIN `{PROJECT_ID}.{BQ_DATASET}.{CF_TABLE}` cf
            ON bs.ticker = cf.ticker AND bs.period_end_date = cf.period_end_date
          LEFT JOIN `{PROJECT_ID}.{BQ_DATASET}.{PRICE_TABLE}` p
            ON bs.ticker = p.ticker AND DATE(p.date) = DATE(bs.period_end_date) -- Price on the exact report date
          WHERE bs.ticker = @ticker_param
            AND {date_cond} -- Placeholder for date condition
          {cte_limit} -- Placeholder for CTE limit/order
        )
        SELECT *
        FROM RankedData
        {outer_limit} -- Placeholder for outer limit
        """
        # Prepare parameters based on date_cond (handle DATE/TIMESTAMP)
        params = [bigquery.ScalarQueryParameter("ticker_param", "STRING", ticker)]
        # This simple replacement assumes date_cond always uses BQ date functions on columns
        # A more robust approach might involve parsing date_cond or passing specific dates
        match = re.search(r"DATE\('(\d{4}-\d{2}-\d{2})'\)", date_cond)
        if match:
             params.append(bigquery.ScalarQueryParameter("date_param", "DATE", match.group(1)))
             date_cond_prepared = date_cond.replace(f"DATE('{match.group(1)}')", "DATE(@date_param)")
        else: # Fallback for range or complex conditions - may need adjustment
            date_cond_prepared = date_cond
            # Add parameters for start/end dates if using BETWEEN
            start_match = re.search(r"DATE\('(\d{4}-\d{2}-\d{2})'\)", date_cond_prepared)
            end_match = re.search(r"AND DATE\('(\d{4}-\d{2}-\d{2})'\)", date_cond_prepared)
            if start_match:
                 params.append(bigquery.ScalarQueryParameter("start_date_param", "DATE", start_match.group(1)))
                 date_cond_prepared = date_cond_prepared.replace(f"DATE('{start_match.group(1)}')", "DATE(@start_date_param)", 1)
            if end_match:
                 params.append(bigquery.ScalarQueryParameter("end_date_param", "DATE", end_match.group(1)))
                 date_cond_prepared = date_cond_prepared.replace(f"DATE('{end_match.group(1)}')", "DATE(@end_date_param)", 1)

        sql_prepared = sql.replace("{date_cond}", date_cond_prepared).replace("{cte_limit}", cte_limit).replace("{outer_limit}", outer_limit)

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        logging.debug(f"[{ticker}] Executing SQL ({query_desc}) with params: {params}")
        try:
            query_job = BQ_CLIENT.query(sql_prepared, job_config=job_config)
            df = query_job.to_dataframe(create_bqstorage_client=False)
            logging.debug(f"[{ticker}] Query ({query_desc}) returned {len(df)} rows")
            return df
        except Exception as e:
            logging.error(f"[{ticker}] BQ query failed ({query_desc}): {e}", exc_info=True)
            raise

    # 1) Exact match
    df_exact = run_query(f"DATE(bs.period_end_date)=DATE('{red}')", "", "LIMIT 1", "Exact Match")
    if not df_exact.empty:
        row = df_exact.iloc[0].to_dict()
        matched_date = row.get("bq_report_end_date", "N/A")
        # Convert Timestamp if necessary
        if isinstance(matched_date, pd.Timestamp): matched_date = matched_date.date()
        logging.info(f"[{ticker}] Found exact BQ data match for {red} (Actual BQ Date: {matched_date})")
        cleaned = clean_data_for_json(row, ticker, "data_fetch_exact")
        cleaned["bq_report_end_date"] = str(matched_date)
        return cleaned

    # 2) Fallback ±30 days
    logging.warning(f"[{ticker}] No exact BQ data match for {red}. Trying fallback +/- 30 days.")
    start = red - timedelta(days=30)
    end   = red + timedelta(days=30)
    df_fb = run_query(
        f"DATE(bs.period_end_date) BETWEEN DATE('{start}') AND DATE('{end}')",
        f"ORDER BY ABS(DATE_DIFF(DATE(bs.period_end_date), DATE('{red}'), DAY))", # No LIMIT in CTE
        "LIMIT 1", # Apply LIMIT after ordering
        f"Fallback {start} to {end}"
    )
    if not df_fb.empty:
        row = df_fb.iloc[0].to_dict()
        matched_date = row.get("bq_report_end_date", "N/A")
        if isinstance(matched_date, pd.Timestamp): matched_date = matched_date.date()
        logging.info(f"[{ticker}] Found fallback BQ data match for {red} (Actual BQ Date: {matched_date})")
        cleaned = clean_data_for_json(row, ticker, "data_fetch_fallback")
        cleaned["bq_report_end_date"] = str(matched_date)
        return cleaned

    # 3) Prior period (~90d back ±30d)
    if prior_period:
        logging.warning(f"[{ticker}] No exact or fallback BQ data match for {red}. Trying prior period look-up.")
        pt = red - timedelta(days=90) # Target prior date
        ps = pt - timedelta(days=30) # Prior start
        pe = pt + timedelta(days=30) # Prior end
        logging.debug(f"[{ticker}] Looking for prior period data between {ps} and {pe} (target: {pt})")
        df_pr = run_query(
            f"DATE(bs.period_end_date) BETWEEN DATE('{ps}') AND DATE('{pe}')",
            f"ORDER BY ABS(DATE_DIFF(DATE(bs.period_end_date), DATE('{pt}'), DAY))", # No LIMIT in CTE
            "LIMIT 1", # Apply LIMIT after ordering
            f"Prior Period {ps} to {pe}"
        )
        if not df_pr.empty:
            row = df_pr.iloc[0].to_dict()
            matched_date = row.get("bq_report_end_date", "N/A")
            if isinstance(matched_date, pd.Timestamp): matched_date = matched_date.date()
            logging.info(f"[{ticker}] Found prior period BQ data match for target {pt} (Actual BQ Date: {matched_date})")
            cleaned = clean_data_for_json(row, ticker, "data_fetch_prior")
            cleaned["bq_report_end_date"] = str(matched_date)
            return cleaned
        else:
            logging.warning(f"[{ticker}] No prior period data found between {ps} and {pe}.")

    # nothing found
    logging.warning(f"[{ticker}] No financial data found for {report_end_date} (prior_period={prior_period}) after all checks.")
    return None
# ────────────────────────────────────────────────────────────────────────────────

# ─── Data Cleaning ─────────────────────────────────────────────────────────────
# (clean_data_for_json function remains the same as in ratio_calculator (3).txt)
def clean_data_for_json(data: dict, ticker: str, context: str = "") -> dict:
    logging.debug(f"[{ticker}] Cleaning data (context: {context}). Input keys: {list(data.keys())}")
    cleaned = {}
    # Keep original date for logging if needed, but exclude from JSON sent to Gemini
    exclude = {'ticker','period_end_date','reported_currency', 'bq_report_end_date'}
    null_count = 0
    valid_count = 0
    for k, v in data.items():
        if k in exclude:
            # logging.debug(f"[{ticker}] Excluding key '{k}'")
            continue
        if pd.isna(v) or v is None:
            # logging.debug(f"[{ticker}] Skipping null/NA value for key '{k}'")
            null_count += 1
            continue

        original_type = type(v).__name__
        try:
            # Convert Timestamps/Dates to ISO strings BEFORE checking np types
            if isinstance(v, (datetime, pd.Timestamp, date)):
                 cleaned[k] = v.isoformat()
                 # logging.debug(f"[{ticker}] Cleaned key '{k}': {original_type} -> isoformat string")
            elif isinstance(v, Decimal):
                cleaned[k] = float(v)
                # logging.debug(f"[{ticker}] Cleaned key '{k}': {original_type} -> float")
            elif isinstance(v, (np.integer, int)):
                cleaned[k] = int(v) # Ensure it's standard int
                # logging.debug(f"[{ticker}] Cleaned key '{k}': {original_type} -> int")
            elif isinstance(v, (np.floating, float)):
                # IMPORTANT: Check for NaN/Inf BEFORE conversion
                if not (np.isnan(v) or np.isinf(v)):
                    cleaned[k] = float(v) # Ensure standard float
                    # logging.debug(f"[{ticker}] Cleaned key '{k}': {original_type} -> float")
                else:
                    # logging.debug(f"[{ticker}] Skipping NaN/Inf float value for key '{k}'")
                    null_count +=1
            else:
                # Fallback: Attempt string conversion for unrecognized types
                cleaned[k] = str(v)
                # logging.debug(f"[{ticker}] Cleaned key '{k}': {original_type} -> str (fallback)")
            valid_count += 1 # Increment only if value is added
        except Exception as e:
            logging.warning(f"[{ticker}] Could not clean value for key '{k}' (type: {original_type}, value: {v}): {e}")
            null_count += 1

    logging.debug(f"[{ticker}] Data cleaning finished. Kept {valid_count} valid keys, skipped {null_count + len(exclude)} keys.")
    return cleaned
# ────────────────────────────────────────────────────────────────────────────────

# ─── Ratio Scaling/Validation ─────────────────────────────────────────────────
# (adjust_ratio_scale function remains the same as in ratio_calculator (3).txt)
def adjust_ratio_scale(ratio, name: str, ticker: str):
    # This function now primarily acts as a validator/converter
    logging.debug(f"[{ticker}] Adjusting/validating ratio '{name}': Input value = {ratio} (type: {type(ratio)})")
    if ratio is None or pd.isna(ratio):
         logging.debug(f"[{ticker}] Ratio '{name}' is None/NA. Returning None.")
         return None
    try:
        val = float(ratio)
        if np.isnan(val) or np.isinf(val):
            logging.warning(f"[{ticker}] Ratio '{name}' is NaN or Inf after float conversion ({ratio}). Returning None.")
            return None
        # Add reasonable bounds check if desired
        # if name == 'debt_to_equity' and (val < -100 or val > 100): return None
        logging.debug(f"[{ticker}] Ratio '{name}' is valid: {val}")
        return val
    except (ValueError, TypeError) as e:
        logging.warning(f"[{ticker}] Could not convert ratio '{name}' value '{ratio}' to float: {e}. Returning None.")
        return None
# ────────────────────────────────────────────────────────────────────────────────

# ─── Gemini Calculation ────────────────────────────────────────────────────────
# (calculate_ratios_with_gemini function remains the same as in ratio_calculator (3).txt)
@retry(stop=stop_after_attempt(4), wait=wait_fixed(10), reraise=True)
def calculate_ratios_with_gemini(ticker: str, red_str: str, current: dict, prior: dict | None) -> dict:
    if not GEMINI_MODEL: raise RuntimeError("Gemini Model not initialized in calculate_ratios_with_gemini")
    logging.info(f"[{ticker}] Calculating ratios with Gemini for report end date: {red_str}")
    if not current:
        logging.warning(f"[{ticker}] Cannot calculate ratios: current data is missing.")
        return {k: None for k in RATIOS} # Return None for all ratios including price_trend_ratio

    # Separate price_trend_ratio as it's calculated differently
    ptr_value = current.get('price_trend_ratio')
    cur_data = {k: v for k, v in current.items() if k != 'price_trend_ratio'}
    pri_data = {k: v for k, v in (prior or {}).items() if k != 'price_trend_ratio'}

    logging.debug(f"[{ticker}] Data for Gemini - Current keys: {list(cur_data.keys())}")
    if pri_data: logging.debug(f"[{ticker}] Data for Gemini - Prior keys: {list(pri_data.keys())}")
    else: logging.debug(f"[{ticker}] Data for Gemini - Prior data is empty.")

    # Ratios to request from Gemini
    ratios_to_calculate = [r for r in RATIOS if r != 'price_trend_ratio']
    logging.debug(f"[{ticker}] Requesting ratios from Gemini: {ratios_to_calculate}")

    prompt = f"""
    Analyze the following financial data for ticker {ticker}, focusing on the period ending around {red_str}.
    Current Period Data:
    ```json
    {json.dumps(cur_data, indent=2, default=str)}
    ```

    Prior Period Data (if available):
    ```json
    {json.dumps(pri_data, indent=2, default=str) if pri_data else '{}'}
    ```

    Instructions:
    1. Calculate the following financial ratios based *only* on the provided data: {', '.join(ratios_to_calculate)}.
    2. Use standard financial formulas. If data required for a ratio is missing or null in the 'Current Period Data', return null for that specific ratio.
    3. If prior data is needed (e.g., for growth rates) and 'Prior Period Data' is empty or missing the necessary fields, return null for that ratio.
    4. Ensure calculations are numerically sound (avoid division by zero). Return null if a calculation is impossible.
    5. Return the results as a single JSON object where keys are the ratio names (exactly as listed above) and values are the calculated ratios (as numbers) or null.
    6. Do not include explanations or any other text outside the JSON object.
    7. Example Output Format:
       {{
         "debt_to_equity": 0.5, "fcf_yield": 0.08, "current_ratio": 1.8, "roe": 0.15,
         "gross_margin": 0.6, "operating_margin": 0.2, "quick_ratio": 1.1, "eps": 2.50,
         "eps_change": 0.10, "revenue_growth": 0.05
       }}

    Output only the JSON object.
    """
    logging.debug(f"[{ticker}] Sending prompt to Gemini (first 500 chars): {prompt[:500]}...")

    json_text = "" # Define outside try block
    try:
        resp = GEMINI_MODEL.generate_content(prompt)
        logging.debug(f"[{ticker}] Raw response text (first 500 chars): {resp.text[:500] if resp.text else 'None'}")

        if not resp.text or not resp.text.strip():
             logging.error(f"[{ticker}] Received empty response from Gemini.")
             raise ValueError("Empty response from Gemini")

        # Improved regex to handle potential leading/trailing whitespace and variations
        text_content = resp.text.strip()
        # Try finding JSON object robustly
        json_start = text_content.find('{')
        json_end = text_content.rfind('}') + 1
        if json_start == -1 or json_end == 0:
             logging.error(f"[{ticker}] Could not find JSON object markers {{ ... }} in Gemini response: {text_content}")
             raise ValueError("No JSON object found in Gemini response")

        json_text = text_content[json_start:json_end]
        logging.debug(f"[{ticker}] Extracted JSON text: {json_text}")

        metrics = json.loads(json_text)
        logging.info(f"[{ticker}] Successfully parsed Gemini response.")
        logging.debug(f"[{ticker}] Parsed metrics from Gemini: {metrics}")

        # Validate and adjust scale for all ratios received from Gemini
        out = {name: adjust_ratio_scale(metrics.get(name), name, ticker) for name in ratios_to_calculate}

    except json.JSONDecodeError as e:
        logging.error(f"[{ticker}] Failed to decode JSON from Gemini response: {e}", exc_info=True)
        logging.error(f"[{ticker}] Faulty JSON text was: {json_text}")
        out = {name: None for name in ratios_to_calculate} # Return nulls for ratios
    except (ValueError, Exception) as e: # Catch broader errors
        logging.error(f"[{ticker}] Error during Gemini ratio calculation or response processing: {e}", exc_info=True)
        out = {name: None for name in ratios_to_calculate} # Return nulls for ratios

    # Add the separately calculated price_trend_ratio
    out['price_trend_ratio'] = adjust_ratio_scale(ptr_value, 'price_trend_ratio', ticker)
    logging.debug(f"[{ticker}] Final calculated/adjusted ratios: {out}")
    return out
# ────────────────────────────────────────────────────────────────────────────────

# --- Insert Row ───────────────────────────────────────────────────────────────
# (insert_row_to_bigquery function remains the same as in ratio_calculator (3).txt)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(3), reraise=True)
def insert_row_to_bigquery(table_id: str, row: dict, ticker: str):
    if not BQ_CLIENT: raise RuntimeError("BQ Client not initialized in insert_row_to_bigquery")
    logging.debug(f"[{ticker}] Preparing row for BigQuery insertion into {table_id}")
    clean_row = {}
    null_ratios = []
    valid_ratio_count = 0
    # Ensure all expected RATIOS keys exist, defaulting to None if missing from input `row` dict
    for ratio_name in RATIOS:
         row_value = row.get(ratio_name) # Get value or None
         if row_value is None or (isinstance(row_value, float) and (np.isnan(row_value) or np.isinf(row_value))):
              clean_row[ratio_name] = None
              null_ratios.append(ratio_name)
         else:
              clean_row[ratio_name] = row_value # Assume already validated float
              valid_ratio_count += 1

    # Add non-ratio fields
    for k, v in row.items():
        if k not in RATIOS:
             clean_row[k] = v # Copy other fields like ticker, dates, etc.

    logging.debug(f"[{ticker}] Cleaned row for insertion: {clean_row}")
    if len(null_ratios) == len(RATIOS):
         logging.error(f"[{ticker}] ALL {len(RATIOS)} ratios are NULL for accession {row.get('accession_number')}. Skipping BQ insert for this row.")
         return # Do not insert if all ratios are null
    elif null_ratios:
         logging.warning(f"[{ticker}] {len(null_ratios)} ratios are NULL in the row being inserted: {', '.join(null_ratios)}")

    logging.info(f"[{ticker}] Inserting row with {valid_ratio_count}/{len(RATIOS)} valid ratios for accession {row.get('accession_number')}")
    try:
        # Define schema explicitly for insert_rows_json to handle None correctly
        # Note: Terraform should create the table with the correct schema
        table = BQ_CLIENT.get_table(table_id) # Get table to access schema if needed, or define static
        errors = BQ_CLIENT.insert_rows_json(table_id, [clean_row]) # Removed schema=table.schema
        if errors:
            logging.error(f"[{ticker}] BigQuery insert errors occurred for accession {row.get('accession_number')}: {errors}")
            raise bigquery.QueryJobError(f"BigQuery insert failed for {ticker}: {errors}")
        else:
            logging.debug(f"[{ticker}] Successfully inserted row for accession {row.get('accession_number')}")
    except Exception as e:
        logging.error(f"[{ticker}] Exception during BigQuery insert for accession {row.get('accession_number')}: {e}", exc_info=True)
        raise # Re-raise to handle in the calling function / retry logic
# ────────────────────────────────────────────────────────────────────────────────


# ─── Filing Processor ──────────────────────────────────────────────────────────
# (process_filing function remains largely the same as in ratio_calculator (3).txt,
#  ensure BQ_CLIENT and GEMINI_MODEL are checked for initialization if called directly)
def process_filing(filing: dict, table_id: str, lock: threading.Lock, counters: dict) -> bool:
    if not BQ_CLIENT or not GEMINI_MODEL:
        logging.error(f"Clients not initialized in process_filing for Ticker={filing.get('Ticker','N/A')}")
        return False # Cannot proceed without clients

    start_time = time.time()
    ticker = filing.get('Ticker', 'MISSING_TICKER')
    acc    = filing.get('AccessionNumber', 'MISSING_ACCESSION')
    red    = filing.get('ReportEndDate') # Date object
    fd     = filing.get('FiledDate')     # Date object

    if not all([ticker, acc, red, fd]):
         logging.error(f"Skipping filing due to missing essential data: Ticker={ticker}, Acc={acc}, ReportEnd={red}, Filed={fd}")
         with lock:
             counters['other_errors'] += 1 # Count as other error
         return False

    logging.info(f"[{ticker}] Processing filing: Acc={acc}, ReportEnd={red}, Filed={fd}")
    rd_str = red.strftime('%Y-%m-%d')
    fd_str = fd.strftime('%Y-%m-%d')

    # --- Price Trend Ratio Calculation ---
    ptr = None
    p20 = None
    p50 = None
    try:
        date_20_days_prior = fd - timedelta(days=20)
        date_50_days_prior = fd - timedelta(days=50)
        logging.debug(f"[{ticker}] Getting price for PTR near {date_20_days_prior} (20d)")
        p20 = get_price_on_or_before(ticker, date_20_days_prior)
        # time.sleep(0.1) # Delay might not be needed if BQ handles load
        logging.debug(f"[{ticker}] Getting price for PTR near {date_50_days_prior} (50d)")
        p50 = get_price_on_or_before(ticker, date_50_days_prior)

        if p20 is not None and p50 is not None:
            if p50 != 0:
                ptr = p20 / p50
                logging.info(f"[{ticker}] Calculated PTR = {p20} / {p50} = {ptr}")
            else:
                logging.warning(f"[{ticker}] Cannot calculate PTR: p50 is zero (p20={p20}).")
                ptr = None
        else:
            logging.warning(f"[{ticker}] Cannot calculate PTR: p20={p20}, p50={p50}")
            ptr = None
    except Exception as e:
        logging.error(f"[{ticker}] Error calculating price trend ratio for Acc={acc}: {e}", exc_info=True)
        ptr = None # Ensure ptr is None if any error occurs

    # --- Fetch Financial Data ---
    cur = None
    pri = None
    try:
        logging.debug(f"[{ticker}] Fetching current financial data for {red}")
        cur = fetch_bigquery_data(ticker, red, prior_period=False)
        if cur:
            logging.info(f"[{ticker}] Successfully fetched current financial data.")
            logging.debug(f"[{ticker}] Fetching prior financial data relative to {red}")
            pri = fetch_bigquery_data(ticker, red, prior_period=True)
            if pri: logging.info(f"[{ticker}] Successfully fetched prior financial data.")
            else: logging.warning(f"[{ticker}] Prior financial data not found.")
        else:
            logging.warning(f"[{ticker}] Current financial data not found for {red}. Cannot calculate most ratios.")
    except Exception as e:
        logging.error(f"[{ticker}] Error fetching financial data for Acc={acc}: {e}", exc_info=True)
        cur = None
        pri = None

    # --- Calculate Ratios ---
    ratios = {k: None for k in RATIOS} # Initialize all to None

    if cur:
        cur['price_trend_ratio'] = ptr # Add PTR to current data
        logging.debug(f"[{ticker}] Added PTR ({ptr}) to current data before Gemini call.")
        try:
            logging.info(f"[{ticker}] Calling Gemini for ratio calculation (Acc={acc})")
            gemini_ratios = calculate_ratios_with_gemini(ticker, rd_str, cur, pri)
            ratios.update(gemini_ratios)
            logging.info(f"[{ticker}] Received ratios from Gemini/calculation step.")
            logging.debug(f"[{ticker}] Ratios after Gemini/calc step: {ratios}")
        except Exception as e:
            logging.error(f"[{ticker}] Error calling Gemini or processing response for Acc={acc}: {e}", exc_info=True)
            ratios['price_trend_ratio'] = adjust_ratio_scale(ptr, 'price_trend_ratio', ticker) # Keep PTR even if Gemini fails
            logging.warning(f"[{ticker}] Setting Gemini-calculated ratios to None due to error. PTR kept: {ratios['price_trend_ratio']}")
            with lock: counters['gemini_errors'] += 1 # Use specific counter
    else:
        logging.warning(f"[{ticker}] Skipping Gemini call as current financial data is missing (Acc={acc}).")
        ratios['price_trend_ratio'] = adjust_ratio_scale(ptr, 'price_trend_ratio', ticker)
        logging.info(f"[{ticker}] Setting only Price Trend Ratio = {ratios['price_trend_ratio']} due to missing data.")

    # --- Prepare Final Row ---
    row = {
        'ticker': ticker,
        'accession_number': acc,
        'report_end_date': rd_str,
        'filed_date': fd_str,
        **ratios, # Add all calculated/adjusted ratios
        'data_source': 'bigquery' if cur else 'none', # Indicate if BQ data was found
        'created_at': datetime.utcnow().isoformat() + "Z" # Add UTC timezone indicator
    }
    logging.debug(f"[{ticker}] Final row data prepared for Acc={acc}: {row}")

    # --- Insert Row ---
    table_full_id = f"{PROJECT_ID}.{BQ_DATASET}.{RATIOS_TABLE}"
    try:
        insert_row_to_bigquery(table_full_id, row, ticker)
        with lock: counters['processed'] += 1
        logging.info(f"[{ticker}] Successfully processed and inserted filing Acc={acc}. Took {time.time()-start_time:.2f}s")
        return True
    except Exception as e:
        # Error already logged in insert_row_to_bigquery
        logging.error(f"[{ticker}] Failed to insert row for Acc={acc}. Took {time.time()-start_time:.2f}s")
        with lock: counters['insert_errors'] += 1
        return False
# ────────────────────────────────────────────────────────────────────────────────


# ─── Main Table Builder / Job Entry Point ──────────────────────────────────────
def run_ratio_calculation_job():
    logging.info("Starting ratio calculation job...")
    if not initialize_clients(): # Ensure clients are ready
         logging.critical("Client initialization failed. Aborting job.")
         return # Or raise an exception

    filings = get_unprocessed_filings()
    if not filings:
        logging.info("No unprocessed filings found. Exiting.")
        return

    logging.info(f"Found {len(filings)} unprocessed filings to process.")
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{RATIOS_TABLE}" # Used only for logging here
    logging.info(f"Target table for inserts: {table_id}")

    # ** REMOVED TABLE DELETION/CREATION **
    # Terraform is now responsible for ensuring the table exists with the correct schema.
    # We just proceed assuming the table exists. If it doesn't, insert_row_to_bigquery will fail.
    logging.info("Proceeding with ratio calculation assuming target table exists.")

    counters = {'processed': 0, 'insert_errors': 0, 'gemini_errors': 0, 'other_errors': 0}
    lock = threading.Lock()

    logging.info(f"Starting processing with up to {MAX_WORKERS} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="RatioCalc") as executor:
        # Pass the full table ID to process_filing, though it recalculates it internally now
        # Could refactor process_filing to accept table_id directly if preferred
        futures = [executor.submit(process_filing, f, table_id, lock, counters) for f in filings]

        # Using tqdm here might just clutter logs in Cloud Run, consider removing or conditional based on environment
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing filings"):
            try:
                result = future.result()
                if not result:
                     logging.debug("A filing processing task completed with an error flag (False).")
            except Exception as exc:
                logging.error(f"An unexpected error occurred in a worker thread: {exc}", exc_info=True)
                with lock: counters['other_errors'] += 1

    logging.info(f"Ratio calculation job finished.")
    logging.info(f"Summary: Processed={counters['processed']}, Insert Errors={counters['insert_errors']}, Gemini Errors={counters['gemini_errors']}, Other Errors={counters['other_errors']}")
    total_errors = counters['insert_errors'] + counters['gemini_errors'] + counters['other_errors']
    if total_errors > 0:
         logging.warning(f"Total errors encountered: {total_errors}")

# ────────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    start_time = time.time()
    logging.info("Ratio Calculator Job script execution started.")
    try:
        run_ratio_calculation_job()
    except Exception as main_err:
         logging.critical(f"Critical error during job execution: {main_err}", exc_info=True)
         # Optionally exit with non-zero status code for Cloud Run Job failure detection
         # import sys
         # sys.exit(1)
    finally:
        end_time = time.time()
        logging.info(f"Ratio Calculator Job script execution finished.")
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")
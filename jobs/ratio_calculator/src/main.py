#!/usr/bin/env python3
# ratio_calculator.py — Batch ratio calculation using GCS CSVs

import os
import logging
import json
import re
import threading
import time
import concurrent.futures
from datetime import datetime, timedelta, date
from decimal import Decimal
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import io 

import pandas as pd
import numpy as np
from tqdm import tqdm

# --- Google Gen AI SDK Imports ---
from google import genai
from google.genai import types

# --- GCP & Lib Imports ---
try:
    from google.cloud import bigquery, secretmanager, storage
    from google.api_core import exceptions
    import google.auth
    GCP_LIBS_AVAILABLE = True
except ImportError:
    logging.error("Failed to import Google Cloud libraries.")
    GCP_LIBS_AVAILABLE = False

# --- Configuration ---
PROJECT_ID            = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET            = os.environ.get("BQ_DATASET_ID")
PRICE_TABLE           = os.environ.get("BQ_PRICE_TABLE_ID", os.environ.get("BQ_PRICES_TABLE_ID", "price_data"))
METADATA_TABLE        = os.environ.get("BQ_METADATA_TABLE_ID")
RATIOS_TABLE          = os.environ.get("BQ_RATIOS_TABLE_ID")
GCS_BUCKET_NAME       = os.environ.get("GCS_BUCKET_NAME", "profit-scout")
GCS_BS_PREFIX         = os.environ.get("GCS_BS_PREFIX", "balance-sheet/")
GCS_IS_PREFIX         = os.environ.get("GCS_IS_PREFIX", "income-statement/")
GCS_CF_PREFIX         = os.environ.get("GCS_CF_PREFIX", "cash-flow/")
GEMINI_SECRET_NAME    = os.environ.get("GEMINI_API_KEY_SECRET_ID")
GEMINI_SECRET_VERSION = os.environ.get("GEMINI_API_KEY_SECRET_VERSION", "latest")
GEMINI_MODEL_NAME     = os.environ.get("GEMINI_MODEL_NAME", "gemini-2.0-flash-001")
MAX_WORKERS           = int(os.environ.get("MAX_WORKERS", "8"))

RATIOS = [
    "debt_to_equity", "fcf_yield", "current_ratio", "roe",
    "gross_margin", "operating_margin", "quick_ratio", "eps",
    "eps_change", "revenue_growth", "price_trend_ratio"
]

# --- Logging Setup ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] [%(module)s:%(lineno)d] - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
if LOG_LEVEL != "DEBUG":
    for logger_name in ["google.api_core", "google.auth", "urllib3", "google.cloud.bigquery", "google.cloud.secretmanager"]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
logging.info(f"Logging initialized at {LOG_LEVEL} level.")

# --- Validate Configuration ---
essential_vars = [
    ('PROJECT_ID', PROJECT_ID),
    ('BQ_DATASET', BQ_DATASET),
    ('PRICE_TABLE', PRICE_TABLE),
    ('METADATA_TABLE', METADATA_TABLE),
    ('RATIOS_TABLE', RATIOS_TABLE),
    ('GCS_BUCKET_NAME', GCS_BUCKET_NAME),
    ('GCS_BS_PREFIX', GCS_BS_PREFIX),
    ('GCS_IS_PREFIX', GCS_IS_PREFIX),
    ('GCS_CF_PREFIX', GCS_CF_PREFIX),
    ('GEMINI_SECRET_NAME', GEMINI_SECRET_NAME),
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}")
    raise RuntimeError(f"Missing essential environment variables: {', '.join(missing_vars)}")

if not GCP_LIBS_AVAILABLE:
    raise RuntimeError("Required libraries (GCP) are not available.")

# --- Global Clients ---
BQ_CLIENT = None
SECRET_CLIENT = None
GENAI_CLIENT = None
STORAGE_CLIENT = None

# --- Secret Manager Helper ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def get_secret(secret_id: str, version: str = "latest") -> str:
    global SECRET_CLIENT
    if not SECRET_CLIENT:
        SECRET_CLIENT = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version}"
    response = SECRET_CLIENT.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8").strip()

# --- Initialize Clients ---
def initialize_clients():
    global BQ_CLIENT, GENAI_CLIENT, STORAGE_CLIENT
    if BQ_CLIENT and GENAI_CLIENT and STORAGE_CLIENT:
        return True

    BQ_CLIENT = bigquery.Client(project=PROJECT_ID)
    STORAGE_CLIENT = storage.Client(project=PROJECT_ID)
    api_key = get_secret(GEMINI_SECRET_NAME, GEMINI_SECRET_VERSION)
    GENAI_CLIENT = genai.Client(api_key=api_key)
    return True
    
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
def get_price_on_or_before(ticker: str, target_date: date) -> float | None:
    """
    Returns the adjusted close for `ticker` on or before `target_date`,
    looking back up to 7 calendar days.
    """
    if not BQ_CLIENT:
        raise RuntimeError("BQ Client not initialized.")
    sd = (target_date - timedelta(days=7)).strftime('%Y-%m-%d')
    td = target_date.strftime('%Y-%m-%d')

    sql = f"""
    SELECT adj_close
      FROM `{PROJECT_ID}.{BQ_DATASET}.{PRICE_TABLE}`
     WHERE ticker = @ticker
       AND DATE(date) BETWEEN DATE(@sd) AND DATE(@td)
     ORDER BY DATE(date) DESC
     LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("sd", "DATE", sd),
            bigquery.ScalarQueryParameter("td", "DATE", td),
        ]
    )
    df = BQ_CLIENT.query(sql, job_config=job_config)\
                   .to_dataframe(create_bqstorage_client=False)
    return float(df.adj_close.iloc[0]) if not df.empty else None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
def get_unprocessed_filings() -> list[dict]:
    """
    Pulls your metadata table, left-joins your ratios table, 
    and returns any filings that don't yet have ratios.
    """
    if not BQ_CLIENT:
        raise RuntimeError("BQ Client not initialized.")
    sql = f"""
    SELECT m.Ticker, m.ReportEndDate, m.FiledDate, m.AccessionNumber
      FROM {PROJECT_ID}.{BQ_DATASET}.{METADATA_TABLE} m
      LEFT JOIN {PROJECT_ID}.{BQ_DATASET}.{RATIOS_TABLE} r
        ON m.AccessionNumber = r.accession_number
     WHERE r.accession_number IS NULL
       AND m.Ticker IS NOT NULL
       AND m.ReportEndDate IS NOT NULL
     ORDER BY m.FiledDate DESC
    """
    df = BQ_CLIENT.query(sql).to_dataframe(create_bqstorage_client=False)
    df["ReportEndDate"] = pd.to_datetime(df["ReportEndDate"]).dt.date
    df["FiledDate"]      = pd.to_datetime(df["FiledDate"]).dt.date
    return df.to_dict('records')

# --- Fetch GCS Data ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
def fetch_gcs_data(ticker: str, accession_number: str, report_end_date, prior_period: bool = False) -> dict | None:
    if not STORAGE_CLIENT:
        raise RuntimeError("Storage Client not initialized in fetch_gcs_data")
    red = pd.to_datetime(report_end_date).date()
    logging.debug(f"[{ticker}] Fetching GCS data for report end date: {red}, prior_period={prior_period}")

    # Function to read a CSV from GCS, with hyphens removed from accession number
    def read_csv_from_gcs(prefix: str) -> pd.DataFrame:
        clean_acc = accession_number.replace('-', '')
        file_name = f"{prefix}{ticker}_{clean_acc}.csv"
        try:
            bucket = STORAGE_CLIENT.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(file_name)
            csv_data = blob.download_as_string()
            df = pd.read_csv(io.BytesIO(csv_data))
            logging.debug(f"[{ticker}] Loaded {file_name}: Columns: {df.columns.tolist()}, Rows: {len(df)}")
            return df
        except Exception as e:
            logging.warning(f"[{ticker}] Failed to load {file_name}: {e}")
            return pd.DataFrame()

    # Load data for the current period
    df_bs = read_csv_from_gcs(GCS_BS_PREFIX)
    df_is = read_csv_from_gcs(GCS_IS_PREFIX)
    df_cf = read_csv_from_gcs(GCS_CF_PREFIX)

    if df_bs.empty and df_is.empty and df_cf.empty:
        logging.warning(f"[{ticker}] No GCS data found for {red}.")
        return None

    # Merge data from all DataFrames
    data = {}
    if not df_bs.empty:
        data.update(df_bs.iloc[0].to_dict())
    if not df_is.empty:
        data.update(df_is.iloc[0].to_dict())
    if not df_cf.empty:
        data.update(df_cf.iloc[0].to_dict())

    if not data:
        logging.warning(f"[{ticker}] No consolidated GCS data for {red}.")
        return None

    # Add price data (still from BigQuery)
    price = get_price_on_or_before(ticker, red)
    logging.info(f"[{ticker}] Price for {red}: {price}")
    if price is not None:
        data['price_adj_close'] = price
    else:
        logging.warning(f"[{ticker}] No price data found for {red}")

    logging.info(f"[{ticker}] Balance Sheet Columns: {df_bs.columns.tolist() if not df_bs.empty else 'Empty'}")
    logging.info(f"[{ticker}] Income Statement Columns: {df_is.columns.tolist() if not df_is.empty else 'Empty'}")
    logging.info(f"[{ticker}] Cash Flow Columns: {df_cf.columns.tolist() if not df_cf.empty else 'Empty'}")
    cleaned = clean_data_for_json(data, ticker, "data_fetch_gcs")
    logging.info(f"[{ticker}] Cleaned Data for LLM: {cleaned}")
    cleaned["bq_report_end_date"] = str(red)
    return cleaned

# --- Data Cleaning ---
def clean_data_for_json(data: dict, ticker: str, context: str = "") -> dict:
    logging.debug(f"[{ticker}] Cleaning data (context: {context}). Input keys: {list(data.keys())}")
    cleaned = {}
    exclude = {'ticker', 'accession_number', 'period_end_date', 'filing_date', 'reported_currency', 'bq_report_end_date'}
    null_count = 0
    valid_count = 0
    for k, v in data.items():
        if k in exclude:
            continue
        if pd.isna(v) or v is None:
            null_count += 1
            continue

        original_type = type(v).__name__
        try:
            if isinstance(v, (datetime, pd.Timestamp, date)):
                cleaned[k] = v.isoformat()
            elif isinstance(v, Decimal):
                cleaned[k] = float(v)
            elif isinstance(v, (np.integer, int)):
                cleaned[k] = int(v)
            elif isinstance(v, (np.floating, float)):
                if not (np.isnan(v) or np.isinf(v)):
                    cleaned[k] = float(v)
                else:
                    null_count += 1
            else:
                cleaned[k] = str(v)
            valid_count += 1
        except Exception as e:
            logging.warning(f"[{ticker}] Could not clean value for key '{k}' (type: {original_type}, value: {v}): {e}")
            null_count += 1

    logging.debug(f"[{ticker}] Data cleaning finished. Kept {valid_count} valid keys, skipped {null_count + len(exclude)} keys.")
    return cleaned

# --- Ratio Scaling/Validation ---
def adjust_ratio_scale(ratio, name: str, ticker: str):
    logging.debug(f"[{ticker}] Adjusting/validating ratio '{name}': Input value = {ratio} (type: {type(ratio)})")
    if ratio is None or pd.isna(ratio):
        logging.debug(f"[{ticker}] Ratio '{name}' is None/NA. Returning None.")
        return None
    try:
        val = float(ratio)
        if np.isnan(val) or np.isinf(val):
            logging.warning(f"[{ticker}] Ratio '{name}' is NaN or Inf after float conversion ({ratio}). Returning None.")
            return None
        logging.debug(f"[{ticker}] Ratio '{name}' is valid: {val}")
        return val
    except (ValueError, TypeError) as e:
        logging.warning(f"[{ticker}] Could not convert ratio '{name}' value '{ratio}' to float: {e}. Returning None.")
        return None

# --- GenAI Calculation ---
@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def calculate_ratios_with_genai(ticker: str, red_str: str, current: dict, prior: dict | None) -> dict:
    if not GENAI_CLIENT:
        raise RuntimeError("GenAI client not initialized in calculate_ratios_with_genai")
    logging.info(f"[{ticker}] Calculating ratios with GenAI for report end date: {red_str}")

    if not current:
        logging.warning(f"[{ticker}] Cannot calculate ratios: current data is missing.")
        return {k: None for k in RATIOS}

    ptr_value = current.get("price_trend_ratio")
    cur_data = {k: v for k, v in current.items() if k != "price_trend_ratio"}
    pri_data = {k: v for k, v in (prior or {}).items() if k != "price_trend_ratio"}

    logging.debug(f"[{ticker}] Data for GenAI - Current keys: {list(cur_data.keys())}")
    if pri_data:
        logging.debug(f"[{ticker}] Data for GenAI - Prior keys: {list(pri_data.keys())}")
    else:
        logging.debug(f"[{ticker}] Data for GenAI - Prior data is empty.")

    ratios_to_calculate = [r for r in RATIOS if r != "price_trend_ratio"]
    logging.debug(f"[{ticker}] Requesting ratios from GenAI: {ratios_to_calculate}")

    prompt = f"""
    Analyze the following financial data for ticker {ticker}, focusing on the period ending around {red_str}.
    Current Period Data:
    ```json
    {json.dumps(cur_data, indent=2, default=str)}
    ```

    Prior Period Data (if available):
    ```json
    {json.dumps(pri_data, indent=2, default=str) if pri_data else '{{}}'}
    ```

    Instructions:
    1. Calculate the following financial ratios based only on the provided data: {', '.join(ratios_to_calculate)}.
    2. Use the following formulas for calculations. If any required field is missing, null, NaN, or causes an invalid calculation (e.g., division by zero), return null for that ratio:
       - fcf_yield: Free Cash Flow Yield = Free Cash Flow / Market Capitalization
         - Free Cash Flow = netcashprovidedbyusedinoperatingactivities - paymentstoacquireotherproductiveassets
         - Market Capitalization = shares_outstanding * price_adj_close
         - Return null if netcashprovidedbyusedinoperatingactivities, paymentstoacquireotherproductiveassets, shares_outstanding, or price_adj_close is missing, null, or if Market Capitalization is zero or negative.
       - debt_to_equity: Total Debt / Total Equity
         - Total Debt = total_liabilities (or sum of short_term_debt and long_term_debt if available)
         - Return null if total_liabilities or total_equity is missing, null, or if total_equity is zero.
       - current_ratio: Current Assets / Current Liabilities
         - Return null if current_assets or current_liabilities is missing, null, or if current_liabilities is zero.
       - roe: Return on Equity = Net Income / Total Equity
         - Return null if netincomeloss or total_equity is missing, null, or if total_equity is zero.
       - gross_margin: Gross Profit / Total Revenue
         - Gross Profit = revenuefromcontractwithcustomerexcludingassessedtax - costofrevenue
         - Return null if revenuefromcontractwithcustomerexcludingassessedtax or costofrevenue is missing, null, or if revenuefromcontractwithcustomerexcludingassessedtax is zero.
       - operating_margin: Operating Income / Total Revenue
         - Return null if operatingincomeloss or revenuefromcontractwithcustomerexcludingassessedtax is missing, null, or if revenuefromcontractwithcustomerexcludingassessedtax is zero.
       - quick_ratio: (Current Assets - Inventory) / Current Liabilities
         - Return null if current_assets, inventorynet, or current_liabilities is missing, null, or if current_liabilities is zero.
       - eps: Earnings Per Share = Net Income / shares_outstanding
         - Return null if netincomeloss or shares_outstanding is missing, null, or if shares_outstanding is zero.
       - eps_change: (Current EPS - Prior EPS) / Prior EPS
         - Use prior period data for Prior EPS. Return null if prior data is missing, null, or if Prior EPS is zero.
       - revenue_growth: (Current Total Revenue - Prior Total Revenue) / Prior Total Revenue
         - Use prior period data. Return null if prior data is missing, null, or if Prior Total Revenue is zero.
    3. Ensure calculations are numerically sound:
       - Avoid division by zero.
       - Handle negative values appropriately (e.g., negative FCF or margins are valid).
       - Return null for any ratio if the result is NaN or infinite.
    4. Return a JSON object where keys are the ratio names (exactly as listed) and values are floating-point numbers or null.
    5. Conform to this JSON schema:
       {{
         "type": "object",
         "properties": {{ {', '.join(f'"{r}": {{"type": ["number", "null"]}}' for r in ratios_to_calculate)} }},
         "required": [{', '.join(f'"{r}"' for r in ratios_to_calculate)}]
       }}
    6. Do not include explanations or text outside the JSON object.
    Example Output:
    {{
      "debt_to_equity": 0.5, "fcf_yield": 0.08, "current_ratio": 1.8, "roe": 0.15,
      "gross_margin": 0.6, "operating_margin": 0.2, "quick_ratio": 1.1, "eps": 2.50,
      "eps_change": 0.10, "revenue_growth": 0.05
    }}
    """
    logging.debug(f"[{ticker}] Sending prompt to Gemini (first 500 chars): {prompt[:500]}...")

    try:
        response = GENAI_CLIENT.models.generate_content(
            model=GEMINI_MODEL_NAME,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=2048,
                response_mime_type="application/json"
            )
        )
        logging.debug(f"[{ticker}] Raw response text (first 500 chars): {response.text[:500] if response.text else 'None'}")

        if not response.text or not response.text.strip():
            logging.error(f"[{ticker}] Received empty response from GenAI.")
            raise ValueError("Empty response from GenAI")

        json_text = response.text.strip()
        try:
            metrics = json.loads(json_text)
            if not isinstance(metrics, dict):
                raise ValueError(f"GenAI response is not a JSON object: {json_text}")
            unexpected_keys = set(metrics) - set(ratios_to_calculate)
            if unexpected_keys:
                logging.warning(f"[{ticker}] Unexpected keys in GenAI response: {unexpected_keys}")
        except json.JSONDecodeError as e:
            logging.error(f"[{ticker}] Failed to decode JSON from GenAI response: {e}. Response: {json_text}")
            raise

        logging.info(f"[{ticker}] Successfully parsed GenAI response. Metrics: {metrics}")
        out = {name: adjust_ratio_scale(metrics.get(name), name, ticker) for name in ratios_to_calculate}

    except Exception as e:
        logging.error(f"[{ticker}] Error processing GenAI response for ratios: {e}", exc_info=True)
        out = {name: None for name in ratios_to_calculate}

    out["price_trend_ratio"] = adjust_ratio_scale(ptr_value, "price_trend_ratio", ticker)
    logging.info(f"[{ticker}] Final calculated/adjusted ratios: {out}")
    return out

# --- Insert Row ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
def insert_row_to_bigquery(table_id: str, row: dict, ticker: str):
    if not BQ_CLIENT:
        raise RuntimeError("BQ Client not initialized in insert_row_to_bigquery")
    logging.debug(f"[{ticker}] Preparing row for BigQuery insertion into {table_id}")
    clean_row = {}
    null_ratios = []
    valid_ratio_count = 0
    for ratio_name in RATIOS:
        row_value = row.get(ratio_name)
        if row_value is None or (isinstance(row_value, float) and (np.isnan(row_value) or np.isinf(row_value))):
            clean_row[ratio_name] = None
            null_ratios.append(ratio_name)
        else:
            clean_row[ratio_name] = row_value
            valid_ratio_count += 1

    for k, v in row.items():
        if k not in RATIOS:
            clean_row[k] = v

    logging.debug(f"[{ticker}] Cleaned row for insertion: {clean_row}")
    if len(null_ratios) == len(RATIOS):
        logging.error(f"[{ticker}] ALL {len(RATIOS)} ratios are NULL for accession {row.get('accession_number')}. Skipping BQ insert.")
        return

    if null_ratios:
        logging.warning(f"[{ticker}] {len(null_ratios)} ratios are NULL in the row being inserted: {', '.join(null_ratios)}")

    logging.info(f"[{ticker}] Inserting row with {valid_ratio_count}/{len(RATIOS)} valid ratios for accession {row.get('accession_number')}")
    try:
        table = BQ_CLIENT.get_table(table_id)
        expected_fields = set(RATIOS + ['ticker', 'accession_number', 'report_end_date', 'filed_date', 'data_source', 'created_at'])
        actual_fields = {f.name for f in table.schema}
        if not expected_fields.issubset(actual_fields):
            logging.error(f"[{ticker}] Table schema mismatch: missing {expected_fields - actual_fields}")
            raise ValueError("Table schema mismatch")

        errors = BQ_CLIENT.insert_rows_json(table_id, [clean_row])
        if errors:
            logging.error(f"[{ticker}] BigQuery insert errors occurred for accession {row.get('accession_number')}: {errors}")
            raise bigquery.QueryJobError(f"BigQuery insert failed for {ticker}: {errors}")
        logging.debug(f"[{ticker}] Successfully inserted row for accession {row.get('accession_number')}")
    except Exception as e:
        logging.error(f"[{ticker}] Exception during BigQuery insert for accession {row.get('accession_number')}: {e}", exc_info=True)
        raise

# --- Find Prior Filing ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
def find_prior_filing(ticker: str, current_accession: str) -> dict | None:
    if not BQ_CLIENT:
        raise RuntimeError("BQ Client not initialized in find_prior_filing")
    sql = f"""
    SELECT m.Ticker, m.ReportEndDate, m.FiledDate, m.AccessionNumber
    FROM {PROJECT_ID}.{BQ_DATASET}.{METADATA_TABLE} m
    WHERE m.Ticker = @ticker
      AND m.AccessionNumber != @current_accession
      AND m.ReportEndDate < (
        SELECT ReportEndDate
        FROM {PROJECT_ID}.{BQ_DATASET}.{METADATA_TABLE}
        WHERE AccessionNumber = @current_accession
      )
    ORDER BY m.ReportEndDate DESC
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("current_accession", "STRING", current_accession)
        ]
    )
    df = BQ_CLIENT.query(sql, job_config=job_config).to_dataframe(create_bqstorage_client=False)
    if df.empty:
        return None
    df["ReportEndDate"] = pd.to_datetime(df["ReportEndDate"]).dt.date
    df["FiledDate"] = pd.to_datetime(df["FiledDate"]).dt.date
    return df.to_dict('records')[0]

# --- Filing Processor ---
def process_filing(filing: dict, table_id: str, lock: threading.Lock, counters: dict) -> bool:
    if not BQ_CLIENT or not GENAI_CLIENT or not STORAGE_CLIENT:
        logging.error(f"Clients not initialized in process_filing for Ticker={filing.get('Ticker','N/A')}")
        return False

    start_time = time.time()
    ticker = filing.get('Ticker', 'MISSING_TICKER')
    acc = filing.get('AccessionNumber', 'MISSING_ACCESSION')
    red = filing.get('ReportEndDate')
    fd = filing.get('FiledDate')

    if not all([ticker, acc, red, fd]):
        logging.error(f"Skipping filing due to missing essential data: Ticker={ticker}, Acc={acc}, ReportEnd={red}, Filed={fd}")
        with lock:
            counters['other_errors'] += 1
        return False

    logging.info(f"[{ticker}] Processing filing: Acc={acc}, ReportEnd={red}, Filed={fd}")
    rd_str = red.strftime('%Y-%m-%d')
    fd_str = fd.strftime('%Y-%m-%d')

    ptr = None
    try:
        date_20_days_prior = fd - timedelta(days=20)
        date_50_days_prior = fd - timedelta(days=50)
        logging.debug(f"[{ticker}] Getting price for PTR near {date_20_days_prior} (20d)")
        p20 = get_price_on_or_before(ticker, date_20_days_prior)
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
        ptr = None

    cur = None
    pri = None
    try:
        logging.debug(f"[{ticker}] Fetching current financial data for {red}")
        cur = fetch_gcs_data(ticker, acc, red, prior_period=False)
        if cur:
            logging.info(f"[{ticker}] Successfully fetched current financial data from GCS.")
            logging.debug(f"[{ticker}] Fetching prior financial data relative to {red}")
            prior_filing = find_prior_filing(ticker, acc)
            if prior_filing:
                pri = fetch_gcs_data(ticker, prior_filing['AccessionNumber'], prior_filing['ReportEndDate'], prior_period=True)
                if pri:
                    logging.info(f"[{ticker}] Successfully fetched prior financial data from GCS.")
                else:
                    logging.warning(f"[{ticker}] Prior financial data not found.")
            else:
                logging.warning(f"[{ticker}] No prior filing found for {red}.")
        else:
            logging.warning(f"[{ticker}] Current financial data not found for {red}. Cannot calculate most ratios.")
    except Exception as e:
        logging.error(f"[{ticker}] Error fetching financial data for Acc={acc}: {e}", exc_info=True)
        cur = None
        pri = None

    ratios = {k: None for k in RATIOS}
    if cur:
        cur['price_trend_ratio'] = ptr
        logging.debug(f"[{ticker}] Added PTR ({ptr}) to current data before GenAI call.")
        try:
            logging.info(f"[{ticker}] Calling GenAI for ratio calculation (Acc={acc})")
            gemini_ratios = calculate_ratios_with_genai(ticker, rd_str, cur, pri)
            ratios.update(gemini_ratios)
            logging.info(f"[{ticker}] Received ratios from GenAI: {ratios}")
        except Exception as e:
            logging.error(f"[{ticker}] Error calling GenAI for Acc={acc}: {e}", exc_info=True)
            ratios['price_trend_ratio'] = adjust_ratio_scale(ptr, 'price_trend_ratio', ticker)
            logging.warning(f"[{ticker}] Setting GenAI-calculated ratios to None. PTR kept: {ratios['price_trend_ratio']}")
            with lock:
                counters['gemini_errors'] += 1
    else:
        logging.warning(f"[{ticker}] Skipping GenAI call as current financial data is missing (Acc={acc}).")
        ratios['price_trend_ratio'] = adjust_ratio_scale(ptr, 'price_trend_ratio', ticker)
        logging.info(f"[{ticker}] Setting only Price Trend Ratio = {ratios['price_trend_ratio']} due to missing data.")

    row = {
        'ticker': ticker,
        'accession_number': acc,
        'report_end_date': rd_str,
        'filed_date': fd_str,
        **ratios,
        'data_source': 'gcs' if cur else 'none',
        'created_at': datetime.utcnow().isoformat() + "Z"
    }
    logging.debug(f"[{ticker}] Final row data prepared for Acc={acc}: {row}")

    try:
        insert_row_to_bigquery(table_id, row, ticker)
        with lock:
            counters['processed'] += 1
        logging.info(f"[{ticker}] Successfully processed and inserted filing Acc={acc}. Took {time.time()-start_time:.2f}s")
        return True
    except Exception as e:
        logging.error(f"[{ticker}] Failed to insert row for Acc={acc}. Took {time.time()-start_time:.2f}s")
        with lock:
            counters['insert_errors'] += 1
        return False

# --- Main Table Builder / Job Entry Point ---
def run_ratio_calculation_job():
    logging.info("Starting ratio calculation job...")
    if not initialize_clients():
        logging.critical("Client initialization failed. Aborting job.")
        return

    filings = get_unprocessed_filings()
    if not filings:
        logging.info("No unprocessed filings found. Exiting.")
        return

    logging.info(f"Found {len(filings)} unprocessed filings to process.")
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{RATIOS_TABLE}"
    logging.info(f"Target table for inserts: {table_id}")

    logging.info("Proceeding with ratio calculation assuming target table exists.")

    counters = {'processed': 0, 'insert_errors': 0, 'gemini_errors': 0, 'other_errors': 0}
    lock = threading.Lock()

    logging.info(f"Starting processing with up to {MAX_WORKERS} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="RatioCalc") as executor:
        futures = [executor.submit(process_filing, f, table_id, lock, counters) for f in filings]

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(filings), desc="Processing filings"):
            try:
                result = future.result()
                if not result:
                    logging.debug("A filing processing task completed with an error flag (False).")
            except Exception as exc:
                logging.error(f"An unexpected error occurred in a worker thread: {exc}", exc_info=True)
                with lock:
                    counters['other_errors'] += 1

    logging.info(f"Ratio calculation job finished.")
    logging.info(f"Summary: Processed={counters['processed']}, Insert Errors={counters['insert_errors']}, Gemini Errors={counters['gemini_errors']}, Other Errors={counters['other_errors']}")
    total_errors = counters['insert_errors'] + counters['gemini_errors'] + counters['other_errors']
    if total_errors > 0:
        logging.warning(f"Total errors encountered: {total_errors}")

if __name__ == '__main__':
    start_time = time.time()
    logging.info("Ratio Calculator Job script execution started.")
    try:
        run_ratio_calculation_job()
    except Exception as main_err:
        logging.critical(f"Critical error during job execution: {main_err}", exc_info=True)
    finally:
        end_time = time.time()
        logging.info(f"Ratio Calculator Job script execution finished.")
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")
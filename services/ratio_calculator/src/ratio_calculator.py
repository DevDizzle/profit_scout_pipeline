# services/ratio_calculator/src/ratio_calculator.py

import os
import logging
import pandas as pd
import numpy as np
from google.cloud import bigquery, secretmanager
from google.cloud.exceptions import NotFound

# Ensure google.generativeai is installed
try:
    import google.generativeai as genai
    genai_installed = True
except ImportError:
    genai_installed = False
    logging.warning("google-generativeai library not found. Ratio calculation cannot use Gemini.")

import json
# Optional: use tqdm if installed, otherwise set it to None
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from datetime import datetime, timedelta, date, timezone
import time
import re
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential
from google.api_core import exceptions
import concurrent.futures
import threading
from decimal import Decimal  # Keep Decimal for potential BQ NUMERIC/BIGNUMERIC

# --- Configuration (using snake_case names) ---
gcp_project_id = os.getenv('gcp_project_id')
bq_dataset_id = os.getenv('bq_dataset_id', 'profit_scout')
bq_metadata_table_id = os.getenv('bq_metadata_table_id')
bq_bs_table_id = os.getenv('bq_bs_table_id', 'balance_sheet')
bq_is_table_id = os.getenv('bq_is_table_id', 'income_statement')
bq_cf_table_id = os.getenv('bq_cf_table_id', 'cash_flow')
bq_price_table_id = os.getenv('bq_price_table_id', 'price_data')
bq_ratios_table_id = os.getenv('bq_ratios_table_id', 'financial_ratios') # Target output table
gemini_api_secret_name = os.getenv('gemini_api_secret_name')
gemini_api_key_secret_version = os.getenv('gemini_api_key_secret_version', 'latest')
gemini_model_name = os.getenv('gemini_model_name', 'gemini-2.0-flash-latest') # Align default with env
# Use the specific variable name from TF for max workers
max_workers = int(os.getenv('max_workers_ratio_calc', 8)) # Align with TF var name

# --- Constants ---
RATIOS = [
    "debt_to_equity", "fcf_yield", "current_ratio", "roe",
    "gross_margin", "operating_margin", "quick_ratio", "eps",
    "eps_change", "revenue_growth", "price_trend_ratio"
]

# --- Logging Setup ---
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
    'bq_dataset_id': bq_dataset_id,
    'bq_metadata_table_id': bq_metadata_table_id,
    'bq_ratios_table_id': bq_ratios_table_id, # Output table is essential
    'gemini_api_secret_name': gemini_api_secret_name
}
# Conditionally check model name only if library installed
if genai_installed:
     essential_vars['gemini_model_name'] = gemini_model_name

missing_vars = [k for k, v in essential_vars.items() if not v]

# Construct full table IDs (using snake_case variables)
metadata_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_metadata_table_id}"
bs_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_bs_table_id}"
is_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_is_table_id}"
cf_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_cf_table_id}"
price_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_price_table_id}"
ratios_table_full_id = f"{gcp_project_id}.{bq_dataset_id}.{bq_ratios_table_id}"


# --- Global Clients (Initialize in main()) ---
bq_client = None
gemini_model = None

# --- Helper Functions ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        logging.info(f"Successfully accessed secret: {secret_id}")
        return payload
    except Exception as e:
        logging.error(f"Failed to access secret '{secret_id}/{version_id}' in project '{project_id}': {e}", exc_info=True)
        raise

# --- Price Data Retrieval Helper ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def get_price_on_or_before(ticker: str, target_date: date) -> float | None:
    """Fetches closing price on or just before target_date from BQ."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    target_date_str = target_date.strftime('%Y-%m-%d')
    start_date_obj = target_date - timedelta(days=7) # Look back 7 days

    query = f"""
    SELECT close AS price
    FROM `{price_table_full_id}`
    WHERE ticker = @ticker AND date <= @target_date AND date >= @start_date
    ORDER BY date DESC
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date_obj),
        ]
    )
    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe()
        if not df.empty and pd.notna(df['price'].iloc[0]):
            return float(df['price'].iloc[0])
        else:
            logging.warning(f"[{ticker}] No price found on or before {target_date_str} in {price_table_full_id}.")
            return None
    except exceptions.GoogleAPICallError as e:
        logging.error(f"[{ticker}] BQ API error fetching price near {target_date_str}: {e}")
        raise
    except Exception as e:
        logging.error(f"[{ticker}] Unexpected error fetching price near {target_date_str}: {e}")
        return None # Do not retry on unexpected errors

# --- Financial Data Retrieval ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
def fetch_bigquery_data(ticker, report_end_date, prior_period=False):
    """Queries BQ for financial data, joining tables. Handles date matching."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    try:
        report_end_date_dt = pd.to_datetime(report_end_date).date()
    except ValueError:
        logging.error(f"[{ticker}] Invalid report_end_date format: {report_end_date}")
        return None

    target_date = report_end_date_dt
    date_condition_field = "bs.period_end_date" # Assume BS is the primary table
    query_params = []

    query_template = f"""
    WITH RankedData AS (
      SELECT
        bs.*,
        -- Select specific columns to avoid name clashes if possible
        inc.total_revenue, inc.cost_of_revenue, inc.gross_profit, inc.operating_income_loss, inc.net_income_common_stockholders, inc.basic_eps, inc.weighted_average_shares_outstanding,
        cf.free_cash_flow, -- Assume this column exists or calculate if needed
        bs.period_end_date as bq_report_end_date,
        ABS(DATE_DIFF(DATE(bs.period_end_date), @target_date_param, DAY)) as date_diff_days
      FROM `{bs_table_full_id}` bs
      LEFT JOIN `{is_table_full_id}` inc ON bs.ticker = inc.ticker AND bs.period_end_date = inc.period_end_date
      LEFT JOIN `{cf_table_full_id}` cf ON bs.ticker = cf.ticker AND bs.period_end_date = cf.period_end_date
      WHERE bs.ticker = @ticker AND DATE({date_condition_field}) BETWEEN @start_date_param AND @end_date_param
    )
    SELECT * EXCEPT(bq_report_end_date, date_diff_days)
    FROM RankedData
    ORDER BY date_diff_days ASC
    LIMIT 1
    """

    if prior_period:
        prior_target_date = report_end_date_dt - timedelta(days=90)
        start_date = prior_target_date - timedelta(days=45)
        end_date = prior_target_date + timedelta(days=45)
        fetch_desc = f"prior period near {prior_target_date}"
        query_params.extend([
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("start_date_param", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date_param", "DATE", end_date),
            bigquery.ScalarQueryParameter("target_date_param", "DATE", prior_target_date),
        ])
    else: # Current period logic
        exact_target_date = target_date
        fetch_desc = f"period {exact_target_date} (exact match)"
        query_params_exact = [
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("start_date_param", "DATE", exact_target_date),
            bigquery.ScalarQueryParameter("end_date_param", "DATE", exact_target_date),
            bigquery.ScalarQueryParameter("target_date_param", "DATE", exact_target_date),
        ]
        job_config_exact = bigquery.QueryJobConfig(query_parameters=query_params_exact)
        try:
            df = bq_client.query(query_template, job_config=job_config_exact).to_dataframe()
            if not df.empty:
                data_dict = df.iloc[0].to_dict()
                cleaned_data = clean_data_for_json(data_dict, ticker, f"Exact fetch {fetch_desc}")
                logging.info(f"[{ticker}] Found data via exact query for {fetch_desc}.")
                cleaned_data['bq_report_end_date'] = str(pd.to_datetime(data_dict.get('period_end_date', report_end_date)).date())
                return cleaned_data
            else:
                logging.warning(f"[{ticker}] No exact match for {exact_target_date}. Searching +/- 30 days.")
                start_date = exact_target_date - timedelta(days=30)
                end_date = exact_target_date + timedelta(days=30)
                fetch_desc = f"period near {exact_target_date} (fallback range search)"
                query_params = [ # Reset params for fallback
                    bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
                    bigquery.ScalarQueryParameter("start_date_param", "DATE", start_date),
                    bigquery.ScalarQueryParameter("end_date_param", "DATE", end_date),
                    bigquery.ScalarQueryParameter("target_date_param", "DATE", exact_target_date),
                ]
        except exceptions.GoogleAPICallError as e:
            logging.error(f"[{ticker}] BQ API error during exact match fetch: {e}")
            raise
        except Exception as e:
            logging.error(f"[{ticker}] Unexpected error during exact match fetch: {e}", exc_info=True)
            return None # Don't retry on unexpected errors

    # Execute the Query (Either prior period or current period fallback)
    if query_params:
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        try:
            df = bq_client.query(query_template, job_config=job_config).to_dataframe()
            if not df.empty:
                data_dict = df.iloc[0].to_dict()
                cleaned_data = clean_data_for_json(data_dict, ticker, f"Fallback fetch {fetch_desc}")
                logging.info(f"[{ticker}] Found data via fallback query for {fetch_desc}.")
                cleaned_data['bq_report_end_date'] = str(pd.to_datetime(data_dict.get('period_end_date', report_end_date)).date())
                return cleaned_data
            else:
                logging.warning(f"[{ticker}] No data found via fallback query for {fetch_desc}.")
                return None
        except exceptions.GoogleAPICallError as e:
            logging.error(f"[{ticker}] BQ API error fetching data for {fetch_desc}: {e}")
            raise
        except Exception as e:
            logging.error(f"[{ticker}] Unexpected error fetching data for {fetch_desc}: {e}", exc_info=True)
            return None # Don't retry on unexpected errors
    else:
        logging.warning(f"[{ticker}] No query parameters set for {fetch_desc}, likely exact match failed.")
        return None

# --- Data Cleaning for JSON ---
def clean_data_for_json(data_dict, ticker, context=""):
    """Cleans dict removing nulls/invalid types for JSON/Gemini prompt."""
    cleaned = {}
    if not isinstance(data_dict, dict): return {}
    exclude_keys = { # Keys not needed for ratio calculation by Gemini
        'ticker', 'period_end_date', 'reported_currency', 'date_diff_rank',
        'bq_report_end_date', 'date_diff_days', 'load_timestamp', 'filing_source_url',
        'cik', 'accession_number', 'filing_date', # Added common metadata keys
        'report_calendar_year', 'report_fiscal_year', 'report_fiscal_period' # Add more if present
    }
    for k, v in data_dict.items():
        if k in exclude_keys or k is None: continue # Also skip None keys
        if pd.isna(v) or v is None: continue
        # Check for specific column names that should be integers
        if k in ['weighted_average_shares_outstanding']: # Add others if needed
            try: cleaned[k] = int(Decimal(v)) # Use Decimal for intermediate precision if needed
            except (ValueError, TypeError, OverflowError): continue # Skip if conversion fails
        # Handle floats, converting Decimal if necessary
        elif isinstance(v, (Decimal, float, np.floating)):
            if np.isinf(float(v)) or np.isnan(float(v)): continue
            else: cleaned[k] = float(v)
        # Handle integers, converting Decimal if necessary
        elif isinstance(v, (int, np.integer)):
             cleaned[k] = int(v)
        # Keep only numeric-like types or explicitly allowed types
        elif isinstance(v, (str, bool, date, datetime, pd.Timestamp)):
             # Exclude string/bool/date unless specifically needed by a ratio calculation (unlikely)
             # logging.debug(f"[{ticker}] {context} - Skipping non-numeric key '{k}' type {type(v)}")
             pass
        else: # Attempt conversion for other potentially numeric types (e.g., BQ NUMERIC as Decimal)
             try:
                 f_val = float(v)
                 if not (np.isinf(f_val) or np.isnan(f_val)): cleaned[k] = f_val
             except (ValueError, TypeError):
                 logging.debug(f"[{ticker}] {context} - Could not serialize key '{k}' type {type(v)}. Skipping.")
    return cleaned

# --- Ratio Calculation & Adjustment ---
def adjust_ratio_scale(ratio, ratio_name, ticker):
    """Adjust ratio scale heuristically if it falls outside expected ranges."""
    if ratio is None: return None
    try:
        if isinstance(ratio, str): ratio_float = float(ratio.replace(',', '').strip())
        elif isinstance(ratio, (int, float, np.number)): ratio_float = float(ratio)
        else: logging.warning(f"[{ticker}] Unexpected type for ratio '{ratio_name}': {type(ratio)}."); return None
    except (ValueError, TypeError) as e: logging.warning(f"[{ticker}] Could not convert ratio '{ratio_name}' value '{ratio}' to float: {e}."); return None

    if np.isnan(ratio_float) or np.isinf(ratio_float): logging.warning(f"[{ticker}] Ratio '{ratio_name}' is NaN or Inf ({ratio_float})."); return None

    # Expected ranges (can be refined)
    expected_ranges = { # Slightly tighter ranges for financial ratios
        'debt_to_equity': (-3.0, 10.0),
        'fcf_yield': (-0.5, 0.30),
        'current_ratio': (0.1, 10.0),
        'roe': (-1.0, 1.0),
        'gross_margin': (0.0, 1.0),
        'operating_margin': (-1.0, 0.6),
        'quick_ratio': (0.1, 8.0),
        'eps': (-50, 200),
        'eps_change': (-5.0, 5.0), # Change relative to prior EPS
        'revenue_growth': (-0.8, 2.0), # Can shrink, high growth less likely sustained YoY
        'price_trend_ratio': (0.5, 2.5)
    }

    if ratio_name not in expected_ranges: return ratio_float

    min_val, max_val = expected_ranges[ratio_name]
    if min_val <= ratio_float <= max_val: return ratio_float

    logging.warning(f"[{ticker}] Ratio '{ratio_name}' ({ratio_float:.4f}) is outside expected range ({min_val}, {max_val}). Applying heuristic.")

    # Heuristic: Percentage adjustment
    if ratio_name in ['gross_margin', 'operating_margin', 'roe', 'fcf_yield', 'eps_change', 'revenue_growth']:
        if abs(ratio_float) > 1.0 and abs(ratio_float) <= 200: # Looks like % but > 1 or < -1
            potential_fix = ratio_float / 100.0
            if min_val <= potential_fix <= max_val:
                logging.warning(f"[{ticker}] Adjusting '{ratio_name}' from {ratio_float:.4f} to {potential_fix:.4f} (assuming percentage).")
                return potential_fix
            else: logging.warning(f"[{ticker}] Division by 100 for '{ratio_name}' ({potential_fix:.4f}) still outside range. Keeping outlier.")
        else: logging.warning(f"[{ticker}] Ratio '{ratio_name}' ({ratio_float:.4f}) significantly outside range/percent heuristic. Keeping outlier.")
    # No other specific adjustments for now, just keep the outlier
    else: logging.warning(f"[{ticker}] Ratio '{ratio_name}' ({ratio_float:.4f}) outside range. Keeping outlier.")

    return ratio_float


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=5, max=60), reraise=True)
def calculate_ratios_with_gemini(ticker, report_end_date_str, current_data, prior_data):
    """Calculates financial ratios using the Gemini model via API."""
    if not genai_installed: return {key: None for key in RATIOS if key != 'price_trend_ratio'} # Return None if lib missing
    if not gemini_model: raise RuntimeError("Gemini client not initialized.")
    if not current_data:
        logging.warning(f"[{ticker}] No current data for {report_end_date_str} passed to Gemini.")
        return {key: None for key in RATIOS if key != 'price_trend_ratio'}

    pre_calc_price_trend_ratio = current_data.pop('price_trend_ratio', None) # Remove pre-calc before sending
    current_data_for_prompt = current_data # Already cleaned by clean_data_for_json
    prior_data_for_prompt = prior_data if prior_data else {}

    prompt = f"""
    Analyze the financial data for stock {ticker} for the period ending around {report_end_date_str}.
    Current Period Data (Key Financials Only): ```json {json.dumps(current_data_for_prompt, indent=2, default=str)} ```
    Prior Period Data (Key Financials Only - May be Empty): ```json {json.dumps(prior_data_for_prompt, indent=2, default=str) if prior_data else "{}"} ```

    Instructions:
    1. Review Current Period JSON data. Use Prior Period data ONLY for 'eps_change' and 'revenue_growth'. If prior data missing for these, return `null` for that ratio.
    2. Calculate the financial ratios listed below based *only* on provided data.
    3. Assume standard financial field names (e.g., 'total_debt', 'total_equity', 'net_income', 'total_revenue', 'free_cash_flow', 'total_current_assets', 'total_current_liabilities', 'inventory', 'basic_eps', 'weighted_average_shares_outstanding', 'market_cap'). If a required field is missing, return `null` for that ratio.
    4. Use standard formulas (e.g., debt_to_equity=total_debt/total_stockholders_equity; roe=net_income_common_stockholders/avg_equity; eps=use 'basic_eps' or calculate).
    5. Handle division by zero returning `null`. Handle missing data returning `null`.
    6. The 'price_trend_ratio' is pre-calculated and NOT needed in your response.
    7. Format the output STRICTLY as a JSON object containing ONLY the requested ratios as key-value pairs.
    8. Keys: "debt_to_equity", "fcf_yield", "current_ratio", "roe", "gross_margin", "operating_margin", "quick_ratio", "eps", "eps_change", "revenue_growth".
    9. Provide numerical values or `null`. Do not include strings like "N/A".

    Ratios to Calculate: debt_to_equity, fcf_yield, current_ratio, roe, gross_margin, operating_margin, quick_ratio, eps, eps_change, revenue_growth

    Return ONLY the JSON object.
    """

    try:
        logging.debug(f"[{ticker}] Sending prompt to Gemini for {report_end_date_str}")
        generation_config=genai.GenerationConfig(
            temperature=0.1, max_output_tokens=2048, response_mime_type="application/json"
        )
        response = gemini_model.generate_content(
            prompt, generation_config=generation_config, request_options={"timeout": 180}
        )

        if not response.candidates:
            logging.error(f"[{ticker}] Gemini response missing candidates for {report_end_date_str}.")
            raise ValueError("Gemini response missing candidates.")

        # Check for safety blocks etc.
        # finish_reason = response.candidates[0].finish_reason ...

        result_text = response.text.strip()
        if not result_text:
             logging.error(f"[{ticker}] Empty response text from Gemini for {report_end_date_str}.")
             raise ValueError("Empty response text from Gemini.")

        try: metrics = json.loads(result_text)
        except json.JSONDecodeError as json_err:
            logging.warning(f"[{ticker}] Gemini response not direct JSON ({json_err}). Trying markdown extraction.")
            cleaned_text = re.sub(r"^```json\s*|\s*```$", "", result_text, flags=re.MULTILINE).strip()
            if not cleaned_text: raise ValueError("Stripped response text empty.") from json_err
            try: metrics = json.loads(cleaned_text)
            except json.JSONDecodeError as inner_err:
                logging.error(f"[{ticker}] Could not parse extracted JSON. Raw: {result_text[:500]} Extracted: {cleaned_text[:500]} Err: {inner_err}")
                raise ValueError(f"Could not parse extracted JSON: {inner_err}") from inner_err

        if not isinstance(metrics, dict):
            logging.error(f"[{ticker}] Parsed Gemini response not dict: {type(metrics)}. Response: {result_text[:500]}...")
            raise ValueError(f"Parsed response is not dict: {type(metrics)}")

        # Adjust results and add back pre-calculated ratio
        adjusted_metrics = {}
        ratios_to_process = [r for r in RATIOS if r != 'price_trend_ratio']
        logging.debug(f"[{ticker}] Gemini raw result dict for {report_end_date_str}: {metrics}")

        for name in ratios_to_process:
            raw_value = metrics.get(name)
            adjusted_metrics[name] = adjust_ratio_scale(raw_value, name, ticker)

        # Add back the potentially adjusted price_trend_ratio
        adjusted_metrics['price_trend_ratio'] = pre_calc_price_trend_ratio
        logging.debug(f"[{ticker}] Final adjusted ratios for {report_end_date_str}: {adjusted_metrics}")
        return adjusted_metrics

    except exceptions.GoogleAPICallError as api_error:
        logging.error(f"[{ticker}] Gemini API call error for {report_end_date_str}: {api_error}")
        raise
    except Exception as e:
        logging.error(f"[{ticker}] Error during Gemini processing for {report_end_date_str}: {e}", exc_info=True)
        final_ratios_on_error = {key: None for key in RATIOS}
        final_ratios_on_error['price_trend_ratio'] = pre_calc_price_trend_ratio
        return final_ratios_on_error

# --- BigQuery Append Operation ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
def insert_row_to_bigquery(table_id, row_data, ticker):
    """Inserts a single row dict into the specified BigQuery table."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    try:
        row_to_insert = {}
        for k, v in row_data.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): row_to_insert[k] = None
            elif isinstance(v, np.generic): row_to_insert[k] = v.item()
            elif isinstance(v, pd.Timestamp): row_to_insert[k] = v.to_pydatetime()
            elif isinstance(v, Decimal): row_to_insert[k] = float(v) # Convert Decimal to float for BQ
            else: row_to_insert[k] = v

        logging.debug(f"[{ticker}] Attempting BQ insert to {table_id}: Keys={list(row_to_insert.keys())}")
        errors = bq_client.insert_rows_json(table_id, [row_to_insert])
        if errors:
            error_details = errors[0].get('errors', [])
            error_str = '; '.join([f"Reason: {e.get('reason', 'N/A')}, Msg: {e.get('message', 'N/A')}" for e in error_details])
            logging.error(f"[{ticker}] BQ insert error to {table_id} for acc {row_data.get('accession_number', 'N/A')}. Details: {error_str}. Row Keys: {list(row_to_insert.keys())}")
            raise bigquery.QueryJobError(f"BQ insert failed", errors=errors)

    except exceptions.GoogleAPICallError as bq_api_error:
        logging.error(f"[{ticker}] BQ API error during insert to {table_id} for {row_data.get('accession_number', 'N/A')}: {bq_api_error}")
        raise
    except Exception as e:
        logging.error(f"[{ticker}] Unexpected error during BQ insert to {table_id} for {row_data.get('accession_number', 'N/A')}: {e}", exc_info=True)
        raise

# --- Function to get unprocessed filings ---
def get_unprocessed_filings():
    """Queries BQ to find filings in metadata not yet in the ratios table."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    query = f"""
    SELECT meta.Ticker, meta.ReportEndDate, meta.FiledDate, meta.AccessionNumber
    FROM `{metadata_table_full_id}` meta
    LEFT JOIN `{ratios_table_full_id}` ratio ON meta.AccessionNumber = ratio.accession_number
    WHERE ratio.accession_number IS NULL -- Process only if not found in ratios table
    -- Optional: Filter for recent filings if needed
    -- AND meta.FiledDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    ORDER BY meta.FiledDate DESC -- Process recent first
    -- LIMIT 100 -- Optional: Limit batch size
    """
    logging.info(f"Querying unprocessed filings (JOIN {metadata_table_full_id} vs {ratios_table_full_id})...")
    try:
        query_job = bq_client.query(query)
        df = query_job.to_dataframe(progress_bar_type='tqdm' if 'tqdm' in globals() else None) # Use tqdm if available
        logging.info(f"Found {len(df)} filings to process for ratios.")
        df['ReportEndDate'] = pd.to_datetime(df['ReportEndDate']).dt.date
        df['FiledDate'] = pd.to_datetime(df['FiledDate']).dt.date
        return df.to_dict('records')
    except exceptions.GoogleAPICallError as e: logging.error(f"BigQuery API error querying unprocessed filings: {e}"); return []
    except Exception as e: logging.error(f"Error querying unprocessed filings: {e}", exc_info=True); return []

# --- Processing Function for Each Filing (Thread Worker) ---
def process_filing(filing_info, ratios_bq_table_id, lock, counters):
    """Processes a single filing: calculate ratios, insert result."""
    ticker = filing_info.get('Ticker')
    accession_number = filing_info.get('AccessionNumber')
    report_end_date_obj = filing_info.get('ReportEndDate')
    filed_date_obj = filing_info.get('FiledDate')

    if not all([ticker, accession_number, report_end_date_obj, filed_date_obj]):
         logging.error(f"[Unknown] Invalid filing info received: {filing_info}. Skipping.")
         with lock: counters['processing_errors'] += 1
         return False

    try:
        # Ensure dates are date objects
        if not isinstance(report_end_date_obj, date): report_end_date_obj = pd.to_datetime(report_end_date_obj).date()
        if not isinstance(filed_date_obj, date): filed_date_obj = pd.to_datetime(filed_date_obj).date()

        report_end_date_str = report_end_date_obj.strftime('%Y-%m-%d')
        logging.info(f"[{ticker}] Processing filing {accession_number} (Report End: {report_end_date_str})...")

        # --- Calculate Price Trend Ratio ---
        calculated_price_trend_ratio = None
        adjusted_price_trend_ratio = None
        try:
            date_20 = filed_date_obj - timedelta(days=20)
            date_50 = filed_date_obj - timedelta(days=50)
            price_20 = get_price_on_or_before(ticker, date_20)
            time.sleep(0.1) # Avoid hitting rate limits if any
            price_50 = get_price_on_or_before(ticker, date_50)
            if price_20 is not None and price_50 is not None and price_50 != 0:
                calculated_price_trend_ratio = price_20 / price_50
                adjusted_price_trend_ratio = adjust_ratio_scale(calculated_price_trend_ratio, 'price_trend_ratio', ticker)
            # else: # Logged within get_price_on_or_before or zero division check
        except Exception as price_err: logging.error(f"[{ticker}] Error calculating price trend for {accession_number}: {price_err}", exc_info=True)

        # --- Fetch Financial Data ---
        bq_data = fetch_bigquery_data(ticker, report_end_date_obj, prior_period=False)
        bq_prior_data = None
        if bq_data:
            bq_actual_report_date = bq_data.get('bq_report_end_date', report_end_date_str)
            logging.info(f"[{ticker}] Found current FS data for {accession_number} (Actual BQ Date: {bq_actual_report_date}).")
            bq_data['price_trend_ratio'] = adjusted_price_trend_ratio # Pass adjusted ratio to Gemini context
            bq_prior_data = fetch_bigquery_data(ticker, report_end_date_obj, prior_period=True)
            if bq_prior_data: logging.info(f"[{ticker}] Found prior FS data for {accession_number} (Actual BQ Date: {bq_prior_data.get('bq_report_end_date', 'N/A')}).")
            else: logging.warning(f"[{ticker}] No prior FS data found for {accession_number}.")
        else: logging.warning(f"[{ticker}] No current FS data found for {accession_number}. Skipping Gemini.")

        # --- Calculate Ratios (Gemini or defaults) ---
        ratios = {key: None for key in RATIOS}
        ratios['price_trend_ratio'] = adjusted_price_trend_ratio # Ensure it's always set

        if bq_data and genai_installed: # Only call if we have data and library
            try:
                gemini_calculated_ratios = calculate_ratios_with_gemini(ticker, report_end_date_str, bq_data, bq_prior_data)
                ratios.update(gemini_calculated_ratios) # Merge results
            except Exception as calc_err:
                logging.error(f"[{ticker}] Failed Gemini calculation for {accession_number}: {calc_err}. Ratios (except price trend) null.")
                with lock: counters['gemini_errors'] += 1
        elif not genai_installed:
             logging.warning(f"[{ticker}] Gemini library not installed. Skipping ratio calculation (except price trend) for {accession_number}.")


        # --- Prepare and Insert Row ---
        row_data = {
            "ticker": ticker,
            "accession_number": accession_number,
            "report_end_date": report_end_date_obj,
            "filed_date": filed_date_obj,
            **ratios,
            "data_source": "bigquery" if bq_data else "none",
            "created_at": datetime.now(timezone.utc) # Switched to UTC
        }

        try:
            # Pass snake_case table id
            insert_row_to_bigquery(ratios_bq_table_id, row_data, ticker)
            with lock: counters['processed'] += 1
            logging.info(f"[{ticker}] Successfully processed and inserted ratios for {accession_number}.")
            return True
        except Exception as insert_err:
            with lock: counters['insert_errors'] += 1
            return False

    except Exception as outer_err:
        logging.error(f"[{ticker}] Unhandled error processing filing {accession_number}: {outer_err}", exc_info=True)
        with lock: counters['processing_errors'] += 1
        return False

# --- Main Orchestrator Function ---
def main():
    """Fetches unprocessed filings and calculates ratios for them using thread pool."""
    global bq_client, gemini_model # Allow modification

    start_time = time.time()
    logging.info("--- Starting Financial Ratio Calculation Job ---")

    # --- Initialize Clients (using snake_case variables) ---
    try:
        logging.info(f"Initializing BigQuery client for project {gcp_project_id}...")
        bq_client = bigquery.Client(project=gcp_project_id)
        bq_client.list_datasets(max_results=1) # Test connection
        logging.info("BigQuery client initialized successfully.")
    except Exception as e:
        logging.critical(f"Failed to initialize BigQuery client: {e}. Exiting.", exc_info=True)
        exit(1)

    if genai_installed:
        try:
            logging.info(f"Fetching Gemini API Key from Secret Manager: {gemini_api_secret_name}...")
            GOOGLE_API_KEY = access_secret_version(
                project_id=gcp_project_id,
                secret_id=gemini_api_secret_name,
                version_id=gemini_api_key_secret_version
            )
            genai.configure(api_key=GOOGLE_API_KEY)
            logging.info(f"Initializing Gemini client with model: {gemini_model_name}...")
            gemini_model = genai.GenerativeModel(model_name=gemini_model_name)
            logging.info("Gemini client initialized.")
        except Exception as e:
            logging.critical(f"Failed to initialize Gemini client: {e}. Exiting.", exc_info=True)
            exit(1)
    else:
        logging.warning("Gemini library not found. Ratio calculations will only include price_trend_ratio.")


    # --- Get Filings to Process ---
    # Pass snake_case variable
    filings_to_process = get_unprocessed_filings()
    if not filings_to_process:
        logging.info("No unprocessed filings found. Job finished.")
        end_time_noproc = time.time()
        logging.info(f"Total execution time: {end_time_noproc - start_time:.2f} seconds")
        return

    total_filings = len(filings_to_process)
    logging.info(f"Found {total_filings} filings to process. Max workers: {max_workers}...") # Use snake_case var

    # --- Process Filings Concurrently ---
    lock = threading.Lock()
    counters = {'processed': 0, 'insert_errors': 0, 'gemini_errors': 0, 'processing_errors': 0}
    futures = []
    progress_bar = None
    if 'tqdm' in globals(): # Check if tqdm was imported
        progress_bar = tqdm(total=total_filings, desc="Calculating Ratios", unit="filing")

    # Use snake_case variable
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='RatioWorker') as executor:
        for filing_info in filings_to_process:
            # Pass snake_case variable
            future = executor.submit(process_filing, filing_info, ratios_table_full_id, lock, counters)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result() # Check for exceptions raised in thread
            except Exception as e:
                logging.error(f"Error retrieving result from future or uncaught thread error: {e}", exc_info=True)
                with lock:
                    counters['processing_errors'] += 1
            finally:
                if progress_bar:
                    progress_bar.update(1)

    if progress_bar:
        progress_bar.close()

    # --- Final Summary ---
    end_time = time.time()
    logging.info(f"--- Ratio Calculation Job Complete ---")
    logging.info(f"Total filings targeted: {total_filings}")
    logging.info(f"Successfully processed & inserted: {counters['processed']}")
    logging.info(f"Gemini calculation errors: {counters['gemini_errors']}")
    logging.info(f"BigQuery insertion errors: {counters['insert_errors']}")
    logging.info(f"Other processing errors: {counters['processing_errors']}")
    total_errors = counters['gemini_errors'] + counters['insert_errors'] + counters['processing_errors']
    logging.info(f"Total errors encountered: {total_errors}")
    # Use snake_case variable
    logging.info(f"Data potentially updated in BigQuery table: {ratios_table_full_id}")
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")

# --- Entry Point ---
if __name__ == "__main__":
    main()
# services/ratio_calculator/src/ratio_calculator.py

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery, secretmanager
from google.cloud.exceptions import NotFound
import google.generativeai as genai
import json
import logging
from tqdm import tqdm # Optional: for progress bar if run manually/interactively
from datetime import datetime, timedelta, date
import time
import re
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential
from google.api_core import exceptions
import concurrent.futures
import threading
from decimal import Decimal # Import Decimal

# --- Configuration ---
# Environment Variable Names
ENV_PROJECT_ID = 'GCP_PROJECT_ID'
ENV_BQ_DATASET_ID = 'BQ_DATASET_ID'
ENV_BQ_METADATA_TABLE = 'BQ_METADATA_TABLE_ID' # e.g., filing_metadata
ENV_BQ_FINANCIALS_BS = 'BQ_FINANCIALS_BS_TABLE_ID' # e.g., balance_sheet
ENV_BQ_FINANCIALS_IS = 'BQ_FINANCIALS_IS_TABLE_ID' # e.g., income_statement
ENV_BQ_FINANCIALS_CF = 'BQ_FINANCIALS_CF_TABLE_ID' # e.g., cash_flow
ENV_BQ_PRICE_TABLE = 'BQ_PRICE_TABLE_ID' # e.g., price_data
ENV_BQ_RATIOS_TABLE = 'BQ_RATIOS_TABLE_ID' # e.g., financial_ratios (NEW NAME)
ENV_GEMINI_KEY_SECRET_ID = 'GEMINI_API_KEY_SECRET_ID' # Name of the secret in Secret Manager
ENV_GEMINI_KEY_SECRET_VERSION = 'GEMINI_API_KEY_SECRET_VERSION'
ENV_GEMINI_MODEL_NAME = 'GEMINI_MODEL_NAME' # <<< NEW: Env var for model name

# Load Configuration from Environment Variables
PROJECT_ID = os.getenv(ENV_PROJECT_ID)
DATASET_ID = os.getenv(ENV_BQ_DATASET_ID, 'profit_scout')
METADATA_TABLE = os.getenv(ENV_BQ_METADATA_TABLE)
FINANCIALS_BS_TABLE = os.getenv(ENV_BQ_FINANCIALS_BS, 'balance_sheet')
FINANCIALS_IS_TABLE = os.getenv(ENV_BQ_FINANCIALS_IS, 'income_statement')
FINANCIALS_CF_TABLE = os.getenv(ENV_BQ_FINANCIALS_CF, 'cash_flow')
PRICE_TABLE = os.getenv(ENV_BQ_PRICE_TABLE, 'price_data')
RATIOS_TABLE = os.getenv(ENV_BQ_RATIOS_TABLE, 'financial_ratios')
GEMINI_SECRET_ID = os.getenv(ENV_GEMINI_KEY_SECRET_ID)
GEMINI_SECRET_VERSION = os.getenv(ENV_GEMINI_KEY_SECRET_VERSION, 'latest')
GEMINI_MODEL_NAME = os.getenv(ENV_GEMINI_MODEL_NAME, 'gemini-2.0-flash-001')

# --- Constants ---
RATIOS = [
    "debt_to_equity", "fcf_yield", "current_ratio", "roe",
    "gross_margin", "operating_margin", "quick_ratio", "eps",
    "eps_change", "revenue_growth", "price_trend_ratio"
]
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 8))

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] [%(module)s:%(lineno)d] - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("google.api_core").setLevel(logging.WARNING)
logging.getLogger("google.auth").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Validate Essential Configuration ---
essential_vars = {
    ENV_PROJECT_ID: PROJECT_ID,
    ENV_BQ_DATASET_ID: DATASET_ID,
    ENV_BQ_METADATA_TABLE: METADATA_TABLE,
    ENV_BQ_RATIOS_TABLE: RATIOS_TABLE, # Check the output table name var
    ENV_GEMINI_KEY_SECRET_ID: GEMINI_SECRET_ID
}
missing_vars = [k for k, v in essential_vars.items() if not v]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}. Exiting.")
    exit(1)

# Construct full table IDs
metadata_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE}"
bs_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_BS_TABLE}"
is_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_IS_TABLE}"
cf_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{FINANCIALS_CF_TABLE}"
price_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{PRICE_TABLE}"
ratios_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{RATIOS_TABLE}" # Use the correct target table name


# --- Global Clients (Initialize in main()) ---
bq_client = None
gemini_model = None

# --- Helper Functions ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    # Ensure client is initialized only once if needed, or pass it if preferred
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    logging.info(f"Successfully accessed secret: {secret_id}")
    return payload

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
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date_obj), # Pass date object
        ]
    )
    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe()
        if not df.empty and pd.notna(df['price'].iloc[0]):
            return float(df['price'].iloc[0])
        else:
            logging.warning(f"[{ticker}] No price found on or before {target_date_str} in {price_table_full_id}.")
            return None
    except exceptions.GoogleAPICallError as e: logging.error(f"[{ticker}] BQ API error fetching price near {target_date_str}: {e}"); raise
    except Exception as e: logging.error(f"[{ticker}] Unexpected error fetching price near {target_date_str}: {e}"); return None

# --- Financial Data Retrieval ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
def fetch_bigquery_data(ticker, report_end_date, prior_period=False):
    """Queries BQ for financial data, joining tables. Handles date matching."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    try: report_end_date_dt = pd.to_datetime(report_end_date).date()
    except ValueError: logging.error(f"[{ticker}] Invalid report_end_date format: {report_end_date}"); return None

    # Logic to determine date_condition, order_by_limit, fetch_desc, query_params
    target_date = report_end_date_dt
    date_condition_field = "bs.period_end_date"
    query_params = [] # Initialize empty list

    # Define the query template once
    query_template = f"""
    WITH RankedData AS (
      SELECT
        bs.*,
        inc.* EXCEPT(ticker, period_end_date, reported_currency),
        cf.* EXCEPT(ticker, period_end_date, reported_currency),
        bs.period_end_date as bq_report_end_date,
        -- Calculate date difference for ordering (absolute difference in days)
        ABS(DATE_DIFF(DATE(bs.period_end_date), @target_date_param, DAY)) as date_diff_days
      FROM `{bs_table_full_id}` bs
      LEFT JOIN `{is_table_full_id}` inc ON bs.ticker = inc.ticker AND bs.period_end_date = inc.period_end_date
      LEFT JOIN `{cf_table_full_id}` cf ON bs.ticker = cf.ticker AND bs.period_end_date = cf.period_end_date
      WHERE bs.ticker = @ticker AND DATE({date_condition_field}) BETWEEN @start_date_param AND @end_date_param
    )
    SELECT * EXCEPT(bq_report_end_date, date_diff_days)
    FROM RankedData
    ORDER BY date_diff_days ASC -- Order by smallest difference
    LIMIT 1
    """

    if prior_period:
        prior_target_date = report_end_date_dt - timedelta(days=90) # Target approx 3 months prior
        start_date = prior_target_date - timedelta(days=45) # Widen search window slightly
        end_date = prior_target_date + timedelta(days=45)
        fetch_desc = f"prior period near {prior_target_date}"
        query_params.extend([
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("start_date_param", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date_param", "DATE", end_date),
            bigquery.ScalarQueryParameter("target_date_param", "DATE", prior_target_date), # The date we want to be closest to
        ])
    else: # Current period logic (first try exact, then fallback range)
        exact_target_date = target_date
        fetch_desc = f"period {exact_target_date} (exact match)"
        # Try exact match first using a narrow range for the initial query
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
                cleaned_data = clean_data_for_json(data_dict, ticker, f"Initial fetch {fetch_desc}")
                logging.debug(f"[{ticker}] Found data via initial query for {fetch_desc}.")
                # Correctly fetch the actual date found by BQ
                cleaned_data['bq_report_end_date'] = str(pd.to_datetime(data_dict.get('period_end_date', report_end_date)).date())
                return cleaned_data
            else:
                 # --- Fallback Logic ---
                logging.warning(f"[{ticker}] No exact match for {exact_target_date}. Searching +/- 30 days.")
                start_date = exact_target_date - timedelta(days=30)
                end_date = exact_target_date + timedelta(days=30)
                fetch_desc = f"period near {exact_target_date} (fallback range search)"
                query_params = [ # Reset params for fallback
                    bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
                    bigquery.ScalarQueryParameter("start_date_param", "DATE", start_date),
                    bigquery.ScalarQueryParameter("end_date_param", "DATE", end_date),
                    bigquery.ScalarQueryParameter("target_date_param", "DATE", exact_target_date), # Still targeting original date
                ]
                # Set query_params for the final attempt below
        except exceptions.GoogleAPICallError as e: logging.error(f"[{ticker}] BQ API error during exact match fetch: {e}"); raise
        except Exception as e: logging.error(f"[{ticker}] Unexpected error during exact match fetch: {e}", exc_info=True); return None


    # --- Execute the Query (Either prior period or current period fallback) ---
    if query_params: # Ensure params are set (either prior or fallback)
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        try:
            df = bq_client.query(query_template, job_config=job_config).to_dataframe()
            if not df.empty:
                data_dict = df.iloc[0].to_dict()
                cleaned_data = clean_data_for_json(data_dict, ticker, f"Fetch {fetch_desc}")
                logging.info(f"[{ticker}] Found data via query for {fetch_desc}.")
                # Correctly fetch the actual date found by BQ
                cleaned_data['bq_report_end_date'] = str(pd.to_datetime(data_dict.get('period_end_date', report_end_date)).date())
                return cleaned_data
            else:
                logging.warning(f"[{ticker}] No data found via query for {fetch_desc}.")
                return None
        except exceptions.GoogleAPICallError as e: logging.error(f"[{ticker}] BQ API error fetching data for {fetch_desc}: {e}"); raise
        except Exception as e: logging.error(f"[{ticker}] Unexpected error fetching data for {fetch_desc}: {e}", exc_info=True); return None
    else: # Should only happen if exact match failed and fallback wasn't attempted (error?)
         logging.warning(f"[{ticker}] No query parameters set for {fetch_desc}, likely exact match failed and no fallback ran.")
         return None


# --- Data Cleaning for JSON ---
def clean_data_for_json(data_dict, ticker, context=""):
    """Cleans dict removing nulls/invalid types for JSON/Gemini prompt."""
    cleaned = {}
    if not isinstance(data_dict, dict): return {}
    # Expanded exclude_keys based on schema understanding
    exclude_keys = {
        'ticker', 'period_end_date', 'reported_currency', 'date_diff_rank',
        'bq_report_end_date', 'date_diff_days', 'load_timestamp', 'filing_source_url',
        'cik' # Add other non-financial metadata if present
    }
    for k, v in data_dict.items():
        if k in exclude_keys: continue
        if pd.isna(v) or v is None: continue
        elif isinstance(v, (datetime, pd.Timestamp, date)): cleaned[k] = v.isoformat()
        elif isinstance(v, Decimal): cleaned[k] = float(v) # Convert Decimal to float
        elif isinstance(v, (np.int64, np.int32, np.int16, int)): cleaned[k] = int(v)
        elif isinstance(v, (np.float64, np.float32, float)):
            if np.isinf(v) or np.isnan(v): continue
            else: cleaned[k] = float(v)
        elif isinstance(v, (str, bool)): cleaned[k] = v
        else:
            # Attempt conversion for other potential types (e.g., BQ NUMERIC/BIGNUMERIC as Decimal)
            try:
                # Check if it behaves like a number first
                if hasattr(v, '__float__'):
                   f_val = float(v)
                   if not (np.isinf(f_val) or np.isnan(f_val)):
                       cleaned[k] = f_val
                   else:
                       logging.warning(f"[{ticker}] {context} - Skipping INF/NaN key '{k}' after float conversion.")
                else:
                    # Fallback to string if not obviously numeric
                    cleaned[k] = str(v)
            except Exception: logging.warning(f"[{ticker}] {context} - Could not serialize key '{k}' type {type(v)}. Skipping.")
    #if not cleaned: logging.warning(f"[{ticker}] {context} - Cleaned data dict is empty.")
    return cleaned


# --- Ratio Calculation & Adjustment ---
def adjust_ratio_scale(ratio, ratio_name, ticker):
    """Adjust ratio scale heuristically if it falls outside expected ranges."""
    if ratio is None: return None
    try:
        # Handle potential strings (e.g., from JSON)
        if isinstance(ratio, str):
            # Remove commas before conversion
            ratio_float = float(ratio.replace(',', '').strip())
        elif isinstance(ratio, (int, float, np.number)):
             ratio_float = float(ratio)
        else:
             logging.warning(f"[{ticker}] Unexpected type for ratio '{ratio_name}': {type(ratio)}. Cannot adjust.")
             return None # Cannot process unknown type
    except (ValueError, TypeError) as e:
        logging.warning(f"[{ticker}] Could not convert ratio '{ratio_name}' value '{ratio}' to float: {e}. Returning None.")
        return None

    if np.isnan(ratio_float) or np.isinf(ratio_float):
        logging.warning(f"[{ticker}] Ratio '{ratio_name}' is NaN or Inf ({ratio_float}). Returning None.")
        return None

    # Expected ranges (can be refined) - slightly more tolerant
    expected_ranges = {
        'debt_to_equity': (-5.0, 20.0),    # Allow for negative equity or high debt
        'fcf_yield': (-1.0, 0.50),       # Can be negative, high yield less common but possible
        'current_ratio': (0.05, 15.0),     # Very low/high possible but suspect
        'roe': (-2.0, 2.0),            # Wider range for ROE
        'gross_margin': (-1.0, 1.0),       # Should be <= 1 typically
        'operating_margin': (-2.0, 1.0),   # Allow larger negative margins
        'quick_ratio': (0.05, 12.0),      # Similar to current ratio
        'eps': (-100, 500),           # Wider range for EPS values
        'eps_change': (-20.0, 20.0),     # Percentage change, allow large swings
        'revenue_growth': (-1.0, 10.0),    # High growth possible, decline limited to -100%
        'price_trend_ratio': (0.2, 5.0)  # Wider range for price movement
    }

    if ratio_name not in expected_ranges:
        return ratio_float # No adjustment if range not defined

    min_val, max_val = expected_ranges[ratio_name]

    # Basic check: Is the ratio within the expected range?
    if min_val <= ratio_float <= max_val:
        return ratio_float # It's within range, no adjustment needed

    # Heuristic Adjustments (Example - refine as needed)
    logging.warning(f"[{ticker}] Ratio '{ratio_name}' ({ratio_float:.4f}) is outside expected range ({min_val}, {max_val}). Applying heuristic.")

    # Example: If a margin is > 1.0, assume it's a percentage like 75.0 instead of 0.75
    if ratio_name in ['gross_margin', 'operating_margin', 'roe', 'fcf_yield']:
        if abs(ratio_float) > 1.5 and abs(ratio_float) <= 200 : # Check if it looks like a percentage > 1.5% and <= 200%
             potential_fix = ratio_float / 100.0
             # Check if the fix brings it into range
             if min_val <= potential_fix <= max_val:
                 logging.warning(f"[{ticker}] Adjusting '{ratio_name}' from {ratio_float:.4f} to {potential_fix:.4f} (assuming percentage).")
                 return potential_fix
             else:
                 logging.warning(f"[{ticker}] Division by 100 for '{ratio_name}' ({potential_fix:.4f}) still outside range ({min_val}, {max_val}). Keeping original outlier.")
                 return ratio_float # Return original outlier if fix doesn't work
        else:
             logging.warning(f"[{ticker}] Ratio '{ratio_name}' ({ratio_float:.4f}) is significantly outside range or close to 1. Keeping original outlier.")
             return ratio_float # Keep original if it's way off or negative > -2

    # For other ratios, simply log the outlier for now, don't adjust unless specific rule applies
    logging.warning(f"[{ticker}] Ratio '{ratio_name}' ({ratio_float:.4f}) is outside range ({min_val}, {max_val}). Keeping original outlier value.")
    return ratio_float # Return the original outlier if no specific heuristic applies


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=5, max=60), reraise=True)
def calculate_ratios_with_gemini(ticker, report_end_date_str, current_data, prior_data):
    """Calculates financial ratios using the Gemini model via API."""
    if not gemini_model: raise RuntimeError("Gemini client not initialized.")
    if not current_data:
        logging.warning(f"[{ticker}] No current data for {report_end_date_str} passed to Gemini.")
        ratios = {key: None for key in RATIOS}
        return ratios

    # Extract pre-calculated ratio, prepare data for prompt
    pre_calc_price_trend_ratio = current_data.get('price_trend_ratio') # Already adjusted in process_filing
    current_data_for_prompt = {k: v for k, v in current_data.items() if k != 'price_trend_ratio' and v is not None} # Exclude None
    prior_data_for_prompt = {k: v for k, v in prior_data.items() if k != 'price_trend_ratio' and v is not None} if prior_data else {} # Exclude None

    # Use the same detailed prompt as before
    prompt = f"""
    Analyze the financial data for stock {ticker} for the period ending around {report_end_date_str}.
    Current Period Data (Key Financials Only): ```json {json.dumps(current_data_for_prompt, indent=2, default=str)} ```
    Prior Period Data (Key Financials Only - May be Empty): ```json {json.dumps(prior_data_for_prompt, indent=2, default=str) if prior_data else "{}"} ```

    Instructions:
    1. Carefully review the provided Current Period JSON data. Use the Prior Period data ONLY for calculating 'eps_change' and 'revenue_growth'. If prior data is missing or incomplete for these calculations, return `null` for that specific ratio.
    2. Calculate the financial ratios listed below based *only* on the provided data. Do NOT use external knowledge or data.
    3. Assume the data represents standard financial statements (Balance Sheet, Income Statement, Cash Flow). Field names are generally standard (e.g., 'total_debt', 'total_equity', 'net_income', 'total_revenue', 'free_cash_flow', 'total_current_assets', 'total_current_liabilities', 'inventory', 'basic_eps', 'weighted_average_shares_outstanding', 'market_cap'). If a required field is missing for a ratio, return `null` for that ratio.
    4. Use standard financial ratio formulas. Examples:
       - debt_to_equity: total_debt / total_stockholders_equity
       - fcf_yield: free_cash_flow / market_cap (if market_cap provided, else null)
       - current_ratio: total_current_assets / total_current_liabilities
       - roe: net_income_common_stockholders / average_total_stockholders_equity (if prior equity available, average; else use current, if available)
       - gross_margin: gross_profit / total_revenue (where gross_profit might be total_revenue - cost_of_revenue)
       - operating_margin: operating_income_loss / total_revenue
       - quick_ratio: (cash_and_equivalents + short_term_investments + accounts_receivable_net) / total_current_liabilities OR (total_current_assets - inventory) / total_current_liabilities (use whichever data is available)
       - eps: Use 'basic_eps' if provided directly. If not, calculate as net_income_common_stockholders / weighted_average_shares_outstanding.
       - eps_change: (current_eps - prior_eps) / abs(prior_eps). Requires 'eps' from both current and prior data. If prior_eps is zero or negative, calculate simple difference or return null if ambiguous. Return null if prior data missing.
       - revenue_growth: (current_total_revenue - prior_total_revenue) / abs(prior_total_revenue). Requires 'total_revenue' from both current and prior data. Return null if prior data missing.
    5. Handle potential division by zero errors gracefully by returning `null` for the affected ratio.
    6. Handle missing data points required for a calculation by returning `null` for that specific ratio.
    7. Ensure all calculations are performed numerically.
    8. The 'price_trend_ratio' is pre-calculated and NOT needed in your response.
    9. Format the output STRICTLY as a JSON object containing ONLY the requested ratios as key-value pairs.
    10. Keys in the JSON should be exactly: "debt_to_equity", "fcf_yield", "current_ratio", "roe", "gross_margin", "operating_margin", "quick_ratio", "eps", "eps_change", "revenue_growth".
    11. Provide numerical values for calculated ratios, or `null` if a ratio cannot be calculated due to missing data or calculation errors (like division by zero). Do not include strings like "N/A" or "missing".

    Ratios to Calculate (excluding price_trend_ratio):
    debt_to_equity, fcf_yield, current_ratio, roe, gross_margin, operating_margin, quick_ratio, eps, eps_change, revenue_growth

    Return ONLY the JSON object. Example: {"debt_to_equity": 0.5, "fcf_yield": 0.05, ... , "revenue_growth": null}
    """

    try:
        logging.debug(f"[{ticker}] Sending prompt to Gemini for {report_end_date_str}")
        # Ensure model is configured for JSON output
        generation_config=genai.GenerationConfig(
            temperature=0.1,
            max_output_tokens=2048, # Increased slightly just in case
            response_mime_type="application/json" # Explicitly request JSON
        )
        response = gemini_model.generate_content(
            prompt,
            generation_config=generation_config,
            request_options={"timeout": 180} # Slightly increased timeout
        )

        # Robust parsing and validation logic
        if not response.candidates:
             logging.error(f"[{ticker}] Gemini response missing candidates for {report_end_date_str}. Prompt: {prompt[:500]}...")
             raise ValueError("Gemini response missing candidates.")

        # Check finish reason if needed (e.g., safety, token limit)
        # finish_reason = response.candidates[0].finish_reason
        # if finish_reason != 1: # 1 = STOP
        #    logging.warning(f"[{ticker}] Gemini generation finished with reason: {finish_reason}. Check response.")

        result_text = response.text.strip()
        if not result_text:
            logging.error(f"[{ticker}] Empty response text from Gemini for {report_end_date_str}.")
            raise ValueError("Empty response text from Gemini.")

        # Attempt to parse the JSON directly (since we requested JSON mime type)
        try:
            metrics = json.loads(result_text)
        except json.JSONDecodeError as json_err:
             # Fallback: try to extract JSON from markdown code block if direct parse fails
             logging.warning(f"[{ticker}] Gemini response was not direct JSON ({json_err}). Trying to extract from markdown.")
             cleaned_text = re.sub(r"^```json\s*|\s*```$", "", result_text, flags=re.MULTILINE).strip()
             if not cleaned_text: raise ValueError("Stripped response text is empty.") from json_err
             try:
                 metrics = json.loads(cleaned_text)
             except json.JSONDecodeError as inner_err:
                 logging.error(f"[{ticker}] Could not parse extracted JSON for {report_end_date_str}. Raw response: {result_text[:500]}... Extracted: {cleaned_text[:500]}... Error: {inner_err}")
                 raise ValueError(f"Could not parse extracted JSON: {inner_err}") from inner_err

        if not isinstance(metrics, dict):
            logging.error(f"[{ticker}] Parsed Gemini response is not a dict: {type(metrics)}. Response: {result_text[:500]}...")
            raise ValueError(f"Parsed response is not dict: {type(metrics)}")

        # Adjust results and add back pre-calculated ratio
        adjusted_metrics = {}
        ratios_to_process = [r for r in RATIOS if r != 'price_trend_ratio'] # Gemini doesn't calculate this one

        # Log received metrics before adjustment for debugging
        logging.debug(f"[{ticker}] Gemini raw result dict for {report_end_date_str}: {metrics}")

        for name in ratios_to_process:
            raw_value = metrics.get(name) # Get value from Gemini result
            adjusted_metrics[name] = adjust_ratio_scale(raw_value, name, ticker) # Adjust scale/type

        adjusted_metrics['price_trend_ratio'] = pre_calc_price_trend_ratio # Add back the pre-calculated one (already adjusted)

        # Log adjusted metrics for debugging
        logging.debug(f"[{ticker}] Final adjusted ratios for {report_end_date_str}: {adjusted_metrics}")
        # logging.info(f"[{ticker}] Successfully processed Gemini response for {report_end_date_str}.")
        return adjusted_metrics

    except exceptions.GoogleAPICallError as api_error:
        logging.error(f"[{ticker}] Gemini API call error for {report_end_date_str}: {api_error}")
        raise # Reraise for tenacity retry
    except Exception as e:
        logging.error(f"[{ticker}] Error during Gemini processing for {report_end_date_str}: {e}", exc_info=True)
        # Construct default dict on error, preserving pre-calculated ratio if possible
        final_ratios_on_error = {key: None for key in RATIOS}
        final_ratios_on_error['price_trend_ratio'] = pre_calc_price_trend_ratio # Use the pre-calculated value
        return final_ratios_on_error # Return defaults on non-API errors

# --- BigQuery Append Operation ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
def insert_row_to_bigquery(table_id, row_data, ticker):
    """Inserts a single row dict into the specified BigQuery table."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    try:
        # Clean invalid floats (NaN, Inf) before insert, convert others to standard types
        row_to_insert = {}
        for k, v in row_data.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                row_to_insert[k] = None
            elif isinstance(v, np.generic): # Convert numpy types to Python equivalents
                row_to_insert[k] = v.item()
            elif isinstance(v, pd.Timestamp): # Convert pandas Timestamps
                row_to_insert[k] = v.to_pydatetime()
            else:
                row_to_insert[k] = v # Keep other types as is (datetime.date, str, int, float, bool, None)

        # Ensure date/datetime objects are handled correctly by the client library
        # (The client library usually handles standard Python date/datetime/timestamp objects)

        logging.debug(f"[{ticker}] Attempting BQ insert to {table_id}: {row_to_insert}")
        errors = bq_client.insert_rows_json(table_id, [row_to_insert])
        if errors:
            # Log detailed error information
            error_details = errors[0].get('errors', [])
            error_str = '; '.join([f"Reason: {e.get('reason', 'N/A')}, Message: {e.get('message', 'N/A')}" for e in error_details])
            logging.error(f"[{ticker}] BQ insert error to {table_id} for accession {row_data.get('accession_number', 'N/A')}. Details: {error_str}. Row Data Sample: {{'ticker': {ticker}, 'report_end_date': {row_data.get('report_end_date')}, ...}}")
            # Consider raising a more specific error if needed, but logging might be sufficient
            raise bigquery.QueryJobError(f"BQ insert failed", errors=errors) # Reraise for tenacity retry
        # else:
            # logging.info(f"[{ticker}] Successfully inserted row for {row_data.get('accession_number')} into {table_id}")

    except exceptions.GoogleAPICallError as bq_api_error:
        logging.error(f"[{ticker}] BQ API call error during insert to {table_id} for {row_data.get('accession_number', 'N/A')}: {bq_api_error}")
        raise # Reraise for tenacity retry
    except Exception as e:
        logging.error(f"[{ticker}] Unexpected error during BQ insert attempt to {table_id} for {row_data.get('accession_number', 'N/A')}: {e}", exc_info=True)
        raise # Reraise for tenacity retry


# --- Function to get unprocessed filings ---
def get_unprocessed_filings():
    """Queries BQ to find filings in metadata not yet in the ratios table."""
    if not bq_client: raise RuntimeError("BigQuery client not initialized.")
    query = f"""
    SELECT meta.Ticker, meta.ReportEndDate, meta.FiledDate, meta.AccessionNumber
    FROM `{metadata_table_full_id}` meta
    WHERE NOT EXISTS (
      SELECT 1
      FROM `{ratios_table_full_id}` ratio
      WHERE meta.AccessionNumber = ratio.accession_number
        -- Optional: Add a check for recent processing if re-runs are possible
        -- AND ratio.created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    )
    -- AND meta.FiledDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) -- Optional: Limit processing scope
    ORDER BY meta.FiledDate DESC -- Process recent filings first
    -- LIMIT 100 -- Optional limit for batch size control during testing
    """
    logging.info(f"Querying unprocessed filings by comparing {metadata_table_full_id} and {ratios_table_full_id}...")
    try:
        # Use query_and_wait for potentially longer queries
        query_job = bq_client.query(query)
        df = query_job.to_dataframe() # Waits for job to complete
        logging.info(f"Found {len(df)} filings to process for ratios.")
        # Convert date columns explicitly if they aren't already
        df['ReportEndDate'] = pd.to_datetime(df['ReportEndDate']).dt.date
        df['FiledDate'] = pd.to_datetime(df['FiledDate']).dt.date
        return df.to_dict('records')
    except exceptions.GoogleAPICallError as e:
        logging.error(f"BigQuery API error querying unprocessed filings: {e}")
        return []
    except Exception as e:
        logging.error(f"Error querying unprocessed filings: {e}", exc_info=True)
        return []

# --- Processing Function for Each Filing (Thread Worker) ---
def process_filing(filing_info, ratios_bq_table_id, lock, counters):
    """Processes a single filing: calculate ratios, insert result."""
    ticker = filing_info.get('Ticker')
    accession_number = filing_info.get('AccessionNumber')
    report_end_date_obj = filing_info.get('ReportEndDate')
    filed_date_obj = filing_info.get('FiledDate')

    # Basic validation of essential info
    if not all([ticker, accession_number, report_end_date_obj, filed_date_obj]):
         logging.error(f"[UnknownTicker/Acc] Invalid filing info received: {filing_info}. Skipping.")
         with lock: counters['processing_errors'] += 1
         return False

    try:
        # Ensure dates are date objects (might be redundant if get_unprocessed_filings handles it)
        if isinstance(report_end_date_obj, (str, pd.Timestamp, datetime)):
            report_end_date_obj = pd.to_datetime(report_end_date_obj).date()
        if isinstance(filed_date_obj, (str, pd.Timestamp, datetime)):
             filed_date_obj = pd.to_datetime(filed_date_obj).date()

        report_end_date_str = report_end_date_obj.strftime('%Y-%m-%d')
        logging.info(f"[{ticker}] Processing filing {accession_number} (Report End: {report_end_date_str})...")


        # --- Calculate Price Trend Ratio ---
        calculated_price_trend_ratio = None
        adjusted_price_trend_ratio = None # Initialize adjusted version
        try:
            # Use filing date as reference for price history
            date_20 = filed_date_obj - timedelta(days=20)
            date_50 = filed_date_obj - timedelta(days=50)
            price_20 = get_price_on_or_before(ticker, date_20)
            time.sleep(0.1) # Small delay between price calls
            price_50 = get_price_on_or_before(ticker, date_50)

            if price_20 is not None and price_50 is not None:
                 if price_50 != 0:
                     calculated_price_trend_ratio = price_20 / price_50
                     # Adjust the ratio immediately after calculation
                     adjusted_price_trend_ratio = adjust_ratio_scale(calculated_price_trend_ratio, 'price_trend_ratio', ticker)
                 else:
                     logging.warning(f"[{ticker}] Price 50 days before {filed_date_obj} was zero for {accession_number}. Cannot calculate price trend.")
            # else: (Implicitly handled by None check later)
                 # logging.debug(f"[{ticker}] Insufficient price data for trend calc {accession_number} (P20:{price_20}, P50:{price_50})")

        except Exception as price_err:
             logging.error(f"[{ticker}] Error calculating price trend for {accession_number}: {price_err}", exc_info=True) # Log stack trace

        # --- Fetch Financial Data ---
        # Fetch current period data
        bq_data = fetch_bigquery_data(ticker, report_end_date_obj, prior_period=False)
        bq_prior_data = None

        if bq_data:
            # Store the actual BQ date found for logging/reference if needed
            bq_actual_report_date = bq_data.get('bq_report_end_date', report_end_date_str) # Use original if key missing
            logging.info(f"[{ticker}] Found current FS data for {accession_number} near {report_end_date_str} (Actual BQ Date: {bq_actual_report_date}).")
            # Add the *adjusted* price trend ratio to the data passed to Gemini (or used in case of Gemini failure)
            bq_data['price_trend_ratio'] = adjusted_price_trend_ratio

            # Fetch prior period data only if current data was found
            bq_prior_data = fetch_bigquery_data(ticker, report_end_date_obj, prior_period=True)
            if bq_prior_data:
                 bq_prior_actual_date = bq_prior_data.get('bq_report_end_date', 'N/A')
                 logging.info(f"[{ticker}] Found prior period FS data for {accession_number} (Actual BQ Date: {bq_prior_actual_date}).")
            else:
                 logging.warning(f"[{ticker}] No prior period FS data found for {accession_number} near {report_end_date_obj - timedelta(days=90)}.")
        else:
             logging.warning(f"[{ticker}] No current period FS data found for {accession_number} near {report_end_date_str}. Skipping Gemini calculation, attempting insert with limited data.")


        # --- Calculate Ratios (Gemini or defaults) ---
        ratios = {key: None for key in RATIOS} # Initialize with None
        # Ensure price trend ratio is carried over even if Gemini fails or is skipped
        ratios['price_trend_ratio'] = adjusted_price_trend_ratio

        if bq_data: # Only call Gemini if we have current financial data
            try:
                # Pass adjusted price trend ratio *within* bq_data for context if needed, but Gemini ignores it
                # Gemini calculates the other ratios
                gemini_calculated_ratios = calculate_ratios_with_gemini(
                    ticker, report_end_date_str, bq_data, bq_prior_data
                )
                # Merge Gemini results (which includes its own handling of price_trend_ratio placeholder)
                ratios.update(gemini_calculated_ratios) # Update the dict, overwriting Nones

            except Exception as calc_err:
                logging.error(f"[{ticker}] Failed Gemini calculation for {accession_number}: {calc_err}. Ratios (except price trend) will be null.")
                # Ratios dict already initialized with Nones, price trend is already set
                with lock: counters['gemini_errors'] += 1
        # else: # No BQ data, ratios dict remains Nones except for price trend

        # --- Prepare and Insert Row ---
        row_data = {
            "ticker": ticker,
            "accession_number": accession_number,
            "report_end_date": report_end_date_obj, # Use date object
            "filed_date": filed_date_obj,           # Use date object
            # Unpack the final ratios dict (contains Gemini results or Nones + price trend)
            **ratios,
            "data_source": "bigquery" if bq_data else "none", # Indicate if FS data was found
            "created_at": datetime.utcnow() # Use UTC timestamp
        }

        try:
            insert_row_to_bigquery(ratios_bq_table_id, row_data, ticker)
            # Only increment processed count on successful insertion
            with lock: counters['processed'] += 1
            logging.info(f"[{ticker}] Successfully processed and inserted ratios for {accession_number}.")
            return True # Indicate success

        except Exception as insert_err:
            # Error logging is handled within insert_row_to_bigquery
            with lock: counters['insert_errors'] += 1
            # Do not increment 'processed' counter here
            return False # Indicate failure

    except Exception as outer_err:
        logging.error(f"[{ticker}] Unhandled error processing filing {accession_number}: {outer_err}", exc_info=True)
        with lock: counters['processing_errors'] += 1
        return False # Indicate failure


# --- Main Orchestrator Function ---
def main():
    """Fetches unprocessed filings and calculates ratios for them using thread pool."""
    global bq_client, gemini_model # Allow modification of global clients

    start_time = time.time()
    logging.info("--- Starting Financial Ratio Calculation Job ---")

    # --- Initialize Clients ---
    try:
        logging.info(f"Initializing BigQuery client for project {PROJECT_ID}...")
        bq_client = bigquery.Client(project=PROJECT_ID)
        # Test connection
        bq_client.list_datasets(max_results=1)
        logging.info("BigQuery client initialized successfully.")
    except Exception as e:
        logging.critical(f"Failed to initialize BigQuery client: {e}. Exiting.", exc_info=True)
        exit(1)

    try:
        logging.info(f"Fetching Gemini API Key from Secret Manager: {GEMINI_SECRET_ID}...")
        GOOGLE_API_KEY = access_secret_version(PROJECT_ID, GEMINI_SECRET_ID, GEMINI_SECRET_VERSION)
        genai.configure(api_key=GOOGLE_API_KEY)
        logging.info(f"Initializing Gemini client with model: {GEMINI_MODEL_NAME}...") # <<< Log model name
        # Use the GEMINI_MODEL_NAME variable read from environment/default
        gemini_model = genai.GenerativeModel(
                model_name=GEMINI_MODEL_NAME, # <<< Use the variable here
                 # Generation config moved to the API call site for clarity
                 # Safety settings can be added here if needed
                 # safety_settings=[...]
             )
        # Optionally test the model (e.g., with a simple count_tokens call if needed)
        # gemini_model.count_tokens("test")
        logging.info("Gemini client initialized.")
    except Exception as e:
        logging.critical(f"Failed to initialize Gemini client: {e}. Exiting.", exc_info=True)
        exit(1)

    # --- Get Filings to Process ---
    filings_to_process = get_unprocessed_filings()
    if not filings_to_process:
        logging.info("No unprocessed filings found. Job finished.")
        end_time_noproc = time.time()
        logging.info(f"Total execution time: {end_time_noproc - start_time:.2f} seconds")
        return

    total_filings = len(filings_to_process)
    logging.info(f"Found {total_filings} filings to process. Max workers: {MAX_WORKERS}...")

    # --- Process Filings Concurrently ---
    lock = threading.Lock()
    # Counters: processed = successfully inserted, others track specific failure points
    counters = {'processed': 0, 'insert_errors': 0, 'gemini_errors': 0, 'processing_errors': 0}
    futures = []

    # Use tqdm for progress bar if installed and running interactively
    progress_bar = tqdm(total=total_filings, desc="Calculating Ratios", unit="filing")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='RatioWorker') as executor:
        for filing_info in filings_to_process:
            future = executor.submit(process_filing, filing_info, ratios_table_full_id, lock, counters)
            futures.append(future)

        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            try:
                # We don't need the return value (True/False) here anymore,
                # as counters are updated within process_filing.
                # Just calling future.result() ensures any exceptions raised in the thread are surfaced.
                future.result()
            except Exception as e:
                # This catches errors *within* the thread execution that weren't caught
                # inside process_filing's main try-except block, or exceptions during
                # future retrieval itself.
                logging.error(f"Error retrieving result from future or uncaught thread error: {e}", exc_info=True)
                # Increment general processing error if the future itself failed badly
                with lock:
                     # Avoid double counting if process_filing already counted it
                     # This error is more about the future/executor itself
                     # Let's keep it simple: if future.result() raises, count as processing error.
                     counters['processing_errors'] += 1
            finally:
                 # Update progress bar regardless of success or failure
                 progress_bar.update(1)

    progress_bar.close() # Close the progress bar

    # --- Final Summary ---
    end_time = time.time()
    logging.info(f"--- Ratio Calculation Job Complete ---")
    logging.info(f"Total filings targeted: {total_filings}")
    # Report counters accurately based on thread updates
    logging.info(f"Successfully processed & inserted: {counters['processed']}")
    logging.info(f"Gemini calculation errors (ratios likely null): {counters['gemini_errors']}")
    logging.info(f"BigQuery insertion errors (data not saved): {counters['insert_errors']}")
    logging.info(f"Other processing errors (filing skipped/failed): {counters['processing_errors']}")
    total_errors = counters['gemini_errors'] + counters['insert_errors'] + counters['processing_errors']
    logging.info(f"Total errors encountered: {total_errors}")
    logging.info(f"Data potentially updated in BigQuery table: {ratios_table_full_id}")
    logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")

# --- Entry Point ---
if __name__ == "__main__":
    main()
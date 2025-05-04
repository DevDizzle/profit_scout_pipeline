#!/usr/bin/env python3
# main.py — Fetch new filings and enrich filing_metadata table with sector, industry, etc.

import os
import datetime
import time
import logging
import json
import requests
import re
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import queue
import pandas as pd

# --- Imports ---
try:
    from google.cloud import bigquery, secretmanager
    from google.cloud.exceptions import NotFound
    import google.auth
    GCP_LIBS_AVAILABLE = True
except ImportError:
    logging.error("Failed to import Google Cloud libraries. Ensure google-cloud-bigquery and google-cloud-secret-manager are installed.")
    GCP_LIBS_AVAILABLE = False

try:
    from sec_api import QueryApi
    SEC_API_LIB_AVAILABLE = True
except ImportError:
    logging.error("Failed to import sec_api. Ensure sec-api is installed.")
    SEC_API_LIB_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError:
    logging.warning("Tenacity library not found. Retries will not be available.")
    def retry(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator
    stop_after_attempt = lambda n: None
    wait_exponential = lambda *args, **kwargs: None

# --- Configuration ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID', 'profit_scout')
BQ_METADATA_TABLE_ID = os.getenv('BQ_METADATA_TABLE_ID', 'filing_metadata')
SEC_API_SECRET_ID = os.getenv('SEC_API_SECRET_ID', 'sec-api-key')
SEC_API_SECRET_VERSION = os.getenv('SEC_API_SECRET_VERSION', 'latest')
LOOKBACK_HOURS = int(os.getenv('LOOKBACK_HOURS', '24'))
FILING_TYPES = os.getenv('FILING_TYPES', '"10-K","10-Q"')
TICKERS_TO_QUERY = os.getenv('TICKERS_TO_QUERY')

# --- SIC to Sector/Industry Mapping ---
SIC_TO_SECTOR_INDUSTRY = {
    range(100, 1000): ("Basic Materials", "Agriculture"),
    range(1000, 1500): ("Basic Materials", "Mining"),
    range(1500, 1800): ("Industrials", "Construction"),
    range(2000, 4000): ("Industrials", "Manufacturing"),
    range(4000, 5000): ("Industrials", "Transportation & Utilities"),
    range(5000, 5200): ("Consumer Cyclical", "Wholesale Trade"),
    range(5200, 6000): ("Consumer Cyclical", "Retail"),
    range(6000, 6800): ("Financial Services", "Finance & Insurance"),
    range(7000, 9000): ("Consumer Defensive", "Services"),
    range(9000, 10000): ("Industrials", "Public Administration"),
    1311: ("Basic Materials", "Oil & Gas Extraction"),
    2821: ("Basic Materials", "Plastic Materials & Synthetics"),
    2834: ("Healthcare", "Pharmaceutical Preparations"),
    3550: ("Industrials", "Special Industry Machinery"),
    3560: ("Industrials", "General Industrial Machinery"),
    3571: ("Technology", "Electronic Computers"),
    3572: ("Technology", "Computer Storage Devices"),
    3714: ("Industrials", "Motor Vehicle Parts"),
    4899: ("Industrials", "Communications Services"),
    6411: ("Financial Services", "Insurance Agents & Brokers"),
    7372: ("Technology", "Prepackaged Software"),
    6770: ("Financial Services", "Blank Checks"),
    6221: ("Financial Services", "Commodity Contracts Dealing"),
    2860: ("Industrials", "Industrial Organic Chemicals"),
    3823: ("Industrials", "Instruments for Measurement")
}

def parse_sic_code(sic_str):
    """Extract or pad numeric SIC code from string (e.g., '100' -> '0100')."""
    if not sic_str:
        logging.debug("SIC string is empty or None; returning None")
        return None
    sic_str = str(sic_str).strip()
    logging.debug(f"Parsing SIC string: '{sic_str}'")
    match = re.match(r'^\d{3,4}', sic_str)
    if match:
        sic_code = match.group(0)
        if len(sic_code) == 3:
            sic_code = '0' + sic_code  # Pad three-digit SICs
        logging.debug(f"Extracted SIC code: '{sic_code}'")
        return sic_code
    logging.warning(f"Could not extract numeric SIC code from '{sic_str}'; returning None")
    return None

def get_sector_industry(sic_code):
    """Map numeric SIC code to sector and industry."""
    if not sic_code or not sic_code.isdigit():
        logging.debug(f"Invalid SIC code '{sic_code}'; returning None for sector and industry")
        return None, None
    sic = int(sic_code)
    for sic_range, (sector, industry) in SIC_TO_SECTOR_INDUSTRY.items():
        if isinstance(sic_range, range) and sic in sic_range:
            return sector, industry
        elif isinstance(sic_range, int) and sic == sic_range:
            return sector, industry
    logging.warning(f"SIC code {sic} not found in mapping; returning None for sector and industry")
    return None, None

# --- Rate Limiter for Query API ---
class RateLimiter:
    def __init__(self, rate_limit, period=1.0):
        self.rate_limit = rate_limit
        self.period = period
        self.requests = queue.Queue()
        self.lock = Lock()

    def acquire(self):
        with self.lock:
            now = time.time()
            while not self.requests.empty() and now - self.requests.queue[0] > self.period:
                self.requests.get()
            if self.requests.qsize() >= self.rate_limit:
                sleep_time = self.period - (now - self.requests.queue[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self.requests.get()
            self.requests.put(now)

# --- Fallback Data Source ---
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30), reraise=True)
def get_ticker_sic_mapping():
    """Fetch ticker-to-SIC mapping from SEC company tickers JSON."""
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {"User-Agent": "Profit Scout contact@profitscout.com"}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        tickers = response.json()
        mapping = {
            item['ticker'].upper(): str(item['sic'])
            for item in tickers.values()
            if item.get('sic')
        }
        logging.info(f"Fetched SIC mapping for {len(mapping)} tickers from SEC JSON")
        return mapping
    except Exception as e:
        logging.error(f"Failed to fetch ticker mapping from SEC JSON: {e}")
        return {}

# --- Pre-fetch SIC/CIK/Exchange Data ---
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=120),
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
    reraise=True
)
def fetch_mapping_data(ticker, api_key):
    """Fetch CIK, SIC, and exchange for a ticker using SEC-API Mapping API."""
    time.sleep(1.0)  # Conservative delay to avoid 429 errors
    try:
        url = f"https://api.sec-api.io/mapping/ticker/{ticker}"
        headers = {"Authorization": api_key}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data and isinstance(data, list) and len(data) > 0:
            result = {
                'cik': data[0].get('cik', ''),
                'sic': data[0].get('sic', ''),
                'exchange': data[0].get('exchange', '')
            }
            logging.debug(f"Fetched Mapping API data for {ticker}: {result}")
            return result
        logging.warning(f"No data returned from Mapping API for {ticker}")
        return {'cik': '', 'sic': '', 'exchange': ''}
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logging.warning(f"Rate limit exceeded for ticker {ticker}: {e}. Retrying...")
            raise
        logging.warning(f"HTTP error for ticker {ticker}: {e}")
        return {'cik': '', 'sic': '', 'exchange': ''}
    except Exception as e:
        logging.warning(f"Failed to fetch Mapping API data for ticker {ticker}: {e}")
        return {'cik': '', 'sic': '', 'exchange': ''}

def pre_fetch_sic_data(tickers, api_key, ticker_sic_mapping):
    """Pre-fetch CIK, SIC, and exchange data for all tickers sequentially."""
    sic_cache_file = "/tmp/sic_cache.json"
    sic_cache = {}
    
    # Load existing cache if available
    if os.path.exists(sic_cache_file):
        try:
            with open(sic_cache_file, 'r') as f:
                sic_cache = json.load(f)
            logging.info(f"Loaded {len(sic_cache)} tickers from {sic_cache_file}")
        except Exception as e:
            logging.warning(f"Failed to load {sic_cache_file}: {e}")

    # First, use SEC JSON for SICs
    for ticker in tickers:
        if ticker not in sic_cache:
            sic = parse_sic_code(ticker_sic_mapping.get(ticker, ''))
            if sic:
                sector, industry = get_sector_industry(sic)
                sic_cache[ticker] = {
                    'cik': '',
                    'sic': sic,
                    'exchange': '',
                    'sector': sector,
                    'industry': industry
                }
    
    # Fetch remaining data from Mapping API sequentially
    remaining_tickers = [t for t in tickers if t not in sic_cache or not sic_cache[t].get('sic')]
    for ticker in remaining_tickers:
        result = fetch_mapping_data(ticker, api_key)
        sic = parse_sic_code(result.get('sic', ''))
        sector, industry = get_sector_industry(sic)
        sic_cache[ticker] = {
            'cik': result.get('cik', ''),
            'sic': sic,
            'exchange': result.get('exchange', ''),
            'sector': sector,
            'industry': industry
        }
        # Save cache incrementally
        try:
            with open(sic_cache_file, 'w') as f:
                json.dump(sic_cache, f)
        except Exception as e:
            logging.warning(f"Failed to save {sic_cache_file} for {ticker}: {e}")
    
    logging.info(f"Pre-fetched SIC data for {len(sic_cache)} tickers")
    return sic_cache

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Validate Configuration ---
essential_vars = [
    ('GCP_PROJECT_ID', GCP_PROJECT_ID),
    ('BQ_DATASET_ID', BQ_DATASET_ID),
    ('BQ_METADATA_TABLE_ID', BQ_METADATA_TABLE_ID),
    ('SEC_API_SECRET_ID', SEC_API_SECRET_ID),
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing essential environment variables: {', '.join(missing_vars)}")
    raise RuntimeError(f"Missing essential environment variables: {', '.join(missing_vars)}")

if not GCP_LIBS_AVAILABLE or not SEC_API_LIB_AVAILABLE:
    raise RuntimeError("Required libraries (GCP, sec-api) are not available.")

# --- Construct Full BQ Table ID ---
METADATA_TABLE_FULL_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_METADATA_TABLE_ID}"

# --- Global Clients and Cache ---
bq_client = None
secret_client = None
query_api = None
ticker_sic_mapping = None
sic_cache = {}  # Cache for SIC/CIK/exchange data

# --- Helper Functions ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    """Accesses a secret version from Google Secret Manager."""
    global secret_client
    if not secret_client:
        logging.info("Initializing Secret Manager client...")
        secret_client = secretmanager.SecretManagerServiceClient()

    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = secret_client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8").strip()
        logging.info(f"Successfully accessed secret: {secret_id}")
        if not payload:
            raise ValueError(f"Secret {secret_id} version {version_id} exists but is empty.")
        return payload
    except google.api_core.exceptions.NotFound:
        logging.error(f"Secret '{secret_id}' or version '{version_id}' not found in project '{project_id}'.")
        raise
    except Exception as e:
        logging.error(f"Failed to access secret '{secret_id}/{version_id}' in project '{project_id}': {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=30), reraise=True)
def get_recent_sec_filings(api_client, lookback_hours, filing_types_str, tickers_list=None, rate_limiter=None):
    """Queries the SEC API for recent filings."""
    if not api_client:
        raise RuntimeError("SEC QueryApi client not initialized.")
    if rate_limiter:
        rate_limiter.acquire()

    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(hours=lookback_hours)
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    query_parts = [
        f'formType:({filing_types_str})',
        f'filedAt:[{start_date_str} TO {end_date_str}]',
        'NOT formType:("10-K/A" OR "10-Q/A")'
    ]
    if tickers_list:
        ticker_query = " OR ".join([f'ticker:"{t.strip().upper()}"' for t in tickers_list])
        query_parts.append(f'({ticker_query})')

    final_query_string = " AND ".join(query_parts)
    logging.info(f"SEC API Query String: {final_query_string}")

    query = {
        "query": {"query_string": {"query": final_query_string}},
        "from": "0",
        "size": "200",
        "sort": [{"filedAt": {"order": "desc"}}]
    }

    try:
        response = api_client.get_filings(query)
        filings = response.get('filings', [])
        logging.info(f"SEC API returned {len(filings)} filings for the last {lookback_hours} hours.")
        return filings
    except Exception as e:
        logging.error(f"Error querying SEC API: {e}")
        raise

def check_filings_exist_in_bq(bq_client_instance, table_id_full, accession_numbers):
    """Checks a list of accession numbers against BigQuery. Returns set of existing numbers."""
    if not bq_client_instance:
        raise RuntimeError("BigQuery client not initialized.")
    if not accession_numbers:
        return set()

    clean_accession_numbers = [str(an).replace('-', '') for an in accession_numbers]

    query = f"""
        SELECT DISTINCT AccessionNumber
        FROM `{table_id_full}`
        WHERE AccessionNumber IN UNNEST(@acc_nos)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("acc_nos", "STRING", clean_accession_numbers)
        ]
    )
    try:
        query_job = bq_client_instance.query(query, job_config=job_config)
        results = query_job.result(timeout=120)
        existing_numbers = {row.AccessionNumber for row in results}
        logging.info(f"Checked {len(accession_numbers)} accession numbers against BQ. Found {len(existing_numbers)} existing.")
        return existing_numbers
    except Exception as e:
        logging.error(f"Error checking BQ for existing filings: {e}")
        raise

def insert_new_filings_to_bq(bq_client_instance, table_id_full, filings_to_insert):
    """Inserts new filing metadata rows into BigQuery with enriched fields."""
    if not bq_client_instance:
        raise RuntimeError("BigQuery client not initialized.")
    if not filings_to_insert:
        logging.info("No new filings to insert into BigQuery.")
        return 0

    rows_to_insert = []
    for filing in filings_to_insert:
        try:
            report_end_date = filing.get('periodOfReport')[:10] if filing.get('periodOfReport') else None
            filed_date = filing.get('filedAt')[:10] if filing.get('filedAt') else None
            accession_no_clean = filing.get('accessionNo', '').replace('-', '')
            ticker = filing.get('ticker', '').upper()
            company_name = filing.get('companyName', '')
            cik = filing.get('cik', '') or sic_cache.get(ticker, {}).get('cik', '')
            sic = sic_cache.get(ticker, {}).get('sic', '')
            sector = sic_cache.get(ticker, {}).get('sector', None)
            industry = sic_cache.get(ticker, {}).get('industry', None)
            exchange = sic_cache.get(ticker, {}).get('exchange', '') or filing.get('primaryExchange', '')

            # Log invalid SIC only once per ticker
            invalid_sic_tickers = set()
            if not sic and ticker not in invalid_sic_tickers:
                logging.warning(f"Invalid or missing SIC code for ticker {ticker}; setting SIC, Sector, Industry to None")
                invalid_sic_tickers.add(ticker)

            if not all([ticker, report_end_date, filed_date, filing.get('formType'), accession_no_clean, filing.get('linkToFilingDetails')]):
                logging.warning(f"Skipping insert due to missing core data in filing: {filing.get('accessionNo', 'N/A')}")
                continue

            # Log the raw filing data for debugging
            logging.debug(f"Raw SEC API filing data for {accession_no_clean}: {filing}")

            row = {
                "Ticker": ticker,
                "ReportEndDate": report_end_date,
                "FiledDate": filed_date,
                "FormType": filing.get('formType'),
                "AccessionNumber": accession_no_clean,
                "LinkToFilingDetails": filing.get('linkToFilingDetails'),
                "SIC": sic,
                "Sector": sector,
                "Industry": industry,
                "CompanyName": company_name,
                "CIK": cik,
                "PrimaryExchange": exchange if exchange else None,
                "IncomeStatementURI": None,
                "BalanceSheetURI": None,
                "CashFlowURI": None
            }
            rows_to_insert.append(row)
            logging.debug(f"Prepared row for {accession_no_clean}: {row}")
        except Exception as format_err:
            logging.warning(f"Error formatting row for filing {filing.get('accessionNo', 'N/A')}: {format_err}")
            continue

    if not rows_to_insert:
        logging.info("No valid rows formatted for BigQuery insertion.")
        return 0

    logging.info(f"Attempting to insert {len(rows_to_insert)} new filing metadata rows into {table_id_full}...")
    try:
        errors = bq_client_instance.insert_rows_json(table_id_full, rows_to_insert)
        if not errors:
            logging.info(f"Successfully inserted {len(rows_to_insert)} rows.")
            return len(rows_to_insert)
        else:
            logging.error(f"Errors encountered during BigQuery insert: {errors}")
            return 0
    except Exception as e:
        logging.error(f"Failed to insert rows into BigQuery: {e}")
        raise

# --- Main Function ---
def main():
    global bq_client, query_api, ticker_sic_mapping, sic_cache
    start_time = time.time()
    logging.info(f"Starting fetch-new-filings job. Lookback: {LOOKBACK_HOURS} hours.")

    try:
        logging.info(f"Initializing BigQuery client for project {GCP_PROJECT_ID}...")
        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        logging.info("BigQuery client initialized.")

        api_key = access_secret_version(GCP_PROJECT_ID, SEC_API_SECRET_ID, SEC_API_SECRET_VERSION)
        query_api = QueryApi(api_key=api_key)
        logging.info("SEC Query API client initialized.")

        # Fetch ticker-to-SIC mapping as a fallback
        ticker_sic_mapping = get_ticker_sic_mapping()

        tickers = None
        if TICKERS_TO_QUERY:
            tickers = [t.strip().upper() for t in TICKERS_TO_QUERY.split(',')]
            logging.info(f"Querying for specific tickers: {tickers}")

        # Pre-fetch SIC/CIK/exchange data for all relevant tickers
        if tickers:
            unique_tickers = set(tickers)
        else:
            # If no specific tickers, we’ll extract tickers from filings after fetching
            unique_tickers = set()
        sic_cache = pre_fetch_sic_data(unique_tickers, api_key, ticker_sic_mapping)

        # Fetch recent filings, splitting tickers into chunks for multithreading
        rate_limiter = RateLimiter(rate_limit=8, period=1.0)  # 8 requests/second
        recent_filings = []
        if tickers:
            chunk_size = 50  # Process tickers in chunks to manage query size
            ticker_chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                    executor.submit(get_recent_sec_filings, query_api, LOOKBACK_HOURS, FILING_TYPES, chunk, rate_limiter)
                    for chunk in ticker_chunks
                ]
                for future in futures:
                    recent_filings.extend(future.result())
        else:
            recent_filings = get_recent_sec_filings(query_api, LOOKBACK_HOURS, FILING_TYPES, rate_limiter=rate_limiter)

        if not recent_filings:
            logging.info("No recent filings found from SEC API.")
            return

        # Update sic_cache with any new tickers from filings
        filing_tickers = set(f.get('ticker', '').upper() for f in recent_filings if f.get('ticker'))
        new_tickers = filing_tickers - set(sic_cache.keys())
        if new_tickers:
            logging.info(f"Pre-fetching SIC data for {len(new_tickers)} new tickers from filings...")
            sic_cache.update(pre_fetch_sic_data(new_tickers, api_key, ticker_sic_mapping))

        accession_numbers_to_check = [f.get('accessionNo') for f in recent_filings if f.get('accessionNo')]
        accession_numbers_clean = [an.replace('-', '') for an in accession_numbers_to_check]

        existing_accession_numbers = check_filings_exist_in_bq(bq_client, METADATA_TABLE_FULL_ID, accession_numbers_clean)

        new_filings_to_process = []
        for filing in recent_filings:
            an_clean = filing.get('accessionNo', '').replace('-', '')
            if an_clean and an_clean not in existing_accession_numbers:
                new_filings_to_process.append(filing)

        logging.info(f"Identified {len(new_filings_to_process)} new filings to insert into BigQuery.")

        # Process filings in batches
        BATCH_SIZE = 50
        inserted_count = 0
        for i in range(0, len(new_filings_to_process), BATCH_SIZE):
            batch_filings = new_filings_to_process[i:i + BATCH_SIZE]
            inserted_count += insert_new_filings_to_bq(bq_client, METADATA_TABLE_FULL_ID, batch_filings)

        workflow_output = []
        if inserted_count > 0:
            for formatted_row in [r for f in new_filings_to_process for r in [{
                "Ticker": f.get('ticker'),
                "AccessionNumber": f.get('accessionNo', '').replace('-', ''),
                "LinkToFilingDetails": f.get('linkToFilingDetails')
            }] if all(r.values())]:
                workflow_output.append(formatted_row)

        if workflow_output:
            output_log = {
                "job_name": "fetch-new-filings",
                "status": "success",
                "new_filings_processed": inserted_count,
                "new_filing_details": workflow_output
            }
            print(json.dumps(output_log))
            logging.info(f"Output details for {len(workflow_output)} new filings prepared for workflow.")
        else:
            logging.info("No new filings processed or output generated for workflow.")

    except google.auth.exceptions.DefaultCredentialsError as cred_err:
        logging.critical(f"GCP Credentials Error: Could not automatically find credentials. Ensure the Cloud Run Job is running with a Service Account that has necessary permissions. Error: {cred_err}")
        raise
    except Exception as e:
        logging.error(f"An error occurred during the fetch-new-filings job: {e}")
        raise
    finally:
        end_time = time.time()
        logging.info(f"fetch-new-filings job finished in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
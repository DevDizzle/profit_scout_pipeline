#!/usr/bin/env python3
# main.py — Batch financial statement extraction to GCS with shares outstanding

import os
import logging
import pandas as pd
import time
from google.cloud import bigquery, secretmanager, storage
from sec_api import XbrlApi
from tenacity import retry, stop_after_attempt, wait_exponential
import re
import json
import sys
import io

# --- Configuration ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'profit-scout-456416')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID', 'profit_scout')
BQ_METADATA_TABLE_ID = os.getenv('BQ_METADATA_TABLE_ID', 'filing_metadata')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'profit-scout')
GCS_BS_PREFIX = os.getenv('GCS_BS_PREFIX', 'balance-sheet/')
GCS_IS_PREFIX = os.getenv('GCS_IS_PREFIX', 'income-statement/')
GCS_CF_PREFIX = os.getenv('GCS_CF_PREFIX', 'cash-flow/')
SEC_API_SECRET_ID = os.getenv('SEC_API_SECRET_ID', 'sec-api-key')
SEC_API_SECRET_VERSION = os.getenv('SEC_API_SECRET_VERSION', 'latest')
MAX_FILINGS_TO_PROCESS = int(os.getenv('MAX_FILINGS_TO_PROCESS', '0'))

# --- Logging ---
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
    ('GCS_BUCKET_NAME', GCS_BUCKET_NAME),
    ('GCS_BS_PREFIX', GCS_BS_PREFIX),
    ('GCS_IS_PREFIX', GCS_IS_PREFIX),
    ('GCS_CF_PREFIX', GCS_CF_PREFIX),
    ('SEC_API_SECRET_ID', SEC_API_SECRET_ID)
]
missing_vars = [name for name, value in essential_vars if not value]
if missing_vars:
    logging.critical(f"Missing env vars: {missing_vars}")
    raise RuntimeError(f"Missing env vars: {missing_vars}")

# --- Table IDs ---
METADATA_TABLE_FULL_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_METADATA_TABLE_ID}"

# --- Global Clients ---
bq_client = None
storage_client = None
xbrl_api_client = None
sec_api_key = None

# --- Helpers ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def access_secret_version(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()

def create_snake_case_name(raw_name):
    if not raw_name or not isinstance(raw_name, str):
        return None
    s = re.sub(r'\s*[:/\\(),%.]+\s*', '_', raw_name.strip())
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-zA-Z0-9_]', '', s)
    s = s.lower().strip('_')
    if not s:
        return None
    if s[0].isdigit() or s in ['select', 'from', 'where']:
        return 'col_' + s
    return s

def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        elif isinstance(v, list):
            for i, val in enumerate(v):
                if isinstance(val, dict):
                    items.extend(flatten_dict(val, f"{new_key}.{i}", sep).items())
                else:
                    items.append((f"{new_key}.{i}", val))
        else:
            items.append((new_key, v))
    return dict(items)

def consolidate_data(df, period_end_date):
    if df.empty:
        return df
    columns = df.columns.tolist()
    base_columns = set()
    for col in columns:
        base = col.split('_')[0]
        base_columns.add(base)
    consolidated_data = {}
    period_end_date = pd.to_datetime(period_end_date, errors='coerce')
    for base in base_columns:
        related_cols = [col for col in columns if col.startswith(f"{base}_") and col.endswith('_value')]
        if not related_cols:
            continue
        candidates = []
        for value_col in related_cols:
            index = value_col.split('_')[1].split('_value')[0]
            period_end_col = f"{base}_{index}_period_enddate"
            segment_col = f"{base}_{index}_segment_dimension"
            if len(df[value_col]) != len(df.index):
                logging.error(f"Length mismatch in {value_col}: values={len(df[value_col])}, index={len(df.index)}")
                continue
            value = df[value_col].iloc[0]
            if pd.isna(value):
                continue
            period = pd.to_datetime(df[period_end_col].iloc[0], errors='coerce') if period_end_col in df.columns else None
            has_segment = segment_col in df.columns and pd.notna(df[segment_col].iloc[0])
            candidates.append({
                'value': value,
                'period': period,
                'has_segment': has_segment,
                'index': index
            })
        if not candidates:
            continue
        selected_candidate = None
        for candidate in candidates:
            if candidate['period'] == period_end_date:
                selected_candidate = candidate
                break
        if not selected_candidate:
            non_segmented = [c for c in candidates if not c['has_segment']]
            if non_segmented:
                non_segmented.sort(key=lambda x: x['period'] if x['period'] is not None else pd.Timestamp.min, reverse=True)
                selected_candidate = non_segmented[0]
            else:
                candidates.sort(key=lambda x: x['period'] if x['period'] is not None else pd.Timestamp.min, reverse=True)
                selected_candidate = candidates[0]
        consolidated_data[base] = selected_candidate['value']
        logging.debug(f"Consolidated {base}: {selected_candidate['value']} (index: {selected_candidate['index']}, period: {selected_candidate['period']}, has_segment: {selected_candidate['has_segment']})")
    if not consolidated_data:
        return pd.DataFrame()
    try:
        df_consolidated = pd.DataFrame([consolidated_data])
        logging.debug(f"Consolidated DataFrame: {df_consolidated.to_dict()}")
        return df_consolidated
    except ValueError as e:
        logging.error(f"ValueError in consolidate_data: {e}. Data: {consolidated_data}")
        return pd.DataFrame()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=30), reraise=True)
def fetch_financial_statements_by_url(xbrl_api, filing_url):
    try:
        xbrl_json = xbrl_api.xbrl_to_json(htm_url=filing_url)
        logging.debug(f"Full XBRL JSON for {filing_url}: {json.dumps(xbrl_json, indent=2)}")
        income_statement = xbrl_json.get("StatementsOfIncome")
        balance_sheet = xbrl_json.get("BalanceSheets")
        cash_flow_statement = xbrl_json.get("StatementsOfCashFlows")
        period_of_report = xbrl_json.get("CoverPage", {}).get("DocumentPeriodEndDate")
        shares_outstanding = None
        for source in [
            xbrl_json.get("CoverPage", {}).get("EntityCommonStockSharesOutstanding"),
            xbrl_json.get("DocumentAndEntityInformation", {}).get("EntityCommonStockSharesOutstanding"),
            xbrl_json.get("CommonStockSharesOutstanding")
        ]:
            if source:
                if isinstance(source, dict) and 'value' in source:
                    shares_outstanding = source['value']
                    break
                elif isinstance(source, (str, int, float)):
                    shares_outstanding = source
                    break
                else:
                    logging.warning(f"Unexpected shares_outstanding format: {source}")
        logging.debug(f"Income statement present: {bool(income_statement)}")
        if income_statement:
            logging.debug(f"Income statement data: {json.dumps(income_statement, indent=2)}")
        logging.debug(f"Balance sheet present: {bool(balance_sheet)}")
        if balance_sheet:
            logging.debug(f"Balance sheet data: {json.dumps(balance_sheet, indent=2)}")
        logging.debug(f"Cash flow statement present: {bool(cash_flow_statement)}")
        if cash_flow_statement:
            logging.debug(f"Cash flow data: {json.dumps(cash_flow_statement, indent=2)}")
        logging.debug(f"Period of report for {filing_url}: {period_of_report}")
        logging.debug(f"Shares outstanding for {filing_url}: {shares_outstanding}")
        return income_statement, balance_sheet, cash_flow_statement, period_of_report, shares_outstanding
    except Exception as e:
        logging.error(f"Error fetching XBRL data for URL {filing_url}: {e}")
        raise

def process_financial_df(data, ticker, statement_type, accession_number, filed_date, period_of_report=None, shares_outstanding=None):
    metadata = {
        'ticker': ticker,
        'accession_number': accession_number,
        'period_end_date': pd.to_datetime(period_of_report, errors='coerce', utc=True),
        'filing_date': pd.to_datetime(filed_date, errors='coerce', utc=True)
    }
    if shares_outstanding is not None:
        logging.debug(f"shares_outstanding type: {type(shares_outstanding)}, value: {shares_outstanding}")
        if isinstance(shares_outstanding, dict) and 'value' in shares_outstanding:
            shares_outstanding = shares_outstanding['value']
            logging.warning(f"Extracted shares_outstanding value: {shares_outstanding}")
    metadata['shares_outstanding'] = pd.to_numeric(shares_outstanding, errors='coerce')
    if not data:
        logging.warning(f"No {statement_type} data for {ticker}_{accession_number}")
        return pd.DataFrame([metadata])
    try:
        if isinstance(data, list):
            flattened_data = [flatten_dict(item) for item in data if isinstance(item, dict)]
            df_financial = pd.DataFrame(flattened_data)
        elif isinstance(data, dict):
            flattened_data = flatten_dict(data)
            df_financial = pd.DataFrame([flattened_data])
        else:
            logging.warning(f"Unexpected {statement_type} data format: {type(data)}")
            return pd.DataFrame([metadata])
        logging.debug(f"Flattened {statement_type} DataFrame for {ticker}_{accession_number}: Columns: {df_financial.columns.tolist()}, Rows: {len(df_financial)}")
        logging.debug(f"Raw flattened data: {df_financial.to_dict()}")
        if df_financial.empty:
            logging.warning(f"Empty {statement_type} DataFrame after flattening")
            return pd.DataFrame([metadata])
        df_financial.columns = [create_snake_case_name(col) or f"col_{i}" for i, col in enumerate(df_financial.columns)]
        df_financial = df_financial.dropna(axis=1, how='all')
        df_consolidated = consolidate_data(df_financial, period_of_report)
        if df_consolidated.empty:
            logging.warning(f"No consolidated {statement_type} data after consolidation")
            return pd.DataFrame([metadata])
        for col in df_consolidated.columns:
            df_consolidated[col] = pd.to_numeric(df_consolidated[col], errors='coerce')
        for col, value in metadata.items():
            df_consolidated[col] = pd.Series([value] * len(df_consolidated)).reindex(df_consolidated.index)
        for col in ['period_end_date', 'filing_date']:
            if col in df_consolidated and not pd.isna(df_consolidated[col].iloc[0]):
                df_consolidated[col] = df_consolidated[col].dt.tz_convert('UTC')
        logging.debug(f"Processed {statement_type} - Columns: {df_consolidated.columns.tolist()}, Rows: {len(df_consolidated)}")
        return df_consolidated
    except Exception as e:
        logging.error(f"Error processing {statement_type}: {e}")
        return pd.DataFrame([metadata])

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def upload_to_gcs(df, bucket_name, prefix, ticker, accession_number, statement_type):
    try:
        bucket = storage_client.bucket(bucket_name)
        file_name = f"{prefix}{ticker}_{accession_number}.csv"
        blob = bucket.blob(file_name)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        logging.info(f"Uploaded {file_name} to gs://{bucket_name}/{file_name} ({len(df)} rows)")
        return f"gs://{bucket_name}/{file_name}"
    except Exception as e:
        logging.error(f"Error uploading {file_name} to gs://{bucket_name}: {e}")
        raise
    finally:
        csv_buffer.close()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def update_metadata_with_uris(ticker, accession_number, income_uri, balance_uri, cashflow_uri):
    try:
        def sanitize_string(s):
            if not isinstance(s, str):
                return s
            return ''.join(c for c in s if ord(c) < 128)
        original_ticker = ticker
        original_accession = accession_number
        ticker = sanitize_string(ticker)
        accession_number = sanitize_string(accession_number)
        income_uri = sanitize_string(income_uri)
        balance_uri = sanitize_string(balance_uri)
        cashflow_uri = sanitize_string(cashflow_uri)
        if ticker != original_ticker:
            logging.warning(f"Sanitized ticker from '{original_ticker}' to '{ticker}'")
        if accession_number != original_accession:
            logging.warning(f"Sanitized accession_number from '{original_accession}' to '{accession_number}'")
        if not ticker or not accession_number:
            logging.error(f"Invalid ticker or accession_number after sanitization: ticker='{ticker}', accession_number='{accession_number}'")
            return
        query = f"""
        UPDATE `{METADATA_TABLE_FULL_ID}`
        SET 
            IncomeStatementURI = @income_uri,
            BalanceSheetURI = @balance_uri,
            CashFlowURI = @cashflow_uri
        WHERE 
            Ticker = @ticker 
            AND AccessionNumber = @accession_number
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
                bigquery.ScalarQueryParameter("accession_number", "STRING", accession_number),
                bigquery.ScalarQueryParameter("income_uri", "STRING", income_uri),
                bigquery.ScalarQueryParameter("balance_uri", "STRING", balance_uri),
                bigquery.ScalarQueryParameter("cashflow_uri", "STRING", cashflow_uri),
            ]
        )
        logging.debug(f"Executing query with ticker='{ticker}', accession_number='{accession_number}'")
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()
        logging.info(f"Updated filing_metadata with URIs for {ticker}_{accession_number}")
    except Exception as e:
        logging.error(f"Error updating URIs for {ticker}_{accession_number}: {e}")
        raise

def check_existing_filings_in_gcs(bucket_name, prefixes, accession_numbers):
    if not accession_numbers:
        return set()
    try:
        existing = set()
        bucket = storage_client.bucket(bucket_name)
        for prefix in prefixes:
            blobs = bucket.list_blobs(prefix=prefix)
            for blob in blobs:
                if blob.name.endswith('.csv'):
                    acc_num = blob.name.split('_')[-1].replace('.csv', '')
                    if acc_num in accession_numbers:
                        existing.add(acc_num)
        return existing
    except Exception as e:
        logging.error(f"Error checking existing filings in gs://{bucket_name}: {e}")
        return set()

# --- Main ---
def main():
    global bq_client, storage_client, xbrl_api_client, sec_api_key
    start_time = time.time()
    processed_count = skipped_count = failed_count = total_checked = 0
    logging.info("--- Starting Financial Extraction Job ---")
    try:
        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        storage_client = storage.Client(project=GCP_PROJECT_ID)
        sec_api_key = access_secret_version(GCP_PROJECT_ID, SEC_API_SECRET_ID, SEC_API_SECRET_VERSION)
        xbrl_api_client = XbrlApi(api_key=sec_api_key)
        query = f"""
            SELECT Ticker, AccessionNumber, LinkToFilingDetails, FiledDate
            FROM `{METADATA_TABLE_FULL_ID}`
            WHERE FormType IN ('10-K', '10-Q')
            ORDER BY FiledDate DESC
        """
        if MAX_FILINGS_TO_PROCESS > 0:
            query += f" LIMIT {MAX_FILINGS_TO_PROCESS}"
        query_job = bq_client.query(query)
        filings = list(query_job.result())
        total_checked = len(filings)
        logging.info(f"Found {total_checked} filings to check")
        accession_numbers = [row.AccessionNumber for row in filings]
        prefixes = [GCS_BS_PREFIX, GCS_IS_PREFIX, GCS_CF_PREFIX]
        existing = check_existing_filings_in_gcs(GCS_BUCKET_NAME, prefixes, accession_numbers)
        to_process = [
            (row.Ticker, row.AccessionNumber, row.AccessionNumber, row.LinkToFilingDetails, row.FiledDate)
            for row in filings if row.AccessionNumber not in existing
        ]
        logging.info(f"{len(to_process)} filings to process (skipped {total_checked - len(to_process)})")
        for ticker, original_accession, accession_number, filing_url, filed_date in to_process:
            log_prefix = f"{ticker}_{accession_number}"
            if not all(c.isascii() for c in ticker) or not all(c.isascii() for c in original_accession):
                logging.error(f"Skipping {log_prefix} due to non-ASCII characters: ticker='{ticker}', accession='{original_accession}'")
                failed_count += 1
                continue
            logging.info(f"Processing {log_prefix}")
            try:
                income_data, balance_data, cashflow_data, period_of_report, shares_outstanding = fetch_financial_statements_by_url(xbrl_api_client, filing_url)
                income_df = process_financial_df(income_data, ticker, 'income_statement', accession_number, filed_date, period_of_report)
                balance_df = process_financial_df(balance_data, ticker, 'balance_sheet', accession_number, filed_date, period_of_report, shares_outstanding)
                cash_flow_df = process_financial_df(cashflow_data, ticker, 'cash_flow', accession_number, filed_date, period_of_report)
                income_uri = balance_uri = cashflow_uri = None
                if not income_df.empty:
                    income_uri = upload_to_gcs(income_df, GCS_BUCKET_NAME, GCS_IS_PREFIX, ticker, accession_number, 'income_statement')
                if not balance_df.empty:
                    balance_uri = upload_to_gcs(balance_df, GCS_BUCKET_NAME, GCS_BS_PREFIX, ticker, accession_number, 'balance_sheet')
                if not cash_flow_df.empty:
                    cashflow_uri = upload_to_gcs(cash_flow_df, GCS_BUCKET_NAME, GCS_CF_PREFIX, ticker, accession_number, 'cash_flow')
                update_metadata_with_uris(ticker, original_accession, income_uri, balance_uri, cashflow_uri)
                processed_count += 1
                logging.info(f"Successfully processed {log_prefix}")
            except Exception as e:
                logging.error(f"Failed processing {log_prefix}: {e}")
                failed_count += 1
        skipped_count = total_checked - (processed_count + failed_count)
    except Exception as e:
        logging.critical(f"Critical error: {e}")
        failed_count += 1
    finally:
        duration = time.time() - start_time
        logging.info(f"--- Financial Extraction Job Finished ---")
        logging.info(f"Duration: {duration:.2f} seconds")
        logging.info(f"Filings Checked: {total_checked}")
        logging.info(f"Filings Processed: {processed_count}")
        logging.info(f"Filings Skipped: {skipped_count}")
        logging.info(f"Filings Failed: {failed_count}")
        print(json.dumps({
            "job_name": "financial-extraction",
            "status": "completed_with_errors" if failed_count > 0 else "completed",
            "processed": processed_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "total_checked": total_checked
        }))
        sys.exit(1 if failed_count > 0 else 0)

if __name__ == "__main__":
    main()
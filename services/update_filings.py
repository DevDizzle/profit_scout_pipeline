import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery, secretmanager
from sec_api import QueryApi
import asyncio

# Explicitly load .env from the parent directory (profit_scout_pipeline/)
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
if not os.path.exists(dotenv_path):
    logging.error(f".env file not found at {dotenv_path}. Please ensure it exists.")
    exit(1)
load_dotenv(dotenv_path)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration variables from environment (defined early)
GCP_PROJECT_ID = os.getenv('gcp_project_id')
BQ_DATASET_ID = os.getenv('bq_dataset_id', 'profit_scout')
BQ_METADATA_TABLE_ID = os.getenv('bq_metadata_table_id', 'filing_metadata')
GCS_BUCKET_NAME = os.getenv('gcs_bucket_name')
GCS_PDF_FOLDER = os.getenv('gcs_pdf_folder', 'sec-pdf/')
SEC_API_SECRET_NAME = os.getenv('sec_api_secret_name', 'sec-api-key')
SEC_API_SECRET_VERSION = os.getenv('sec_api_secret_version', 'latest')

# Validate essential environment variables early
required_vars = [
    'gcp_project_id',
    'gcs_bucket_name',
    'sec_api_secret_name',
    'bq_dataset_id',
    'bq_metadata_table_id'
]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    logging.error(f"Missing essential environment variables: {', '.join(missing_vars)}")
    exit(1)

# Updated import statements (after .env loading to ensure variables are set)
from subscriber.src.subscriber import handle_filing_notification
from pdf_summarizer.src.pdf_summarizer import generate_sec_analysis, initialize_clients as init_pdf_clients
from news_summarizer.src.news_summarizer import process_news_summary_event, initialize_clients as init_news_clients

# Initialize BigQuery client
bq_client = bigquery.Client(project=GCP_PROJECT_ID)

def get_sec_api_key():
    """
    Retrieve the SEC API key from Google Secret Manager.
    
    Returns:
        str: The SEC API key.
    
    Raises:
        Exception: If the secret cannot be accessed.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{SEC_API_SECRET_NAME}/versions/{SEC_API_SECRET_VERSION}"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Failed to access SEC API key: {e}", exc_info=True)
        raise

# Initialize SEC API and summarizer clients (after configuration variables)
sec_api_key = get_sec_api_key()
query_api = QueryApi(api_key=sec_api_key)
init_pdf_clients()
init_news_clients()

async def main():
    """
    Main asynchronous function to update the dataset with the latest SEC filings.
    """
    # Query BigQuery for tickers and their maximum FiledDate
    query = f"""
    SELECT Ticker, MAX(FiledDate) as max_filed_date
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_METADATA_TABLE_ID}`
    GROUP BY Ticker
    """
    try:
        job = bq_client.query(query)
        results = job.result()
        tickers_max_dates = {row.Ticker: row.max_filed_date for row in results}
    except Exception as e:
        logging.error(f"Failed to query BigQuery: {e}", exc_info=True)
        raise

    today = datetime.now().date()
    today_str = today.strftime('%Y-%m-%d')

    # Process each ticker
    for ticker, max_date in tickers_max_dates.items():
        max_date_str = max_date.strftime('%Y-%m-%d') if max_date else '1900-01-01'
        logging.info(f"Checking filings for {ticker} since {max_date_str}")

        # Construct SEC API query for new 10-Q and 10-K filings
        query = {
            "query": {
                "query_string": {
                    "query": f"ticker:{ticker} AND formType:(\"10-Q\", \"10-K\") AND filedAt:>{max_date_str}"
                }
            },
            "from": "0",
            "size": "100",
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        try:
            response = query_api.get_filings(query)
            filings = response.get('filings', [])
        except Exception as e:
            logging.error(f"Failed to query SEC API for {ticker}: {e}", exc_info=True)
            continue

        # Process each new filing
        for filing in filings:
            # Handle varying key names for accession number
            accession_number = filing.get('accessionNumber') or filing.get('accessionNo') or filing.get('accession_number')
            if not accession_number:
                logging.error(f"No accession number found in filing: {filing}")
                continue

            accession_number_cleaned = accession_number.replace('-', '')
            form_type = filing.get('formType', '')
            filing_url = filing.get('linkToFilingDetails', '')
            filed_at = filing.get('filedAt', '')

            logging.info(f"Processing filing for {ticker}: {accession_number}")

            # Create event data dictionary for processing
            event_data = {
                'ticker': ticker,
                'accession_no': accession_number,
                'form_type': form_type,
                'filing_url': filing_url,
                'filed_at': filed_at,
            }

            try:
                # Process the filing (uploads PDF to GCS and loads financial data to BigQuery)
                await handle_filing_notification(event_data)

                # Construct the GCS path where the PDF is stored
                gcs_pdf_path = f"{GCS_PDF_FOLDER}{ticker}_{accession_number_cleaned}.pdf"

                # Generate PDF summary
                await generate_sec_analysis(gcs_pdf_path, ticker, accession_number_cleaned)

                # Generate news summary
                await process_news_summary_event(gcs_pdf_path, ticker, accession_number_cleaned)

                logging.info(f"Finished processing filing for {ticker}: {accession_number}")
            except Exception as e:
                logging.error(f"Error processing filing for {ticker}: {accession_number} - {e}", exc_info=True)
                continue

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Script execution failed: {e}", exc_info=True)
        exit(1)
# /home/eraphaelparra/profit_scout_pipeline/services/listener/src/processing.py
import asyncio
import json
import logging
import os
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

# --- Configuration ---
# Match variable names used in Terraform and potentially injected into Cloud Run
GCP_PROJECT_ID = os.getenv('gcp_project_id') # Assuming consistency, changed to lowercase
# --- Use Terraform variable name ---
PUBSUB_TOPIC_ID = os.getenv('pubsub_topic_id') # e.g., 'sec-filings-notifications'

# --- Validation (Using corrected names) ---
if not GCP_PROJECT_ID:
    # Consider changing other files (listener.py, subscriber.py) to use lowercase if desired
    GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID') # Fallback check for uppercase if needed during transition
    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID/gcp_project_id environment variable is not set.")

if not PUBSUB_TOPIC_ID:
    raise ValueError("pubsub_topic_id environment variable is not set.")

# --- Initialize Pub/Sub Client ---
# Reuse the client instance
try:
    publisher = pubsub_v1.PublisherClient()
    # Use validated GCP_PROJECT_ID and PUBSUB_TOPIC_ID
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)
except Exception as e:
    logging.exception(f"Failed to initialize Pub/Sub publisher: {e}")
    publisher = None

# --- Processing Function ---

async def process_new_filing(ticker: str, accession_no: str, form_type: str, filing_url: str, filed_at: str):
    """
    Formats filing data and publishes it to a Google Cloud Pub/Sub topic.
    """
    if not publisher:
        logging.error("Pub/Sub publisher client is not initialized. Cannot publish message.")
        return

    try:
        message_payload = {
            "ticker": ticker,
            "accession_no": accession_no,
            "form_type": form_type,
            "filing_url": filing_url,
            "filed_at": filed_at,
        }
        message_json = json.dumps(message_payload)
        message_bytes = message_json.encode("utf-8")

        logging.info(f"Publishing message for {ticker} ({accession_no}) to {topic_path}")
        publish_future = publisher.publish(topic_path, data=message_bytes)
        message_id = await asyncio.wrap_future(publish_future)
        logging.info(f"Successfully published message for {accession_no}. Message ID: {message_id}")

    except NotFound:
         logging.error(f"Pub/Sub topic {topic_path} not found during publish operation.")
    except Exception as e:
        logging.exception(f"Failed to publish message for {accession_no}: {e}")
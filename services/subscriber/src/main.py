# /services/subscriber/src/main.py

import base64
import json
import logging
import os
import asyncio
from flask import Flask, request
from services.subscriber.src.subscriber import handle_filing_notification

app = Flask(__name__)

# Configure logging if needed
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@app.route('/', methods=['POST'])
def index():
    """Entry point for Cloud Run service receiving Pub/Sub push messages."""
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        logging.error("Bad Pub/Sub message format received.")
        return "Bad Request: Invalid Pub/Sub message format", 400

    try:
        # Decode the Pub/Sub message
        payload = envelope['message']['data']
        decoded = base64.b64decode(payload).decode('utf-8')
        event_data = json.loads(decoded)
        logging.info(f"Received message triggering subscriber for AccessionNumber: {event_data.get('accession_no')}")

        # Run the async handler in a sync context
        asyncio.run(handle_filing_notification(event_data))

        logging.info(f"Successfully processed AccessionNumber: {event_data.get('accession_no')}")
        # 204 No Content, empty body
        return "", 204

    except Exception as e:
        acc = event_data.get('accession_no', 'N/A')
        logging.exception(f"Error processing Pub/Sub message for AccessionNumber: {acc}: {e}")
        return "Internal Server Error", 500

if __name__ == '__main__':
    PORT = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=PORT, debug=True)

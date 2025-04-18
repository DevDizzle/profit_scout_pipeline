import base64
import json
import logging
import os
import asyncio
from flask import Flask, request
from subscriber import handle_filing_notification # Assuming your handler is in subscriber.py

app = Flask(__name__)

# Configure logging basic setup if not handled elsewhere for Flask
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@app.route('/', methods=['POST'])
async def index():
    """ Entry point for Cloud Run service receiving Pub/Sub push messages. """
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        logging.error("Bad Pub/Sub message format received.")
        return "Bad Request: Invalid Pub/Sub message format", 400

    try:
        message_data_encoded = envelope['message']['data']
        message_data_decoded = base64.b64decode(message_data_encoded).decode('utf-8')
        event_data = json.loads(message_data_decoded)
        logging.info(f"Received message triggering subscriber for AccessionNumber: {event_data.get('accession_no')}")

        await handle_filing_notification(event_data)

        logging.info(f"Successfully processed AccessionNumber: {event_data.get('accession_no')}")
        return "OK", 204 # Use 204 No Content for success ACK without body
    except Exception as e:
        logging.exception(f"Error processing Pub/Sub message for AccessionNumber: {event_data.get('accession_no', 'N/A')}: {e}")
        # Return error status (e.g., 500) to signal failure to Pub/Sub
        return f"Internal Server Error", 500

if __name__ == '__main__':
     # For local testing (optional)
     PORT = int(os.getenv("PORT", 8080))
     app.run(host='0.0.0.0', port=PORT, debug=True) # Enable debug for local dev ease
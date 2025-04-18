# services/pdf_summarizer/src/main.py
import os
import base64
import json
import asyncio
from flask import Flask, request
import logging # Import logging if using Flask logging extensions
from pdf_summarizer import handle_gcs_event # Import the handler

app = Flask(__name__)

# Configure logging if needed for Flask app itself
# gunicorn_logger = logging.getLogger('gunicorn.error')
# app.logger.handlers = gunicorn_logger.handlers
# app.logger.setLevel(gunicorn_logger.level)

@app.route('/', methods=['POST'])
async def index():
    ''' Cloud Run service endpoint for Eventarc GCS push messages. '''
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        app.logger.error("Bad Eventarc message format: missing 'message'.")
        return "Bad Request: Invalid Eventarc message format", 400

    try:
        # Eventarc uses different encoding/structure than Pub/Sub direct push
        # GCS Eventarc payload is directly in 'message', not base64 encoded
        # The actual GCS event data is the 'message' field itself (already JSON)
        gcs_event_data = envelope['message']

        # Check if it has the expected GCS event structure (adjust based on actual Eventarc payload)
        if not isinstance(gcs_event_data, dict) or 'bucket' not in gcs_event_data or 'name' not in gcs_event_data:
             app.logger.error(f"Invalid GCS event payload structure within Eventarc message: {gcs_event_data}")
             return "Bad Request: Invalid GCS event payload", 400

        app.logger.info(f"Received valid GCS event via Eventarc for: gs://{gcs_event_data.get('bucket')}/{gcs_event_data.get('name')}")

        # Await the async handler
        await handle_gcs_event(gcs_event_data)

        app.logger.info(f"Successfully processed event for: gs://{gcs_event_data.get('bucket')}/{gcs_event_data.get('name')}")
        # Return 200 OK range to acknowledge the message to Eventarc
        return "OK", 204 # 204 No Content is typical for successful background processing

    except Exception as e:
        app.logger.exception(f"Error processing GCS event: {e}") # Use logger.exception to include traceback
        # Return error status (e.g., 500) to signal failure to Eventarc for potential retry
        return f"Internal Server Error", 500

if __name__ == '__main__':
    PORT = int(os.getenv("PORT", 8080))
    # Use 'debug=False' when running with Gunicorn in production
    app.run(host='0.0.0.0', port=PORT, debug=False)
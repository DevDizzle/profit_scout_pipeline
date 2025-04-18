# services/news_summarizer/src/main.py
import os, base64, json, asyncio, logging
from flask import Flask, request
from news_summarizer import handle_gcs_event # Import the handler

app = Flask(__name__)
# Optional: Configure Flask logger to integrate with Gunicorn/Cloud Run logging
# app.logger.setLevel(logging.INFO)

@app.route('/', methods=['POST'])
async def index():
    ''' Cloud Run service endpoint for Eventarc GCS push messages. '''
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        app.logger.error("Bad Eventarc message format: missing 'message'.")
        return "Bad Request: Invalid Eventarc message format", 400
    try:
        gcs_event_data = envelope['message'] # Eventarc GCS payload is directly in message
        if not isinstance(gcs_event_data, dict) or 'bucket' not in gcs_event_data or 'name' not in gcs_event_data:
             app.logger.error(f"Invalid GCS event payload structure: {gcs_event_data}")
             return "Bad Request: Invalid GCS event payload", 400
        app.logger.info(f"News Summarizer received GCS event via Eventarc for: gs://{gcs_event_data.get('bucket')}/{gcs_event_data.get('name')}")
        await handle_gcs_event(gcs_event_data)
        app.logger.info(f"Successfully processed news summary event for: gs://{gcs_event_data.get('bucket')}/{gcs_event_data.get('name')}")
        return "OK", 204
    except Exception as e:
        app.logger.exception(f"Error processing news summary GCS event: {e}")
        return f"Internal Server Error", 500

if __name__ == '__main__':
    PORT = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=PORT, debug=False) # Debug=False for production
# services/news_summarizer/src/main.py

import os
import base64
import json
import logging
import asyncio
from flask import Flask, request

# import the handler by its full module path
from services.news_summarizer.src.news_summarizer import handle_gcs_event

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():
    """ Cloud Run service endpoint for Eventarc GCS push messages. """
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        app.logger.error("Bad Eventarc message format: missing 'message'.")
        return "Bad Request: Invalid Eventarc message format", 400

    try:
        gcs_event_data = envelope['message']
        if (
            not isinstance(gcs_event_data, dict)
            or 'bucket' not in gcs_event_data
            or 'name' not in gcs_event_data
        ):
            app.logger.error(f"Invalid GCS event payload structure: {gcs_event_data}")
            return "Bad Request: Invalid GCS event payload", 400

        app.logger.info(
            f"News Summarizer received GCS event for: "
            f"gs://{gcs_event_data['bucket']}/{gcs_event_data['name']}"
        )

        # run the async handler
        asyncio.run(handle_gcs_event(gcs_event_data))

        app.logger.info(
            f"Successfully processed news summary event for: "
            f"gs://{gcs_event_data['bucket']}/{gcs_event_data['name']}"
        )
        return "OK", 204

    except Exception as e:
        app.logger.exception(f"Error processing news summary GCS event: {e}")
        return "Internal Server Error", 500


if __name__ == '__main__':
    PORT = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=PORT, debug=False)

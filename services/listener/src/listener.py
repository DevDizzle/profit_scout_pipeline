import asyncio
import json
import logging
import os
import signal
import websockets
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from google.cloud import secretmanager

# Assuming process_new_filing is defined in a separate module
from processing import process_new_filing

# --- Configuration ---
# Load config from environment variables
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
SECRET_ID = os.getenv('SEC_API_SECRET_ID', 'sec-api-key') # Default secret name
SECRET_VERSION = os.getenv('SEC_API_SECRET_VERSION', 'latest')
WEBSOCKET_URL = os.getenv('SEC_WEBSOCKET_URL', "wss://sec-api.io/stream") # Default URL, replace if needed

if not GCP_PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID environment variable is not set.")
if not WEBSOCKET_URL:
     raise ValueError("SEC_WEBSOCKET_URL environment variable is not set or is empty.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variable to store the fetched API key
SEC_API_KEY = None

# --- Helper Functions ---

def access_secret_version(project_id, secret_id, version_id="latest"):
    """
    Accesses a secret version from Google Secret Manager.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        logging.info(f"Successfully accessed secret: {secret_id}")
        return payload
    except Exception as e:
        logging.error(f"Failed to access secret {secret_id}: {e}")
        raise  # Re-raise the exception to halt startup if secret is critical

def log_retry_attempt(retry_state):
    """Logs details of a retry attempt using tenacity."""
    logging.warning(
        f"Retrying connection attempt {retry_state.attempt_number} "
        f"after error. Waiting {retry_state.outcome_wait:.2f} seconds..."
    )

async def handle_message(message, ws):
    """Handles processing of a single WebSocket message."""
    try:
        data = json.loads(message)
        logging.debug(f"Received raw data: {data}") # More verbose logging if needed

        form_type = data.get('formType')
        ticker = data.get('ticker') # Get ticker for logging even if skipped

        logging.info(f"Received event: Form={form_type}, Ticker={ticker}")

        if form_type in ['10-K', '10-Q']:
            # Extract other necessary fields
            accession_no = data.get('accessionNo')
            filing_url = data.get('linkToFilingDetails')
            filed_at = data.get('filedAt')

            # Validate required fields
            if all([ticker, accession_no, filing_url, filed_at]):
                logging.info(f"Processing {form_type} for {ticker} (Accession: {accession_no})")
                # Call downstream function asynchronously
                asyncio.create_task(process_new_filing(ticker, accession_no, form_type, filing_url, filed_at))
            else:
                logging.warning(f"Skipping {form_type} for {ticker} (Accession: {data.get('accessionNo', 'N/A')}): Missing required fields in payload.")
        else:
             logging.debug(f"Skipping event: Form={form_type}, Ticker={ticker} (Not 10-K or 10-Q)")

    except json.JSONDecodeError:
        logging.error(f"Failed to parse JSON message: {message[:200]}...") # Log partial message
    except Exception as e:
        logging.exception(f"Error processing message: {e}") # Use logging.exception to include traceback

# --- Main Listener Logic ---

async def listen_to_sec_stream(shutdown_event):
    """
    Listens to the sec-api.io Stream API, handling connections, messages,
    and reacting to the shutdown event.
    """
    global SEC_API_KEY # Use the globally fetched key
    if not SEC_API_KEY:
         logging.error("SEC API Key was not loaded successfully. Cannot connect.")
         return # Or raise an error

    websocket_connection = None # Keep track of the connection

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10), before_sleep=log_retry_attempt)
    async def connect_with_retry():
        """Establishes WebSocket connection with retry logic."""
        nonlocal websocket_connection
        logging.info(f"Attempting to connect to {WEBSOCKET_URL}...")
        # Pass the fetched key in headers
        websocket_connection = await websockets.connect(
            WEBSOCKET_URL,
            extra_headers={"Authorization": f"Bearer {SEC_API_KEY}"}
        )
        logging.info("Successfully connected to SEC Stream API.")
        return websocket_connection

    while not shutdown_event.is_set():
        try:
            ws = await connect_with_retry()

            # Listen for messages until disconnected or shutdown requested
            while not shutdown_event.is_set():
                try:
                    # Set a timeout for receiving messages to periodically check shutdown_event
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    await handle_message(message, ws)
                except asyncio.TimeoutError:
                    continue # No message received, check shutdown_event and loop
                except websockets.exceptions.ConnectionClosedOK:
                    logging.info("WebSocket connection closed normally.")
                    break # Exit inner loop to reconnect
                except websockets.exceptions.ConnectionClosedError as e:
                    logging.warning(f"WebSocket connection closed with error: {e}")
                    break # Exit inner loop to reconnect

            # If connection closed, reset retry state if tenacity didn't handle it (it should)
            # And close the connection explicitly if it's still somehow open
            if ws and ws.open:
                 await ws.close()
                 logging.info("Explicitly closed WebSocket connection.")
            websocket_connection = None # Clear connection reference

        except RetryError as e:
             logging.error(f"Connection failed after multiple retries: {e}. Waiting before trying again...")
             # Wait longer before restarting the connection attempt cycle if all retries fail
             await asyncio.sleep(30)
        except websockets.exceptions.InvalidHandshake as e:
            logging.error(f"Connection failed - Invalid Handshake: {e}. Check URL and API Key/Auth.")
            # Wait significantly longer or potentially exit if auth fails repeatedly
            await asyncio.sleep(60)
        except Exception as e:
            logging.exception(f"Unexpected error in connection loop: {e}")
            await asyncio.sleep(10) # Wait before trying to reconnect after unexpected errors

        # Ensure we don't immediately retry if shutdown was requested during sleep/error
        if shutdown_event.is_set():
             logging.info("Shutdown requested, breaking connection loop.")
             break

    logging.info("Listener loop finished.")
    # Final cleanup if connection is somehow still open
    if websocket_connection and websocket_connection.open:
        await websocket_connection.close()
        logging.info("Final WebSocket connection cleanup.")


async def main():
    """
    Main function to set up signal handling and run the listener.
    """
    global SEC_API_KEY
    SEC_API_KEY = access_secret_version(GCP_PROJECT_ID, SECRET_ID, SECRET_VERSION)

    if not SEC_API_KEY:
        logging.critical("Failed to obtain SEC API Key from Secret Manager. Exiting.")
        return

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def signal_handler():
        logging.info("Shutdown signal received!")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
             loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
             # Signal handlers might not be available on all platforms (e.g., Windows)
             logging.warning(f"Signal handler for {sig} not supported on this platform.")

    logging.info("Starting SEC filing listener service...")
    listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))

    # Wait for the listener task to complete (which happens when shutdown_event is set)
    await listener_task
    logging.info("Listener task completed. Service shutting down.")


if __name__ == "__main__":
    # Consider basic argument parsing here if needed (e.g., for config overrides)
    asyncio.run(main())
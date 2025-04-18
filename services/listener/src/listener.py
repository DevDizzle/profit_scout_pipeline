# /home/eraphaelparra/profit_scout_pipeline/services/listener/src/listener.py
import asyncio
import json
import logging
import os
import signal
import websockets
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from google.cloud import secretmanager

# --- Updated Import ---
# Import using the full path from the project root
from services.listener.src.processing import process_new_filing

# --- Configuration (Using snake_case aligned with variables.tf) ---
# Load config from environment variables using snake_case names
gcp_project_id = os.getenv('gcp_project_id')
sec_api_secret_name = os.getenv('sec_api_secret_name', 'sec-api-key') # Use TF var name, keep default val
sec_api_secret_version = os.getenv('sec_api_secret_version', 'latest') # Keep this name
sec_websocket_url = os.getenv('sec_websocket_url', "wss://sec-api.io/stream") # Use TF var name

# --- Validation (Using snake_case variable names) ---
if not gcp_project_id:
    raise ValueError("gcp_project_id environment variable is not set.")
if not sec_websocket_url:
     raise ValueError("sec_websocket_url environment variable is not set or is empty.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variable to store the fetched API key
SEC_API_KEY = None

# --- Helper Functions ---

def access_secret_version(project_id, secret_id, version_id="latest"):
    """
    Accesses a secret version from Google Secret Manager.
    Note: 'secret_id' here corresponds to the secret *name* (e.g., sec_api_secret_name)
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        api_key = response.payload.data.decode("UTF-8")
        logging.info(f"Successfully accessed secret: {secret_id}")
        return api_key
    except Exception as e:
        logging.exception(f"Failed to access secret version ({secret_id}/{version_id}): {e}")
        return None

def log_retry_attempt(retry_state):
    """
    Logs a warning message before a retry attempt using tenacity.
    """
    logging.warning(
        f"Retrying connection (attempt {retry_state.attempt_number}): "
        f"Waiting {retry_state.outcome_wait:.2f} seconds before next attempt."
    )

async def handle_message(message, websocket):
    """
    Processes a single message received from the WebSocket stream.
    """
    try:
        data = json.loads(message)
        form_type = data.get("formType", "").upper()

        if form_type in ["10-K", "10-Q"]:
            required_fields = ["ticker", "accessionNo", "linkToFilingDetails", "filedAt"]
            if all(field in data for field in required_fields):
                ticker = data['ticker']
                accession_no = data['accessionNo']
                filing_url = data['linkToFilingDetails']
                filed_at = data['filedAt']

                logging.info(f"Received relevant filing: {form_type} for {ticker}")
                # Schedule the processing task asynchronously
                # Pass extracted data to the processing function
                asyncio.create_task(process_new_filing(
                    ticker=ticker,
                    accession_no=accession_no,
                    form_type=form_type,
                    filing_url=filing_url,
                    filed_at=filed_at
                ))
            else:
                logging.warning(f"Skipping filing due to missing fields: {data}")
        else:
            logging.debug(f"Skipping form type: {form_type}")

    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON message: {message[:100]}...")
    except Exception as e:
        logging.exception(f"Error processing message: {e}")

@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry_error_callback=lambda retry_state: logging.error(
        f"Connection failed after {retry_state.attempt_number} attempts."
    ),
    before_sleep=log_retry_attempt
)
async def connect_with_retry(url, api_key):
    """
    Connects to the WebSocket with retry logic.
    """
    logging.info(f"Attempting to connect to WebSocket: {url}")
    headers = {"Authorization": f"Bearer {api_key}"}
    async with websockets.connect(url, extra_headers=headers, open_timeout=10) as websocket:
        logging.info("WebSocket connection established successfully.")
        return websocket

async def listen_to_sec_stream(shutdown_event):
    """
    Listens to the SEC stream, handles messages, and manages reconnection.
    """
    global SEC_API_KEY
    websocket_connection = None

    while not shutdown_event.is_set():
        if not SEC_API_KEY:
            logging.error("SEC API Key was not loaded successfully. Cannot connect.")
            await asyncio.sleep(30)
            continue

        try:
            # Use the snake_case variable for the URL
            websocket_connection = await connect_with_retry(sec_websocket_url, SEC_API_KEY)

            while not shutdown_event.is_set():
                try:
                    message = await asyncio.wait_for(websocket_connection.recv(), timeout=60.0)
                    await handle_message(message, websocket_connection)
                except asyncio.TimeoutError:
                    logging.debug("WebSocket receive timeout, checking connection...")
                    try:
                         await websocket_connection.ping()
                    except ConnectionClosedOK:
                         logging.info("Connection closed while sending ping. Reconnecting.")
                         break
                except ConnectionClosedOK:
                    logging.info("WebSocket connection closed normally by server. Reconnecting.")
                    break
                except ConnectionClosedError as e:
                     logging.warning(f"WebSocket connection closed with error: {e}. Reconnecting.")
                     break
                except Exception as e:
                    logging.exception(f"Error during message handling: {e}")
                    await asyncio.sleep(5)

        except RetryError:
            logging.error("Connection failed after multiple retries. Waiting 60 seconds before next attempt.")
            await asyncio.sleep(60)
        except (InvalidHandshake, ConnectionRefusedError, websockets.exceptions.InvalidURI) as e:
             logging.error(f"Connection failed - {type(e).__name__}: {e}. Waiting 60 seconds.")
             await asyncio.sleep(60)
        except Exception as e:
            logging.exception(f"Unexpected error in connection loop: {e}")
            if websocket_connection and websocket_connection.open:
                await websocket_connection.close()
            await asyncio.sleep(30)

        finally:
            if websocket_connection and websocket_connection.open:
                 await websocket_connection.close()
                 logging.info("Explicitly closed WebSocket connection.")
            websocket_connection = None

        if not shutdown_event.is_set():
             await asyncio.sleep(5)

    logging.info("Shutdown requested, breaking connection loop.")
    if websocket_connection and websocket_connection.open:
        await websocket_connection.close()
        logging.info("Final WebSocket connection cleanup.")

async def main():
    """
    Main function to set up signal handling and run the listener.
    """
    global SEC_API_KEY
    # Fetch API key using the snake_case variable names
    SEC_API_KEY = access_secret_version(
        project_id=gcp_project_id,
        secret_id=sec_api_secret_name, # Use the secret *name* here
        version_id=sec_api_secret_version
    )

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
             logging.warning(f"Signal handler for {sig} not supported on this platform.")

    logging.info("Starting SEC filing listener service...")
    listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))

    await listener_task
    logging.info("Listener task completed. Service shutting down.")

if __name__ == "__main__":
    # If running directly, ensure environment variables are loaded (e.g., via .env file and python-dotenv)
    # from dotenv import load_dotenv
    # load_dotenv()
    # # Reload variables after loading .env if necessary
    # gcp_project_id = os.getenv('gcp_project_id')
    # sec_api_secret_name = os.getenv('sec_api_secret_name', 'sec-api-key')
    # sec_api_secret_version = os.getenv('sec_api_secret_version', 'latest')
    # sec_websocket_url = os.getenv('sec_websocket_url', "wss://sec-api.io/stream")
    # # Need to re-validate after potentially reloading
    # if not gcp_project_id: raise ValueError("gcp_project_id env var not set.")
    # if not sec_websocket_url: raise ValueError("sec_websocket_url env var not set.")

    asyncio.run(main())
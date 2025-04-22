# services/listener/src/listener.py

import asyncio
import json
import logging
import os
import signal

import websockets
from websockets.exceptions import (
    ConnectionClosedOK,
    ConnectionClosedError,
    InvalidHandshake,
    InvalidURI,
)
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from google.cloud import secretmanager

from services.listener.src.processing import process_new_filing


# —————————————————————————————————————————————————————————————————————————
# Configuration (loaded once at import)
# —————————————————————————————————————————————————————————————————————————

gcp_project_id         = os.getenv("gcp_project_id")
sec_api_secret_name    = os.getenv("sec_api_secret_name",    "sec-api-key")
sec_api_secret_version = os.getenv("sec_api_secret_version", "latest")
sec_websocket_url      = os.getenv("sec_websocket_url",      "wss://sec-api.io/stream")

if not gcp_project_id:
    raise ValueError("gcp_project_id environment variable is not set.")
if not sec_websocket_url:
    raise ValueError("sec_websocket_url environment variable is not set or is empty.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global holding your live SEC key – set in main()
SEC_API_KEY = ""


# —————————————————————————————————————————————————————————————————————————
# Helpers
# —————————————————————————————————————————————————————————————————————————

def access_secret_version(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Fetches a secret from GCP Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    api_key = response.payload.data.decode("UTF-8")
    logging.info(f"Successfully accessed secret: {secret_id}")
    return api_key

def log_retry_attempt(retry_state):
    logging.warning(
        f"Retrying connection (attempt {retry_state.attempt_number}): "
        f"waiting {retry_state.outcome_wait:.2f}s before next attempt."
    )

@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry_error_callback=lambda state: logging.error(
        f"Connection failed after {state.attempt_number} attempts."
    ),
    before_sleep=log_retry_attempt
)
async def connect_with_retry(url: str, api_key: str) -> websockets.WebSocketClientProtocol:
    """
    Opens the WebSocket with tenacity retry; on success returns the ws.
    """
    logging.info(f"Attempting to connect to WebSocket: {url}")
    headers = {"Authorization": f"Bearer {api_key}"}
    async with websockets.connect(
        url,
        extra_headers=headers,
        open_timeout=10
    ) as ws:
        logging.info("WebSocket connection established successfully.")
        return ws


async def handle_message(message: str, websocket: websockets.WebSocketClientProtocol):
    """
    Parse & dispatch a single SEC message.
    """
    try:
        data = json.loads(message)
        form_type = data.get("formType", "").upper()

        if form_type in ("10-K", "10-Q"):
            required = ("ticker", "accessionNo", "linkToFilingDetails", "filedAt")
            if all(field in data for field in required):
                logging.info(f"Received relevant filing: {form_type} for {data['ticker']}")
                asyncio.create_task(process_new_filing(
                    ticker=data["ticker"],
                    accession_no=data["accessionNo"],
                    form_type=form_type,
                    filing_url=data["linkToFilingDetails"],
                    filed_at=data["filedAt"],
                ))
            else:
                logging.warning(f"Skipping filing due to missing fields: {data}")
        else:
            logging.debug(f"Skipping form type: {form_type}")

    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON message: {message[:100]}...")
    except Exception as e:
        logging.exception(f"Error processing message: {e}")


# —————————————————————————————————————————————————————————————————————————
# Core loop
# —————————————————————————————————————————————————————————————————————————

async def listen_to_sec_stream(shutdown_event: asyncio.Event):
    """
    Connect → read messages → reconnect on error → loop until shutdown_event is set.
    """
    global SEC_API_KEY

    # Always execute the missing‐key branch first, even if shutdown already requested
    if SEC_API_KEY is None:
        logging.error("SEC API Key was not loaded successfully. Cannot connect.")
        await asyncio.sleep(30)
        return

    websocket_connection = None
    while not shutdown_event.is_set():
        try:
            # 1) handshake with retry
            websocket_connection = await connect_with_retry(sec_websocket_url, SEC_API_KEY)

            # 2) read loop
            while not shutdown_event.is_set():
                try:
                    msg = await asyncio.wait_for(websocket_connection.recv(), timeout=60.0)
                    await handle_message(msg, websocket_connection)

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
            logging.error(
                "Connection failed after multiple retries. Waiting 60 seconds before next attempt."
            )
            await asyncio.sleep(60)

        except (InvalidHandshake, ConnectionRefusedError, InvalidURI) as e:
            logging.error(f"Connection failed - {type(e).__name__}: {e}. Waiting 60 seconds.")
            await asyncio.sleep(60)

        except Exception as e:
            logging.exception(f"Unexpected error in connection loop: {e}")
            await asyncio.sleep(30)

        finally:
            if websocket_connection and websocket_connection.open:
                await websocket_connection.close()
                logging.info("Explicitly closed WebSocket connection.")
            websocket_connection = None

        if not shutdown_event.is_set():
            await asyncio.sleep(5)

    # Final shutdown
    logging.info("Shutdown requested, breaking connection loop.")
    if websocket_connection and websocket_connection.open:
        await websocket_connection.close()
        logging.info("Final WebSocket connection cleanup.")


# —————————————————————————————————————————————————————————————————————————
# Entrypoint
# —————————————————————————————————————————————————————————————————————————

async def main():
    global SEC_API_KEY

    SEC_API_KEY = access_secret_version(
        project_id=gcp_project_id,
        secret_id=sec_api_secret_name,
        version_id=sec_api_secret_version,
    )
    if not SEC_API_KEY:
        logging.critical("Failed to obtain SEC API Key from Secret Manager. Exiting.")
        return

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _on_signal():
        logging.info("Shutdown signal received!")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            logging.warning(f"Signal handler for {sig} not supported on this platform.")

    logging.info("Starting SEC filing listener service…")
    await listen_to_sec_stream(shutdown_event)
    logging.info("Listener task completed. Service shutting down.")


if __name__ == "__main__":
    asyncio.run(main())

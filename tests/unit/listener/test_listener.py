# tests/unit/listener/test_listener.py
import asyncio
import json
import logging
import os
import signal
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call # Import call
from tenacity import RetryError
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, InvalidHandshake

# --- Imports ---
# Import functions from the listener module using its full path from the project root
from services.listener.src.listener import (
    # access_secret_version, # No longer testing directly
    handle_message,
    listen_to_sec_stream,
    log_retry_attempt,
    main,
)
# Import the processing function using its full path
from services.listener.src.processing import process_new_filing

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)

# Define the base path for patching, corresponding to the location of listener.py
LISTENER_MODULE_PATH = "services.listener.src.listener"
# Define the base path for patching processing.py functions called *from listener.py*
# Ensure this matches the import within listener.py if it calls processing.py
PROCESSING_MODULE_PATH_IN_LISTENER = "services.listener.src.listener.process_new_filing" # Or "services.listener.src.processing.process_new_filing" if called directly

class TestListener(unittest.IsolatedAsyncioTestCase):
    """
    Test suite for the listener.py module.
    """

    # Removed direct tests for access_secret_version as main covers the integration

    def test_log_retry_attempt(self):
        """
        Test the log_retry_attempt function.
        """
        with patch(f"{LISTENER_MODULE_PATH}.logging.warning") as mock_warning:
            mock_retry_state = MagicMock()
            mock_retry_state.attempt_number = 3
            mock_retry_state.outcome_wait = 5.0
            log_retry_attempt(mock_retry_state)
            mock_warning.assert_called_once()

    # --- Updated test_handle_message_valid_10k ---
    # Patch the target function process_new_filing where it's imported/used in listener.py
    # Assuming listener.py does: from services.listener.src.processing import process_new_filing
    # If listener.py imports differently, adjust the patch target
    @patch("services.listener.src.listener.process_new_filing", new_callable=AsyncMock)
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_handle_message_valid_10k(self, mock_logging_info, mock_create_task, mock_process_filing):
        """
        Test handling a valid 10-K message triggers processing task.
        """
        ticker = "AAPL"
        accession_no = "0001234567-23-000001"
        form_type = "10-K"
        filing_url = "http://example.com/filing"
        filed_at = "2023-10-27T12:00:00Z"

        message = json.dumps({
                "formType": form_type, "ticker": ticker, "accessionNo": accession_no,
                "linkToFilingDetails": filing_url, "filedAt": filed_at,
        })
        mock_ws = AsyncMock()

        # Simulate create_task executing the coroutine
        async def run_coro(coro): await coro
        mock_create_task.side_effect = run_coro

        await handle_message(message, mock_ws)

        mock_create_task.assert_called_once()
        # Check the mock of the function *itself* was awaited with correct args
        # This assumes handle_message extracts args correctly and passes them
        mock_process_filing.assert_awaited_once_with(
             ticker=ticker, accession_no=accession_no, form_type=form_type,
             filing_url=filing_url, filed_at=filed_at
        )
        mock_logging_info.assert_called()

    @patch(f"{LISTENER_MODULE_PATH}.logging.warning")
    async def test_handle_message_missing_fields(self, mock_logging_warning):
        """
        Test handling a message with missing fields.
        """
        message = json.dumps({"formType": "10-K", "ticker": "AAPL"}) # Missing fields
        mock_ws = AsyncMock()
        # Patch create_task to prevent it from actually running anything
        with patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task") as mock_create_task_skip:
            await handle_message(message, mock_ws)
            mock_create_task_skip.assert_not_called() # Should not be called if fields missing
        mock_logging_warning.assert_called_once()


    @patch(f"{LISTENER_MODULE_PATH}.logging.debug")
    async def test_handle_message_skip_other_forms(self, mock_logging_debug):
        """ Test handling a message with a non-10-K/10-Q form type. """
        message = json.dumps({"formType": "8-K", "ticker": "AAPL"})
        mock_ws = AsyncMock()
        with patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task") as mock_create_task_skip:
             await handle_message(message, mock_ws)
             mock_create_task_skip.assert_not_called() # Should not process other forms
        mock_logging_debug.assert_called_once()

    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_handle_message_invalid_json(self, mock_logging_error):
        """ Test handling an invalid JSON message. """
        message = "invalid json"
        mock_ws = AsyncMock()
        await handle_message(message, mock_ws)
        mock_logging_error.assert_called_once()

    @patch(f"{LISTENER_MODULE_PATH}.logging.exception")
    async def test_handle_message_exception(self, mock_logging_exception):
        """ Test handling an exception during message processing. """
        message = json.dumps({"formType": "10-K", "ticker": "AAPL"}) # Valid message
        mock_ws = AsyncMock()
        # Simulate error during json.loads inside handle_message
        with patch(f"{LISTENER_MODULE_PATH}.json.loads", side_effect=Exception("Test exception")):
            await handle_message(message, mock_ws)
        mock_logging_exception.assert_called_once()

    # === Connection Tests ===
    # Patch connect_with_retry as it contains the retry logic and actual connection call
    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_listen_to_sec_stream_connection_success(self, mock_logging_error, mock_logging_info, mock_connect_with_retry):
        """ Test successful WebSocket connection via retry mechanism. """
        mock_ws = AsyncMock()
        # Simulate normal closure after first successful connection and recv attempt
        mock_ws.recv.side_effect = [ConnectionClosedOK()]
        mock_connect_with_retry.return_value = mock_ws # Simulate successful connection

        shutdown_event = asyncio.Event()
        # Patch the global API key set by main()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1) # Allow time for connect and recv error
            shutdown_event.set() # Shutdown outer loop
            await listener_task

        mock_connect_with_retry.assert_awaited_once() # Ensure connection attempt happened
        mock_logging_error.assert_not_called() # No connection errors expected

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry", side_effect=RetryError("Simulated RetryError"))
    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_listen_to_sec_stream_connection_retry_error(self, mock_logging_error, mock_connect_with_retry):
        """ Test connection failure after multiple retries (simulated by RetryError). """
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            await listener_task # Should exit due to RetryError

        self.assertIn("Connection failed after multiple retries", mock_logging_error.call_args[0][0])
        mock_connect_with_retry.assert_awaited_once()

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.warning")
    async def test_listen_to_sec_stream_connection_closed_error(self, mock_logging_warning, mock_connect_with_retry):
        """ Test handling of WebSocket connection closed with error during recv. """
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = ConnectionClosedError(1006, "Connection reset by peer")
        mock_connect_with_retry.return_value = mock_ws # Simulate connection success first

        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await listener_task

        mock_connect_with_retry.assert_awaited_once()
        self.assertTrue(any("WebSocket connection closed with error" in call[0][0] for call in mock_logging_warning.call_args_list))

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry", side_effect=InvalidHandshake("Invalid handshake"))
    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_listen_to_sec_stream_invalid_handshake(self, mock_logging_error, mock_connect_with_retry):
        """ Test handling of invalid handshake during connection attempt. """
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            await listener_task

        self.assertIn("Connection failed - InvalidHandshake", mock_logging_error.call_args[0][0])
        mock_connect_with_retry.assert_awaited_once()

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry", side_effect=Exception("Unexpected connection error"))
    @patch(f"{LISTENER_MODULE_PATH}.logging.exception")
    async def test_listen_to_sec_stream_unexpected_error(self, mock_logging_exception, mock_connect_with_retry):
        """ Test handling of unexpected error in connection loop. """
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            await listener_task
        self.assertIn("Unexpected error in connection loop", mock_logging_exception.call_args[0][0])
        mock_connect_with_retry.assert_awaited_once()


    # === Main Function Tests (Updated) ===
    @patch(f"{LISTENER_MODULE_PATH}.os.getenv") # Patch os.getenv where it's called in listener
    @patch(f"{LISTENER_MODULE_PATH}.access_secret_version")
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task")
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.get_running_loop")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_main_success(self, mock_logging_info, mock_get_running_loop, mock_create_task, mock_access_secret, mock_os_getenv):
        """ Test the main function successfully initializes and runs the listener task. """
        # --- Arrange ---
        # Configure mock os.getenv to return values for the snake_case names
        TEST_PROJECT_ID = "test-proj-id"
        TEST_SECRET_NAME = "test-secret-name"
        TEST_SECRET_VERSION = "test-version"
        TEST_WS_URL = "ws://test-url"

        def getenv_side_effect(key, default=None):
            if key == 'gcp_project_id': return TEST_PROJECT_ID
            if key == 'sec_api_secret_name': return TEST_SECRET_NAME
            if key == 'sec_api_secret_version': return TEST_SECRET_VERSION
            if key == 'sec_websocket_url': return TEST_WS_URL
            return default # Return default for any other keys if needed

        mock_os_getenv.side_effect = getenv_side_effect

        # Configure mock access_secret_version
        mock_access_secret.return_value = "test_api_key_from_secret"

        # Configure other mocks for main function execution
        mock_loop = MagicMock()
        mock_get_running_loop.return_value = mock_loop
        mock_task = AsyncMock() # Mock the listener task
        mock_create_task.return_value = mock_task

        # --- Act ---
        await main()

        # --- Assert ---
        # Check getenv was called for expected keys (adjust based on listener.py's imports)
        mock_os_getenv.assert_any_call('gcp_project_id')
        mock_os_getenv.assert_any_call('sec_api_secret_name', 'sec-api-key') # Check default if used
        mock_os_getenv.assert_any_call('sec_api_secret_version', 'latest')
        mock_os_getenv.assert_any_call('sec_websocket_url', "wss://sec-api.io/stream")

        # Check access_secret_version called with values from mocked getenv
        mock_access_secret.assert_called_once_with(
             project_id=TEST_PROJECT_ID,
             secret_id=TEST_SECRET_NAME,
             version_id=TEST_SECRET_VERSION
        )

        # Check listener task was created and awaited
        mock_create_task.assert_called_once() # Task for listen_to_sec_stream
        mock_task.assert_awaited()

        # Check signal handlers were added
        self.assertEqual(mock_loop.add_signal_handler.call_count, 2)
        mock_logging_info.assert_called() # Check logs

    @patch(f"{LISTENER_MODULE_PATH}.os.getenv") # Patch os.getenv where it's called in listener
    @patch(f"{LISTENER_MODULE_PATH}.access_secret_version")
    @patch(f"{LISTENER_MODULE_PATH}.logging.critical")
    async def test_main_secret_access_failure(self, mock_logging_critical, mock_access_secret, mock_os_getenv):
        """ Test the main function exits if secret access fails. """
         # --- Arrange ---
        TEST_PROJECT_ID = "test-proj-id"
        TEST_SECRET_NAME = "test-secret-name"
        TEST_SECRET_VERSION = "test-version"
        TEST_WS_URL = "ws://test-url"

        def getenv_side_effect(key, default=None):
            # Only need to return values relevant to calling access_secret_version
            if key == 'gcp_project_id': return TEST_PROJECT_ID
            if key == 'sec_api_secret_name': return TEST_SECRET_NAME
            if key == 'sec_api_secret_version': return TEST_SECRET_VERSION
            # Don't need WS URL for this test path
            return default

        mock_os_getenv.side_effect = getenv_side_effect
        mock_access_secret.return_value = None # Simulate secret access failure

        # --- Act ---
        await main()

        # --- Assert ---
        # Check access_secret_version was called correctly
        mock_access_secret.assert_called_once_with(
             project_id=TEST_PROJECT_ID,
             secret_id=TEST_SECRET_NAME,
             version_id=TEST_SECRET_VERSION
        )
        # Check critical log message
        mock_logging_critical.assert_called_once_with("Failed to obtain SEC API Key from Secret Manager. Exiting.")

    @patch(f"{LISTENER_MODULE_PATH}.os.getenv")
    @patch(f"{LISTENER_MODULE_PATH}.access_secret_version", return_value="test_api_key") # Assume success
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task", return_value=AsyncMock()) # Mock task needed
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.get_running_loop")
    @patch(f"{LISTENER_MODULE_PATH}.logging.warning")
    async def test_main_signal_handler_not_supported(self, mock_logging_warning, mock_get_running_loop, mock_create_task, mock_access_secret, mock_os_getenv):
        """ Test the main function when signal handlers are not supported. """
        # Arrange: Mock getenv to return necessary values
        def getenv_side_effect(key, default=None):
            if key == 'gcp_project_id': return "proj"
            if key == 'sec_api_secret_name': return "secret"
            if key == 'sec_api_secret_version': return "latest"
            if key == 'sec_websocket_url': return "ws://test"
            return default
        mock_os_getenv.side_effect = getenv_side_effect

        # Arrange: Mock loop to raise error on add_signal_handler
        mock_loop = MagicMock()
        def signal_handler_side_effect(sig, handler):
             if sig == signal.SIGTERM: raise NotImplementedError
             else: pass # Allow SIGINT
        mock_loop.add_signal_handler.side_effect = signal_handler_side_effect
        mock_get_running_loop.return_value = mock_loop

        # Act
        await main()

        # Assert
        mock_logging_warning.assert_any_call(f"Signal handler for {signal.SIGTERM} not supported on this platform.")
        self.assertEqual(mock_loop.add_signal_handler.call_count, 2) # Still called for both


    # === Stream Tests (Simplified focusing on connect_with_retry) ===
    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry", side_effect=Exception("Simulated error to trigger sleep"))
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_listen_to_sec_stream_shutdown_during_sleep(self, mock_logging_info, mock_connect_with_retry):
        """ Test shutdown during sleep in connection loop after an error. """
        shutdown_event = asyncio.Event()
        async def set_shutdown_event_after_delay():
            await asyncio.sleep(0.1)
            shutdown_event.set()

        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            shutdown_task = asyncio.create_task(set_shutdown_event_after_delay())
            await asyncio.gather(listener_task, shutdown_task)

        mock_logging_info.assert_any_call("Shutdown requested, breaking connection loop.")

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_listen_to_sec_stream_final_cleanup(self, mock_logging_info, mock_connect_with_retry):
        """ Test final WebSocket connection cleanup if connection remains open on exit. """
        mock_ws = AsyncMock()
        mock_ws.open = True
        mock_ws.close = AsyncMock()
        mock_connect_with_retry.return_value = mock_ws

        shutdown_event = asyncio.Event()
        async def trigger_shutdown_after_connect(*args, **kwargs):
            await asyncio.sleep(0.01); shutdown_event.set(); return mock_ws
        mock_connect_with_retry.side_effect = trigger_shutdown_after_connect

        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            await listen_to_sec_stream(shutdown_event)

        mock_logging_info.assert_any_call("Final WebSocket connection cleanup.")
        mock_ws.close.assert_awaited_once()

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_listen_to_sec_stream_explicit_close(self, mock_logging_info, mock_connect_with_retry):
        """ Test explicit WebSocket connection close after ConnectionClosedOK. """
        mock_ws = AsyncMock()
        mock_ws.open = True
        mock_ws.recv.side_effect = ConnectionClosedOK()
        mock_ws.close = AsyncMock()
        mock_connect_with_retry.return_value = mock_ws

        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await listener_task

        mock_logging_info.assert_any_call("Explicitly closed WebSocket connection.")
        mock_ws.close.assert_awaited_once()

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry") # Patch the retry wrapper
    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_listen_to_sec_stream_no_api_key(self, mock_logging_error, mock_connect_with_retry):
        """ Test handling of missing SEC API key (listener should exit quickly). """
        shutdown_event = asyncio.Event()
        # Patch the global var set by main()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", None):
            # Run listen directly, bypassing main's API key check
            await listen_to_sec_stream(shutdown_event)

        mock_logging_error.assert_called_once_with("SEC API Key was not loaded successfully. Cannot connect.")
        mock_connect_with_retry.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
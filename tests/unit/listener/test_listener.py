# tests/unit/listener/test_listener.py
import asyncio
import json
import logging
import os
import signal
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call
from tenacity import RetryError
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, InvalidHandshake

# Imports
from services.listener.src.listener import (
    handle_message,
    listen_to_sec_stream,
    log_retry_attempt,
    main,
)
from services.listener.src.processing import process_new_filing

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Define base paths
LISTENER_MODULE_PATH = "services.listener.src.listener"
PROCESSING_MODULE_PATH_IN_LISTENER = f"{LISTENER_MODULE_PATH}.process_new_filing"

class TestListener(unittest.IsolatedAsyncioTestCase):
    """Test suite for listener.py with global asyncio.sleep patching."""

    async def asyncSetUp(self):
        """Start patches before each test."""
        self.sleep_patcher = patch(f"{LISTENER_MODULE_PATH}.asyncio.sleep", return_value=None)
        self.mock_sleep = self.sleep_patcher.start()

    async def asyncTearDown(self):
        """Stop patches after each test."""
        self.sleep_patcher.stop()

    def test_log_retry_attempt(self):
        """Test log_retry_attempt function."""
        with patch(f"{LISTENER_MODULE_PATH}.logging.warning") as mock_warning:
            mock_retry_state = MagicMock()
            mock_retry_state.attempt_number = 3
            mock_retry_state.outcome_wait = 5.0
            log_retry_attempt(mock_retry_state)
            mock_warning.assert_called_once()

    @patch(PROCESSING_MODULE_PATH_IN_LISTENER, new_callable=AsyncMock)
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_handle_message_valid_10k(self, mock_logging_info, mock_create_task, mock_process_filing):
        """Test handling a valid 10-K message triggers processing task."""
        ticker, accession_no, form_type, filing_url, filed_at = "AAPL", "0001234567-23-000001", "10-K", "http://example.com/filing", "2023-10-27T12:00:00Z"
        message = json.dumps({"formType": form_type, "ticker": ticker, "accessionNo": accession_no, "linkToFilingDetails": filing_url, "filedAt": filed_at})
        mock_ws = AsyncMock()
        await handle_message(message, mock_ws)
        mock_create_task.assert_called_once()
        mock_process_filing.assert_called_once_with(ticker=ticker, accession_no=accession_no, form_type=form_type, filing_url=filing_url, filed_at=filed_at)
        mock_logging_info.assert_called()

    @patch(f"{LISTENER_MODULE_PATH}.logging.warning")
    async def test_handle_message_missing_fields(self, mock_logging_warning):
        """Test handling a message with missing fields."""
        message = json.dumps({"formType": "10-K", "ticker": "AAPL"})
        mock_ws = AsyncMock()
        with patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task") as mock_create_task_skip:
            await handle_message(message, mock_ws)
            mock_create_task_skip.assert_not_called()
        mock_logging_warning.assert_called_once()

    @patch(f"{LISTENER_MODULE_PATH}.logging.debug")
    async def test_handle_message_skip_other_forms(self, mock_logging_debug):
        """Test handling a non-10-K/10-Q form type."""
        message = json.dumps({"formType": "8-K", "ticker": "AAPL"})
        mock_ws = AsyncMock()
        with patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task") as mock_create_task_skip:
            await handle_message(message, mock_ws)
            mock_create_task_skip.assert_not_called()
        mock_logging_debug.assert_called_once()

    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_handle_message_invalid_json(self, mock_logging_error):
        """Test handling an invalid JSON message."""
        message = "invalid json"
        mock_ws = AsyncMock()
        await handle_message(message, mock_ws)
        mock_logging_error.assert_called_once()

    @patch(f"{LISTENER_MODULE_PATH}.logging.exception")
    async def test_handle_message_exception(self, mock_logging_exception):
        """Test handling an exception during message processing."""
        message = json.dumps({"formType": "10-K", "ticker": "AAPL"})
        mock_ws = AsyncMock()
        with patch(f"{LISTENER_MODULE_PATH}.json.loads", side_effect=Exception("Test exception")):
            await handle_message(message, mock_ws)
        mock_logging_exception.assert_called_once()

    # Connection Tests
    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_listen_to_sec_stream_connection_success(self, mock_logging_error, mock_logging_info, mock_connect_with_retry):
        """Test successful WebSocket connection."""
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = [ConnectionClosedOK(None, None)]
        mock_connect_with_retry.return_value = mock_ws
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await asyncio.wait_for(listener_task, timeout=5.0)
        mock_connect_with_retry.assert_awaited_once()
        mock_logging_error.assert_not_called()

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry", side_effect=RetryError("Simulated RetryError"))
    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_listen_to_sec_stream_connection_retry_error(self, mock_logging_error, mock_connect_with_retry):
        """Test connection failure after retries."""
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await asyncio.wait_for(listener_task, timeout=5.0)
        self.assertIn("Connection failed after multiple retries", mock_logging_error.call_args[0][0])
        mock_connect_with_retry.assert_awaited_once()
        self.mock_sleep.assert_awaited_once_with(60)

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.warning")
    async def test_listen_to_sec_stream_connection_closed_error(self, mock_logging_warning, mock_connect_with_retry):
        """Test handling WebSocket connection closed with error."""
        mock_ws = AsyncMock()
        close_frame_rcvd = websockets.frames.Close(1006, "Connection reset by peer")
        mock_ws.recv.side_effect = ConnectionClosedError(rcvd=close_frame_rcvd, sent=None)
        mock_connect_with_retry.return_value = mock_ws
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await asyncio.wait_for(listener_task, timeout=5.0)
        mock_connect_with_retry.assert_awaited_once()
        self.assertTrue(any("WebSocket connection closed with error" in call[0][0] for call in mock_logging_warning.call_args_list))

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry", side_effect=InvalidHandshake("Invalid handshake"))
    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_listen_to_sec_stream_invalid_handshake(self, mock_logging_error, mock_connect_with_retry):
        """Test handling invalid handshake."""
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await asyncio.wait_for(listener_task, timeout=5.0)
        self.assertIn("Connection failed - InvalidHandshake", mock_logging_error.call_args[0][0])
        mock_connect_with_retry.assert_awaited_once()
        self.mock_sleep.assert_awaited_once_with(60)

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry", side_effect=Exception("Unexpected connection error"))
    @patch(f"{LISTENER_MODULE_PATH}.logging.exception")
    async def test_listen_to_sec_stream_unexpected_error(self, mock_logging_exception, mock_connect_with_retry):
        """Test handling unexpected error."""
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await asyncio.wait_for(listener_task, timeout=5.0)
        self.assertIn("Unexpected error in connection loop", mock_logging_exception.call_args[0][0])
        mock_connect_with_retry.assert_awaited_once()
        self.mock_sleep.assert_awaited_once_with(30)

    # Main Function Tests
    @patch(f"{LISTENER_MODULE_PATH}.os.getenv")
    @patch(f"{LISTENER_MODULE_PATH}.access_secret_version")
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task")
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.get_running_loop")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_main_success(self, mock_logging_info, mock_get_running_loop, mock_create_task, mock_access_secret, mock_os_getenv):
        """Test main function initializes and runs listener task."""
        TEST_PROJECT_ID, TEST_SECRET_NAME, TEST_SECRET_VERSION, TEST_WS_URL = "test-proj", "test-secret", "test-ver", "ws://test"
        def getenv_side_effect(key, default=None):
            return {'gcp_project_id': TEST_PROJECT_ID, 'sec_api_secret_name': TEST_SECRET_NAME,
                    'sec_api_secret_version': TEST_SECRET_VERSION, 'sec_websocket_url': TEST_WS_URL}.get(key, default)
        mock_os_getenv.side_effect = getenv_side_effect
        mock_access_secret.return_value = "test_key"
        mock_loop, mock_task = MagicMock(), AsyncMock()
        mock_get_running_loop.return_value = mock_loop
        mock_create_task.return_value = mock_task
        await main()
        mock_os_getenv.assert_any_call('gcp_project_id')
        mock_access_secret.assert_called_once_with(project_id=TEST_PROJECT_ID, secret_id=TEST_SECRET_NAME, version_id=TEST_SECRET_VERSION)
        mock_create_task.assert_called_once()
        mock_task.assert_awaited()
        self.assertEqual(mock_loop.add_signal_handler.call_count, 2)
        mock_logging_info.assert_called()

    @patch(f"{LISTENER_MODULE_PATH}.os.getenv")
    @patch(f"{LISTENER_MODULE_PATH}.access_secret_version")
    @patch(f"{LISTENER_MODULE_PATH}.logging.critical")
    async def test_main_secret_access_failure(self, mock_logging_critical, mock_access_secret, mock_os_getenv):
        """Test main exits if secret access fails."""
        TEST_PROJECT_ID, TEST_SECRET_NAME, TEST_SECRET_VERSION = "test-proj", "test-secret", "test-ver"
        def getenv_side_effect(key, default=None):
            return {'gcp_project_id': TEST_PROJECT_ID, 'sec_api_secret_name': TEST_SECRET_NAME,
                    'sec_api_secret_version': TEST_SECRET_VERSION}.get(key, default)
        mock_os_getenv.side_effect = getenv_side_effect
        mock_access_secret.return_value = None
        await main()
        mock_access_secret.assert_called_once_with(project_id=TEST_PROJECT_ID, secret_id=TEST_SECRET_NAME, version_id=TEST_SECRET_VERSION)
        mock_logging_critical.assert_called_once_with("Failed to obtain SEC API Key from Secret Manager. Exiting.")

    @patch(f"{LISTENER_MODULE_PATH}.os.getenv")
    @patch(f"{LISTENER_MODULE_PATH}.access_secret_version", return_value="test_key")
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.create_task", return_value=AsyncMock())
    @patch(f"{LISTENER_MODULE_PATH}.asyncio.get_running_loop")
    @patch(f"{LISTENER_MODULE_PATH}.logging.warning")
    async def test_main_signal_handler_not_supported(self, mock_logging_warning, mock_get_running_loop, mock_create_task, mock_access_secret, mock_os_getenv):
        """Test main when signal handlers are not supported."""
        def getenv_side_effect(key, default=None):
            return {'gcp_project_id': "p", 'sec_api_secret_name': "s", 'sec_api_secret_version': "v", 'sec_websocket_url': "ws"}.get(key, default)
        mock_os_getenv.side_effect = getenv_side_effect
        mock_loop = MagicMock()
        def signal_handler_side_effect(sig, handler):
            if sig == signal.SIGTERM:
                raise NotImplementedError
        mock_loop.add_signal_handler.side_effect = signal_handler_side_effect
        mock_get_running_loop.return_value = mock_loop
        await main()
        mock_logging_warning.assert_any_call(f"Signal handler for {signal.SIGTERM} not supported on this platform.")
        self.assertEqual(mock_loop.add_signal_handler.call_count, 2)

    # Stream Tests
    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry", side_effect=Exception("Simulated error to trigger sleep"))
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_listen_to_sec_stream_shutdown_during_sleep(self, mock_logging_info, mock_connect_with_retry):
        """Test shutdown during sleep after an error."""
        shutdown_event = asyncio.Event()
        async def set_shutdown_event_after_delay():
            await asyncio.sleep(0.1)
            shutdown_event.set()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            shutdown_task = asyncio.create_task(set_shutdown_event_after_delay())
            await asyncio.wait_for(asyncio.gather(listener_task, shutdown_task), timeout=5.0)
        mock_logging_info.assert_any_call("Shutdown requested, breaking connection loop.")
        self.mock_sleep.assert_awaited()

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_listen_to_sec_stream_final_cleanup(self, mock_logging_info, mock_connect_with_retry):
        """Test final WebSocket cleanup."""
        mock_ws = AsyncMock()
        mock_ws.open = True
        mock_ws.close = AsyncMock()
        mock_connect_with_retry.return_value = mock_ws
        shutdown_event = asyncio.Event()
        async def trigger_shutdown_after_connect(*args, **kwargs):
            await asyncio.sleep(0.01)
            shutdown_event.set()
            return mock_ws
        mock_connect_with_retry.side_effect = trigger_shutdown_after_connect
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            await asyncio.wait_for(listen_to_sec_stream(shutdown_event), timeout=5.0)
        mock_logging_info.assert_any_call("Final WebSocket connection cleanup.")
        mock_ws.close.assert_awaited_once()

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.info")
    async def test_listen_to_sec_stream_explicit_close(self, mock_logging_info, mock_connect_with_retry):
        """Test explicit close after ConnectionClosedOK."""
        mock_ws = AsyncMock()
        mock_ws.open = True
        mock_ws.recv.side_effect = ConnectionClosedOK(None, None)
        mock_ws.close = AsyncMock()
        mock_connect_with_retry.return_value = mock_ws
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", "fake_key"):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await asyncio.wait_for(listener_task, timeout=5.0)
        mock_logging_info.assert_any_call("Explicitly closed WebSocket connection.")
        mock_ws.close.assert_awaited_once()

    @patch(f"{LISTENER_MODULE_PATH}.connect_with_retry")
    @patch(f"{LISTENER_MODULE_PATH}.logging.error")
    async def test_listen_to_sec_stream_no_api_key(self, mock_logging_error, mock_connect_with_retry):
        """Test handling missing API key."""
        shutdown_event = asyncio.Event()
        with patch(f"{LISTENER_MODULE_PATH}.SEC_API_KEY", None):
            listener_task = asyncio.create_task(listen_to_sec_stream(shutdown_event))
            await asyncio.sleep(0.1)
            shutdown_event.set()
            await asyncio.wait_for(listener_task, timeout=5.0)
        mock_logging_error.assert_called_once_with("SEC API Key was not loaded successfully. Cannot connect.")
        mock_connect_with_retry.assert_not_awaited()
        self.mock_sleep.assert_awaited_once_with(30)

if __name__ == "__main__":
    unittest.main()
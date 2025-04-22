# tests/unit/subscriber/test_main.py
import base64
import json
import pytest
from unittest.mock import patch, AsyncMock

# Your Flask app lives here:
from services.subscriber.src.main import app

@pytest.fixture
def client():
    app.testing = True
    return app.test_client()

def _encode_message(payload: dict) -> str:
    """Helper to serialize & base64‑encode a JSON payload."""
    raw = json.dumps(payload).encode("utf-8")
    return base64.b64encode(raw).decode("utf-8")

def test_bad_request_no_message(client):
    """If 'message' key is missing, we should get a 400."""
    resp = client.post("/", json={})
    assert resp.status_code == 400
    assert b"Invalid Pub/Sub message format" in resp.data

def test_success_triggers_handler_and_returns_204(client):
    """A well‑formed envelope should call your handler and return 204."""
    # Build a minimal valid event_data
    event = {
        "ticker": "T",
        "accession_no": "ACC-1",
        "form_type": "10-K",
        "filing_url": "http://example.com",
        "filed_at": "2023-01-01T00:00:00Z",
    }
    envelope = {"message": {"data": _encode_message(event)}}

    # Patch out the async handler to avoid doing real work
    with patch(
        "services.subscriber.src.main.handle_filing_notification",
        new_callable=AsyncMock,
    ) as mock_handler:
        resp = client.post("/", json=envelope)

    assert resp.status_code == 204
    # Ensure your handler was awaited once with the decoded dict
    mock_handler.assert_awaited_once_with(event)

def test_handler_exception_returns_500(client):
    """If the handler raises, we should see a 500 response."""
    event = {
        "ticker": "T",
        "accession_no": "ACC-2",
        "form_type": "10-Q",
        "filing_url": "http://example.com",
        "filed_at": "2023-02-02T00:00:00Z",
    }
    envelope = {"message": {"data": _encode_message(event)}}

    with patch(
        "services.subscriber.src.main.handle_filing_notification",
        new_callable=AsyncMock,
    ) as mock_handler:
        mock_handler.side_effect = RuntimeError("boom")
        resp = client.post("/", json=envelope)

    assert resp.status_code == 500
    # Optionally, you can inspect resp.data or logs if you want to assert on the body/message


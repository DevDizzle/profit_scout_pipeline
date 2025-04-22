import pytest
import json
from unittest.mock import AsyncMock, patch
from services.news_summarizer.src.main import app

@pytest.fixture
def client():
    return app.test_client()

def test_bad_request_no_message(client):
    """If 'message' key is missing, we should get a 400."""
    resp = client.post('/', json={})
    assert resp.status_code == 400
    assert b"Invalid Eventarc message format" in resp.data

def test_bad_payload_structure(client):
    """If message lacks 'bucket' or 'name', we should get a 400."""
    envelope = {'message': {'foo': 'bar'}}
    resp = client.post('/', json=envelope)
    assert resp.status_code == 400
    assert b"Invalid GCS event payload" in resp.data

@patch('services.news_summarizer.src.main.handle_gcs_event', new_callable=AsyncMock)
def test_success_triggers_handler_and_returns_204(mock_handler, client):
    """A well‑formed envelope should call the handler and return 204."""
    envelope = {'message': {'bucket': 'my-bkt', 'name': 'sec-pdf/AAPL_12345.pdf'}}
    resp = client.post('/', json=envelope)
    assert resp.status_code == 204
    mock_handler.assert_awaited_once_with(envelope['message'])

@patch('services.news_summarizer.src.main.handle_gcs_event', new_callable=AsyncMock)
def test_handler_exception_returns_500(mock_handler, client):
    """If the handler raises, we should see a 500 response."""
    envelope = {'message': {'bucket': 'my-bkt', 'name': 'sec-pdf/AAPL_12345.pdf'}}
    mock_handler.side_effect = RuntimeError("boom")
    resp = client.post('/', json=envelope)
    assert resp.status_code == 500
    assert b"Internal Server Error" in resp.data

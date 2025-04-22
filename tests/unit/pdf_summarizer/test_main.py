# tests/unit/pdf_summarizer/test_main.py

import os
import sys
import types
import json
import pytest

# 1) Set required env vars before importing your app
os.environ["gcp_project_id"] = "test-project"
os.environ["gcs_bucket_name"] = "test-bucket"
os.environ["gemini_api_secret_name"] = "dummy-secret"

# 2) Stub out google.* modules so imports in pdf_summarizer and main never fail
for mod in [
    "google",
    "google.cloud",
    "google.cloud.storage",
    "google.cloud.secretmanager",
    "google.api_core",
    "google.api_core.exceptions",
    "google.generativeai",
]:
    sys.modules[mod] = types.ModuleType(mod)

# 3) Add your service src dir so `import main` picks up services/pdf_summarizer/src/main.py
SRC = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "services", "pdf_summarizer", "src"
    )
)
sys.path.insert(0, SRC)

# 4) Now import your app and async view
import flask
import main
from main import app, index

# 5) Stub out the real handler so `await handle_gcs_event()` never raises
@pytest.fixture(autouse=True)
def stub_handle(monkeypatch):
    async def dummy(event_data):
        return None
    monkeypatch.setattr(main, "handle_gcs_event", dummy)


@pytest.mark.asyncio
async def test_index_no_json(monkeypatch):
    # Patch get_json on the Request class
    monkeypatch.setattr(
        flask.wrappers.Request,
        "get_json",
        lambda self, **kwargs: None,
    )

    with app.test_request_context("/", method="POST", data="", content_type="application/json"):
        body, code = await index()
        assert code == 400
        assert "Bad Request: Invalid Eventarc message format" in body


@pytest.mark.asyncio
async def test_index_missing_message_field(monkeypatch):
    monkeypatch.setattr(
        flask.wrappers.Request,
        "get_json",
        lambda self, **kwargs: {"foo": "bar"},
    )

    with app.test_request_context(
        "/", method="POST",
        data=json.dumps({"foo": "bar"}),
        content_type="application/json"
    ):
        body, code = await index()
        assert code == 400
        assert "Bad Request: Invalid Eventarc message format" in body


@pytest.mark.asyncio
async def test_index_invalid_message_structure(monkeypatch):
    monkeypatch.setattr(
        flask.wrappers.Request,
        "get_json",
        lambda self, **kwargs: {"message": "not a dict"},
    )

    with app.test_request_context(
        "/", method="POST",
        data=json.dumps({"message": "not a dict"}),
        content_type="application/json"
    ):
        body, code = await index()
        assert code == 400
        assert "Bad Request: Invalid GCS event payload" in body


@pytest.mark.asyncio
async def test_index_success(monkeypatch):
    monkeypatch.setattr(
        flask.wrappers.Request,
        "get_json",
        lambda self, **kwargs: {"message": {"bucket": "foo", "name": "bar.pdf"}},
    )

    with app.test_request_context("/", method="POST", data=json.dumps({}), content_type="application/json"):
        body, code = await index()
        assert code == 204
        assert body == "OK"

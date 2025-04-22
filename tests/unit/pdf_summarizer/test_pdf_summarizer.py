# tests/unit/pdf_summarizer/test_pdf_summarizer.py

import os
import sys
import types
import pytest

# 1) Set the env vars before importing pdf_summarizer
os.environ["gcp_project_id"] = "test-project"
os.environ["gcs_bucket_name"] = "test-bucket"
os.environ["gemini_api_secret_name"] = "dummy-secret"

# 2) Stub out google.* modules so pdf_summarizer.py imports cleanly
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

# 3) Add your service src dir so `import pdf_summarizer` finds services/pdf_summarizer/src/pdf_summarizer.py
SRC = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "services", "pdf_summarizer", "src"
    )
)
sys.path.insert(0, SRC)

import pdf_summarizer

@pytest.mark.asyncio
async def test_handle_gcs_event_ignores_wrong_bucket(monkeypatch):
    # Arrange: Pretend clients initialize successfully
    monkeypatch.setattr(pdf_summarizer, "initialize_clients", lambda: True)
    called = False

    async def fake_generate_sec_analysis(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(pdf_summarizer, "generate_sec_analysis", fake_generate_sec_analysis)

    # Act: Event for wrong bucket
    ev = {"bucket": "other-bucket", "name": pdf_summarizer.gcs_pdf_folder + "ABC_123.pdf"}
    await pdf_summarizer.handle_gcs_event(ev)

    # Assert: Should not invoke analysis
    assert not called

@pytest.mark.asyncio
async def test_handle_gcs_event_filters_non_pdf_and_prefix(monkeypatch):
    monkeypatch.setattr(pdf_summarizer, "initialize_clients", lambda: True)
    called = False

    async def fake_generate_sec_analysis(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(pdf_summarizer, "generate_sec_analysis", fake_generate_sec_analysis)

    # Act: Wrong prefix and wrong extension
    ev1 = {"bucket": pdf_summarizer.gcs_bucket_name, "name": "wrong-prefix/ABC_123.pdf"}
    await pdf_summarizer.handle_gcs_event(ev1)
    ev2 = {"bucket": pdf_summarizer.gcs_bucket_name, "name": pdf_summarizer.gcs_pdf_folder + "XYZ_123.txt"}
    await pdf_summarizer.handle_gcs_event(ev2)

    # Assert: Should not invoke analysis
    assert not called

@pytest.mark.asyncio
async def test_handle_gcs_event_success(monkeypatch):
    monkeypatch.setattr(pdf_summarizer, "initialize_clients", lambda: True)
    called = False

    # Prepare a mixed-case PDF name
    ev_name = f"{pdf_summarizer.gcs_pdf_folder}TICKER_ACC123.PdF"

    async def fake_generate_sec_analysis(gcs_blob_path, ticker, accession_no):
        nonlocal called, ev_name
        called = True
        # Verify parsing and preservation of the original path
        assert ticker == "TICKER"
        assert accession_no == "ACC123"
        assert gcs_blob_path == ev_name

    monkeypatch.setattr(pdf_summarizer, "generate_sec_analysis", fake_generate_sec_analysis)

    # Act
    ev = {"bucket": pdf_summarizer.gcs_bucket_name, "name": ev_name}
    await pdf_summarizer.handle_gcs_event(ev)

    # Assert
    assert called

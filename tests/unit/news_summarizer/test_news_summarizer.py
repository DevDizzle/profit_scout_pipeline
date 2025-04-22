import os
import pytest
import tempfile
from unittest.mock import MagicMock, patch
from datetime import date

# bring in the functions under test
from services.news_summarizer.src.news_summarizer import (
    initialize_clients,
    get_filing_date_from_metadata,
    save_text_to_gcs_sync,
    check_gcs_blob_exists,
)

# ensure required env vars for initialize_clients
os.environ.update({
    'gcp_project_id': 'test-proj',
    'gcp_region': 'us-central1',
    'bq_dataset_id': 'test_ds',
    'bq_metadata_table_id': 'meta_tbl',
    'gcs_bucket_name': 'test-bkt',
    'gemini_model_name': 'test-model',
})

def test_initialize_clients_idempotent(monkeypatch):
    # stub out the real clients
    fake_bq = MagicMock()
    fake_storage = MagicMock()
    monkeypatch.setattr(
        'services.news_summarizer.src.news_summarizer.bigquery.Client',
        lambda project: fake_bq
    )
    monkeypatch.setattr(
        'services.news_summarizer.src.news_summarizer.storage.Client',
        lambda project: fake_storage
    )
    # force gemini not installed path
    monkeypatch.setattr(
        'services.news_summarizer.src.news_summarizer.genai_installed',
        False
    )

    # first initialization should return True
    assert initialize_clients() is True
    # second call should be a no‑op (still True)
    assert initialize_clients() is True

def test_get_filing_date_from_metadata_found(monkeypatch):
    """Returns the date from the first row of the query result."""
    fake_row = MagicMock()
    fake_row.FiledDate = date(2022, 1, 2)
    fake_job = MagicMock()
    fake_job.result.return_value = [fake_row]

    fake_client = MagicMock()
    fake_client.query.return_value = fake_job

    monkeypatch.setattr(
        'services.news_summarizer.src.news_summarizer.bq_client',
        fake_client
    )

    result = get_filing_date_from_metadata('ACC123')
    assert result == date(2022, 1, 2)
    fake_client.query.assert_called_once()

def test_get_filing_date_from_metadata_none(monkeypatch):
    """If the query returns no rows, we get None."""
    fake_job = MagicMock()
    fake_job.result.return_value = []

    fake_client = MagicMock()
    fake_client.query.return_value = fake_job

    monkeypatch.setattr(
        'services.news_summarizer.src.news_summarizer.bq_client',
        fake_client
    )

    assert get_filing_date_from_metadata('ACC456') is None

def test_save_text_to_gcs_sync_success(monkeypatch, tmp_path):
    """Happy path: writes temp file and uploads, returns True."""
    fake_blob = MagicMock()
    fake_bucket = MagicMock()
    fake_bucket.blob.return_value = fake_blob

    from services.news_summarizer.src.news_summarizer import save_text_to_gcs_sync
    result = save_text_to_gcs_sync(fake_bucket, 'foo/bar.txt', 'hello world')
    assert result is True
    fake_blob.upload_from_filename.assert_called_once()

def test_save_text_to_gcs_sync_failure(monkeypatch):
    """If upload throws, we catch and return False."""
    fake_blob = MagicMock()
    fake_bucket = MagicMock()
    fake_bucket.blob.return_value = fake_blob
    fake_blob.upload_from_filename.side_effect = RuntimeError("oops")

    from services.news_summarizer.src.news_summarizer import save_text_to_gcs_sync
    result = save_text_to_gcs_sync(fake_bucket, 'foo/bar.txt', 'hello world')
    assert result is False

def test_check_gcs_blob_exists(monkeypatch):
    """Returns True/False based on blob.exists(), catches exceptions."""
    fake_blob = MagicMock()
    fake_bucket = MagicMock()
    fake_bucket.blob.return_value = fake_blob
    fake_blob.exists.return_value = True

    from services.news_summarizer.src.news_summarizer import check_gcs_blob_exists
    assert check_gcs_blob_exists(fake_bucket, 'some/blob') is True
    fake_blob.exists.assert_called_once()

    # now simulate an error
    fake_blob.exists.side_effect = RuntimeError("err")
    assert check_gcs_blob_exists(fake_bucket, 'some/blob') is False

# /home/eraphaelparra/profit_scout_pipeline/tests/integration/test_main_integration.py

import pytest
import json
import os
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, mock_open

# --- Updated Imports based on provided structure ---
# Assuming 'pytest' is run from the 'profit_scout_pipeline' root directory
try:
    # Absolute imports from the project root ('profit_scout_pipeline')
    from services.pdf_summarizer.src.main import app as flask_app
    import services.pdf_summarizer.src.pdf_summarizer as pdf_summarizer
except ImportError as e:
    # Provide a helpful message if imports fail
    print(f"ImportError: {e}. Please ensure 'pytest' is run from the 'profit_scout_pipeline' directory,")
    print("and that Python can find the 'services' package (e.g., check PYTHONPATH or __init__.py files if needed).")
    pytest.skip("Skipping integration tests due to import issues.", allow_module_level=True)
# --- End Import Update ---


# Mark tests as asyncio to handle async functions
pytestmark = pytest.mark.asyncio

@pytest.fixture(scope='module') # Scope='module' to setup app context once per file
def app():
    """Create and configure a new app instance for testing."""
    flask_app.config.update({
        "TESTING": True,
        "DEBUG": False, # Usually keep debug off for testing stability
    })
    # Any other app configuration needed for tests
    yield flask_app

@pytest.fixture
def client(app):
    """A test client for the app."""
    return app.test_client()

@pytest.fixture(autouse=True)
def mock_dependencies(mocker):
    """Mocks all external dependencies (GCP, Gemini, os.getenv, tempfile)."""
    mocks = {}
    # Define the base path for patching targets within the pdf_summarizer module
    target_module_path = "services.pdf_summarizer.src.pdf_summarizer"

    # 1. Mock os.getenv - Patch it where it's used (pdf_summarizer and main)
    mock_env = {
        'gcp_project_id': 'test-gcp-project',
        'gcs_bucket_name': 'test-pdf-bucket',
        'gemini_api_secret_name': 'test-gemini-secret',
        'gemini_api_key_secret_version': 'latest',
        'gemini_model_name': 'gemini-test-model',
        'gcs_analysis_txt_prefix': 'test-analysis-output/',
        'gcs_pdf_folder': 'test-sec-pdf-input/',
        'PORT': '8080' # Mock PORT for main.py's __name__ == '__main__' block if relevant
        # Add any other env vars your code might check
    }
    # Patch os.getenv within the scope of the modules using it
    mocks['getenv_pdf'] = mocker.patch(f'{target_module_path}.os.getenv', lambda k, default=None: mock_env.get(k, default))
    mocks['getenv_main'] = mocker.patch(f'services.pdf_summarizer.src.main.os.getenv', lambda k, default=None: mock_env.get(k, default))


    # 2. Mock Secret Manager Client
    mock_secret_client = MagicMock()
    mock_access_response = MagicMock()
    mock_payload = MagicMock()
    mock_payload.data = b'fake-test-api-key'
    mock_access_response.payload = mock_payload
    mock_secret_client.access_secret_version.return_value = mock_access_response
    mocks['secret_client_class'] = mocker.patch(
        f'{target_module_path}.secretmanager.SecretManagerServiceClient', # Target the import in pdf_summarizer
        return_value=mock_secret_client
    )
    mocks['secret_client_instance'] = mock_secret_client

    # 3. Mock Storage Client
    mock_storage_client = MagicMock()
    mock_bucket = MagicMock(name='MockGCSBucket')
    mock_pdf_blob = MagicMock(name='MockPDFBlob')
    mock_txt_blob = MagicMock(name='MockTxtBlob')

    # Configure PDF blob download behavior
    mock_pdf_blob.exists.return_value = True
    mock_pdf_blob.download_to_filename = MagicMock()

    # Configure TXT blob upload behavior
    mock_txt_blob.upload_from_filename = MagicMock()
    mock_txt_blob.metadata = None # Initialize metadata

    # Make bucket.blob return the correct mock based on the path
    def blob_side_effect(blob_name, **kwargs):
        if blob_name.endswith('.pdf'):
            return mock_pdf_blob
        elif blob_name.endswith('.txt'):
            return mock_txt_blob
        else:
            return MagicMock() # Default mock if needed
    mock_bucket.blob.side_effect = blob_side_effect

    mock_storage_client.bucket.return_value = mock_bucket
    mocks['storage_client_class'] = mocker.patch(
        f'{target_module_path}.storage.Client', # Target the import in pdf_summarizer
        return_value=mock_storage_client
    )
    mocks['storage_client_instance'] = mock_storage_client
    mocks['bucket'] = mock_bucket
    mocks['pdf_blob'] = mock_pdf_blob
    mocks['txt_blob'] = mock_txt_blob

    # 4. Mock Gemini (google.generativeai)
    pdf_summarizer.genai_installed = True # Assume installed for testing the flow

    mock_genai_module = MagicMock()
    mock_genai_module.configure = MagicMock()

    # Mock the GenerativeModel class and its instance
    mock_gemini_model_instance = AsyncMock(name='MockGeminiModel')
    mock_genai_module.GenerativeModel.return_value = mock_gemini_model_instance

    # Mock the generate_content response
    mock_gemini_response = AsyncMock() # Mock the response object itself
    mock_gemini_response.text = "This is the mocked AI analysis summary."
    mock_gemini_model_instance.generate_content = AsyncMock(return_value=mock_gemini_response)

    # Mock file operations
    mock_uploaded_file = MagicMock(name='MockGeminiUploadedFile')
    mock_uploaded_file.name = "gemini-files/fake-test-upload-id"
    mock_uploaded_file.state = MagicMock()
    mock_uploaded_file.state.name = "ACTIVE"
    mock_genai_module.upload_file = MagicMock(return_value=mock_uploaded_file)
    mock_genai_module.get_file = MagicMock(return_value=mock_uploaded_file)
    mock_genai_module.delete_file = MagicMock()

    mocks['genai_module'] = mocker.patch(
        f'{target_module_path}.genai', # Target the import in pdf_summarizer
        mock_genai_module,
        create=True # Needed if the import might fail and module doesn't exist
    )
    mocks['gemini_model_instance'] = mock_gemini_model_instance
    mocks['uploaded_file'] = mock_uploaded_file

    # 5. Mock tempfile.NamedTemporaryFile
    mock_temp_pdf = mock_open()
    mock_temp_txt = mock_open()
    mock_temp_pdf.return_value.name = "/tmp/fake_temp_file.pdf"
    mock_temp_txt.return_value.name = "/tmp/fake_temp_file.txt"

    def named_temp_file_side_effect(*args, **kwargs):
        suffix = kwargs.get('suffix', '')
        if suffix == '.pdf':
             mock = mock_temp_pdf()
             mock.name = "/tmp/fake_temp_file.pdf"
             return mock
        elif suffix == '.txt':
            mock = mock_temp_txt()
            mock.name = "/tmp/fake_temp_file.txt"
            return mock
        else:
             m = mock_open().return_value
             m.name = "/tmp/fake_other_temp"
             return m

    mocks['named_temp_file'] = mocker.patch(
        f'{target_module_path}.tempfile.NamedTemporaryFile',
        side_effect=named_temp_file_side_effect
    )
    mocks['os_remove'] = mocker.patch(f'{target_module_path}.os.remove')
    mocks['os_path_exists'] = mocker.patch(f'{target_module_path}.os.path.exists', return_value=True)


    # 6. Patch initialize_clients
    # Also set the *global* variables in pdf_summarizer to our mock instances
    mocker.patch(f'{target_module_path}.initialize_clients', return_value=True)
    pdf_summarizer.storage_client = mock_storage_client
    pdf_summarizer.gemini_api_key = 'fake-test-api-key'
    pdf_summarizer.gemini_model = mock_gemini_model_instance

    # 7. Mock logging within the modules if needed to suppress output or check calls
    mocks['log_pdf_info'] = mocker.patch(f'{target_module_path}.logging.info')
    mocks['log_pdf_error'] = mocker.patch(f'{target_module_path}.logging.error')
    mocks['log_pdf_warning'] = mocker.patch(f'{target_module_path}.logging.warning')
    mocks['log_main_info'] = mocker.patch(f'services.pdf_summarizer.src.main.app.logger.info')
    mocks['log_main_error'] = mocker.patch(f'services.pdf_summarizer.src.main.app.logger.error')
    mocks['log_main_exception'] = mocker.patch(f'services.pdf_summarizer.src.main.app.logger.exception')


    yield mocks # Provide mocks dict to the test function


# --- Test Cases ---

async def test_successful_pdf_processing(client, mock_dependencies, mocker):
    """
    Tests the end-to-end flow for a valid GCS event triggering PDF analysis.
    """
    # Arrange: Prepare the simulated Eventarc GCS event
    target_module_path = "services.pdf_summarizer.src.pdf_summarizer" # For spying
    ticker = "TESTCO"
    accession_no = "0009876543-21-000001"
    pdf_filename = f"{ticker}_{accession_no}.pdf"
    gcs_pdf_path = f"{mock_dependencies['getenv_pdf']('gcs_pdf_folder')}{pdf_filename}"
    expected_txt_path = f"{mock_dependencies['getenv_pdf']('gcs_analysis_txt_prefix')}{ticker}_{accession_no}.txt"
    temp_pdf_path = "/tmp/fake_temp_file.pdf" # From mock_dependencies
    temp_txt_path = "/tmp/fake_temp_file.txt" # From mock_dependencies

    gcs_event = {
        "bucket": mock_dependencies['getenv_pdf']('gcs_bucket_name'),
        "name": gcs_pdf_path,
        "metageneration": "1",
        "timeCreated": "2025-04-21T18:00:00.123Z",
        "updated": "2025-04-21T18:00:00.123Z"
    }
    eventarc_message = {
        "message": gcs_event,
        "subscription": "projects/test-gcp-project/subscriptions/test-sub"
    }

    # Spy on key internal functions to verify calls
    handle_event_spy = mocker.spy(pdf_summarizer, 'handle_gcs_event')
    generate_analysis_spy = mocker.spy(pdf_summarizer, 'generate_sec_analysis')
    save_text_spy = mocker.spy(pdf_summarizer, 'save_text_to_gcs')
    delete_gemini_spy = mocker.spy(pdf_summarizer, 'delete_gemini_file_async')

    # Act: Send the simulated event to the Flask app endpoint
    response = await client.post('/', data=json.dumps(eventarc_message), content_type='application/json')

    # Assert: Verify the interactions and results
    assert response.status_code == 204 # Success acknowledgement from main.py

    # Verify main.py called handle_gcs_event correctly
    handle_event_spy.assert_called_once_with(gcs_event)

    # Verify handle_gcs_event called generate_sec_analysis correctly
    generate_analysis_spy.assert_called_once_with(
        gcs_blob_path=gcs_pdf_path,
        ticker=ticker,
        accession_no=accession_no
    )

    # --- Verify generate_sec_analysis internal calls ---
    mock_getenv = mock_dependencies['getenv_pdf'] # Use the correct mock getter

    # Check GCS PDF download
    mock_dependencies['storage_client_instance'].bucket.assert_any_call(mock_getenv('gcs_bucket_name'))
    mock_dependencies['bucket'].blob.assert_any_call(gcs_pdf_path)
    mock_dependencies['pdf_blob'].exists.assert_called_once()
    mock_dependencies['named_temp_file'].assert_any_call(delete=False, suffix=".pdf")
    mock_dependencies['pdf_blob'].download_to_filename.assert_called_once_with(temp_pdf_path)

    # Check Gemini upload
    mock_dependencies['genai_module'].upload_file.assert_called_once()
    upload_call_args, upload_call_kwargs = mock_dependencies['genai_module'].upload_file.call_args
    assert upload_call_args[0] == temp_pdf_path
    assert upload_call_kwargs['display_name'] == f"{ticker}_{accession_no}_SEC_Filing.pdf"

    # Check Gemini content generation
    mock_dependencies['gemini_model_instance'].generate_content.assert_called_once()
    gen_call_args, _ = mock_dependencies['gemini_model_instance'].generate_content.call_args
    assert isinstance(gen_call_args[0], list)
    # Access prompt text using the variable from the imported module
    assert pdf_summarizer.qualitative_analysis_prompt in gen_call_args[0]
    assert gen_call_args[0][1] == mock_dependencies['uploaded_file'] # Check the mock file object

    # Check GCS Text upload (via save_text_to_gcs spy)
    save_text_spy.assert_called_once()
    save_call_args, _ = save_text_spy.call_args
    assert save_call_args[0] == mock_dependencies['bucket']
    assert save_call_args[1] == expected_txt_path
    assert save_call_args[2] == "This is the mocked AI analysis summary."
    assert save_call_args[3]['ticker'] == ticker
    assert save_call_args[3]['accessionNumber'] == accession_no

    # Verify the underlying GCS blob upload for the text file
    mock_dependencies['named_temp_file'].assert_any_call(delete=False, mode='w', encoding='utf-8', suffix=".txt")
    mock_dependencies['bucket'].blob.assert_any_call(expected_txt_path)
    # Check metadata set correctly on the mock blob object before upload attempt
    assert mock_dependencies['txt_blob'].metadata is not None
    assert mock_dependencies['txt_blob'].metadata['originalPDFPath'] == f"gs://{mock_getenv('gcs_bucket_name')}/{gcs_pdf_path}"
    mock_dependencies['txt_blob'].upload_from_filename.assert_called_once_with(temp_txt_path, content_type='text/plain')


    # Check Gemini file deletion was awaited
    delete_gemini_spy.assert_called_once()
    mock_dependencies['genai_module'].delete_file.assert_called_once_with(mock_dependencies['uploaded_file'].name)


    # Check temporary file cleanup
    assert mock_dependencies['os_remove'].call_count == 2
    mock_dependencies['os_remove'].assert_any_call(temp_pdf_path)
    mock_dependencies['os_remove'].assert_any_call(temp_txt_path)
    assert mock_dependencies['os_path_exists'].call_count >= 2 # Called before each remove

    # Check logs for success messages if needed
    mock_dependencies['log_main_info'].assert_any_call(f"Received valid GCS event via Eventarc for: gs://{gcs_event['bucket']}/{gcs_event['name']}")
    mock_dependencies['log_pdf_info'].assert_any_call(f"Successfully processed PDF analysis event for {gcs_pdf_path}")
    mock_dependencies['log_main_info'].assert_any_call(f"Successfully processed event for: gs://{gcs_event['bucket']}/{gcs_event['name']}")


async def test_invalid_event_format(client, mock_dependencies):
    """Tests the endpoint's handling of a malformed Eventarc message."""
    # Arrange: Missing 'message' field
    invalid_envelope = { "subscription": "projects/test-gcp-project/subscriptions/test-sub" }

    # Act
    response = await client.post('/', data=json.dumps(invalid_envelope), content_type='application/json')

    # Assert
    assert response.status_code == 400
    assert b"Invalid Eventarc message format" in response.data
    mock_dependencies['log_main_error'].assert_called_with("Bad Eventarc message format: missing 'message'.")

async def test_gcs_event_missing_name(client, mock_dependencies, mocker):
    """Tests the endpoint's handling of a GCS event missing the 'name' field."""
    # Arrange
    mock_getenv = mock_dependencies['getenv_main'] # Use main's mock env
    gcs_event = { "bucket": mock_getenv('gcs_bucket_name') }
    eventarc_message = { "message": gcs_event, "subscription": "..." }
    handle_event_spy = mocker.spy(pdf_summarizer, 'handle_gcs_event')

    # Act
    response = await client.post('/', data=json.dumps(eventarc_message), content_type='application/json')

    # Assert
    assert response.status_code == 400
    assert b"Invalid GCS event payload" in response.data
    mock_dependencies['log_main_error'].assert_called_with(f"Invalid GCS event payload structure within Eventarc message: {gcs_event}")
    handle_event_spy.assert_not_called()

async def test_filename_parsing_error(client, mock_dependencies, mocker):
    """Tests that handle_gcs_event ignores events with unparseable filenames."""
    # Arrange
    mock_getenv = mock_dependencies['getenv_pdf'] # Use pdf's mock env
    gcs_pdf_folder = mock_getenv('gcs_pdf_folder')
    invalid_gcs_path = f"{gcs_pdf_folder}InvalidFilename.pdf"
    gcs_event = {
        "bucket": mock_getenv('gcs_bucket_name'),
        "name": invalid_gcs_path,
    }
    eventarc_message = { "message": gcs_event, "subscription": "..." }
    handle_event_spy = mocker.spy(pdf_summarizer, 'handle_gcs_event')
    generate_analysis_spy = mocker.spy(pdf_summarizer, 'generate_sec_analysis')

    # Act
    response = await client.post('/', data=json.dumps(eventarc_message), content_type='application/json')

    # Assert
    assert response.status_code == 204 # main.py succeeds
    handle_event_spy.assert_called_once_with(gcs_event)
    generate_analysis_spy.assert_not_called()
    # Check for the specific log message from the parsing error
    mock_dependencies['log_pdf_error'].assert_any_call(
        f"Failed to parse ticker/accession number from filename '{invalid_gcs_path}': "
        f"Filename 'InvalidFilename.pdf' did not match expected TICKER_ACCESSIONNUMBER.pdf format. Cannot process."
    )


# --- Add more tests as suggested previously ---
# e.g., test_pdf_not_in_correct_folder_prefix, test_gcs_blob_does_not_exist,
#       test_gemini_upload_fails, test_gemini_analysis_fails, etc.
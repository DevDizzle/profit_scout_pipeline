# Profit Scout - Financial Data & Analysis Pipeline

## Overview

This project implements a data pipeline on Google Cloud Platform (GCP) designed to ingest real-time SEC filings (10-K, 10-Q), load daily stock prices, generate AI-driven summaries (from filings and recent news), and calculate financial ratios. The processed data (structured financials, PDFs, text summaries, ratios) is stored in BigQuery and Google Cloud Storage (GCS), providing a foundation for financial analysis, ML modeling, and application backends.

## Architecture Overview

The pipeline utilizes a microservices architecture running primarily on Cloud Run (Services and Jobs), orchestrated through Pub/Sub, Eventarc, and Cloud Scheduler.

**(Optional: Consider adding a Mermaid diagram here later to visualize the flow)**

**Key Data Flows:**

1.  **SEC Filing Ingestion:**
    * `listener` (Cloud Run Service) receives real-time filing events via WebSocket.
    * Publishes relevant filing details (10-K/Q) to a Pub/Sub topic (`sec-filing-notification`).
    * `subscriber` (Cloud Run Service) consumes Pub/Sub messages from its subscription.
    * `subscriber` fetches XBRL data & PDF (via sec-api), writes financials (`balance_sheet`, `income_statement`, `cash_flow`) & metadata (`filing_metadata`) to BigQuery, uploads PDF (named `ticker_ANcleaned.pdf`) to GCS.
2.  **PDF Analysis (Event-Driven):**
    * GCS PDF upload by `subscriber` triggers Eventarc.
    * Eventarc invokes `pdf_summarizer` (Cloud Run Service).
    * `pdf_summarizer` downloads PDF, uses Gemini File API for qualitative analysis, saves result (`QualitativeAnalysis_ticker_ANcleaned.txt`) to GCS.
    * Eventarc invokes `news_summarizer` (Cloud Run Service) via a separate trigger on the same GCS event.
    * `news_summarizer` parses filename, queries BQ (`filing_metadata`) for filing date, uses Vertex AI Gemini + Search Tool for recent news analysis (30-day lookback), saves result (`NewsSummary_ticker_ANcleaned.txt`) to GCS.
3.  **Batch Processing (Scheduled):**
    * Cloud Scheduler triggers `price_loader` (Cloud Run Job) daily.
    * `price_loader` queries BQ `filing_metadata` for tickers, fetches incremental daily prices from `yfinance`, appends to BQ `price_data` table.
    * Cloud Scheduler triggers `ratio_calculator` (Cloud Run Job) periodically (e.g., daily/hourly).
    * `ratio_calculator` queries BQ (`filing_metadata`, `financial_ratios`) for unprocessed filings, fetches required BQ data, uses Gemini API for calculations, appends results to BQ `financial_ratios` table.

## Project Structure

This project uses a monorepo structure:
```
profit_scout_pipeline/
|
├── services/                    # Code for individual microservices
│   ├── listener/                # WebSocket listener -> Pub/Sub publisher
│   │   ├── src/
│   │   ├── Dockerfile
│   │   └── requirements.in / .txt
│   ├── subscriber/              # Pub/Sub -> BQ (Financials, Metadata), GCS (PDF)
│   │   ├── src/                 # subscriber.py, main.py
│   │   ├── Dockerfile
│   │   └── requirements.in / .txt
│   ├── price_loader/            # Scheduled Job: yfinance -> BQ (Prices)
│   │   ├── src/                 # price_loader.py
│   │   ├── Dockerfile
│   │   └── requirements.in / .txt
│   ├── ratio_calculator/        # Scheduled Job: BQ -> Gemini -> BQ (Ratios)
│   │   ├── src/                 # ratio_calculator.py
│   │   ├── Dockerfile
│   │   └── requirements.in / .txt
│   ├── pdf_summarizer/          # Eventarc (GCS PDF) -> Gemini -> GCS (TXT Summary)
│   │   ├── src/                 # pdf_summarizer.py, main.py
│   │   ├── Dockerfile
│   │   └── requirements.in / .txt
│   └── news_summarizer/         # Eventarc (GCS PDF) -> BQ -> Vertex AI -> GCS (TXT Summary)
│       ├── src/                 # news_summarizer.py, main.py
│       ├── Dockerfile
│       └── requirements.in / .txt
│
├── infrastructure/              # Infrastructure as Code (Terraform)
│   └── terraform/
│       ├── main.tf              # Defines GCP resources (BQ, GCS, PubSub, Secrets, Run, Eventarc, etc.)
│       ├── variables.tf
│       └── ...                  # modules/, envs/, etc.
│
├── cicd/                        # CI/CD Pipeline Definitions
│   └── cloudbuild.yaml          # Cloud Build: Builds service images, pushes to Artifact Registry
│
├── tests/                       # Automated Tests (Placeholder - TO BE ADDED)
│   ├── unit/
│   └── integration/
│
├── .gitignore
├── README.md                    # This file
└── .env.example                 # Template for local development environment variables
```

## Components Deep Dive

*(This section provides a high-level overview. More details are in the source code.)*

### 1. Listener (`services/listener/`)
* **Purpose:** Ingest real-time SEC filing notifications.
* **Trigger:** External WebSocket stream (`sec-api.io`).
* **Function:** Connects securely, handles reconnections, filters 10-K/Q, publishes details (`ticker`, `accession_no`, `form_type`, `filing_url`, `filed_at`) to Pub/Sub.
* **Output:** Pub/Sub messages.
* **Deployment:** Cloud Run Service (long-running, potentially min_instances=1).

### 2. Subscriber (`services/subscriber/`)
* **Purpose:** Process core filing data upon notification.
* **Trigger:** Pub/Sub topic subscription (push).
* **Function:** Consumes message, fetches XBRL data & PDF, parses financials, cleans accession number (removes hyphens), writes financials to BQ (`balance_sheet`, `income_statement`, `cash_flow`) using cleaned AN, writes metadata to BQ (`filing_metadata`) using cleaned AN, uploads PDF to GCS (named `Ticker_AccesnsionNumber.pdf`) without GCS metadata.
* **Output:** BQ table rows, GCS PDF object.
* **Deployment:** Cloud Run Service.

### 3. Price Loader (`services/price_loader/`)
* **Purpose:** Maintain daily stock price history.
* **Trigger:** Cloud Scheduler (time-based).
* **Function:** Queries BQ `filing_metadata` for distinct tickers, fetches incremental daily prices from `yfinance` based on `MAX(date)` in `price_data`, appends new data to BQ `price_data` table. Handles initial load.
* **Output:** BQ table rows (`price_data`).
* **Deployment:** Cloud Run Job.

### 4. Ratio Calculator (`services/ratio_calculator/`)
* **Purpose:** Calculate financial ratios based on processed filings.
* **Trigger:** Cloud Scheduler (time-based).
* **Function:** Queries BQ to find filings in `filing_metadata` missing from `financial_ratios`. Fetches required financials/prices from BQ, calculates price trend, calls Gemini API for other ratio calculations, appends results to BQ `financial_ratios` table. Uses threading for concurrency.
* **Output:** BQ table rows (`financial_ratios`).
* **Deployment:** Cloud Run Job.

### 5. PDF Summarizer (`services/pdf_summarizer/`)
* **Purpose:** Generate qualitative analysis directly from the filing PDF.
* **Trigger:** Eventarc (GCS PDF object creation in `GCS_PDF_FOLDER`).
* **Function:** Parses filename for ticker/AN(cleaned). Downloads PDF, uploads to Gemini File API, calls Gemini model with prompt, saves text summary (`Ticker_AccesnsionNumber.txt`) with basic metadata to GCS (`GCS_ANALYSIS_TXT_PREFIX`).
* **Output:** GCS TXT object.
* **Deployment:** Cloud Run Service.

### 6. News Summarizer (`services/news_summarizer/`)
* **Purpose:** Generate summary/assessment based on recent news around the filing date.
* **Trigger:** Eventarc (GCS PDF object creation in `GCS_PDF_FOLDER`).
* **Function:** Parses filename for ticker/AN(cleaned). Queries BQ `filing_metadata` for filing date. Calls Vertex AI Gemini model with Search Tool enabled (30-day lookback). Saves text summary (`Ticker_AccesnsionNumber.txt`) to GCS (`GCS_NEWS_SUMMARY_PREFIX`).
* **Output:** GCS TXT object.
* **Deployment:** Cloud Run Service.

## Setup & Configuration

### Prerequisites

* Python 3 (e.g., 3.11+)
* Google Cloud Platform (GCP) Project (e.g., `profit-scout-456416`)
* Enabled GCP APIs: Cloud Run, Cloud Build, Artifact Registry, Secret Manager, BigQuery API, Cloud Storage API, Pub/Sub, Eventarc, Cloud Scheduler, IAM, Vertex AI API.
* Terraform CLI (for infrastructure provisioning).
* `gcloud` CLI configured locally or use Cloud Shell.
* `sec-api.io` Account and API Key.
* Gemini API Key (if using direct Gemini API for `ratio_calculator`/`pdf_summarizer`).

### IAM / Service Accounts

It is recommended to create dedicated service accounts via Terraform for each distinct service or job with the principle of least privilege. Key roles needed across different services include:
* `roles/run.invoker`
* `roles/pubsub.publisher` / `roles/pubsub.subscriber`
* `roles/secretmanager.secretAccessor`
* `roles/bigquery.dataEditor` / `roles/bigquery.jobUser` / `roles/bigquery.dataViewer`
* `roles/storage.objectAdmin` / `roles/storage.objectCreator` / `roles/storage.objectViewer`
* `roles/logging.logWriter`
* `roles/eventarc.eventReceiver`
* `roles/aiplatform.user` (for Vertex AI endpoint usage)
* `roles/iam.serviceAccountUser` (for services/triggers acting as other service accounts)
*(Specific bindings should be defined in Terraform)*

### Secrets

Store sensitive keys in **Google Secret Manager**:
1.  **SEC API Key:** Used by `listener` and `subscriber`.
    * Secret Name Example: `sec-api-key`
    * Accessed via env var: `SEC_API_SECRET_ID`
2.  **Gemini API Key:** Used by `ratio_calculator` and `pdf_summarizer` (if using direct Gemini API).
    * Secret Name Example: `gemini-api-key`
    * Accessed via env var: `GEMINI_API_KEY_SECRET_ID`

### Environment Variables

Configure these via the deployment mechanism (Terraform for Cloud Run Services/Jobs). Refer to `.env.example` for local development (use ADC locally, don't put secrets in `.env`).

*(List ALL required environment variables identified across all services - ensure this list is complete and matches code):*
* `GCP_PROJECT_ID`
* `GCP_REGION` (e.g., `us-central1` - for Vertex AI client)
* `BQ_DATASET_ID` (e.g., `profit_scout`)
* `BQ_METADATA_TABLE_ID` (e.g., `filing_metadata`)
* `BQ_FINANCIALS_BS_TABLE_ID` (e.g., `balance_sheet`)
* `BQ_FINANCIALS_IS_TABLE_ID` (e.g., `income_statement`)
* `BQ_FINANCIALS_CF_TABLE_ID` (e.g., `cash_flow`)
* `BQ_PRICE_TABLE_ID` (e.g., `price_data`)
* `BQ_RATIOS_TABLE_ID` (e.g., `financial_ratios`)
* `GCS_BUCKET_NAME`
* `GCS_PDF_FOLDER` (Input for summarizers, output for subscriber)
* `GCS_ANALYSIS_TXT_PREFIX` (Output for pdf_summarizer)
* `GCS_NEWS_SUMMARY_PREFIX` (Output for news_summarizer)
* `SEC_API_SECRET_ID`
* `SEC_API_SECRET_VERSION`
* `SEC_WEBSOCKET_URL`
* `GEMINI_API_KEY_SECRET_ID`
* `GEMINI_API_KEY_SECRET_VERSION`
* `GEMINI_MODEL_NAME` (e.g., `gemini-2.0-flash-001` or Vertex model ID)
* `MAX_WORKERS` (Optional, for threaded batch jobs)
* `PUB_SUB_TOPIC_ID` (Used by listener to publish, e.g., `sec-filing-notification`)
* `PORT` (Optional, defaults to 8080 for Cloud Run services)

### GCP Resource Setup (via Terraform)

The `infrastructure/terraform/` directory contains the definitions for all necessary GCP resources. This includes creating/managing:
* GCS Bucket
* BigQuery Dataset (`profit_scout`)
* BigQuery Tables (schemas defined in TF): `filing_metadata`, `balance_sheet`(import), `income_statement`(import), `cash_flow`(import), `price_data`(import), `financial_ratios`(create)
* Pub/Sub Topic (`sec-filing-notification`) & Subscription (`subscriber-sub`)
* Secret Manager Secret definitions
* Artifact Registry Docker Repository (`profit-scout-repo`)
* IAM Service Accounts and Bindings
* Cloud Run Services & Jobs (definitions referencing images built by Cloud Build)
* Eventarc Triggers (GCS -> pdf_summarizer, GCS -> news_summarizer)
* Cloud Scheduler Jobs (Triggering price_loader, ratio_calculator)

Run `terraform init`, `terraform plan`, and `terraform apply` within `infrastructure/terraform/` to provision/update resources. Use `terraform import` for existing BQ tables.

## Local Development Setup

1.  Clone the repository from Cloud Source Repositories.
2.  Ensure Python 3.11+ and `pip` are installed.
3.  Set up Application Default Credentials (ADC): `gcloud auth application-default login`.
4.  Install `pip-tools`: `pip install pip-tools`.
5.  Install dependencies *per service*:
    ```bash
    cd services/listener
    pip-compile --generate-hashes requirements.in # Only if requirements.in changed
    pip install --require-hashes -r requirements.txt
    cd ../subscriber
    pip-compile --generate-hashes requirements.in # Only if requirements.in changed
    pip install --require-hashes -r requirements.txt
    # Repeat for price_loader, ratio_calculator, pdf_summarizer, news_summarizer...
    cd ../.. # Back to root
    ```
6.  Copy `.env.example` to `.env` in the root directory.
7.  Fill in necessary **non-secret** values in `.env` (like `GCP_PROJECT_ID`, table names if different from defaults). The Python scripts load these using `os.getenv()`. Secrets (`SEC_API_SECRET_ID`, `GEMINI_API_KEY_SECRET_ID`) point to names in Secret Manager, accessed via ADC.
8.  Run individual services locally if needed (ensure required services like Pub/Sub emulator or BQ are accessible or mocked):
    * Batch Jobs: `python services/price_loader/src/price_loader.py` / `python services/ratio_calculator/src/ratio_calculator.py`
    * Web Services: `python services/subscriber/src/main.py` / `python services/pdf_summarizer/src/main.py` / `python services/news_summarizer/src/main.py` (uses Flask dev server)
    * Listener: `python services/listener/src/listener.py`

## Building & Deploying

1.  **Build Images:** Commit changes to Git. Cloud Build (configured via `cicd/cloudbuild.yaml` and potentially triggered by pushes to your Cloud Source Repository) will automatically build Docker images for each service and push them to Artifact Registry, tagged with the commit SHA. Manual build: `gcloud builds submit . --config=cicd/cloudbuild.yaml`.
2.  **Deploy Infrastructure & Services:**
    * Navigate to `infrastructure/terraform/`.
    * Run `terraform init` (first time or after provider changes).
    * Run `terraform plan -var="gcp_project_id=profit-scout-456416" -var="docker_image_tag=COMMIT_SHA"` (replace COMMIT_SHA with the tag built by Cloud Build, or use variable passing from Cloud Build). Review the plan.
    * Run `terraform apply -var="gcp_project_id=profit-scout-456416" -var="docker_image_tag=COMMIT_SHA"` to create/update GCP resources and deploy/update the Cloud Run services/jobs to use the specified image tag.

## Dependencies

Dependencies are managed *per service* using `requirements.in` (for direct dependencies) and `requirements.txt` (for pinned, resolved dependencies with hashes). Use `pip-compile --generate-hashes` within each service directory to update `requirements.txt` after modifying `requirements.in`. Install using `pip install --require-hashes -r requirements.txt`.
>>>>>>> master

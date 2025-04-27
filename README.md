# Profit Scout - Financial Data & Analysis Pipeline (v2 - Workflow Architecture)

## Overview

This project implements a data pipeline on Google Cloud Platform (GCP) designed to ingest SEC filings (10-K, 10-Q), load daily stock prices, generate AI-driven summaries (from filings and recent news), and calculate financial ratios. The pipeline leverages **Cloud Run Jobs orchestrated by Cloud Workflow** for the main filing processing, ensuring modularity and reliability. The processed data (structured financials, PDFs, text summaries, ratios) is stored in BigQuery and Google Cloud Storage (GCS), providing a foundation for financial analysis, ML modeling, and application backends.

## Architecture Overview (v2)

The pipeline utilizes **Cloud Run Jobs** for discrete processing tasks, orchestrated primarily by **Cloud Workflow** for the main filing ingestion and analysis sequence. Standalone **Cloud Run Jobs** triggered by **Cloud Scheduler** handle periodic tasks like price loading and ratio calculation.

**![Profit Scout Data Pipeline Overview](../images/data_pipeline.png)

*Figure: Profit Scout Data Pipeline Overview***

**Key Data Flows:**

1.  **Scheduled Filing Processing (via Cloud Workflow):**
    * Cloud Scheduler triggers a Cloud Workflow (e.g., M-F 6 AM).
    * **Step 1 (`fetch-new-filings` Job):** Queries SEC API for recent filings, checks against BQ `filing_metadata` for existing records, inserts *new* metadata directly into BQ `filing_metadata`. Passes details of new filings to the workflow.
    * **Step 2 (`download-filing-pdf` Job - per new filing):** Takes filing details (from Step 1), downloads the PDF via `sec-api` filing link, uploads PDF (named e.g., `Ticker_AccesnsionNumber.pdf`) to GCS (`GCS_PDF_FOLDER`). Passes PDF GCS path to the workflow.
    * **Step 3 (Run in Parallel - per new filing):**
        * **Step 3a (`generate-qualitative-analysis` Job):** Takes PDF path (from Step 2) & filing details, downloads PDF, uses Gemini File API for qualitative analysis, saves result TXT (e.g., `Ticker_AccesnsionNumber_analysis.txt`) to GCS (`GCS_QUALITATIVE_TXT_PREFIX`). Saves analysis metadata (e.g., `Ticker_AccesnsionNumber_metadata.csv`) to GCS (`GCS_METADATA_CSV_PREFIX`).
        * **Step 3b (`generate-headline-assessment` Job):** Takes filing details (from Step 1), uses Vertex AI Gemini + Search Tool for recent news analysis (e.g., 14-day lookback), saves result TXT (e.g., `RiskAssessment_Ticker_YYYYMMDD.txt`) to GCS (`GCS_NEWS_SUMMARY_PREFIX`).
    * Workflow manages the execution and potential retries of these jobs.
2.  **Batch Processing (Scheduled):**
    * **`price-loader-job`:** Triggered by Cloud Scheduler (e.g., M-F 4 PM). Queries BQ `filing_metadata` for tickers, fetches incremental daily prices from `yfinance`, appends to BQ `price_data` table.
    * **`ratio-calculator-job`:** Triggered by Cloud Scheduler (e.g., M-F 4:30 PM). Queries BQ (`filing_metadata`, `financial_ratios`) for unprocessed filings (based on metadata added by Step 1 but not yet in ratios table), fetches required BQ data (including prices updated by `price-loader-job`), uses Gemini API for calculations, appends results to BQ `financial_ratios` table. *(Ensures it only calculates for new/missing filings)*.

## Project Structure (v2)

This project uses a monorepo structure, transitioning to job-focused components:

```
profit_scout_pipeline/
|
├── jobs/                   # Code for individual Cloud Run Jobs
│   ├── fetch_filings/      # Scheduled via Workflow: SEC API -> BQ Metadata Check -> BQ Metadata Insert
│   │   ├── src/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── download_pdf/       # Triggered by Workflow: Filing Link -> GCS PDF
│   │   ├── src/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── generate_qualitative_analysis/ # Triggered by Workflow: GCS PDF -> Gemini -> GCS TXT/CSV
│   │   ├── src/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── generate_headline_assessment/ # Triggered by Workflow: Filing Details -> Vertex AI -> GCS TXT
│   │   ├── src/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── price_loader/       # Scheduled Job: yfinance -> BQ Prices
│   │   ├── src/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── ratio_calculator/   # Scheduled Job: BQ -> Gemini -> BQ Ratios
│       ├── src/
│       ├── Dockerfile
│       └── requirements.txt
│
├── infrastructure/         # Infrastructure as Code (Terraform)
│   └── terraform/
│       ├── main.tf         # Core resources (APIs, BQ, GCS, Secrets, Artifact Registry)
│       ├── iam.tf          # Service Accounts & IAM Bindings
│       ├── cloudrun_jobs.tf # Cloud Run Job definitions
│       ├── scheduling.tf   # Cloud Scheduler job definitions
│       ├── workflow.tf     # Cloud Workflow definition
│       └── variables.tf
│
├── cicd/                   # CI/CD Pipeline Definitions
│   └── cloudbuild.yaml     # Cloud Build: Builds job images, pushes to Artifact Registry
│
├── tests/                  # Automated Tests (To be developed)
│   ├── unit/
│   └── integration/
│
├── .gitignore
├── README.md               # This file
└── .env.example            # Template for local development environment variables
```
## Components Deep Dive (v2)

*(This section provides a high-level overview. More details are in the source code for each job.)*

### 1. Fetch New Filings (`jobs/fetch_filings/`)
* **Purpose:** Identify and record metadata for newly filed 10-Ks/10-Qs.
* **Trigger:** Cloud Workflow (initiated by Cloud Scheduler, e.g., M-F 6 AM).
* **Function:** Queries SEC API for recent filings (e.g., last 24h). Compares `AccessionNumber` against BQ `filing_metadata`. Inserts metadata for *new* filings into BQ. Outputs list of new filing details for the workflow.
* **Output:** BQ rows (`filing_metadata`), Workflow variables (list of new filings).
* **Deployment:** Cloud Run Job.

### 2. Download Filing PDF (`jobs/download_pdf/`)
* **Purpose:** Download the official PDF for a specific new filing.
* **Trigger:** Cloud Workflow (for each new filing identified by Job 1).
* **Function:** Receives filing details (including `LinkToFilingDetails`). Checks GCS for existing PDF. If missing, downloads PDF via `sec-api` filing link, uploads to GCS (`GCS_PDF_FOLDER`).
* **Output:** GCS PDF object (or path for workflow).
* **Deployment:** Cloud Run Job.

### 3. Generate Qualitative Analysis (`jobs/generate_qualitative_analysis/`)
* **Purpose:** Generate qualitative analysis directly from the filing PDF using Gemini.
* **Trigger:** Cloud Workflow (for each new filing PDF from Job 2).
* **Function:** Receives PDF GCS path and filing details. Checks for existing analysis TXT. Downloads PDF, uploads to Gemini File API, calls Gemini model with prompt, saves text summary to GCS (`GCS_ANALYSIS_TXT_PREFIX`) and metadata to GCS (`GCS_METADATA_CSV_PREFIX`).
* **Output:** GCS TXT object, GCS CSV object.
* **Deployment:** Cloud Run Job.

### 4. Generate Headline Assessment (`jobs/generate_headline_assessment/`)
* **Purpose:** Generate summary/assessment based on recent news around the filing date using Vertex AI.
* **Trigger:** Cloud Workflow (for each new filing identified by Job 1).
* **Function:** Receives filing details. Checks for existing assessment TXT. Calls Vertex AI Gemini model with Search Tool enabled. Saves text summary to GCS (`GCS_NEWS_SUMMARY_PREFIX`).
* **Output:** GCS TXT object.
* **Deployment:** Cloud Run Job.

### 5. Price Loader (`jobs/price_loader/`)
* **Purpose:** Maintain daily stock price history.
* **Trigger:** Cloud Scheduler (time-based, e.g., M-F 4 PM).
* **Function:** Queries BQ `filing_metadata` for distinct tickers, fetches incremental daily prices from `yfinance` based on `MAX(date)` in `price_data`, appends new data to BQ `price_data` table.
* **Output:** BQ table rows (`price_data`).
* **Deployment:** Cloud Run Job.

### 6. Ratio Calculator (`jobs/ratio_calculator/`)
* **Purpose:** Calculate financial ratios based on processed filings and prices.
* **Trigger:** Cloud Scheduler (time-based, e.g., M-F 4:30 PM, after price_loader).
* **Function:** Queries BQ to find filings in `filing_metadata` missing from `financial_ratios`. Fetches required financials/prices from BQ, calls Gemini API for ratio calculations (if needed), appends results to BQ `financial_ratios` table. *(Script modified to append, not replace)*.
* **Output:** BQ table rows (`financial_ratios`).
* **Deployment:** Cloud Run Job.

## Setup & Configuration

### Prerequisites

* Python 3 (e.g., 3.11+)
* Google Cloud Platform (GCP) Project (e.g., `profit-scout-456416`)
* Enabled GCP APIs: Cloud Run, Cloud Build, Artifact Registry, Secret Manager, BigQuery API, Cloud Storage API, Cloud Scheduler, IAM, Vertex AI API, **Cloud Workflows API**.
* Terraform CLI (for infrastructure provisioning).
* `gcloud` CLI configured locally or use Cloud Shell.
* `sec-api.io` Account and API Key.
* Gemini API Key (if using direct Gemini API for `generate-qualitative-analysis` or `ratio_calculator`).

### IAM / Service Accounts

Create dedicated service accounts via Terraform for each distinct job with the principle of least privilege. Key roles needed include:
* `roles/run.invoker` (for Scheduler/Workflow to trigger jobs)
* `roles/secretmanager.secretAccessor`
* `roles/bigquery.dataEditor` / `roles/bigquery.jobUser`
* `roles/storage.objectAdmin`
* `roles/logging.logWriter`
* `roles/aiplatform.user` (for Vertex AI endpoint usage in `generate-headline-assessment`)
* `roles/workflows.invoker` (for Scheduler to trigger workflow)
* `roles/iam.serviceAccountUser` (for Workflow/Scheduler acting as other SAs)
*(Specific bindings defined in Terraform)*

### Secrets

Store sensitive keys in **Google Secret Manager**:
1.  **SEC API Key:** Used by `fetch-new-filings` and `download-filing-pdf`.
    * Secret Name Example: `sec-api-key`
    * Accessed via env var: `SEC_API_SECRET_ID`
2.  **Gemini API Key:** Used by `generate-qualitative-analysis` and `ratio_calculator` (if using direct Gemini API).
    * Secret Name Example: `gemini-api-key`
    * Accessed via env var: `GEMINI_API_KEY_SECRET_ID`

### Environment Variables

Configure these via Terraform when defining the Cloud Run Jobs and Workflow. Refer to `.env.example` for local development.

*(List environment variables required by the refactored Python job scripts):*
* `GCP_PROJECT_ID`
* `GCP_REGION` (e.g., `us-central1`)
* `BQ_DATASET_ID`
* `BQ_METADATA_TABLE_ID`
* `BQ_FINANCIALS_BS_TABLE_ID` (Used by ratio_calculator)
* `BQ_FINANCIALS_IS_TABLE_ID` (Used by ratio_calculator)
* `BQ_FINANCIALS_CF_TABLE_ID` (Used by ratio_calculator)
* `BQ_PRICE_TABLE_ID` (Used by price_loader, ratio_calculator)
* `BQ_RATIOS_TABLE_ID`
* `GCS_BUCKET_NAME`
* `GCS_PDF_FOLDER` (Output for download_pdf, input for generate_qualitative_analysis)
* `GCS_QUALITATIVE_TXT_PREFIX` (Output for generate_qualitative_analysis)
* `GCS_METADATA_CSV_PREFIX` (Output for generate_qualitative_analysis)
* `GCS_NEWS_SUMMARY_PREFIX` (Output for generate_headline_assessment, uses this name in code)
* `SEC_API_SECRET_ID`
* `SEC_API_SECRET_VERSION`
* `GEMINI_API_KEY_SECRET_ID`
* `GEMINI_API_KEY_SECRET_VERSION`
* `GEMINI_MODEL_NAME` (Model ID for Gemini/Vertex)
* `LOOKBACK_HOURS` (For fetch-new-filings)
* `FILING_TYPES` (For fetch-new-filings)
* `MAX_WORKERS` (Optional, for threaded jobs like ratio_calculator)
* `PORT` (Optional, less relevant for jobs, but Cloud Run default is 8080 if ever needed)
* *(Potentially others depending on exact refactoring)*

### GCP Resource Setup (via Terraform)

The `infrastructure/terraform/` directory contains the definitions for all necessary GCP resources. This includes creating/managing:
* GCS Bucket
* BigQuery Dataset & Tables
* Secret Manager Secret definitions
* Artifact Registry Docker Repository
* IAM Service Accounts and Bindings
* **Cloud Run Job definitions** (referencing images built by Cloud Build)
* **Cloud Workflow definition**
* **Cloud Scheduler Jobs** (Triggering the main Workflow, price_loader, ratio_calculator)

Run `terraform init`, `terraform plan`, and `terraform apply` within `infrastructure/terraform/` to provision/update resources.

## Local Development Setup

1.  Clone the repository.
2.  Ensure Python 3.11+ and `pip` are installed.
3.  Set up Application Default Credentials (ADC): `gcloud auth application-default login`.
4.  Install `pip-tools`: `pip install pip-tools`.
5.  Install dependencies *per job*:
    ```bash
    cd jobs/fetch_filings
    # pip-compile --generate-hashes requirements.in # Optional
    pip install -r requirements.txt
    cd ../download_pdf
    # pip-compile --generate-hashes requirements.in # Optional
    pip install -r requirements.txt
    # Repeat for other jobs...
    cd ../.. # Back to root
    ```
6.  Copy `.env.example` to `.env`. Fill in **non-secret** values. Secrets are referenced by name (`*_SECRET_ID`) and fetched via ADC.
7.  Run individual jobs locally (requires setting appropriate environment variables, secrets accessible via ADC):
    ```bash
    # Example for fetch-filings job
    export GCP_PROJECT_ID="profit-scout-456416"
    export BQ_DATASET_ID="profit_scout"
    # ... set other env vars ...
    python jobs/fetch_filings/src/main.py

    # Example for ratio calculator
    export GCP_PROJECT_ID="profit-scout-456416"
    # ... set other env vars ...
    python jobs/ratio_calculator/src/ratio_calculator.py
    ```
    *(Note: Simulating the full workflow locally is complex; focus on testing individual job logic)*

## Building & Deploying

1.  **Build Images:** Commit changes. Cloud Build (`cicd/cloudbuild.yaml`) builds Docker images for each job and pushes them to Artifact Registry (tagged, e.g., with commit SHA). Manual build: `gcloud builds submit . --config=cicd/cloudbuild.yaml`.
2.  **Deploy Infrastructure & Jobs:**
    * Navigate to `infrastructure/terraform/`.
    * Run `terraform init`.
    * Run `terraform plan -var="gcp_project_id=profit-scout-456416" -var="docker_image_tag=COMMIT_SHA"` (replace COMMIT_SHA). Review plan.
    * Run `terraform apply -var="gcp_project_id=profit-scout-456416" -var="docker_image_tag=COMMIT_SHA"` to create/update GCP resources (Jobs, Workflow, Schedules, etc.).

## Dependencies

Dependencies are managed *per job* using `requirements.txt` (pinned dependencies). Use `pip-tools` (`pip-compile`) optionally if using `requirements.in` for managing direct dependencies. Install using `pip install -r requirements.txt`.
# -------------------------------------
# General GCP Settings
# -------------------------------------
variable "gcp_project_id" {
  description = "The GCP project ID where resources will be deployed."
  type        = string
  default     = "profit-scout-456416"
}
variable "gcp_region" {
  description = "The primary GCP region for deploying resources (e.g., Cloud Run, Workflow)."
  type        = string
  default     = "us-central1"
}
variable "gcp_location" {
  description = "The location for resources like GCS Buckets and BigQuery Datasets (can be multi-region like 'US')."
  type        = string
  default     = "us-central1"
}# -------------------------------------
# Artifact Registry Settings
# -------------------------------------
variable "artifact_registry_repo_name" {
  description = "The name of the Artifact Registry Docker repository."
  type        = string
  default     = "profit-scout-repo"
}
variable "docker_image_tag" {
  description = "The Docker image tag (e.g., commit SHA or 'latest') to deploy for jobs."
  type        = string
  default     = "latest"
}# -------------------------------------
# Google Cloud Storage Settings
# -------------------------------------
variable "gcs_bucket_name" {
  description = "The name of the main GCS bucket for storing PDFs, summaries, and financial CSVs."
  type        = string
  default     = "profit-scout"
}
variable "gcs_pdf_prefix" {
  description = "Folder prefix within GCS bucket for storing input PDFs."
  type        = string
  default     = "sec-pdf/"
}
variable "gcs_analysis_txt_prefix" {
  description = "Folder prefix within GCS bucket for qualitative analysis TXT output."
  type        = string
  default     = "sec-analysis/"
}
variable "gcs_headline_assessment_prefix" {
  description = "Folder prefix within GCS bucket for headline assessment TXT output."
  type        = string
  default     = "headline-analysis/"
}
variable "gcs_bs_prefix" {
  description = "Folder prefix within GCS bucket for balance sheet CSVs."
  type        = string
  default     = "balance-sheet/"
}
variable "gcs_is_prefix" {
  description = "Folder prefix within GCS bucket for income statement CSVs."
  type        = string
  default     = "income-statement/"
}
variable "gcs_cf_prefix" {
  description = "Folder prefix within GCS bucket for cash flow CSVs."
  type        = string
  default     = "cash-flow/"
}# -------------------------------------
# BigQuery Settings
# -------------------------------------
variable "bq_dataset_id" {
  description = "The BigQuery dataset ID."
  type        = string
  default     = "profit_scout"
}
variable "bq_metadata_table_id" {
  description = "The table ID for filing metadata."
  type        = string
  default     = "filing_metadata"
}
variable "bq_price_table_id" {
  description = "The table ID for stock price data (used by price_loader, ratio_calculator)."
  type        = string
  default     = "price_data"
}
variable "bq_ratios_table_id" {
  description = "The table ID for calculated financial ratios."
  type        = string
  default     = "financial_ratios"
}# -------------------------------------
# Secret Manager Settings
# -------------------------------------
variable "sec_api_secret_name" {
  description = "Name of the Secret Manager secret holding the SEC API key."
  type        = string
  default     = "sec-api-key"
}
variable "gemini_api_secret_name" {
  description = "Name of the Secret Manager secret holding the Gemini API key."
  type        = string
  default     = "gemini-api-key"
}
variable "api_secret_version" {
  description = "Version of secrets to use (e.g., 'latest')."
  type        = string
  default     = "latest"
}
# -------------------------------------
# Cloud Run Job Names
# -------------------------------------
variable "fetch_filings_job_name" {
  description = "Name for the 'fetch-new-filings' Cloud Run Job."
  type        = string
  default     = "profit-scout-fetch-filings"
}
variable "download_pdf_job_name" {
  description = "Name for the 'download-filing-pdf' Cloud Run Job."
  type        = string
  default     = "profit-scout-download-pdf"
}
variable "qualitative_analysis_job_name" {
  description = "Name for the 'qualitative-analysis' Cloud Run Job."
  type        = string
  default     = "profit-scout-qualitative-analysis"
}
variable "headline_assessment_job_name" {
  description = "Name for the 'headline-assessment' Cloud Run Job."
  type        = string
  default     = "profit-scout-headline-assessment"
}
variable "price_loader_job_name" {
  description = "Name for the 'price-loader' Cloud Run Job."
  type        = string
  default     = "profit-scout-price-loader"
}
variable "ratio_calculator_job_name" {
  description = "Name for the 'ratio-calculator' Cloud Run Job."
  type        = string
  default     = "profit-scout-ratio-calculator"
}
variable "financial_extraction_job_name" {
  description = "Name for the 'financial-extraction' Cloud Run Job."
  type        = string
  default     = "profit-scout-financial-extraction"
}# -------------------------------------
# Default Resource Allocations
# -------------------------------------
variable "default_job_cpu" {
  description = "Default CPU allocation for Cloud Run Jobs."
  type        = string
  default     = "1000m"
}
variable "default_job_memory" {
  description = "Default Memory allocation for Cloud Run Jobs."
  type        = string
  default     = "1Gi"
}
variable "default_job_timeout" {
  description = "Default execution timeout for Cloud Run Job tasks."
  type        = string
  default     = "3600s"
}
variable "default_job_retries" {
  description = "Default maximum retries for failed Cloud Run Job tasks."
  type        = number
  default     = 1
}# -------------------------------------
# Specific Job Settings
# -------------------------------------
variable "fetch_filings_lookback_hours" {
  description = "How many hours back the fetch-filings job should query the SEC API."
  type        = number
  default     = 24
}
variable "fetch_filings_types" {
  description = "Comma-separated, quoted list of filing types for fetch-filings job."
  type        = string
  default     = "\"10-K\",\"10-Q\""
}
variable "fetch_filings_tickers" {
  description = "Optional: Comma-separated list of specific tickers for fetch-filings job (leave empty to fetch all)."
  type        = string
  default     = ""
}
variable "ratio_calculator_max_workers" {
  description = "Max concurrent workers for the ratio_calculator job."
  type        = number
  default     = 8
}# -------------------------------------
# Gemini SDK Settings
# -------------------------------------
variable "gemini_model_name" {
  description = "Default Gemini model ID."
  type        = string
  default     = "gemini-2.0-flash-001"
}
variable "gemini_temperature" {
  description = "Temperature parameter for Gemini model generation."
  type        = number
  default     = 0.2
}
variable "gemini_max_tokens" {
  description = "Maximum output tokens for Gemini model generation."
  type        = number
  default     = 8192
}
variable "gemini_req_timeout" {
  description = "Request timeout (in seconds) for Gemini model generation."
  type        = number
  default     = 300
}# -------------------------------------
# Cloud Workflow Settings
# -------------------------------------
variable "workflow_name" {
  description = "Name for the main Cloud Workflow."
  type        = string
  default     = "profit-scout-main-filing-workflow"
}
variable "workflow_region" {
  description = "Region for deploying the Cloud Workflow (must support Workflows)."
  type        = string
  default     = "us-central1"
}
variable "workflow_sa_account_id" {
  description = "Account ID (part before @) for the Workflow's dedicated service account."
  type        = string
  default     = "profit-scout-workflow-sa"
}# -------------------------------------
# Cloud Scheduler Settings
# -------------------------------------
variable "scheduler_sa_email" {
  description = "The email address of the default service account Cloud Scheduler uses to trigger targets."
  type        = string
  default     = "profit-scout-scheduler@profit-scout-456416.iam.gserviceaccount.com"
}
variable "scheduler_timezone" {
  description = "Timezone for Cloud Scheduler jobs (e.g., 'America/New_York', 'UTC')."
  type        = string
  default     = "America/New_York"
}
variable "main_workflow_schedule" {
  description = "Cron schedule for the main filing processing workflow (triggers fetch-filings)."
  type        = string
  default     = "0 6 * * MON-FRI"
}
variable "price_loader_schedule" {
  description = "Cron schedule for the price loader job."
  type        = string
  default     = "0 16 * * MON-FRI"
}
variable "ratio_calculator_schedule" {
  description = "Cron schedule for the ratio calculator job."
  type        = string
  default     = "30 16 * * MON-FRI"
}


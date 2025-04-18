# infrastructure/terraform/variables.tf

# -------------------------------------
# General GCP Settings
# -------------------------------------
variable "gcp_project_id" {
  description = "The GCP project ID where resources will be deployed."
  type        = string
  default     = "profit-scout-456416" # Your specific project ID
}

variable "gcp_region" {
  description = "The primary GCP region for deploying resources."
  type        = string
  default     = "us-central1" # Choose your preferred region
}

# -------------------------------------
# Artifact Registry Settings
# -------------------------------------
variable "artifact_registry_repo_name" {
  description = "The name of the Artifact Registry Docker repository."
  type        = string
  default     = "profit-scout-repo" # Name for your Docker image repository
}

# -------------------------------------
# Google Cloud Storage Settings
# -------------------------------------
variable "gcs_bucket_name" {
  description = "The name of the GCS bucket for storing PDFs and summaries."
  type        = string
  default     = "profit-scout" # Your existing bucket name
}

variable "gcs_pdf_folder" {
  description = "Folder within GCS bucket for storing input PDFs."
  type        = string
  default     = "sec-pdf/" # Your specified folder
}

variable "gcs_analysis_txt_prefix" {
  description = "Folder prefix within GCS bucket for pdf_summarizer output."
  type        = string
  default     = "sec-analysis/" # Your specified folder
}

variable "gcs_news_summary_prefix" {
  description = "Folder prefix within GCS bucket for news_summarizer output."
  type        = string
  default     = "headline-analysis/" # Your specified folder
}

# -------------------------------------
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
variable "bq_bs_table_id" {
  description = "The table ID for balance sheet data."
  type        = string
  default     = "balance_sheet"
}
variable "bq_is_table_id" {
  description = "The table ID for income statement data."
  type        = string
  default     = "income_statement"
}
variable "bq_cf_table_id" {
  description = "The table ID for cash flow data."
  type        = string
  default     = "cash_flow"
}
variable "bq_price_table_id" {
  description = "The table ID for stock price data."
  type        = string
  default     = "price_data"
}
variable "bq_ratios_table_id" {
  description = "The table ID for calculated financial ratios."
  type        = string
  default     = "financial_ratios" # Your specified table name
}

# -------------------------------------
# Secret Manager Settings
# -------------------------------------
variable "sec_api_secret_name" {
  description = "Name of the Secret Manager secret holding the sec-api.io key."
  type        = string
  default     = "sec-api-key" # Your specified secret name
}
variable "gemini_api_secret_name" {
  description = "Name of the Secret Manager secret holding the Gemini API key."
  type        = string
  default     = "gemini-api-key" # Your specified secret name
}

# -------------------------------------
# Pub/Sub Settings
# -------------------------------------
variable "pubsub_topic_id" {
  description = "The ID of the Pub/Sub topic for filing notifications."
  type        = string
  default     = "sec-filing-notification"
}

# -------------------------------------
# Cloud Run / Deployment Settings
# -------------------------------------
variable "docker_image_tag" {
  description = "The Docker image tag (e.g., commit SHA) to deploy for services/jobs."
  type        = string
  default     = "latest" # Should be overridden by CI/CD pipeline (e.g., using $COMMIT_SHA)
}

variable "price_loader_job_name" {
  description = "The name of the Cloud Run Job for the price loader."
  type        = string
  default     = "profit-scout-price-loader"
}

variable "ratio_calculator_job_name" {
  description = "The name of the Cloud Run Job for the ratio calculator."
  type        = string
  default     = "profit-scout-ratio-calculator"
}

# Add service name variables if you want them configurable, otherwise use defaults in cloudrun.tf
# variable "listener_service_name" { type = string; default = "profit-scout-listener" }
# ... etc ...

# -------------------------------------
# IAM / Scheduler Settings
# -------------------------------------
variable "scheduler_sa_email" {
  description = "The email address of the service account Cloud Scheduler uses to trigger Cloud Run Jobs. Find default in IAM: service-[PROJECT_NUMBER]@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
  type        = string
  default     = "service-264262717470@gcp-sa-cloudscheduler.iam.gserviceaccount.com" # Your specific default SA email
}

# --- ADDED SCHEDULER VARIABLES ---
variable "scheduler_timezone" {
  description = "Timezone for Cloud Scheduler jobs (e.g., 'America/New_York', 'UTC')."
  type        = string
  default     = "America/New_York"
}

variable "price_loader_schedule" {
  description = "Cron schedule for the price loader job."
  type        = string
  default     = "5 16 * * *" # Default: 4:05 PM daily (adjust as needed)
}

variable "ratio_calculator_schedule" {
  description = "Cron schedule for the ratio calculator job."
  type        = string
  default     = "35 16 * * *" # Default: 4:35 PM daily (adjust as needed)
}
# --- END ADDED ---

# -------------------------------------
# Service Specific Settings
# -------------------------------------
variable "max_workers_ratio_calc" {
  description = "Max concurrent workers for the ratio_calculator job."
  type        = number
  default     = 8
}

variable "sec_websocket_url" {
  description = "WebSocket URL for the SEC API listener."
  type        = string
  default     = "wss://sec-api.io/stream"
  sensitive   = false
}

variable "gemini_model_name" {
  description = "Default Gemini model name used by summarizer/calculator."
  type        = string
  default     = "gemini-2.0-flash-001"
}
# infrastructure/terraform/variables.tf

# -------------------------------------
# General GCP Settings
# -------------------------------------
variable "gcp_project_id" {
  description = "The GCP project ID where resources will be deployed."
  type        = string
  default     = "profit-scout-456416" # <- Ensure this is your correct Project ID
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
}


# -------------------------------------
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
  default     = "latest" # Should ideally be overridden by CI/CD pipeline (e.g., using $COMMIT_SHA)
}

# -------------------------------------
# Google Cloud Storage Settings
# -------------------------------------
variable "gcs_bucket_name" {
  description = "The name of the GCS bucket for storing PDFs and summaries."
  type        = string
  default     = "profit-scout" # <- Ensure this matches your bucket name
}

variable "gcs_pdf_folder" {
  description = "Folder within GCS bucket for storing input PDFs downloaded by download-pdf job."
  type        = string
  default     = "SEC_Filings_Russell1000/" # Matches path used in profitscoutrussell1000.txt
}

variable "gcs_analysis_txt_prefix" {
  description = "Folder prefix within GCS bucket for qualitative_analysis job TXT output."
  type        = string
  default     = "Qualitative_Analysis_TXT_R1000/" # Matches path used in profitscoutrussell1000.txt
}

variable "gcs_analysis_metadata_csv_prefix" {
  description = "Folder prefix within GCS bucket for qualitative_analysis job metadata CSV output."
  type        = string
  default     = "Qualitative_Metadata_R1000/" # Matches path used in profitscoutrussell1000.txt
}

variable "gcs_headline_assessment_prefix" {
  description = "Folder prefix within GCS bucket for headline_assessment job TXT output."
  type        = string
  # Defaulting to RiskAssessments to match profitscoutrussell1000.txt Part 5 output path
  # Your old config used 'headline-analysis/' for news_summarizer - choose which convention you prefer
  default     = "RiskAssessments/"
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
  description = "The table ID for balance sheet data (used by ratio_calculator)."
  type        = string
  default     = "balance_sheet"
}
variable "bq_is_table_id" {
  description = "The table ID for income statement data (used by ratio_calculator)."
  type        = string
  default     = "income_statement"
}
variable "bq_cf_table_id" {
  description = "The table ID for cash flow data (used by ratio_calculator)."
  type        = string
  default     = "cash_flow"
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
}

# -------------------------------------
# Secret Manager Settings
# -------------------------------------
variable "sec_api_secret_name" {
  description = "Name of the Secret Manager secret holding the sec-api.io key."
  type        = string
  default     = "sec-api-key"
}
variable "gemini_api_secret_name" {
  description = "Name of the Secret Manager secret holding the Gemini API key (used by qualitative_analysis, ratio_calculator)."
  type        = string
  default     = "gemini-api-key"
}
variable "api_secret_version" {
  description = "Version of secrets to use (e.g., 'latest')."
  type        = string
  default     = "latest"
}

# -------------------------------------
# Cloud Run Job Settings
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
  description = "Name for the 'generate-qualitative-analysis' Cloud Run Job."
  type        = string
  default     = "profit-scout-qualitative-analysis"
}

variable "headline_assessment_job_name" {
  description = "Name for the 'generate-headline-assessment' Cloud Run Job."
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

# Default Resource Allocations (can be overridden per job if needed)
variable "default_job_cpu" {
  description = "Default CPU allocation for Cloud Run Jobs."
  type        = string
  default     = "1000m" # 1 vCPU
}

variable "default_job_memory" {
  description = "Default Memory allocation for Cloud Run Jobs."
  type        = string
  default     = "1Gi" # 1 GiB Memory
}

variable "default_job_timeout" {
  description = "Default execution timeout for Cloud Run Job tasks."
  type        = string
  default     = "3600s" # 1 hour
}

variable "default_job_retries" {
  description = "Default maximum retries for failed Cloud Run Job tasks."
  type        = number
  default     = 1 # Retry once on failure
}

# Specific Job Settings
variable "fetch_filings_lookback_hours" {
  description = "How many hours back the fetch-filings job should query the SEC API."
  type        = number
  default     = 24
}

variable "fetch_filings_types" {
  description = "Comma-separated, quoted list of filing types for fetch-filings job."
  type        = string
  default     = "\"10-K\",\"10-Q\"" # Ensure quotes are included for the query string
}

variable "fetch_filings_tickers" {
  description = "Optional: Comma-separated list of specific tickers for fetch-filings job (leave empty to fetch all)."
  type        = string
  default     = ""
}

variable "ratio_calculator_max_workers" {
  description = "Max concurrent workers for the ratio_calculator job."
  type        = number
  default     = 8 # From original variables.tf
}

variable "gemini_model_name" {
  description = "Default Gemini model ID"
  type        = string
  default     = "gemini-2.0-flash-001" # Ensure compatibility with API/endpoint used
}


# -------------------------------------
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
  default     = "us-central1" # Match default region, ensure it supports Workflows
}

variable "workflow_sa_account_id" {
  description = "Account ID (part before @) for the Workflow's dedicated service account."
  type        = string
  default     = "profit-scout-workflow-sa"
}

# -------------------------------------
# Cloud Scheduler Settings
# -------------------------------------
variable "scheduler_sa_email" {
  description = "The email address of the default service account Cloud Scheduler uses to trigger targets. Find in IAM: service-[PROJECT_NUMBER]@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
  type        = string
  # IMPORTANT: Replace this with your project's actual Cloud Scheduler SA email
  default     =  "profit-scout-scheduler@profit-scout-456416.iam.gserviceaccount.com"
}

variable "scheduler_timezone" {
  description = "Timezone for Cloud Scheduler jobs (e.g., 'America/New_York', 'UTC'). Affects schedule interpretation."
  type        = string
  default     = "America/New_York" # E.g., for EDT/EST
}

variable "main_workflow_schedule" {
  description = "Cron schedule for the main filing processing workflow (triggers fetch-filings)."
  type        = string
  default     = "0 6 * * MON-FRI" # M-F 6:00 AM
}

variable "price_loader_schedule" {
  description = "Cron schedule for the price loader job."
  type        = string
  default     = "0 16 * * MON-FRI" # M-F 4:00 PM (16:00)
}

variable "ratio_calculator_schedule" {
  description = "Cron schedule for the ratio calculator job."
  type        = string
  default     = "30 16 * * MON-FRI" # M-F 4:30 PM (16:30)
}
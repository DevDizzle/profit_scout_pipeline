# infrastructure/terraform/iam.tf

# --- Data Sources ---
data "google_project" "current" {}

# -------------------------------------
# Service Accounts
# -------------------------------------

# --- NEW Service Accounts for Workflow and Jobs ---
resource "google_service_account" "fetch_filings_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-fetch-filings-sa"
  display_name = "Profit Scout Fetch Filings Job SA"
}

resource "google_service_account" "download_pdf_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-download-pdf-sa"
  display_name = "Profit Scout Download PDF Job SA"
}

resource "google_service_account" "qualitative_analysis_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-qual-anlys-sa" # Shortened name
  display_name = "Profit Scout Qualitative Analysis Job SA"
}

resource "google_service_account" "headline_assessment_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-headln-asmt-sa" # Shortened name
  display_name = "Profit Scout Headline Assessment Job SA"
}

resource "google_service_account" "workflow_sa" {
  project      = var.gcp_project_id
  account_id   = var.workflow_sa_account_id
  display_name = "Profit Scout Main Workflow SA"
}

variable "terraform_deployer_sa_account_id" {
  description = "Account ID for the service account used to run Terraform apply."
  type        = string
  default     = "tf-deployer-sa" 
}

resource "google_service_account" "terraform_deployer_sa" {
  project      = var.gcp_project_id
  account_id   = var.terraform_deployer_sa_account_id
  display_name = "Terraform Deployer Service Account"
}

# --- EXISTING Service Accounts (Kept) ---
resource "google_service_account" "price_loader_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-price-loader-sa" # Keep existing ID
  display_name = "Profit Scout Price Loader Job SA"
}

resource "google_service_account" "ratio_calculator_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-ratio-calc-sa" # Keep existing ID
  display_name = "Profit Scout Ratio Calculator Job SA"
}


# --- REMOVED OLD Service Accounts ---
# listener_sa, subscriber_sa, pdf_summarizer_sa, news_summarizer_sa definitions removed.

# -------------------------------------
# IAM Bindings - Resource Specific
# -------------------------------------

# --- Secret Manager Access ---
# SEC API Key Access
resource "google_secret_manager_secret_iam_member" "sec_accessor_fetch_filings" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.sec_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.fetch_filings_sa.member
}
resource "google_secret_manager_secret_iam_member" "sec_accessor_download_pdf" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.sec_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.download_pdf_sa.member
}
# Gemini API Key Access
resource "google_secret_manager_secret_iam_member" "gemini_accessor_qualitative" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.gemini_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.qualitative_analysis_sa.member
}
resource "google_secret_manager_secret_iam_member" "gemini_accessor_ratio_calculator" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.gemini_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.ratio_calculator_sa.member # Keep existing
}
# REMOVED: Old SA accessors


# --- GCS Bucket Access ---
# Grant roles needed by the jobs to read/write specific objects
resource "google_storage_bucket_iam_member" "storage_admin_download_pdf" {
  bucket = google_storage_bucket.main_bucket.name
  role   = "roles/storage.objectAdmin" # Needs to write PDFs
  member = google_service_account.download_pdf_sa.member
}
resource "google_storage_bucket_iam_member" "storage_admin_qualitative" {
  bucket = google_storage_bucket.main_bucket.name
  role   = "roles/storage.objectAdmin" # Needs read PDF, write TXT/CSV
  member = google_service_account.qualitative_analysis_sa.member
}
resource "google_storage_bucket_iam_member" "storage_admin_headline" {
  bucket = google_storage_bucket.main_bucket.name
  role   = "roles/storage.objectAdmin" # Needs to write TXT
  member = google_service_account.headline_assessment_sa.member
}
# REMOVED: Old SA GCS bindings (subscriber, pdf_summarizer, news_summarizer)

# --- BigQuery Dataset Access ---
# Grant BQ Data Editor for jobs writing data, Viewer for reading
resource "google_bigquery_dataset_iam_member" "bq_editor_fetch_filings" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataEditor" # Needs to write metadata
  member     = google_service_account.fetch_filings_sa.member
}
resource "google_bigquery_dataset_iam_member" "bq_editor_price_loader" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataEditor" # Keep existing
  member     = google_service_account.price_loader_sa.member
}
resource "google_bigquery_dataset_iam_member" "bq_editor_ratio_calculator" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataEditor" # Keep existing (needs read/write)
  member     = google_service_account.ratio_calculator_sa.member
}
# Ratio calculator might only need viewer on financials if it only reads them
# resource "google_bigquery_dataset_iam_member" "bq_viewer_ratio_calculator" {
#   project    = var.gcp_project_id
#   dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
#   role       = "roles/bigquery.dataViewer"
#   member     = google_service_account.ratio_calculator_sa.member
# }
# REMOVED: Old SA BQ bindings (subscriber, news_summarizer)

# -------------------------------------
# IAM Bindings - Project Level
# -------------------------------------

# --- Artifact Registry Reader ---
# Allow all job service accounts (and workflow SA) to pull images
resource "google_project_iam_member" "artifact_registry_reader" {
  for_each = toset([
    google_service_account.fetch_filings_sa.email,
    google_service_account.download_pdf_sa.email,
    google_service_account.qualitative_analysis_sa.email,
    google_service_account.headline_assessment_sa.email,
    google_service_account.price_loader_sa.email,
    google_service_account.ratio_calculator_sa.email,
    # Workflow SA doesn't pull images, but job SAs do
  ])
  project = var.gcp_project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${each.value}"
}

# --- Log Writer ---
# Allow all job service accounts (and workflow SA) to write logs
resource "google_project_iam_member" "log_writer" {
  for_each = toset([
    google_service_account.fetch_filings_sa.email,
    google_service_account.download_pdf_sa.email,
    google_service_account.qualitative_analysis_sa.email,
    google_service_account.headline_assessment_sa.email,
    google_service_account.price_loader_sa.email,
    google_service_account.ratio_calculator_sa.email,
    google_service_account.workflow_sa.email,
  ])
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${each.value}"
}

# --- BigQuery Job/Read Session Users ---
# Grant roles needed to run queries/load jobs/read data
resource "google_project_iam_member" "bq_job_user" {
  for_each = toset([
    google_service_account.fetch_filings_sa.email, # Runs queries, inserts rows
    google_service_account.price_loader_sa.email, # Runs queries, loads data
    google_service_account.ratio_calculator_sa.email # Runs queries, inserts rows
    # Other jobs don't interact directly with BQ
  ])
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${each.value}"
}

# Ratio calculator also needs readSessionUser if using Storage Read API via pandas-gbq
resource "google_project_iam_member" "bq_readsession_ratio_calculator" {
  project = var.gcp_project_id
  role    = "roles/bigquery.readSessionUser"
  member  = google_service_account.ratio_calculator_sa.member
}
# REMOVED: Old SA BQ Job User bindings

# --- Vertex AI / AI Platform User ---
# Grant access to use Vertex AI endpoints
resource "google_project_iam_member" "ai_user_headline_assessment" {
  project = var.gcp_project_id
  role    = "roles/aiplatform.user"
  member  = google_service_account.headline_assessment_sa.member # This job uses Vertex AI
}
resource "google_project_iam_member" "ai_user_ratio_calculator" {
  project = var.gcp_project_id
  role    = "roles/aiplatform.user"
  member  = google_service_account.ratio_calculator_sa.member # Keep existing (if using Vertex for Gemini)
}
# REMOVED: Old SA AI user bindings (pdf_summarizer, news_summarizer)

# --- Service Account User Role ---
# Allow Workflow SA to act as the Job SAs when invoking them

# Define a map of the relevant job service accounts
locals {
  workflow_target_job_sas = {
    fetch_filings        = google_service_account.fetch_filings_sa
    download_pdf         = google_service_account.download_pdf_sa
    qualitative_analysis = google_service_account.qualitative_analysis_sa
    headline_assessment  = google_service_account.headline_assessment_sa
  }
}

resource "google_service_account_iam_member" "workflow_act_as_job_sas" {
  # Iterate over the map defined above
  for_each = local.workflow_target_job_sas

  # service_account_id is the SA being granted permission upon (the Job SA)
  # Use .name which resolves to the full SA identifier (projects/../serviceAccounts/...)
  service_account_id = each.value.name
  role               = "roles/iam.serviceAccountUser"
  # member is the Workflow SA being granted the permission
  member             = google_service_account.workflow_sa.member

  # depends_on is implicitly handled by referencing each.value
}

# Allow Scheduler SA to act as itself (needed for OIDC token generation)
# (This block remains the same)
resource "google_project_iam_member" "scheduler_act_as_self" {
  project = var.gcp_project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${var.scheduler_sa_email}"
}

# Grant Deployer SA permission to act as Scheduler SA
resource "google_service_account_iam_member" "deployer_sa_act_as_scheduler" {
  # The SA being granted permission upon (Scheduler SA)
  service_account_id = "projects/${var.gcp_project_id}/serviceAccounts/${var.scheduler_sa_email}"
  role               = "roles/iam.serviceAccountUser"
  # The member being granted the permission (Deployer SA)
  member             = google_service_account.terraform_deployer_sa.member
}

# --- Invoker Roles ---
# Allow Workflow SA to invoke Cloud Run Jobs
resource "google_project_iam_member" "workflow_run_invoker" {
  project = var.gcp_project_id
  role    = "roles/run.invoker"
  member  = google_service_account.workflow_sa.member
}

# Allow Scheduler SA to invoke the Workflow
resource "google_project_iam_member" "scheduler_workflow_invoker" {
  project = var.gcp_project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${var.scheduler_sa_email}"
}

# Allow Scheduler SA to invoke the standalone Cloud Run Jobs directly
resource "google_project_iam_member" "scheduler_run_job_invoker" {
  # Grant at project level, fine-grained access controlled by job IAM later if needed
  project = var.gcp_project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${var.scheduler_sa_email}"
}

# --- Cloud Run Job Specific IAM (Optional Fine-Grained Invocation) ---
# This is generally NOT needed if granting roles/run.invoker at project level above.
# If you need stricter control (e.g., Scheduler can ONLY invoke specific jobs),
# grant roles/run.invoker per job using google_cloud_run_v2_job_iam_member
# Example (uncomment and adapt if needed):
# resource "google_cloud_run_v2_job_iam_member" "price_loader_scheduler_invoker" {
#   project  = var.gcp_project_id
#   location = var.gcp_region
#   name     = google_cloud_run_v2_job.price_loader_job.name # Assumes job resource name
#   role     = "roles/run.invoker"
#   member   = "serviceAccount:${var.scheduler_sa_email}"
# }
# resource "google_cloud_run_v2_job_iam_member" "ratio_calculator_scheduler_invoker" {
#   project  = var.gcp_project_id
#   location = var.gcp_region
#   name     = google_cloud_run_v2_job.ratio_calculator_job.name # Assumes job resource name
#   role     = "roles/run.invoker"
#   member   = "serviceAccount:${var.scheduler_sa_email}"
# }
# REMOVED: Old Job Invoker bindings


# --- REMOVED Obsolete Bindings ---
# Removed: google_cloud_run_v2_service_iam_member "subscriber_invoker" (allUsers)
# Removed: google_pubsub_topic_iam_member "listener_pubsub_publisher"
# Removed: google_service_account_iam_member "subscriber_sa_pubsub_token_creator"
# Removed: google_project_iam_member "pdf_summarizer_eventarc_receiver"
# Removed: google_project_iam_member "news_summarizer_eventarc_receiver"
# Removed: google_project_iam_member "gcs_pubsub_publisher"
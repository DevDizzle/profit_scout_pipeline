# infrastructure/terraform/iam.tf

data "google_project" "current" {}

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
resource "google_secret_manager_secret_iam_member" "sec_accessor_fetch_filings" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.sec_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.fetch_filings_sa.email}"
}
resource "google_secret_manager_secret_iam_member" "sec_accessor_download_pdf" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.sec_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.download_pdf_sa.email}"
}
resource "google_secret_manager_secret_iam_member" "sec_accessor_financial_extraction" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.sec_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.financial_extraction_sa.email}"
}
resource "google_secret_manager_secret_iam_member" "gemini_accessor_qualitative" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.gemini_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.qualitative_analysis_sa.email}"
}
resource "google_secret_manager_secret_iam_member" "gemini_accessor_headline_assessment" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.gemini_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.headline_assessment_sa.email}"
}
resource "google_secret_manager_secret_iam_member" "gemini_accessor_ratio_calculator" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.gemini_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ratio_calculator_sa.email}"
}
resource "google_secret_manager_secret_iam_member" "sec_accessor_headline_assessment" {
  project   = var.gcp_project_id
  secret_id = google_secret_manager_secret.sec_api_key_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.headline_assessment_sa.email}"
}
resource "google_storage_bucket_iam_member" "storage_admin_download_pdf" {
  bucket = google_storage_bucket.main_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.download_pdf_sa.email}"
}
resource "google_storage_bucket_iam_member" "storage_admin_qualitative" {
  bucket = google_storage_bucket.main_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.qualitative_analysis_sa.email}"
}
resource "google_storage_bucket_iam_member" "storage_admin_headline_assessment" {
  bucket = google_storage_bucket.main_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.headline_assessment_sa.email}"
}
resource "google_storage_bucket_iam_member" "storage_admin_financial_extraction" {
  bucket = google_storage_bucket.main_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.financial_extraction_sa.email}"
}
resource "google_storage_bucket_iam_member" "storage_viewer_ratio_calculator" {
  bucket = google_storage_bucket.main_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.ratio_calculator_sa.email}"
}
resource "google_bigquery_dataset_iam_member" "bq_editor_fetch_filings" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.fetch_filings_sa.email}"
}
resource "google_bigquery_dataset_iam_member" "bq_viewer_download_pdf" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.download_pdf_sa.email}"
}
resource "google_bigquery_dataset_iam_member" "bq_viewer_qualitative_analysis" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.qualitative_analysis_sa.email}"
}
resource "google_bigquery_dataset_iam_member" "bq_editor_headline_assessment" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.headline_assessment_sa.email}"
}
resource "google_bigquery_dataset_iam_member" "bq_editor_price_loader" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.price_loader_sa.email}"
}
resource "google_bigquery_dataset_iam_member" "bq_editor_ratio_calculator" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.ratio_calculator_sa.email}"
}
resource "google_bigquery_dataset_iam_member" "bq_viewer_financial_extraction" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.financial_extraction_sa.email}"
}
resource "google_project_iam_member" "bq_job_user" {
  for_each = {
    fetch_filings        = google_service_account.fetch_filings_sa.email
    download_pdf         = google_service_account.download_pdf_sa.email
    qualitative_analysis = google_service_account.qualitative_analysis_sa.email
    headline_assessment  = google_service_account.headline_assessment_sa.email
    price_loader         = google_service_account.price_loader_sa.email
    ratio_calculator     = google_service_account.ratio_calculator_sa.email
    financial_extraction = google_service_account.financial_extraction_sa.email
  }
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${each.value}"
}
resource "google_project_iam_member" "artifact_registry_reader" {
  for_each = {
    fetch_filings        = google_service_account.fetch_filings_sa.email
    download_pdf         = google_service_account.download_pdf_sa.email
    qualitative_analysis = google_service_account.qualitative_analysis_sa.email
    headline_assessment  = google_service_account.headline_assessment_sa.email
    price_loader         = google_service_account.price_loader_sa.email
    ratio_calculator     = google_service_account.ratio_calculator_sa.email
    financial_extraction = google_service_account.financial_extraction_sa.email
  }
  project = var.gcp_project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${each.value}"
}
resource "google_project_iam_member" "log_writer" {
  for_each = {
    fetch_filings        = google_service_account.fetch_filings_sa.email
    download_pdf         = google_service_account.download_pdf_sa.email
    qualitative_analysis = google_service_account.qualitative_analysis_sa.email
    headline_assessment  = google_service_account.headline_assessment_sa.email
    price_loader         = google_service_account.price_loader_sa.email
    ratio_calculator     = google_service_account.ratio_calculator_sa.email
    financial_extraction = google_service_account.financial_extraction_sa.email
    workflow             = google_service_account.workflow_sa.email
  }
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${each.value}"
}
resource "google_project_iam_member" "bq_readsession_ratio_calculator" {
  project = var.gcp_project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.ratio_calculator_sa.email}"
}
resource "google_project_iam_member" "ai_user_headline_assessment" {
  project = var.gcp_project_id
  role    = "roles/aiplatform.admin"
  member  = "serviceAccount:${google_service_account.headline_assessment_sa.email}"
}
resource "google_project_iam_member" "ai_user_ratio_calculator" {
  project = var.gcp_project_id
  role    = "roles/aiplatform.admin"
  member  = "serviceAccount:${google_service_account.ratio_calculator_sa.email}"
}
locals {
  workflow_target_job_sas = {
    fetch_filings        = google_service_account.fetch_filings_sa
    download_pdf         = google_service_account.download_pdf_sa
    qualitative_analysis = google_service_account.qualitative_analysis_sa
    headline_assessment  = google_service_account.headline_assessment_sa
    price_loader         = google_service_account.price_loader_sa
    ratio_calculator     = google_service_account.ratio_calculator_sa
    financial_extraction = google_service_account.financial_extraction_sa
  }
}
resource "google_service_account_iam_member" "workflow_act_as_job_sas" {
  for_each           = local.workflow_target_job_sas
  service_account_id = each.value.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.workflow_sa.email}"
}
resource "google_project_iam_member" "scheduler_act_as_self" {
  project = var.gcp_project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${var.scheduler_sa_email}"
}
resource "google_service_account_iam_member" "deployer_sa_act_as_scheduler" {
  service_account_id = "projects/${var.gcp_project_id}/serviceAccounts/${var.scheduler_sa_email}"
  role               = "roles/iam.serviceAccountUser"
  member     = "serviceAccount:${google_service_account.terraform_deployer_sa.email}"
}
resource "google_project_iam_member" "workflow_run_invoker" {
  project = var.gcp_project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
}
resource "google_project_iam_member" "scheduler_workflow_invoker" {
  project = var.gcp_project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${var.scheduler_sa_email}"
}
resource "google_project_iam_member" "scheduler_run_job_invoker" {
  project = var.gcp_project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${var.scheduler_sa_email}"
}
resource "google_project_iam_member" "bq_readsession_headline_assessment" {
  project = var.gcp_project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.headline_assessment_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "bq_editor_financial_extraction" {
  project    = var.gcp_project_id
  dataset_id = google_bigquery_dataset.profit_scout_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.financial_extraction_sa.email}"
}

# ----------------------------------------------------
# Allow Workflow SA to invoke each Cloud Run Job
# ----------------------------------------------------

locals {
  workflow_invocable_jobs = {
    fetch_filings        = google_cloud_run_v2_job.fetch_filings_job.name
    download_pdf         = google_cloud_run_v2_job.download_pdf_job.name
    financial_extraction = google_cloud_run_v2_job.financial_extraction_job.name
    headline_assessment  = google_cloud_run_v2_job.headline_assessment_job.name
    qualitative_analysis = google_cloud_run_v2_job.qualitative_analysis_job.name
  }
}

resource "google_cloud_run_v2_job_iam_member" "workflow_can_invoke_jobs" {
  for_each = local.workflow_invocable_jobs

  project  = var.gcp_project_id
  location = var.gcp_region
  name     = each.value
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.workflow_sa.email}"
}

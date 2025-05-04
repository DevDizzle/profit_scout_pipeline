# infrastructure/terraform/main.tf

# --- Enable GCP APIs ---
resource "google_project_service" "required_apis" {
  project = var.gcp_project_id
  for_each = toset([
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "cloudscheduler.googleapis.com",
    "aiplatform.googleapis.com",
    "compute.googleapis.com",
    "workflows.googleapis.com",
    "workflowexecutions.googleapis.com",
    "discoveryengine.googleapis.com"
  ])
  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
}

# --- Artifact Registry Repository ---
resource "google_artifact_registry_repository" "docker_repo" {
  project       = var.gcp_project_id
  location      = var.gcp_region
  repository_id = var.artifact_registry_repo_name
  format        = "DOCKER"
  depends_on = [
    google_project_service.required_apis["artifactregistry.googleapis.com"],
  ]
}

# --- Google Cloud Storage Bucket ---
resource "google_storage_bucket" "main_bucket" {
  project                     = var.gcp_project_id
  name                        = var.gcs_bucket_name
  location                    = var.gcp_location
  force_destroy               = false
  uniform_bucket_level_access = true
  depends_on = [google_project_service.required_apis["storage.googleapis.com"]]
}

# --- BigQuery Dataset ---
resource "google_bigquery_dataset" "profit_scout_dataset" {
  project     = var.gcp_project_id
  dataset_id  = var.bq_dataset_id
  location    = var.gcp_location
  depends_on = [google_project_service.required_apis["bigquery.googleapis.com"]]
}

# --- Secret Manager Secrets ---
resource "google_secret_manager_secret" "sec_api_key_secret" {
  project   = var.gcp_project_id
  secret_id = var.sec_api_secret_name
  replication {
    auto {}
  }
  depends_on = [google_project_service.required_apis["secretmanager.googleapis.com"]]
}

resource "google_secret_manager_secret" "gemini_api_key_secret" {
  project   = var.gcp_project_id
  secret_id = var.gemini_api_secret_name
  replication {
    auto {}
  }
  depends_on = [google_project_service.required_apis["secretmanager.googleapis.com"]]
}
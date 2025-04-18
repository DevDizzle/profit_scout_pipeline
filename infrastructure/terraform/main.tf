# infrastructure/terraform/main.tf

# --- Enable Necessary GCP APIs ---
resource "google_project_service" "required_apis" {
  project = var.gcp_project_id

  for_each = toset([
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "eventarc.googleapis.com",
    "cloudscheduler.googleapis.com",
    "iam.googleapis.com",
    "aiplatform.googleapis.com",
    "compute.googleapis.com"
  ])

  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
} # End of resource "google_project_service"

# --- Artifact Registry Repository ---
resource "google_artifact_registry_repository" "docker_repo" {
  provider      = google # Or google-beta if specific features needed
  project       = var.gcp_project_id
  location      = var.gcp_region
  repository_id = var.artifact_registry_repo_name
  description   = "Docker repository for Profit Scout pipeline services"
  format        = "DOCKER"

  depends_on = [
    google_project_service.required_apis["artifactregistry.googleapis.com"],
    google_project_service.required_apis["cloudbuild.googleapis.com"]
  ]
} # End of resource "google_artifact_registry_repository"

# --- Google Cloud Storage Bucket ---
resource "google_storage_bucket" "main_bucket" {
  project                     = var.gcp_project_id
  name                        = var.gcs_bucket_name
  location                    = "US-CENTRAL1" # Corrected Location
  force_destroy               = false
  uniform_bucket_level_access = true

  lifecycle_rule {
    action { type = "AbortIncompleteMultipartUpload" }
    condition { age = 1 }
  }
  # versioning { enabled = true } # Optional

  depends_on = [ google_project_service.required_apis["storage.googleapis.com"] ]

  # NOTE: Run 'terraform import google_storage_bucket.main_bucket profit-scout'
} # End of resource "google_storage_bucket"

# --- BigQuery Dataset ---
resource "google_bigquery_dataset" "profit_scout_dataset" {
  project     = var.gcp_project_id
  dataset_id  = var.bq_dataset_id
  location    = "US"              # Verified Location
  description = "Dataset for Profit Scout pipeline data"

  depends_on = [ google_project_service.required_apis["bigquery.googleapis.com"] ]

  # NOTE: Run 'terraform import google_bigquery_dataset.profit_scout_dataset profit-scout-456416/profit_scout'
} # End of resource "google_bigquery_dataset"

# --- Pub/Sub Topic ---
resource "google_pubsub_topic" "filing_notifications" {
  project = var.gcp_project_id
  name    = var.pubsub_topic_id

  depends_on = [ google_project_service.required_apis["pubsub.googleapis.com"] ]

  # NOTE: Run 'terraform import google_pubsub_topic.filing_notifications projects/profit-scout-456416/topics/sec-filing-notification'
} # End of resource "google_pubsub_topic"

# --- Secret Manager Secrets (Definitions only) ---
resource "google_secret_manager_secret" "sec_api_key_secret" {
  project   = var.gcp_project_id
  secret_id = var.sec_api_secret_name # Should be "sec-api-key"

  replication {
    auto {} # Correct syntax for automatic replication
  }

  depends_on = [ google_project_service.required_apis["secretmanager.googleapis.com"] ]

  # NOTE: Run 'terraform import google_secret_manager_secret.sec_api_key_secret projects/profit-scout-456416/secrets/sec-api-key'
} # End of resource "google_secret_manager_secret" sec_api_key_secret

resource "google_secret_manager_secret" "gemini_api_key_secret" {
  project   = var.gcp_project_id
  secret_id = var.gemini_api_secret_name 

  replication {
    auto {} 
  }

  depends_on = [ google_project_service.required_apis["secretmanager.googleapis.com"] ]

  # NOTE: Run 'terraform import google_secret_manager_secret.gemini_api_key_secret projects/profit-scout-456416/secrets/gemini-api-key'
} # End of resource "google_secret_manager_secret" gemini_api_key_secret
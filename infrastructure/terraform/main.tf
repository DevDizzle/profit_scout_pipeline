# infrastructure/terraform/main.tf

# --- Enable GCP APIs ---
resource "google_project_service" "required_apis" {
  project = var.gcp_project_id

  # Ensure all necessary APIs are included
  for_each = toset([
    # Core
    "run.googleapis.com",                 # Cloud Run API (Services & Jobs)
    "cloudbuild.googleapis.com",          # Cloud Build API
    "artifactregistry.googleapis.com",    # Artifact Registry API
    "secretmanager.googleapis.com",       # Secret Manager API
    "bigquery.googleapis.com",            # BigQuery API
    "storage.googleapis.com",             # Cloud Storage API
    "iam.googleapis.com",                 # IAM API
    "cloudscheduler.googleapis.com",      # Cloud Scheduler API
    "aiplatform.googleapis.com",          # Vertex AI API (for headline_assessment)
    "compute.googleapis.com",             # Often needed implicitly
    # NEW: Workflow APIs
    "workflows.googleapis.com",
    "workflowexecutions.googleapis.com"
    # REMOVED: "pubsub.googleapis.com" (unless needed elsewhere)
    # REMOVED: "eventarc.googleapis.com"
  ])

  service                    = each.key
  disable_dependent_services = false # Keep dependent services enabled
  disable_on_destroy         = false # Keep APIs enabled even if Terraform destroys resources using them
}

# --- Artifact Registry Repository ---
resource "google_artifact_registry_repository" "docker_repo" {
  project       = var.gcp_project_id
  location      = var.gcp_region # Use regional repository
  repository_id = var.artifact_registry_repo_name
  description   = "Docker repository for Profit Scout pipeline jobs"
  format        = "DOCKER"

  # Ensure APIs are enabled before creating repo
  depends_on = [
    google_project_service.required_apis["artifactregistry.googleapis.com"],
  ]
}

# --- Google Cloud Storage Bucket ---
resource "google_storage_bucket" "main_bucket" {
  project                     = var.gcp_project_id
  name                        = var.gcs_bucket_name
  location                    = var.gcp_location # Use location variable (e.g., "US")
  force_destroy               = false # Set to true only for non-production cleanup
  uniform_bucket_level_access = true

  # Optional: Add lifecycle rules, versioning, etc. as needed
  # lifecycle_rule {
  #   action { type = "Delete" }
  #   condition { age = 30 } # Example: delete objects older than 30 days
  # }

  depends_on = [google_project_service.required_apis["storage.googleapis.com"]]
}

# --- BigQuery Dataset ---
resource "google_bigquery_dataset" "profit_scout_dataset" {
  project     = var.gcp_project_id
  dataset_id  = var.bq_dataset_id
  location    = var.gcp_location
  description = "Dataset for Profit Scout pipeline data"

  depends_on = [google_project_service.required_apis["bigquery.googleapis.com"]]
}

# --- REMOVED: Pub/Sub Topic ---
# The google_pubsub_topic.filing_notifications resource is removed
# as the workflow orchestration replaces the Pub/Sub notification mechanism.

# --- Secret Manager Secrets ---
# Define the secrets (versions are managed outside Terraform or assumed 'latest')
resource "google_secret_manager_secret" "sec_api_key_secret" {
  project   = var.gcp_project_id
  secret_id = var.sec_api_secret_name

  replication {
    auto {}
  }
  # Optional: Add labels, expiration, etc.

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
# infrastructure/terraform/iam.tf

# --- Data Source to get Project Number ---
data "google_project" "current" {}

# --- Service Accounts ---
# Definitions remain the same
resource "google_service_account" "listener_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-listener-sa"
  display_name = "Profit Scout Listener Service Account"
}

resource "google_service_account" "subscriber_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-subscriber-sa"
  display_name = "Profit Scout Subscriber Service Account"
}

resource "google_service_account" "price_loader_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-price-loader-sa"
  display_name = "Profit Scout Price Loader Service Account"
}

resource "google_service_account" "ratio_calculator_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-ratio-calc-sa"
  display_name = "Profit Scout Ratio Calculator Service Account"
}

resource "google_service_account" "pdf_summarizer_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-pdf-sum-sa"
  display_name = "Profit Scout PDF Summarizer Service Account"
}

resource "google_service_account" "news_summarizer_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-news-sum-sa"
  display_name = "Profit Scout News Summarizer Service Account"
}

# --- IAM Bindings ---
# Removing locals block that depends on SA members and defining bindings individually

# --- Resource Specific Bindings ---

# Pub/Sub Topic IAM (Listener needs publish permission)
resource "google_pubsub_topic_iam_member" "listener_pubsub_publisher" {
  project = var.gcp_project_id
  topic   = var.pubsub_topic_id
  role    = "roles/pubsub.publisher"
  member  = google_service_account.listener_sa.member
}

# Allow Pub/Sub Service Agent to create tokens for Subscriber SA (for push subscription OIDC)
resource "google_service_account_iam_member" "subscriber_sa_pubsub_token_creator" {
  service_account_id = google_service_account.subscriber_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# BigQuery Dataset IAM - Editors
resource "google_bigquery_dataset_iam_member" "bq_editor_subscriber" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.subscriber_sa.member
}
resource "google_bigquery_dataset_iam_member" "bq_editor_price_loader" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.price_loader_sa.member
}
resource "google_bigquery_dataset_iam_member" "bq_editor_ratio_calculator" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.ratio_calculator_sa.member
}

# BigQuery Dataset IAM - Viewers
resource "google_bigquery_dataset_iam_member" "bq_viewer_news_summarizer" {
  project    = var.gcp_project_id
  dataset_id = var.bq_dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.news_summarizer_sa.member
}

# Secret Manager Secret IAM - SEC API Key Accessors
resource "google_secret_manager_secret_iam_member" "sec_accessor_listener" {
  project   = var.gcp_project_id
  secret_id = var.sec_api_secret_name
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.listener_sa.member
}
resource "google_secret_manager_secret_iam_member" "sec_accessor_subscriber" {
  project   = var.gcp_project_id
  secret_id = var.sec_api_secret_name
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.subscriber_sa.member
}

# Secret Manager Secret IAM - Gemini API Key Accessors
resource "google_secret_manager_secret_iam_member" "gemini_accessor_ratio_calculator" {
  project   = var.gcp_project_id
  secret_id = var.gemini_api_secret_name
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.ratio_calculator_sa.member
}
resource "google_secret_manager_secret_iam_member" "gemini_accessor_pdf_summarizer" {
  project   = var.gcp_project_id
  secret_id = var.gemini_api_secret_name
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.pdf_summarizer_sa.member
}

# GCS Bucket IAM - Object Admins
resource "google_storage_bucket_iam_member" "storage_admin_subscriber" {
  bucket = var.gcs_bucket_name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.subscriber_sa.member
}
resource "google_storage_bucket_iam_member" "storage_admin_pdf_summarizer" {
  bucket = var.gcs_bucket_name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.pdf_summarizer_sa.member
}
resource "google_storage_bucket_iam_member" "storage_admin_news_summarizer" {
  bucket = var.gcs_bucket_name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.news_summarizer_sa.member
}

# Cloud Run Job IAM (Allow Cloud Scheduler SA to invoke Jobs)
resource "google_cloud_run_v2_job_iam_member" "price_loader_scheduler_invoker" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.price_loader_job_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.scheduler_sa_email}"
}
resource "google_cloud_run_v2_job_iam_member" "ratio_calculator_scheduler_invoker" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.ratio_calculator_job_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.scheduler_sa_email}"
}


# --- Project Level Bindings ---

# BigQuery Job User (needed for querying/loading)
resource "google_project_iam_member" "bq_job_user_subscriber" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.subscriber_sa.member
}
resource "google_project_iam_member" "bq_job_user_price_loader" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.price_loader_sa.member
}
resource "google_project_iam_member" "bq_job_user_ratio_calculator" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.ratio_calculator_sa.member
}
resource "google_project_iam_member" "bq_job_user_news_summarizer" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.news_summarizer_sa.member
}

# Log Writer (for all services)
resource "google_project_iam_member" "log_writer_listener" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.listener_sa.member
}
resource "google_project_iam_member" "log_writer_subscriber" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.subscriber_sa.member
}
resource "google_project_iam_member" "log_writer_price_loader" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.price_loader_sa.member
}
resource "google_project_iam_member" "log_writer_ratio_calculator" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.ratio_calculator_sa.member
}
resource "google_project_iam_member" "log_writer_pdf_summarizer" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.pdf_summarizer_sa.member
}
resource "google_project_iam_member" "log_writer_news_summarizer" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.news_summarizer_sa.member
}

# AI Platform User (for services using Vertex/Gemini)
resource "google_project_iam_member" "ai_user_ratio_calculator" {
  project = var.gcp_project_id
  role    = "roles/aiplatform.user"
  member  = google_service_account.ratio_calculator_sa.member
}
resource "google_project_iam_member" "ai_user_pdf_summarizer" {
  project = var.gcp_project_id
  role    = "roles/aiplatform.user"
  member  = google_service_account.pdf_summarizer_sa.member
}
resource "google_project_iam_member" "ai_user_news_summarizer" {
  project = var.gcp_project_id
  role    = "roles/aiplatform.user"
  member  = google_service_account.news_summarizer_sa.member
}

# Eventarc Event Receiver (for services triggered by Eventarc)
resource "google_project_iam_member" "pdf_summarizer_eventarc_receiver" {
  project = var.gcp_project_id
  role    = "roles/eventarc.eventReceiver"
  member  = google_service_account.pdf_summarizer_sa.member
}
resource "google_project_iam_member" "news_summarizer_eventarc_receiver" {
  project = var.gcp_project_id
  role    = "roles/eventarc.eventReceiver"
  member  = google_service_account.news_summarizer_sa.member
}
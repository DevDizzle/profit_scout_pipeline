# infrastructure/terraform/scheduler.tf

# --- Cloud Scheduler Job for Price Loader ---
resource "google_cloud_scheduler_job" "trigger_price_loader" {
  project  = var.gcp_project_id
  region   = var.gcp_region
  name     = "trigger-profit-scout-price-loader" # Name for the scheduler job
  schedule = var.price_loader_schedule         # e.g., "5 16 * * *" (4:05 PM daily)
  time_zone = var.scheduler_timezone         # e.g., "America/New_York"

  attempt_deadline = "3500s" # Slightly less than job timeout (default 60min)

  # Target: Execute the Price Loader Cloud Run Job
  http_target {
    # URI for executing a Cloud Run v2 Job
    uri = "https://${var.gcp_region}-run.googleapis.com/apis/run.googleapis.com/v1/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${google_cloud_run_v2_job.price_loader_job.name}:run"
    http_method = "POST"
    # Use OIDC token authentication with the Scheduler's service account
    oidc_token {
      service_account_email = var.scheduler_sa_email
      # Audience should match the invocation URI for Cloud Run v2 Jobs
      audience = "https://${var.gcp_region}-run.googleapis.com/apis/run.googleapis.com/v1/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${google_cloud_run_v2_job.price_loader_job.name}:run"
    }
  }

  # Ensure the Cloud Run Job and its invoker permission exist first
  depends_on = [
    google_cloud_run_v2_job.price_loader_job,
    google_cloud_run_v2_job_iam_member.price_loader_scheduler_invoker
  ]
}


# --- Cloud Scheduler Job for Ratio Calculator ---
resource "google_cloud_scheduler_job" "trigger_ratio_calculator" {
  project  = var.gcp_project_id
  region   = var.gcp_region
  name     = "trigger-profit-scout-ratio-calculator" # Name for the scheduler job
  schedule = var.ratio_calculator_schedule         # e.g., "35 16 * * *" (4:35 PM daily)
  time_zone = var.scheduler_timezone         # e.g., "America/New_York"

  attempt_deadline = "3500s" # Slightly less than job timeout

  # Target: Execute the Ratio Calculator Cloud Run Job
  http_target {
    # URI for executing a Cloud Run v2 Job
    uri = "https://${var.gcp_region}-run.googleapis.com/apis/run.googleapis.com/v1/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${google_cloud_run_v2_job.ratio_calculator_job.name}:run"
    http_method = "POST"
    # Use OIDC token authentication with the Scheduler's service account
    oidc_token {
      service_account_email = var.scheduler_sa_email
      # Audience should match the invocation URI
      audience = "https://${var.gcp_region}-run.googleapis.com/apis/run.googleapis.com/v1/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${google_cloud_run_v2_job.ratio_calculator_job.name}:run"
    }
  }

   # Ensure the Cloud Run Job and its invoker permission exist first
  depends_on = [
    google_cloud_run_v2_job.ratio_calculator_job,
    google_cloud_run_v2_job_iam_member.ratio_calculator_scheduler_invoker
  ]
}
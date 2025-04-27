# infrastructure/terraform/scheduling.tf

# --- Trigger for the Main Filing Workflow ---
resource "google_cloud_scheduler_job" "trigger_main_workflow" {
  project     = var.gcp_project_id
  region      = var.workflow_region # Use workflow region for scheduler job location
  name        = "trigger-main-filing-workflow"
  description = "Triggers the main filing processing workflow daily (M-F)"
  schedule    = var.main_workflow_schedule # e.g., "0 6 * * MON-FRI"
  time_zone   = var.scheduler_timezone   # e.g., "America/New_York"

  http_target {
    # Use the Workflow Execution API endpoint
    # NOTE: Ensure the google_workflows_workflow resource is named "main_workflow"
    uri = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.main_workflow.id}/executions"

    http_method = "POST"
    # No body needed for basic execution trigger, can add arguments in body if needed later
    # body        = base64encode(jsonencode({ argument = jsonencode({ key = "value" }) }))

    oidc_token {
      # Use the default Cloud Scheduler service account
      service_account_email = var.scheduler_sa_email
      # Audience should be the API endpoint being called
      audience = "https://workflowexecutions.googleapis.com/"
      # Alternate audience (sometimes needed): google_workflows_workflow.main_workflow.id
    }
  }

  attempt_deadline = "320s" # Allow some time for the request to complete

  retry_config {
    retry_count = 1
  }

  depends_on = [
    # Ensure workflow and necessary IAM bindings exist first
    google_workflows_workflow.main_workflow,
    google_project_iam_member.scheduler_workflow_invoker,
    google_project_iam_member.scheduler_act_as_self,
  ]
}


# --- Trigger for the Price Loader Job ---
resource "google_cloud_scheduler_job" "trigger_price_loader" {
  project     = var.gcp_project_id
  region      = var.gcp_region # Location for the scheduler job itself
  name        = "trigger-price-loader-job"
  description = "Triggers the price loader Cloud Run job daily (M-F)"
  schedule    = var.price_loader_schedule # e.g., "0 16 * * MON-FRI"
  time_zone   = var.scheduler_timezone

  http_target {
    # Use the Cloud Run v2 Job :run endpoint URI format
    uri = "https://${var.gcp_region}-run.googleapis.com/apis/run.googleapis.com/v1/${google_cloud_run_v2_job.price_loader_job.id}:run"

    http_method = "POST"
    # No body needed to just run the job

    oidc_token {
      # Use the default Cloud Scheduler service account
      service_account_email = var.scheduler_sa_email
      # Audience should be the URL being called (the :run endpoint)
      audience = "https://${var.gcp_region}-run.googleapis.com/apis/run.googleapis.com/v1/${google_cloud_run_v2_job.price_loader_job.id}:run"
    }
  }

  attempt_deadline = "320s"

  retry_config {
    retry_count = 1
  }

  depends_on = [
    # Ensure job and necessary IAM bindings exist first
    google_cloud_run_v2_job.price_loader_job,
    google_project_iam_member.scheduler_run_job_invoker,
    google_project_iam_member.scheduler_act_as_self,
    # Optional: depends_on fine-grained invoker role if used in iam.tf
    # google_cloud_run_v2_job_iam_member.price_loader_scheduler_invoker,
  ]
}


# --- Trigger for the Ratio Calculator Job ---
resource "google_cloud_scheduler_job" "trigger_ratio_calculator" {
  project     = var.gcp_project_id
  region      = var.gcp_region # Location for the scheduler job itself
  name        = "trigger-ratio-calculator-job"
  description = "Triggers the ratio calculator Cloud Run job daily (M-F)"
  schedule    = var.ratio_calculator_schedule # e.g., "30 16 * * MON-FRI"
  time_zone   = var.scheduler_timezone

  http_target {
    # Use the Cloud Run v2 Job :run endpoint URI format
    uri = "https://${var.gcp_region}-run.googleapis.com/apis/run.googleapis.com/v1/${google_cloud_run_v2_job.ratio_calculator_job.id}:run"

    http_method = "POST"
    # No body needed to just run the job

    oidc_token {
      # Use the default Cloud Scheduler service account
      service_account_email = var.scheduler_sa_email
      # Audience should be the URL being called (the :run endpoint)
      audience = "https://${var.gcp_region}-run.googleapis.com/apis/run.googleapis.com/v1/${google_cloud_run_v2_job.ratio_calculator_job.id}:run"
    }
  }

  attempt_deadline = "320s"

  retry_config {
    retry_count = 1
  }

  depends_on = [
    # Ensure job and necessary IAM bindings exist first
    google_cloud_run_v2_job.ratio_calculator_job,
    google_project_iam_member.scheduler_run_job_invoker,
    google_project_iam_member.scheduler_act_as_self,
    # Optional: depends_on fine-grained invoker role if used in iam.tf
    # google_cloud_run_v2_job_iam_member.ratio_calculator_scheduler_invoker,
  ]
}
# infrastructure/terraform/eventarc.tf

# --- Eventarc Trigger for PDF Summarizer ---
# Triggers when a new PDF file is finalized in the specified GCS folder

resource "google_eventarc_trigger" "trigger_pdf_summarizer" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = "trigger-gcs-pdf-summarizer" # Name for this trigger resource

  # --- CORRECTED: Define ALL matching criteria here ---
  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized" # Event for object creation/overwrite
  }
  matching_criteria {
    attribute = "bucket"
    value     = var.gcs_bucket_name # Filter for events in our bucket
  }
  matching_criteria {
    attribute = "name"
    operator  = "starts-with"        # Filter for files in the PDF folder
    value     = var.gcs_pdf_folder   # Ensure this variable includes trailing '/' if needed
  }
  # Optional: Add suffix filter if necessary (service code also checks)
  # matching_criteria {
  #   attribute = "name"
  #   operator  = "ends-with"
  #   value     = ".pdf"
  # }
  # --- END CORRECTION ---

  # Define the destination (the Cloud Run service)
  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.pdf_summarizer_service.name # Link to the service resource
      region  = var.gcp_region
      # path = "/" # Optional: Specify a sub-path if needed
    }
  }

  # Service account for the trigger to invoke the target service
  service_account = google_service_account.pdf_summarizer_sa.email

  # Ensure required APIs are enabled and service exists
  depends_on = [
    google_project_service.required_apis["eventarc.googleapis.com"],
    google_project_service.required_apis["run.googleapis.com"],
    google_cloud_run_v2_service.pdf_summarizer_service
  ]
}


# --- Eventarc Trigger for News Summarizer ---
# Also triggers when a new PDF file is finalized in the specified GCS folder

resource "google_eventarc_trigger" "trigger_news_summarizer" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = "trigger-gcs-news-summarizer"

  # --- CORRECTED: Define ALL matching criteria here ---
  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = var.gcs_bucket_name
  }
  matching_criteria {
    attribute = "name"
    operator  = "starts-with"
    value     = var.gcs_pdf_folder
  }
  # --- END CORRECTION ---

  # Define the destination (the news_summarizer Cloud Run service)
  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.news_summarizer_service.name # Link to the service resource
      region  = var.gcp_region
    }
  }

  # Use the target service's own SA
  service_account = google_service_account.news_summarizer_sa.email

  depends_on = [
    google_project_service.required_apis["eventarc.googleapis.com"],
    google_project_service.required_apis["run.googleapis.com"],
    google_cloud_run_v2_service.news_summarizer_service
  ]
}
# --- Fetch Filings Job (Runs at 12:00 PM and 5:00 PM, Mon–Fri) ---
resource "google_cloud_scheduler_job" "trigger_fetch_filings" {
  project          = var.gcp_project_id
  region           = var.gcp_region
  name             = "profit-scout-fetch-filings"
  schedule         = "0 12,17 * * MON-FRI"  # 12:00 PM, 5:00 PM
  time_zone        = var.scheduler_timezone
  attempt_deadline = "1800s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${var.fetch_filings_job_name}:execute"
    oidc_token {
      service_account_email = var.scheduler_sa_email
    }
  }

  depends_on = [
    google_cloud_run_v2_job.fetch_filings_job,
    google_project_iam_member.scheduler_run_job_invoker,
    google_project_iam_member.scheduler_act_as_self,
  ]
}

# --- Download PDF (Runs at 12:15 PM and 5:15 PM) ---
resource "google_cloud_scheduler_job" "trigger_download_pdf" {
  project          = var.gcp_project_id
  region           = var.gcp_region
  name             = "profit-scout-download-pdf"
  schedule         = "15 12,17 * * MON-FRI"  # 12:15 PM, 5:15 PM
  time_zone        = var.scheduler_timezone
  attempt_deadline = "1800s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${var.download_pdf_job_name}:execute"
    oidc_token {
      service_account_email = var.scheduler_sa_email
    }
  }

  depends_on = [google_cloud_run_v2_job.download_pdf_job]
}

# --- Financial Extraction (Runs at 12:30 PM and 5:30 PM) ---
resource "google_cloud_scheduler_job" "trigger_financial_extraction" {
  project          = var.gcp_project_id
  region           = var.gcp_region
  name             = "profit-scout-financial-extraction"
  schedule         = "30 12,17 * * MON-FRI"  # 12:30 PM, 5:30 PM
  time_zone        = var.scheduler_timezone
  attempt_deadline = "1800s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${var.financial_extraction_job_name}:execute"
    oidc_token {
      service_account_email = var.scheduler_sa_email
    }
  }

  depends_on = [google_cloud_run_v2_job.financial_extraction_job]
}

# --- Headline Assessment (Runs at 12:30 PM and 5:30 PM) ---
resource "google_cloud_scheduler_job" "trigger_headline_assessment" {
  project          = var.gcp_project_id
  region           = var.gcp_region
  name             = "profit-scout-headline-assessment"
  schedule         = "30 12,17 * * MON-FRI"  # 12:30 PM, 5:30 PM
  time_zone        = var.scheduler_timezone
  attempt_deadline = "1800s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${var.headline_assessment_job_name}:execute"
    oidc_token {
      service_account_email = var.scheduler_sa_email
    }
  }

  depends_on = [google_cloud_run_v2_job.headline_assessment_job]
}

# --- Qualitative Analysis (Runs at 12:45 PM and 5:45 PM) ---
resource "google_cloud_scheduler_job" "trigger_qualitative_analysis" {
  project          = var.gcp_project_id
  region           = var.gcp_region
  name             = "profit-scout-qualitative-analysis"
  schedule         = "45 12,17 * * MON-FRI"  # 12:45 PM, 5:45 PM
  time_zone        = var.scheduler_timezone
  attempt_deadline = "1800s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${var.qualitative_analysis_job_name}:execute"
    oidc_token {
      service_account_email = var.scheduler_sa_email
    }
  }

  depends_on = [google_cloud_run_v2_job.qualitative_analysis_job]
}

# --- Price Loader (Runs at 4:00 PM) ---
resource "google_cloud_scheduler_job" "trigger_price_loader" {
  project          = var.gcp_project_id
  region           = var.gcp_region
  name             = "profit-scout-price-loader"
  schedule         = "0 16 * * MON-FRI"  # 4:00 PM
  time_zone        = var.scheduler_timezone
  attempt_deadline = "1800s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${var.price_loader_job_name}:execute"
    oidc_token {
      service_account_email = var.scheduler_sa_email
    }
  }

  depends_on = [google_cloud_run_v2_job.price_loader_job]
}

# --- Ratio Calculator (Runs at 4:15 PM) ---
resource "google_cloud_scheduler_job" "trigger_ratio_calculator" {
  project          = var.gcp_project_id
  region           = var.gcp_region
  name             = "profit-scout-ratio-calculator"
  schedule         = "15 16 * * MON-FRI"  # 4:15 PM
  time_zone        = var.scheduler_timezone
  attempt_deadline = "1800s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.gcp_project_id}/locations/${var.gcp_region}/jobs/${var.ratio_calculator_job_name}:execute"
    oidc_token {
      service_account_email = var.scheduler_sa_email
    }
  }

  depends_on = [google_cloud_run_v2_job.ratio_calculator_job]
}
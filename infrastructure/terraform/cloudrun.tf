# infrastructure/terraform/cloudrun.tf

# Common definitions
locals {
  # Construct the base image path - ensure region and repo name match cloudbuild.yaml and variables.tf
  artifact_registry_base = "${var.gcp_region}-docker.pkg.dev/${var.gcp_project_id}/${var.artifact_registry_repo_name}"

  # Define common environment variables needed by multiple services
  common_env_vars = [
    { name = "GCP_PROJECT_ID", value = var.gcp_project_id },
    { name = "BQ_DATASET_ID", value = var.bq_dataset_id },
    { name = "GCS_BUCKET_NAME", value = var.gcs_bucket_name },
  ]
}

# --- Cloud Run Service: Listener ---
resource "google_cloud_run_v2_service" "listener_service" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = "profit-scout-listener" # Service name

  template {
    service_account = google_service_account.listener_sa.email
    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }
    containers {
      image = "${local.artifact_registry_base}/listener:${var.docker_image_tag}"

      # --- CORRECTED Env Vars ---
      env {
        name  = "SEC_WEBSOCKET_URL"
        value = var.sec_websocket_url
      }
      env {
        name  = "PUB_SUB_TOPIC_ID"
        value = var.pubsub_topic_id
      }
      env {
        name  = "SEC_API_SECRET_ID"
        value = var.sec_api_secret_name
      }
      # --- END CORRECTION ---

      # Add common env vars using dynamic block (syntax was already correct here)
      dynamic "env" {
        for_each = local.common_env_vars
        content {
          name  = env.value.name
          value = env.value.value
        }
      }

      resources {
        limits = {
          cpu    = "1000m"
          memory = "512Mi"
        }
        cpu_idle = false
        startup_cpu_boost = true
      }
    }
  }
}


# --- Cloud Run Service: Subscriber ---
resource "google_cloud_run_v2_service" "subscriber_service" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = "profit-scout-subscriber"

  template {
    service_account = google_service_account.subscriber_sa.email
    containers {
      image = "${local.artifact_registry_base}/subscriber:${var.docker_image_tag}"
      ports { container_port = 8080 }

      # --- CORRECTED Env Vars ---
      env {
        name  = "BQ_METADATA_TABLE_ID" # Python code env var name
        value = var.bq_metadata_table_id # TF variable name
      }
      env {
        name  = "BQ_FINANCIALS_BS_TABLE_ID"
        value = var.bq_bs_table_id
      }
      env {
        name  = "BQ_FINANCIALS_IS_TABLE_ID"
        value = var.bq_is_table_id
      }
      env {
        name  = "BQ_FINANCIALS_CF_TABLE_ID"
        value = var.bq_cf_table_id
      }
      env {
        name  = "GCS_PDF_FOLDER"
        value = var.gcs_pdf_folder
      }
      env {
        name  = "SEC_API_SECRET_ID"
        value = var.sec_api_secret_name
      }
      env {
        name  = "SEC_API_SECRET_VERSION"
        value = "latest" # Hardcoded, could make variable
      }
      # --- END CORRECTION ---

      # Add common env vars
      dynamic "env" {
        for_each = local.common_env_vars
        content {
          name  = env.value.name
          value = env.value.value
        }
      }

      resources { limits = { cpu = "1000m", memory = "512Mi" } }
    }
  }
}


# --- Cloud Run Service: PDF Summarizer ---
resource "google_cloud_run_v2_service" "pdf_summarizer_service" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = "profit-scout-pdf-summarizer"

  template {
    service_account = google_service_account.pdf_summarizer_sa.email
    containers {
      image = "${local.artifact_registry_base}/pdf-summarizer:${var.docker_image_tag}"
      ports { container_port = 8080 }

      # --- CORRECTED Env Vars ---
      env {
        name  = "GEMINI_API_KEY_SECRET_ID"
        value = var.gemini_api_secret_name
      }
      env {
        name  = "GEMINI_API_KEY_SECRET_VERSION"
        value = "latest" # Or use var.gemini_api_secret_version if defined
      }
      env {
        name  = "GEMINI_MODEL_NAME"
        value = var.gemini_model_name
      }
      env {
        name  = "GCS_ANALYSIS_TXT_PREFIX"
        value = var.gcs_analysis_txt_prefix
      }
      env {
        name  = "GCS_PDF_PREFIX" # Renamed from GCS_PDF_FOLDER in python code? Check consistency. Assuming GCS_PDF_FOLDER is correct.
        value = var.gcs_pdf_folder
      }
      # --- END CORRECTION ---

      # Add common env vars
      dynamic "env" {
        for_each = local.common_env_vars
        content {
          name  = env.value.name
          value = env.value.value
        }
      }

      resources { limits = { cpu = "1000m", memory = "1Gi" } }
    }
  }
}


# --- Cloud Run Service: News Summarizer ---
resource "google_cloud_run_v2_service" "news_summarizer_service" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = "profit-scout-news-summarizer"

  template {
    service_account = google_service_account.news_summarizer_sa.email
    containers {
      image = "${local.artifact_registry_base}/news-summarizer:${var.docker_image_tag}"
      ports { container_port = 8080 }

      # --- CORRECTED Env Vars ---
      env {
        name  = "GCP_REGION" # Python code uses this name
        value = var.gcp_region
      }
      env {
        name  = "BQ_METADATA_TABLE_ID" # Python code uses this name
        value = var.bq_metadata_table_id
      }
      env {
        name  = "GCS_NEWS_SUMMARY_PREFIX"
        value = var.gcs_news_summary_prefix
      }
       env {
        name  = "GCS_PDF_PREFIX" # Python code uses this name
        value = var.gcs_pdf_folder
      }
      env {
        name  = "GEMINI_MODEL_NAME" # Python code uses this name
        value = var.gemini_model_name
      }
      # --- END CORRECTION ---

      # Add common env vars
      dynamic "env" {
        for_each = local.common_env_vars
        content {
          name  = env.value.name
          value = env.value.value
        }
      }

      resources { limits = { cpu = "1000m", memory = "1Gi" } }
    }
  }
}


# --- Cloud Run Job: Price Loader ---
resource "google_cloud_run_v2_job" "price_loader_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.price_loader_job_name

  template {
    task_count = 1
    template {
      service_account = google_service_account.price_loader_sa.email
      timeout         = "3600s"
      max_retries     = 2

      containers {
        image = "${local.artifact_registry_base}/price-loader:${var.docker_image_tag}"

        # --- CORRECTED Env Vars ---
        env {
          name  = "BQ_METADATA_TABLE_ID" # Python code uses this name
          value = var.bq_metadata_table_id
        }
        env {
          name  = "BQ_PRICES_TABLE_ID" # Python code uses this name
          value = var.bq_price_table_id
        }
        # --- END CORRECTION ---

        # Add common env vars
        dynamic "env" {
          for_each = local.common_env_vars
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        resources { limits = { cpu = "1000m", memory = "1Gi" } }
      }
    }
  }
  depends_on = [
    google_service_account.price_loader_sa,
    google_bigquery_table.price_data,
    google_bigquery_table.filing_metadata
  ]
}

# --- Cloud Run Job: Ratio Calculator ---
resource "google_cloud_run_v2_job" "ratio_calculator_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.ratio_calculator_job_name

  template {
    task_count = 1
    template {
      service_account = google_service_account.ratio_calculator_sa.email
      timeout         = "3600s"
      max_retries     = 2

      containers {
        image = "${local.artifact_registry_base}/ratio-calculator:${var.docker_image_tag}"

        # --- CORRECTED Env Vars ---
        env {
          name  = "BQ_METADATA_TABLE_ID" # Python code uses this name
          value = var.bq_metadata_table_id
        }
        env {
          name  = "BQ_FINANCIALS_BS_TABLE_ID"
          value = var.bq_bs_table_id
        }
        env {
          name  = "BQ_FINANCIALS_IS_TABLE_ID"
          value = var.bq_is_table_id
        }
        env {
          name  = "BQ_FINANCIALS_CF_TABLE_ID"
          value = var.bq_cf_table_id
        }
        env {
          name  = "BQ_PRICE_TABLE_ID"
          value = var.bq_price_table_id
        }
        env {
          name  = "BQ_RATIOS_TABLE_ID"
          value = var.bq_ratios_table_id
        }
        env {
          name  = "GEMINI_API_KEY_SECRET_ID"
          value = var.gemini_api_secret_name
        }
        env {
          name  = "GEMINI_API_KEY_SECRET_VERSION"
          value = "latest" # Or use var.gemini_api_secret_version
        }
        env {
          name  = "GEMINI_MODEL_NAME"
          value = var.gemini_model_name
        }
        env {
          name  = "MAX_WORKERS"
          value = var.max_workers_ratio_calc # Terraform handles type conversion
        }
        # --- END CORRECTION ---

        # Add common env vars
        dynamic "env" {
          for_each = local.common_env_vars
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        resources { limits = { cpu = "2000m", memory = "2Gi" } }
      }
    }
  }
  depends_on = [
    google_service_account.ratio_calculator_sa,
    google_bigquery_table.financial_ratios,
    google_bigquery_table.price_data,
    google_bigquery_table.balance_sheet,
    google_bigquery_table.income_statement,
    google_bigquery_table.cash_flow,
    google_bigquery_table.filing_metadata
  ]
}
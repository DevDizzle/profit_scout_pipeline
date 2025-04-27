# infrastructure/terraform/cloudrun_jobs.tf

# --- Locals ---
locals {
  # Base path for Docker images in Artifact Registry
  artifact_registry_base = "${var.gcp_region}-docker.pkg.dev/${var.gcp_project_id}/${var.artifact_registry_repo_name}"

  # Common environment variables for many jobs
  common_env_vars = {
    GCP_PROJECT_ID = var.gcp_project_id
    BQ_DATASET_ID  = var.bq_dataset_id
    # Note: Bucket name often needed, added specifically where required by script
    # GCS_BUCKET_NAME = var.gcs_bucket_name # Added per-job as needed
  }

  # Common job settings
  default_job_template = {
    # task_count = 1 # Default is 1, uncomment if changing
    template = {
      # Default values, can be overridden per job
      timeout         = var.default_job_timeout
      max_retries     = var.default_job_retries
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2" # Use Gen2 environment
      containers = [{
        # image, resources, env[] defined per job
        resources = {
          limits = {
            cpu    = var.default_job_cpu
            memory = var.default_job_memory
          }
          # startup_cpu_boost = true # Optional: boost CPU at startup
        }
      }]
    }
  }
}

# --- Job Definitions ---

resource "google_cloud_run_v2_job" "fetch_filings_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.fetch_filings_job_name

  template {
    template {
      service_account       = google_service_account.fetch_filings_sa.email
      timeout               = local.default_job_template.template.timeout
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = "${local.artifact_registry_base}/fetch-filings:${var.docker_image_tag}"
        
        env {
          name  = "GCP_PROJECT_ID"
          value = local.common_env_vars.GCP_PROJECT_ID
        }
        env {
          name  = "BQ_DATASET_ID"
          value = local.common_env_vars.BQ_DATASET_ID
        }
        env {
          name  = "BQ_METADATA_TABLE_ID"
          value = var.bq_metadata_table_id
        }
        env {
          name  = "SEC_API_SECRET_ID"
          value = var.sec_api_secret_name
        }
        env {
          name  = "SEC_API_SECRET_VERSION"
          value = var.api_secret_version
        }
        env {
          name  = "LOOKBACK_HOURS"
          value = tostring(var.fetch_filings_lookback_hours)
        }
        env {
          name  = "FILING_TYPES"
          value = var.fetch_filings_types
        }
        env {
          name  = "TICKERS_TO_QUERY"
          value = var.fetch_filings_tickers
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }

        resources {
          limits = local.default_job_template.template.containers[0].resources.limits
        }
      }
    }
  }

  depends_on = [google_service_account.fetch_filings_sa]
}



# --- Job 2: Download PDF ---
resource "google_cloud_run_v2_job" "download_pdf_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.download_pdf_job_name

  template {
    template {
      service_account       = google_service_account.download_pdf_sa.email
      timeout               = "600s"
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = "${local.artifact_registry_base}/download-pdf:${var.docker_image_tag}"

        env {
          name  = "GCP_PROJECT_ID"
          value = local.common_env_vars.GCP_PROJECT_ID
        }
        env {
          name  = "GCS_BUCKET_NAME"
          value = var.gcs_bucket_name
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
          value = var.api_secret_version
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
        env {
          name  = "INPUT_TICKER"
          value = "PLACEHOLDER_TICKER"
        }
        env {
          name  = "INPUT_ACCESSION_NUMBER"
          value = "PLACEHOLDER_ACCNO"
        }
        env {
          name  = "INPUT_FILING_URL"
          value = "PLACEHOLDER_URL"
        }

        resources {
          limits = local.default_job_template.template.containers[0].resources.limits
        }
      }
    }
  }

  depends_on = [google_service_account.download_pdf_sa]
}

# --- Job 3: Generate Qualitative Analysis ---
resource "google_cloud_run_v2_job" "qualitative_analysis_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.qualitative_analysis_job_name

  template {
    template {
      service_account       = google_service_account.qualitative_analysis_sa.email
      timeout               = "1800s"
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = "${local.artifact_registry_base}/generate-qualitative-analysis:${var.docker_image_tag}"

        resources {
          limits = {
            cpu    = "2000m"
            memory = "2Gi"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = local.common_env_vars.GCP_PROJECT_ID
        }
        env {
          name  = "GCS_BUCKET_NAME"
          value = var.gcs_bucket_name
        }
        env {
          name  = "GCS_ANALYSIS_TXT_PREFIX"
          value = var.gcs_analysis_txt_prefix
        }
        env {
          name  = "GCS_METADATA_CSV_PREFIX"
          value = var.gcs_analysis_metadata_csv_prefix
        }
        env {
          name  = "GEMINI_API_SECRET_ID"
          value = var.gemini_api_secret_name
        }
        env {
          name  = "GEMINI_API_KEY_SECRET_VERSION"
          value = var.api_secret_version
        }
        env {
          name  = "GEMINI_MODEL_NAME"
          value = var.gemini_model_name
        }
        env {
          name  = "GEMINI_TEMPERATURE"
          value = "0.2"
        }
        env {
          name  = "GEMINI_MAX_TOKENS"
          value = "8192"
        }
        env {
          name  = "GEMINI_REQ_TIMEOUT"
          value = "300"
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
        env {
          name  = "INPUT_PDF_GCS_PATH"
          value = "gs://placeholder/placeholder.pdf"
        }
        env {
          name  = "INPUT_TICKER"
          value = "PLACEHOLDER_TICKER"
        }
        env {
          name  = "INPUT_ACCESSION_NUMBER"
          value = "PLACEHOLDER_ACCNO"
        }
        env {
          name  = "INPUT_FILING_DATE"
          value = "1970-01-01"
        }
        env {
          name  = "INPUT_FORM_TYPE"
          value = "PLACEHOLDER_FORM"
        }
      }
    }
  }

  depends_on = [google_service_account.qualitative_analysis_sa]
}

# --- Job 4: Generate Headline Assessment ---
resource "google_cloud_run_v2_job" "headline_assessment_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.headline_assessment_job_name

  template {
    template {
      service_account       = google_service_account.headline_assessment_sa.email
      timeout               = "1800s"
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = "${local.artifact_registry_base}/generate-headline-assessment:${var.docker_image_tag}"

        resources {
          limits = {
            cpu    = "2000m"
            memory = "2Gi"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = local.common_env_vars.GCP_PROJECT_ID
        }
        env {
          name  = "GCP_REGION"
          value = var.gcp_region
        }
        env {
          name  = "GCS_BUCKET_NAME"
          value = var.gcs_bucket_name
        }
        env {
          name  = "GCS_NEWS_SUMMARY_PREFIX"
          value = var.gcs_headline_assessment_prefix
        }
        env {
          name  = "GEMINI_MODEL_NAME"
          value = var.gemini_model_name
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
        env {
          name  = "INPUT_TICKER"
          value = "PLACEHOLDER_TICKER"
        }
        env {
          name  = "INPUT_FILING_DATE"
          value = "1970-01-01"
        }
        env {
          name  = "INPUT_COMPANY_NAME"
          value = "PLACEHOLDER_NAME"
        }
      }
    }
  }

  depends_on = [google_service_account.headline_assessment_sa]
}

# --- Job 5: Price Loader ---
resource "google_cloud_run_v2_job" "price_loader_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.price_loader_job_name

  template {
    template {
      service_account       = google_service_account.price_loader_sa.email
      timeout               = local.default_job_template.template.timeout
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = "${local.artifact_registry_base}/price-loader:${var.docker_image_tag}"

        resources {
          limits = local.default_job_template.template.containers[0].resources.limits
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = local.common_env_vars.GCP_PROJECT_ID
        }
        env {
          name  = "BQ_DATASET_ID"
          value = local.common_env_vars.BQ_DATASET_ID
        }
        env {
          name  = "BQ_METADATA_TABLE_ID"
          value = var.bq_metadata_table_id
        }
        env {
          name  = "BQ_PRICES_TABLE_ID"
          value = var.bq_price_table_id
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
      }
    }
  }

  depends_on = [google_service_account.price_loader_sa]
}

# --- Job 6: Ratio Calculator ---
resource "google_cloud_run_v2_job" "ratio_calculator_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.ratio_calculator_job_name

  template {
    template {
      service_account       = google_service_account.ratio_calculator_sa.email
      timeout               = "5400s"
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = "${local.artifact_registry_base}/ratio-calculator:${var.docker_image_tag}"

        resources {
          limits = {
            cpu    = "2000m"
            memory = "2Gi"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = local.common_env_vars.GCP_PROJECT_ID
        }
        env {
          name  = "BQ_DATASET_ID"
          value = local.common_env_vars.BQ_DATASET_ID
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
          name  = "BQ_METADATA_TABLE_ID"
          value = var.bq_metadata_table_id
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
          value = var.api_secret_version
        }
        env {
          name  = "GEMINI_MODEL_NAME"
          value = var.gemini_model_name
        }
        env {
          name  = "MAX_WORKERS"
          value = tostring(var.ratio_calculator_max_workers)
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
      }
    }
  }

  depends_on = [google_service_account.ratio_calculator_sa]
}

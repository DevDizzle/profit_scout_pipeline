# infrastructure/terraform/cloudrun_jobs.tf

locals {
  artifact_registry_base = "${var.gcp_region}-docker.pkg.dev/${var.gcp_project_id}/${var.artifact_registry_repo_name}"
  common_env_vars = {
    GCP_PROJECT_ID = var.gcp_project_id
    BQ_DATASET_ID  = var.bq_dataset_id
  }
  default_job_template = {
    template = {
      timeout               = var.default_job_timeout
      max_retries           = var.default_job_retries
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      containers = [
        {
          resources = {
            limits = {
              cpu    = var.default_job_cpu
              memory = var.default_job_memory
            }
          }
        }
      ]
    }
  }
}

locals {
  fetch_filings_image         = "${local.artifact_registry_base}/fetch-filings:latest"
  download_pdf_image          = "${local.artifact_registry_base}/download-pdf:latest"
  qualitative_analysis_image  = "${local.artifact_registry_base}/qualitative-analysis:latest"
  headline_assessment_image   = "${local.artifact_registry_base}/headline-assessment:latest"
  price_loader_image          = "${local.artifact_registry_base}/price-loader:latest"
  ratio_calculator_image      = "${local.artifact_registry_base}/ratio-calculator:latest"
  financial_extraction_image  = "${local.artifact_registry_base}/financial-extraction:latest"
}

resource "google_cloud_run_v2_job" "fetch_filings_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.fetch_filings_job_name
  deletion_protection = false

  template {
    template {
      service_account       = google_service_account.fetch_filings_sa.email
      timeout               = local.default_job_template.template.timeout
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = local.fetch_filings_image
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

resource "google_cloud_run_v2_job" "download_pdf_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.download_pdf_job_name
  deletion_protection = false

  template {
    template {
      service_account       = google_service_account.download_pdf_sa.email
      timeout               = "3600s"
      max_retries           = var.default_job_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = local.download_pdf_image
        env {
          name  = "GCP_PROJECT_ID"
          value = local.common_env_vars.GCP_PROJECT_ID
        }
        env {
          name  = "GCS_BUCKET_NAME"
          value = var.gcs_bucket_name
        }
        env {
          name  = "BQ_DATASET_ID"
          value = var.bq_dataset_id
        }
        env {
          name  = "BQ_METADATA_TABLE_ID"
          value = var.bq_metadata_table_id
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

  depends_on = [google_service_account.download_pdf_sa]
}

resource "google_cloud_run_v2_job" "qualitative_analysis_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.qualitative_analysis_job_name
  deletion_protection = false

  template {
    template {
      service_account       = google_service_account.qualitative_analysis_sa.email
      timeout               = "1800s"
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = local.qualitative_analysis_image
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
          name  = "GCS_PDF_PREFIX"
          value = var.gcs_pdf_prefix
        }
        env {
          name  = "GCS_ANALYSIS_TXT_PREFIX"
          value = var.gcs_analysis_txt_prefix
        }
        env {
          name  = "BQ_METADATA_TABLE_ID"
          value = var.bq_metadata_table_id
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
          value = tostring(var.gemini_temperature)
        }
        env {
          name  = "GEMINI_MAX_TOKENS"
          value = tostring(var.gemini_max_tokens)
        }
        env {
          name  = "GEMINI_REQ_TIMEOUT"
          value = tostring(var.gemini_req_timeout)
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
      }
    }
  }

  depends_on = [google_service_account.qualitative_analysis_sa]
}

resource "google_cloud_run_v2_job" "headline_assessment_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.headline_assessment_job_name
  deletion_protection = false

  template {
    template {
      service_account       = google_service_account.headline_assessment_sa.email
      timeout               = "1800s"
      max_retries           = var.default_job_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = local.headline_assessment_image
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
          name  = "GCS_HEADLINE_OUTPUT_FOLDER"
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
      }
    }
  }

  depends_on = [google_service_account.headline_assessment_sa]
}

resource "google_cloud_run_v2_job" "price_loader_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.price_loader_job_name
  deletion_protection = false

  template {
    template {
      service_account       = google_service_account.price_loader_sa.email
      timeout               = local.default_job_template.template.timeout
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = local.price_loader_image
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

resource "google_cloud_run_v2_job" "ratio_calculator_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.ratio_calculator_job_name
  deletion_protection = false

  template {
    template {
      service_account       = google_service_account.ratio_calculator_sa.email
      timeout               = "5400s"
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = local.ratio_calculator_image
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
          name  = "GCS_BUCKET_NAME"
          value = var.gcs_bucket_name
        }
        env {
          name  = "GCS_BS_PREFIX"
          value = var.gcs_bs_prefix
        }
        env {
          name  = "GCS_IS_PREFIX"
          value = var.gcs_is_prefix
        }
        env {
          name  = "GCS_CF_PREFIX"
          value = var.gcs_cf_prefix
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

resource "google_cloud_run_v2_job" "financial_extraction_job" {
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = var.financial_extraction_job_name
  deletion_protection = false

  template {
    template {
      service_account       = google_service_account.financial_extraction_sa.email
      timeout               = "3600s"
      max_retries           = local.default_job_template.template.max_retries
      execution_environment = local.default_job_template.template.execution_environment

      containers {
        image = local.financial_extraction_image
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
          name  = "BQ_METADATA_TABLE_ID"
          value = var.bq_metadata_table_id
        }
        env {
          name  = "GCS_BUCKET_NAME"
          value = var.gcs_bucket_name
        }
        env {
          name  = "GCS_BS_PREFIX"
          value = var.gcs_bs_prefix
        }
        env {
          name  = "GCS_IS_PREFIX"
          value = var.gcs_is_prefix
        }
        env {
          name  = "GCS_CF_PREFIX"
          value = var.gcs_cf_prefix
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
          name  = "MAX_FILINGS_TO_PROCESS"
          value = "0"
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
      }
    }
  }

  depends_on = [google_service_account.financial_extraction_sa]
}

resource "google_service_account" "fetch_filings_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-fetch-filings-sa"
  display_name = "Profit Scout Fetch Filings Job SA"
}

resource "google_service_account" "download_pdf_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-download-pdf-sa"
  display_name = "Profit Scout Download PDF Job SA"
}

resource "google_service_account" "qualitative_analysis_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-qual-anlys-sa"
  display_name = "Profit Scout Qualitative Analysis Job SA"
}

resource "google_service_account" "headline_assessment_sa" {
  project      = var.gcp_project_id
  account_id   = "headline-assess-sa"
  display_name = "Headline Assessment Service Account"
}

resource "google_service_account" "price_loader_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-price-loader-sa"
  display_name = "Profit Scout Price Loader Job SA"
}

resource "google_service_account" "ratio_calculator_sa" {
  project      = var.gcp_project_id
  account_id   = "profit-scout-ratio-calc-sa"
  display_name = "Profit Scout Ratio Calculator Job SA"
}

resource "google_service_account" "financial_extraction_sa" {
  project      = var.gcp_project_id
  account_id   = "financial-extract-sa"
  display_name = "Financial Extraction Service Account"
}
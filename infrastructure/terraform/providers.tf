# infrastructure/terraform/providers.tf

terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }

  backend "gcs" {
    bucket = "profit-scout"
    prefix = "profit-scout/terraform"
  }
}

provider "google" {
  project = "profit-scout-456416"
  region  = "us-central1"
}
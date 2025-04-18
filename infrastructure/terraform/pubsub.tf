# infrastructure/terraform/pubsub.tf

# --- Pub/Sub Subscription for Subscriber Service ---
# Creates a PUSH subscription that sends messages from the filing topic
# directly to the subscriber Cloud Run service endpoint.

resource "google_pubsub_subscription" "subscriber_push_subscription" {
  project = var.gcp_project_id
  name    = "profit-scout-subscriber-sub" # Name for the subscription resource
  topic   = google_pubsub_topic.filing_notifications.id # Link to the topic defined in main.tf

  ack_deadline_seconds = 60 # Default is 10s. Increase if subscriber processing might take longer. Max 600.

  # Configure Push Delivery to the Cloud Run service
  push_config {
    push_endpoint = google_cloud_run_v2_service.subscriber_service.uri # Get URL from the service resource

    oidc_token {
      # Use the subscriber service's own service account to authenticate the push request
      service_account_email = google_service_account.subscriber_sa.email
      # audience = "YOUR_CUSTOM_AUDIENCE" # Optional: Can set custom audience if needed, otherwise defaults are usually fine for Cloud Run invocation
    }
    # Can add attributes or wrapper settings if needed, defaults are usually fine
    # attributes = { x-goog-version = "v1" }
  }

  # Configure retry policy (optional, default is exponential backoff)
  retry_policy {
    minimum_backoff = "10s" # Default
    maximum_backoff = "600s" # Default
  }

  # Configure dead lettering (Recommended for production)
  #dead_letter_policy {
    #dead_letter_topic = google_pubsub_topic.subscriber_dead_letter_topic.id # Requires defining another topic
    #max_delivery_attempts = 5 # Default
  #}

  message_retention_duration = "604800s" # 7 days (default)

  depends_on = [
    google_pubsub_topic.filing_notifications,
    google_cloud_run_v2_service.subscriber_service,
    google_service_account.subscriber_sa
    # Add IAM binding dependency if needed, though SA email reference might be enough
    # google_project_iam_member.run_invokers_binding["serviceAccount:${google_service_account.subscriber_sa.email}"] # Example complex dependency
  ]
}

# Optional: Define Dead Letter Topic if using dead_letter_policy
# resource "google_pubsub_topic" "subscriber_dead_letter_topic" {
#   project = var.gcp_project_id
#   name    = "${var.pubsub_topic_id}-subscriber-dlq"
# }
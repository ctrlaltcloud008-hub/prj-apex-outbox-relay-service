# The relay owns no topics: it publishes outbox entries to whichever topic each
# row names, so it needs project-wide publish rights. Topics live in the repos
# of their logical producers (ingestion: video.received; probe: video.validated;
# orchestrator: transcode.job.requested*; worker: transcode.job.completed).
resource "google_project_iam_member" "relay_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.relay_sa_email}"
}

# Daily Partitioned DML cleanup of PUBLISHED outbox rows past retention.
resource "google_cloud_scheduler_job" "outbox_cleanup" {
  project  = var.project_id
  region   = var.project_region
  name     = "outbox-published-cleanup"
  schedule = "0 3 * * *" # daily at 03:00

  http_target {
    http_method = "POST"
    uri         = "${var.service_url}/cleanup"

    oidc_token {
      service_account_email = var.scheduler_service_account_email
    }
  }

  retry_config {
    retry_count = 1
  }
}

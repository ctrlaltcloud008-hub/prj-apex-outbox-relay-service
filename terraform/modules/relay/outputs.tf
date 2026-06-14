output "cleanup_scheduler_job_name" {
  description = "Name of the Cloud Scheduler job running the outbox cleanup."
  value       = google_cloud_scheduler_job.outbox_cleanup.name
}

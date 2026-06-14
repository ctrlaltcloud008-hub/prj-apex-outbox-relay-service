variable "project_id" {
  type        = string
  description = "The unique identifier for the GCP project for resource organization and billing."
  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id must not be empty."
  }
}

variable "project_region" {
  type        = string
  description = "The GCP region where the resources will be deployed, impacting latency and compliance."
  validation {
    condition     = length(var.project_region) > 0
    error_message = "The project_region must be specified."
  }
}

variable "environment" {
  type        = string
  description = "The deployment environment (e.g., dev, staging, prod) for resource organization and management."
}

variable "relay_sa_email" {
  type        = string
  description = "Service account email the outbox relay runs as."
}

variable "service_url" {
  type        = string
  description = "The base URL of the outbox relay service (used by Cloud Scheduler for the cleanup job)."
}

variable "scheduler_service_account_email" {
  type        = string
  description = "Service account email used by Cloud Scheduler to authenticate against the cleanup endpoint."
}

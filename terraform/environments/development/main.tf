# video.received and video.received.dlq moved to prj-apex-ingestion-service
# (their logical producer). After applying ingestion, remove them from this
# repo's old state: terraform state rm 'module.pubsub' (or import on the
# ingestion side) so they are not destroyed here.
module "relay" {
  source                          = "../../modules/relay"
  project_id                      = var.project_id
  project_region                  = var.project_region
  environment                     = var.environment
  relay_sa_email                  = var.relay_sa_email
  service_url                     = var.service_url
  scheduler_service_account_email = var.scheduler_service_account_email
}

output "raw_bucket_name" {
  value       = google_storage_bucket.raw.name
  description = "Bronze object-storage bucket."
}

output "bigquery_datasets" {
  value       = { for layer, dataset in google_bigquery_dataset.layers : layer => dataset.dataset_id }
  description = "BigQuery dataset IDs by lakehouse layer."
}

output "pipeline_service_account_email" {
  value       = google_service_account.pipeline.email
  description = "Workload identity for Airflow and dbt."
}

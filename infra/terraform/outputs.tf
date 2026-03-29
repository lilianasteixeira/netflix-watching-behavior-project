output "gcs_bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "Data lake GCS bucket name"
}

output "bq_dataset_raw" {
  value       = google_bigquery_dataset.raw.dataset_id
  description = "Raw dataset id"
}

output "bq_dataset_curated" {
  value       = google_bigquery_dataset.curated.dataset_id
  description = "Curated dataset id"
}

output "bq_dataset_marts" {
  value       = google_bigquery_dataset.marts.dataset_id
  description = "Marts dataset id"
}


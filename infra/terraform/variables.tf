variable "project_id" {
  description = "GCP project id"
  type        = string
  default     = "netflix-user-behavior-490622"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "bucket_name" {
  description = "GCS bucket name for the data lake"
  type        = string
  default     = "netflix-user-behavior-data-lake"
}

variable "raw_prefix" {
  description = "Prefix for raw objects in GCS"
  type        = string
  default     = "raw/netflix_behavior"
}

variable "bq_dataset_raw" {
  description = "BigQuery dataset for raw ingestion"
  type        = string
  default     = "netflix_raw"
}

variable "bq_dataset_curated" {
  description = "BigQuery dataset for dbt staging/intermediate models"
  type        = string
  default     = "netflix_curated"
}

variable "bq_dataset_marts" {
  description = "BigQuery dataset for dbt marts (analytics-ready)"
  type        = string
  default     = "netflix_marts"
}

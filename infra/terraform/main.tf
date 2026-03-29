provider "google" {
  credentials = file("./keys/netflix-user-behavior-creds.json")
  project     = var.project_id
  region      = var.region
}

# ─── GCS Data Lake ────────────────────────────────────────────────────────────
resource "google_storage_bucket" "data_lake" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition {
      age            = 30
      matches_prefix = ["${var.raw_prefix}/"]
    }
  }
}

# ─── BigQuery Datasets ────────────────────────────────────────────────────────

# Raw: GCS external tables / BQ load jobs land here
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.bq_dataset_raw
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = false
  description                = "Raw ingestion layer — data loaded directly from GCS Parquet files"
}

# Curated: dbt staging + intermediate models
resource "google_bigquery_dataset" "curated" {
  dataset_id                 = var.bq_dataset_curated
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = false
  description                = "Curated layer — cleaned, typed, and deduplicated models (dbt staging/intermediate)"
}

# Marts: dbt final models exposed to BI / analytics
resource "google_bigquery_dataset" "marts" {
  dataset_id                 = var.bq_dataset_marts
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = false
  description                = "Data marts — analytics-ready aggregations and dimensional models (dbt marts)"
}

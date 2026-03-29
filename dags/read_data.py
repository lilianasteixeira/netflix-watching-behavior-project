"""
DAG: kaggle_netflix_to_gcs
Description: Downloads ALL files from the Netflix 2025 User Behavior dataset
             via mlcroissant (no credentials needed for public datasets),
             converts each RecordSet to its own Parquet file, and uploads
             them all to GCS under a Hive-style partition path.

Terraform variable alignment:
    project_id   = "netflix-user-behavior-490622"
    bucket_name  = "netflix-user-behavior-data-lake"
    raw_prefix   = "raw/netflix_behavior"
    region       = "europe-west1"

GCS output layout:
    gs://netflix-user-behavior-data-lake/
      raw/netflix_behavior/
        <record_set_name>/
          year=YYYY/month=MM/day=DD/
            <record_set_name>_YYYY-MM-DD.parquet

Requirements:
    pip install mlcroissant pandas pyarrow apache-airflow-providers-google
"""

import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

# ──────────────────────────────────────────────────────────────────────────────
# Config — mirrors Terraform variables exactly
# ──────────────────────────────────────────────────────────────────────────────
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID  = "netflix-user-behavior-490622"
BUCKET_NAME = "netflix-user-behavior-data-lake"
RAW_PREFIX  = "raw/netflix_behavior"

CROISSANT_URL = (
    "https://www.kaggle.com/datasets/"
    "sayeeduddin/netflix-2025user-behavior-dataset-210k-records/"
    "croissant/download"
)

LOCAL_STAGING = Path("/tmp/netflix_croissant")

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Default args
# ──────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase + snake_case column names; strip Croissant RecordSet prefix."""
    df.columns = [
        c.split("/")[-1].strip().lower().replace(" ", "_")
        for c in df.columns
    ]
    return df


def _sanitise_name(name: str) -> str:
    """
    Converts a RecordSet name to a safe GCS folder name.
    e.g. 'movies.csv' → 'movies', 'watch history' → 'watch_history'
    """
    import re
    # Strip file extensions
    name = re.sub(r"\.\w+$", "", name)
    # Replace spaces and special chars with underscore
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()
    return name


def _get_record_set_names(metadata_json: dict) -> List[str]:
    """
    Extracts RecordSet names from the Croissant JSON-LD metadata dict.
    Falls back to parsing @id if 'name' is absent.
    Skips any RecordSet whose name looks like a README / non-tabular file.
    Returns list of (original_name, sanitised_name) tuples.
    """
    skip_keywords = {"readme", "license", "changelog"}
    names = []
    for rs in metadata_json.get("recordSet", []):
        name = rs.get("name") or rs.get("@id", "").split("/")[-1]
        if name and not any(kw in name.lower() for kw in skip_keywords):
            names.append({"original": name, "sanitised": _sanitise_name(name)})
    return names


# ──────────────────────────────────────────────────────────────────────────────
# Task 1: discover RecordSets and push names via XCom
# ──────────────────────────────────────────────────────────────────────────────
def _discover_record_sets(**context) -> List[dict]:
    import mlcroissant as mlc

    log.info("Fetching Croissant metadata from: %s", CROISSANT_URL)
    dataset = mlc.Dataset(CROISSANT_URL)
    metadata_json = dataset.metadata.to_json()

    names = _get_record_set_names(metadata_json)
    if not names:
        raise ValueError("No usable RecordSets found in Croissant metadata.")

    log.info("Found %d RecordSet(s):", len(names))
    for n in names:
        log.info("  %s → folder: %s", n["original"], n["sanitised"])
    return names   # pushed to XCom automatically


# ──────────────────────────────────────────────────────────────────────────────
# Task 2: download ALL RecordSets and convert each to Parquet
# ──────────────────────────────────────────────────────────────────────────────
def _load_and_convert_all(**context) -> List[dict]:
    import mlcroissant as mlc

    record_sets: List[dict] = context["ti"].xcom_pull(
        task_ids="discover_record_sets"
    )
    execution_date: datetime = context["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")

    LOCAL_STAGING.mkdir(parents=True, exist_ok=True)

    log.info("Loading dataset …")
    dataset = mlc.Dataset(CROISSANT_URL)

    results: List[dict] = []

    for rs in record_sets:
        original_name = rs["original"]
        sanitised_name = rs["sanitised"]
        log.info("─── Processing RecordSet: '%s' → folder: '%s' ───", original_name, sanitised_name)

        try:
            records = list(dataset.records(record_set=original_name))
        except Exception as exc:
            log.warning("Skipping RecordSet '%s' — error: %s", original_name, exc)
            continue

        if not records:
            log.warning("RecordSet '%s' returned 0 records — skipping.", original_name)
            continue

        df = pd.DataFrame(records)
        df = _normalise_columns(df)

        # Ingestion metadata
        df["_ingestion_date"] = date_str
        df["_record_set"]     = sanitised_name
        df["_source"]         = "kaggle/sayeeduddin/netflix-2025user-behavior-dataset-210k-records"

        log.info(
            "RecordSet '%s' → %d rows × %d cols | columns: %s",
            sanitised_name, len(df), len(df.columns), list(df.columns),
        )

        # Use sanitised name for the Parquet filename — no .csv extension
        parquet_file = LOCAL_STAGING / f"{sanitised_name}_{date_str}.parquet"
        df.to_parquet(parquet_file, engine="pyarrow", compression="snappy", index=False)

        size_mb = parquet_file.stat().st_size // (1024 ** 2)
        log.info("Written: %s (%d MB)", parquet_file, size_mb)
        results.append({"path": str(parquet_file), "sanitised_name": sanitised_name})

    if not results:
        raise RuntimeError("No Parquet files were produced — check RecordSet errors above.")

    log.info("Total Parquet files produced: %d", len(results))
    return results   # pushed to XCom


# ──────────────────────────────────────────────────────────────────────────────
# Task 3: upload all Parquet files to GCS
# ──────────────────────────────────────────────────────────────────────────────
def _upload_all_to_gcs(**context) -> List[str]:
    results: List[dict] = context["ti"].xcom_pull(
        task_ids="load_and_convert_all"
    )
    execution_date: datetime = context["execution_date"]

    year  = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day   = execution_date.strftime("%d")

    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    gcs_uris: List[str] = []

    for item in results:
        local_file = Path(item["path"])
        sanitised_name = item["sanitised_name"]

        # Each RecordSet goes into its own clean prefix (no .csv in folder name)
        # GCS layout: raw/netflix_behavior/<sanitised_name>/year=YYYY/month=MM/day=DD/
        gcs_object = (
            f"{RAW_PREFIX}/{sanitised_name}/"
            f"year={year}/month={month}/day={day}/"
            f"{local_file.name}"
        )

        log.info("Uploading %s → gs://%s/%s", local_file.name, BUCKET_NAME, gcs_object)
        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=gcs_object,
            filename=str(local_file),
            mime_type="application/octet-stream",
        )

        gcs_uri = f"gs://{BUCKET_NAME}/{gcs_object}"
        gcs_uris.append(gcs_uri)
        log.info("Uploaded: %s", gcs_uri)

    log.info("All uploads complete (%d files).", len(gcs_uris))
    return gcs_uris


# ──────────────────────────────────────────────────────────────────────────────
# Task 4: clean up local staging
# ──────────────────────────────────────────────────────────────────────────────
def _cleanup(**context) -> None:
    if LOCAL_STAGING.exists():
        shutil.rmtree(LOCAL_STAGING)
        log.info("Removed local staging dir: %s", LOCAL_STAGING)


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="kaggle_netflix_to_gcs",
    description=(
        "Ingests ALL files from the Netflix 2025 User Behavior dataset "
        "via mlcroissant → GCS (Parquet, snappy, one prefix per RecordSet)"
    ),
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "kaggle", "croissant", "gcs", "ingestion", "raw"],
    doc_md=__doc__,
) as dag:

    discover = PythonOperator(
        task_id="discover_record_sets",
        python_callable=_discover_record_sets,
    )

    load_and_convert_all = PythonOperator(
        task_id="load_and_convert_all",
        python_callable=_load_and_convert_all,
    )

    upload_all = PythonOperator(
        task_id="upload_all_to_gcs",
        python_callable=_upload_all_to_gcs,
    )

    cleanup = PythonOperator(
        task_id="cleanup_local_files",
        python_callable=_cleanup,
        trigger_rule="all_done",
    )

    discover >> load_and_convert_all >> upload_all >> cleanup
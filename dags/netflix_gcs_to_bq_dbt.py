"""
DAG: netflix_gcs_to_bq_dbt
Description:
    Stage 1 — For each of the 6 source tables:
                 1. Downloads all Parquet files for that table from GCS
                 2. Decodes all bytes columns to plain UTF-8 strings in-memory
                 3. Uploads a single clean Parquet to GCS (temp location)
                 4. Loads it into BigQuery with WRITE_TRUNCATE
               This avoids BigQuery storing byte columns as base64 BYTES.
    Stage 2 — Runs dbt (staging views → mart tables → tests).

GCS layout expected:
    raw/netflix_behavior/<table_name>/year=YYYY/month=MM/day=DD/<file>.parquet

BigQuery raw tables produced (plain STRING columns, no base64):
    netflix_raw.movies
    netflix_raw.users
    netflix_raw.watch_history
    netflix_raw.reviews
    netflix_raw.recommendation_logs
    netflix_raw.search_logs
"""

import io
import logging
import os
import subprocess
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID     = os.getenv("GCP_PROJECT_ID",  "netflix-user-behavior-490622")
BUCKET_NAME    = os.getenv("GCS_BUCKET_NAME", "netflix-user-behavior-data-lake")
RAW_PREFIX     = os.getenv("GCS_RAW_PREFIX",  "raw/netflix_behavior")
BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW",  "netflix_raw")
BQ_DATASET_CURATED = os.getenv("BQ_DATASET_CURATED",  "netflix_curated")
BQ_DATASET_MARTS = os.getenv("BQ_DATASET_MARTS",  "netflix_marts")

# Temp GCS prefix for cleaned Parquet files before BQ load
CLEAN_PREFIX = "tmp/netflix_behavior_clean"

EXPECTED_TABLES = {
    "movies",
    "users",
    "watch_history",
    "reviews",
    "recommendation_logs",
    "search_logs",
}

# Primary key per table — used to deduplicate when multiple DAG runs
# have written the same records to GCS (keep latest _ingestion_date)
TABLE_PRIMARY_KEYS = {
    "movies":              ["movie_id"],
    "users":               ["user_id"],
    "watch_history":       ["session_id"],
    "reviews":             ["review_id"],
    "recommendation_logs": ["recommendation_id"],
    "search_logs":         ["search_id"],
}

DBT_PROJECT_DIR = Path("/opt/airflow/jobs/dbt")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _decode_bytes_columns(df):
    """
    Parquet files produced by mlcroissant store string fields as Python bytes
    objects (b'Movie', b'Action', ...). BigQuery's load job sees these as BYTES
    and stores them as base64-encoded strings.

    This function detects any column where the first non-null value is a bytes
    object and decodes the entire column to UTF-8 string in-place.
    """
    import pandas as pd

    for col in df.columns:
        # Find first non-null value to check the type
        sample = df[col].dropna()
        if len(sample) == 0:
            continue
        if isinstance(sample.iloc[0], bytes):
            df[col] = df[col].apply(
                lambda v: v.decode("utf-8") if isinstance(v, bytes) else v
            )
    return df


def _group_blobs_by_table(blobs, raw_prefix):
    """
    Groups GCS blob objects by their immediate subfolder under raw_prefix.
    raw/netflix_behavior/watch_history/year=2026/... → "watch_history"
    """
    table_files = defaultdict(list)
    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue
        relative   = blob.name[len(raw_prefix) + 1:]   # strip prefix + "/"
        table_name = relative.split("/")[0]              # first segment
        table_files[table_name].append(blob)
    return table_files


# ─── Task 1: decode + load each table ─────────────────────────────────────────
def _load_gcs_to_bq(**context) -> None:
    """
    For each source table:
      1. Download all Parquet blobs for that table
      2. Concatenate into one DataFrame
      3. Decode all bytes columns → plain str
      4. Write to a single clean Parquet in memory
      5. Upload clean Parquet to GCS tmp location
      6. Load into BigQuery with WRITE_TRUNCATE (guaranteed no duplicates)
      7. Delete the tmp Parquet from GCS
    """
    import pandas as pd
    from google.cloud import bigquery, storage
    from google.oauth2 import service_account

    key_path   = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/keys/gcp-sa.json")
    creds      = service_account.Credentials.from_service_account_file(key_path)
    gcs_client = storage.Client(project=PROJECT_ID, credentials=creds)
    bq_client  = bigquery.Client(project=PROJECT_ID, credentials=creds)

    # ── 1. List and group all blobs ──────────────────────────────────────────
    all_blobs = list(gcs_client.list_blobs(BUCKET_NAME, prefix=RAW_PREFIX + "/"))
    table_blobs = _group_blobs_by_table(all_blobs, RAW_PREFIX)

    if not table_blobs:
        raise RuntimeError(
            f"No Parquet files found under gs://{BUCKET_NAME}/{RAW_PREFIX}/"
        )

    log.info("Found %d table(s): %s", len(table_blobs), list(table_blobs.keys()))

    missing = EXPECTED_TABLES - set(table_blobs.keys())
    if missing:
        log.warning("Expected tables not found in GCS: %s", missing)

    errors = {}

    for table_name, blobs in table_blobs.items():
        log.info("=" * 60)
        log.info("Processing table: %s (%d file(s))", table_name, len(blobs))

        try:
            # ── 2. Download + concatenate all parquet files for this table ───
            frames = []
            for blob in blobs:
                raw_bytes = blob.download_as_bytes()
                df_part   = pd.read_parquet(io.BytesIO(raw_bytes))
                frames.append(df_part)
                log.info("  Downloaded %s (%d rows)", blob.name.split("/")[-1], len(df_part))

            df = pd.concat(frames, ignore_index=True)
            log.info("  Combined: %d rows × %d cols (before dedup)", *df.shape)

            # ── 2b. Deduplicate on primary key, keep latest ingestion date ───
            pk_cols = TABLE_PRIMARY_KEYS.get(table_name)
            if pk_cols and all(c in df.columns for c in pk_cols):
                before = len(df)
                df = (
                    df.sort_values("_ingestion_date", ascending=False)
                      .drop_duplicates(subset=pk_cols, keep="first")
                      .reset_index(drop=True)
                )
                removed = before - len(df)
                if removed:
                    log.info("  Deduped %d duplicate row(s) on %s", removed, pk_cols)
                else:
                    log.info("  No duplicates found on %s", pk_cols)

            log.info("  Final shape: %d rows × %d cols", *df.shape)

            # ── 3. Decode bytes columns → plain str ──────────────────────────
            df = _decode_bytes_columns(df)
            log.info("  Bytes columns decoded to UTF-8 strings")

            # ── 4. Serialize to clean Parquet in memory ──────────────────────
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
            buffer.seek(0)

            # ── 5. Upload clean Parquet to GCS tmp location ──────────────────
            clean_blob_name = f"{CLEAN_PREFIX}/{table_name}_clean.parquet"
            clean_blob      = gcs_client.bucket(BUCKET_NAME).blob(clean_blob_name)
            clean_blob.upload_from_file(buffer, content_type="application/octet-stream")
            clean_uri = f"gs://{BUCKET_NAME}/{clean_blob_name}"
            log.info("  Uploaded clean parquet → %s", clean_uri)

            # ── 6. Load into BigQuery ────────────────────────────────────────
            table_ref  = f"{PROJECT_ID}.{BQ_DATASET_RAW}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True,
            )

            load_job = bq_client.load_table_from_uri(
                clean_uri, table_ref, job_config=job_config
            )
            load_job.result()

            bq_table = bq_client.get_table(table_ref)
            log.info(
                "  ✓ Loaded → %s | %d rows | %d cols",
                table_ref, bq_table.num_rows, len(bq_table.schema),
            )

            # ── 7. Cleanup tmp GCS file ──────────────────────────────────────
            clean_blob.delete()
            log.info("  Deleted tmp file: %s", clean_blob_name)

        except Exception as exc:
            log.exception("  ✗ Failed to process table '%s': %s", table_name, exc)
            errors[table_name] = str(exc)

    if errors:
        raise RuntimeError(f"Failed to load {len(errors)} table(s): {errors}")

    log.info("All tables loaded successfully.")


# ─── Task 2: dbt runner ────────────────────────────────────────────────────────
def _run_dbt(command: list, **context) -> None:
    """Runs an arbitrary dbt command inside the Airflow container."""
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(DBT_PROJECT_DIR),
        "GCP_PROJECT_ID":      PROJECT_ID,
        "BQ_DATASET_RAW":      BQ_DATASET_RAW,
        "BQ_DATASET_CURATED":  BQ_DATASET_CURATED,
        "BQ_DATASET_MARTS":    BQ_DATASET_MARTS,
    }

    full_command = ["dbt"] + command + [
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR),
    ]

    log.info("Running: %s", " ".join(full_command))
    result = subprocess.run(
        full_command,
        env=env,
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR),
    )

    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(
            f"dbt command failed: {' '.join(command)}\n{result.stderr}"
        )

    log.info("dbt command succeeded: %s", " ".join(command))


# ─── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="netflix_gcs_to_bq_dbt",
    description=(
        "Decodes bytes columns, loads 6 Netflix tables from GCS into BigQuery, "
        "then runs dbt to build curated views and analytics marts."
    ),
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "bigquery", "dbt", "transform", "marts"],
    doc_md=__doc__,
) as dag:

    load_to_bq = PythonOperator(
        task_id="load_gcs_to_bq_raw",
        python_callable=_load_gcs_to_bq,
    )

    dbt_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=_run_dbt,
        op_kwargs={"command": ["deps"]},
    )

    dbt_run_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=_run_dbt,
        op_kwargs={"command": ["run", "--select", "models/staging"]},
    )

    dbt_run_marts = PythonOperator(
        task_id="dbt_run_marts",
        python_callable=_run_dbt,
       op_kwargs={"command": ["run", "--select", "models/marts"]},
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=_run_dbt,
        op_kwargs={"command": ["test"]},
    )

    load_to_bq >> dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test
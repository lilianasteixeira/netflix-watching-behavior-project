"""
DAG: inspect_gcs_parquet_schema
Description:
    Reads every Parquet file found under the raw GCS prefix,
    prints column names + dtypes + a sample row for each file,
    and also dumps a JSON summary to /tmp/schema_report.json
    so you can copy-paste it easily from the logs.

    Trigger manually — no schedule.
"""

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

PROJECT_ID  = os.getenv("GCP_PROJECT_ID",  "netflix-user-behavior-490622")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "netflix-user-behavior-data-lake")
RAW_PREFIX  = os.getenv("GCS_RAW_PREFIX",  "raw/netflix_behavior")

default_args = {
    "owner": "data-engineering",
    "retries": 0,
}


def _inspect_schema(**context) -> None:
    import io
    import pandas as pd
    from google.cloud import storage
    from google.oauth2 import service_account

    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/keys/gcp-sa.json")
    creds    = service_account.Credentials.from_service_account_file(key_path)
    client   = storage.Client(project=PROJECT_ID, credentials=creds)

    blobs = [
        b for b in client.list_blobs(BUCKET_NAME, prefix=RAW_PREFIX + "/")
        if b.name.endswith(".parquet")
    ]

    if not blobs:
        raise RuntimeError(
            f"No Parquet files found under gs://{BUCKET_NAME}/{RAW_PREFIX}/"
        )

    log.info("Found %d Parquet file(s)", len(blobs))

    report = {}

    for blob in blobs:
        uri = f"gs://{BUCKET_NAME}/{blob.name}"
        log.info("=" * 70)
        log.info("FILE: %s", uri)
        log.info("=" * 70)

        raw_bytes = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(raw_bytes))

        # ── Schema ──────────────────────────────────────────────────────────
        log.info("SHAPE: %d rows × %d columns", *df.shape)
        log.info("COLUMNS + DTYPES:")
        schema = {}
        for col, dtype in df.dtypes.items():
            log.info("  %-40s %s", col, dtype)
            schema[col] = str(dtype)

        # ── Null counts ──────────────────────────────────────────────────────
        log.info("NULL COUNTS:")
        for col, null_count in df.isnull().sum().items():
            if null_count > 0:
                log.info("  %-40s %d nulls (%.1f%%)",
                         col, null_count, 100 * null_count / len(df))

        # ── Unique value counts for low-cardinality columns ──────────────────
        log.info("UNIQUE VALUES (columns with <=30 distinct values):")
        unique_vals = {}
        for col in df.columns:
            n_unique = df[col].nunique()
            if n_unique <= 30:
                vals = sorted(df[col].dropna().unique().tolist())
                # cast to str so json.dumps works on any dtype
                vals_str = [str(v) for v in vals]
                log.info("  %-40s (%d) → %s", col, n_unique, vals_str)
                unique_vals[col] = vals_str

        # ── Sample rows ──────────────────────────────────────────────────────
        log.info("SAMPLE ROWS (first 3):")
        for i, row in df.head(3).iterrows():
            log.info("  row %d: %s", i, row.to_dict())

        # ── Collect for JSON report ──────────────────────────────────────────
        file_key = blob.name.split("/")[-1]   # just the filename
        report[file_key] = {
            "gcs_path": uri,
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1]),
            "schema": schema,
            "unique_values": unique_vals,
            "sample_row": {
                k: str(v) for k, v in df.iloc[0].to_dict().items()
            },
        }

    # ── Write JSON summary ───────────────────────────────────────────────────
    report_path = Path("/tmp/schema_report.json")
    report_path.write_text(json.dumps(report, indent=2, default=str))
    log.info("=" * 70)
    log.info("JSON SCHEMA REPORT (copy everything between the markers):")
    log.info(">>>BEGIN_SCHEMA_REPORT")
    log.info(json.dumps(report, indent=2, default=str))
    log.info("<<<END_SCHEMA_REPORT")
    log.info("Written to %s", report_path)


with DAG(
    dag_id="inspect_gcs_parquet_schema",
    description="Inspects schema, dtypes, nulls and sample rows of all raw GCS Parquet files",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,   # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "debug", "schema"],
) as dag:

    PythonOperator(
        task_id="inspect_schema",
        python_callable=_inspect_schema,
    )

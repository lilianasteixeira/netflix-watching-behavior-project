from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
import requests

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

PROJECT_ID = "netflix-user-behavior-490622"
DATASET = "netflix_marts"

def check_marts_ready():
    """
    Optional: Ensure marts table has fresh data
    """
    hook = BigQueryHook()
    
    query = f"""
    SELECT COUNT(*) as row_count
    FROM `{PROJECT_ID}.{DATASET}.user_engagement_summary`
    """

    result = hook.get_first(query)
    
    if result[0] == 0:
        raise ValueError("Marts table is empty!")
    
    print(f"✅ Marts ready with {result[0]} rows")


def trigger_streamlit_refresh():
    """
    Trigger Streamlit refresh (lightweight)
    """
    try:
        requests.get("http://streamlit:8501")
        print("✅ Streamlit pinged successfully")
    except Exception as e:
        print(f"⚠️ Streamlit not reachable: {e}")


with DAG(
    dag_id="streamlit_dashboard_refresh",
    schedule_interval="@hourly",  # or match your dbt DAG schedule
    catchup=False,
    default_args=default_args,
    tags=["dashboard", "streamlit"],
) as dag:

    check_data = PythonOperator(
        task_id="check_marts_data",
        python_callable=check_marts_ready
    )

    refresh_dashboard = PythonOperator(
        task_id="refresh_streamlit",
        python_callable=trigger_streamlit_refresh
    )

    check_data >> refresh_dashboard
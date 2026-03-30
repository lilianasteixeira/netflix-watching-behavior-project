from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import webbrowser  # built-in Python library

def open_dashboard_for_export(**context):
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    url = os.getenv("STREAMLIT_URL", "http://streamlit:8501")
    
    print(f"✅ Dashboard is ready for export on: {url}")
    print(f"Date: {execution_date}")
    print("\nManual steps for PDF export:")
    print("1. Open the URL above")
    print("2. Apply your desired filters")
    print("3. Use browser Print → Save as PDF (Landscape recommended)")
    
    # Optional: just open the browser (works only if running locally)
    # webbrowser.open(url)

with DAG(
    dag_id="notify_netflix_dashboard_export",
    default_args={
        "owner": "data_team",
        "retries": 1,
    },
    description="Reminds / prepares for dashboard PDF export (no extra installs)",
    schedule_interval="0 9 * * *",   # Daily at 9 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["netflix", "reporting"],
) as dag:

    notify_task = PythonOperator(
        task_id="prepare_dashboard_export",
        python_callable=open_dashboard_for_export,
        provide_context=True,
    )
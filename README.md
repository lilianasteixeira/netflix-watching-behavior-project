## Problem Description

Netflix generates large volumes of user viewing data, but stakeholders lacked an easy way to analyze engagement trends, content performance, and genre popularity across different subscription plans, countries, and time periods.

This project solves that challenge by building a complete end-to-end analytics solution:

- An automated data pipeline using **Apache Airflow**, **dbt**, and **Google BigQuery**
- An interactive **Streamlit dashboard** that lets users easily filter and explore the data

Users can now quickly track key metrics (total users, active rate, average watch time), visualize user activity trends, analyze content completion rates, and understand genre distribution — turning raw data into clear, actionable insights for product, content, and marketing teams.
## Architecture Overview

![Data Pipeline Architecture](./assets/architecture.png)

This project implements a complete **end-to-end data engineering pipeline** on Google Cloud Platform (GCP). It ingests raw data from Kaggle, processes it through a modern data stack, and delivers interactive analytics via a Streamlit dashboard.

### Pipeline Flow

**1. Data Ingestion**
- An **Apache Airflow DAG** downloads raw `.csv` files from a **Kaggle Dataset** and  converts it to optimized **Parquet** format, and stores it in **Google Cloud Storage (GCS)**, which acts as the **Data Lake**.

**2. Orchestration & Transformation**
- **Apache Airflow** serves as the central **orchestrator and job manager**.
- **dbt (data build tool)** performs all data transformations.
- Data is processed into different layers in **BigQuery**:
  - **Raw layer**
  - **Curated layer**
  - **Marts layer** (business-ready models)

**3. Data Warehouse**
- **Google BigQuery** is used as the scalable **Data Warehouse**.

**4. Analytics & Visualization**
- A **Streamlit** application connects directly to the **marts layer** in BigQuery.
- It provides interactive dashboards and business insights to end users.

### Infrastructure Management

The infrastructure is fully managed as code using:
- **Terraform** – Infrastructure as Code (IaC)
- **Docker** – Containerization of services (Airflow, etc.)

### Tech Stack

- **Orchestration**: Apache Airflow
- **Transformation**: dbt (data build tool)
- **Data Lake**: Google Cloud Storage
- **Data Warehouse**: Google BigQuery
- **Visualization**: Streamlit
- **IaC & Containerization**: Terraform + Docker

---

## Instructions

- First take a look on architecture image or [file](./architecture.excalidraw);
- Next, configure infra. For that follow [terraform instructions](./infra/terraform/README.md) and next build [docker image](./docker-compose-airflow.yml), the simple way is right click on the file and select `Compose Up` option or `docker compose -f 'docker-compose-airflow.yml' up -d --build`; 
**Note**: Check on docker if all the service initiate correctly, if not make a compose down and up again.
- After configure infra, go to [Airflow UI](http://localhost:9000). The user and password are configured on [docker image](./docker-compose-airflow.yml) on `airflow-init`;
- Inside [Airflow UI](http://localhost:9000), you will see all dags available, the correct order to run them:
    1. **kaggle_netflix_to_gcs**: this dag reads all datasets available from [kaggle netflix](https://www.kaggle.com/datasets/sayeeduddin/netflix-2025user-behavior-dataset-210k-records/);
    2. **netflix_gcs_to_bq_dbt**: Perform all tranformations using dbt models, creating the layers raw (Bronze), curated (Silver) and marts (Gold) and saving them on BigQuery;
    3. **streamlit_dashboard_refresh**: used to build and refresh dashbord using netflix_marts data;
    **Note**: the `inspect_gcs_parquet_schema` dag only exists if you need to chech the schemas, you don't need to trigger this one.
- Finally to see the dashboard open [Streamlit app](http://localhost:8501/);
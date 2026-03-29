FROM apache/airflow:2.11.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
    

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY ./dags /opt/airflow/dags
COPY ./dbt /opt/airflow/jobs/dbt
COPY ./requirements.txt /opt/airflow/requirements.txt

ENV PYTHONPATH=/opt/airflow/jobs/dbt

USER airflow
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --prefer-binary -r /opt/airflow/requirements.txt

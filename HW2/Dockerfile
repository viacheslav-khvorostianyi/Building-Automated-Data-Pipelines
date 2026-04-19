FROM apache/airflow:3.0.0
# Build-time args — must match the base image (python --version → 3.12)
ARG AIRFLOW_VERSION=3.0.0
ARG PYTHON_VERSION=3.12

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


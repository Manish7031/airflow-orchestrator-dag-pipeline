FROM apache/airflow:3.0.0

COPY airflow-sdk /opt/airflow/airflow-sdk

RUN pip install -e /opt/airflow/airflow-sdk
FROM apache/airflow:2.10.0

USER airflow

COPY requirements.txt  .

RUN pip install -r requirements.txt
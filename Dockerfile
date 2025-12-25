FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    netcat-openbsd \
    build-essential && \
    apt-get clean

USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
  -r /requirements.txt


COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
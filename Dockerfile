FROM apache/airflow:2.4.3

RUN umask 0002; \
    mkdir -p /opt/airflow/data

COPY requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt
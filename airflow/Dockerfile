FROM apache/airflow:slim-3.1.7-python3.12

COPY airflow/requirements.txt /tmp/apexflow-airflow-requirements.txt
RUN pip install --no-cache-dir apache-airflow==3.1.7 \
    -r /tmp/apexflow-airflow-requirements.txt

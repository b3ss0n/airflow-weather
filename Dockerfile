# Свой образ = официальный Airflow + библиотеки для разработки.
# Собирается один раз (docker compose build), а не при каждом старте контейнера.
ARG AIRFLOW_IMAGE=apache/airflow:3.3.0
FROM ${AIRFLOW_IMAGE}

# pytest нужен для tests/test_dags.py (проверка импорта DAG'ов)
RUN pip install --no-cache-dir pytest

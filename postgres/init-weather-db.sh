#!/bin/bash
# Создаёт отдельного пользователя и базу weather для данных проекта.
# Метаданные Airflow живут в базе airflow, данные погоды — здесь.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER weather WITH PASSWORD '${WEATHER_DB_PASSWORD}';
    CREATE DATABASE weather OWNER weather;
EOSQL

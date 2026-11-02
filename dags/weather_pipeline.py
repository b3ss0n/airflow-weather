"""Погодный пайплайн. Шаг 3: загрузка сырого JSON из Open-Meteo в таблицу raw_weather."""

import json
from datetime import timedelta

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

# Города и координаты. Список маленький и меняется редко, поэтому лежит прямо в файле.
CITIES = [
    {"name": "Kazan", "lat": 55.7887, "lon": 49.1221},
    {"name": "Innopolis", "lat": 55.7512, "lon": 48.7442},
    {"name": "Moscow", "lat": 55.7558, "lon": 37.6173},
    {"name": "Saint Petersburg", "lat": 59.9343, "lon": 30.3351},
]

# Архивный эндпоинт отдаёт полные сутки и за вчера, поэтому он один и для свежих, и для старых дат
API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = "temperature_2m,precipitation,wind_speed_10m"


@dag(
    dag_id="weather_pipeline",
    # Run за сутки X стартует в 00:00 UTC следующих суток и обрабатывает данные за X
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["weather"],
    doc_md="Каждый день грузит почасовую погоду за прошедшие сутки для нескольких городов.",
)
def weather_pipeline():
    @task
    def create_tables() -> None:
        # IF NOT EXISTS: безопасно запускать при каждом run'е
        PostgresHook(postgres_conn_id="weather_db").run(
            """
            CREATE TABLE IF NOT EXISTS raw_weather (
                city    text  NOT NULL,
                date    date  NOT NULL,
                payload jsonb NOT NULL,
                PRIMARY KEY (city, date)
            )
            """
        )

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def fetch(city: dict, data_interval_start=None) -> str:
        # Сутки, за которые грузим данные: начало интервала данных (UTC)
        day = data_interval_start.strftime("%Y-%m-%d")
        response = requests.get(
            API_URL,
            params={
                "latitude": city["lat"],
                "longitude": city["lon"],
                "start_date": day,
                "end_date": day,
                "hourly": HOURLY,
                "timezone": "UTC",
            },
            timeout=30,
        )
        response.raise_for_status()

        # Upsert: повторный запуск за ту же дату перезапишет строку, а не создаст дубль
        PostgresHook(postgres_conn_id="weather_db").run(
            """
            INSERT INTO raw_weather (city, date, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (city, date) DO UPDATE SET payload = EXCLUDED.payload
            """,
            parameters=(city["name"], day, json.dumps(response.json())),
        )
        # В XCom уходит только имя города (сам JSON остаётся в базе)
        return city["name"]

    # Сначала таблица, потом по одной задаче fetch на каждый город
    create_tables() >> fetch.expand(city=CITIES)


weather_pipeline()

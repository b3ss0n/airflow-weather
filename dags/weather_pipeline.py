"""Погодный пайплайн. Шаг 3: загрузка сырого JSON из Open-Meteo в raw_weather.
Шаг 4: проверка качества данных (validate).
Шаг 6: дневная сводка по городам (build_daily_stats)."""

import json
import math
from datetime import datetime, timedelta, timezone

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

# Границы проверки качества
HOURS_PER_DAY = 24
MIN_TEMP, MAX_TEMP = -60, 50


def check_hourly(hourly: dict, day: str) -> list[str]:
    """Возвращает список проблем в почасовых данных за сутки (пустой список = данные в порядке)."""
    problems = []
    timestamps = hourly.get("time", [])
    expected_hours = {f"{day}T{hour:02d}:00" for hour in range(HOURS_PER_DAY)}
    if not isinstance(timestamps, list) or any(not isinstance(ts, str) for ts in timestamps):
        problems.append("time: ожидался список строк с часами UTC")
    elif len(timestamps) != HOURS_PER_DAY:
        problems.append(f"часов {len(timestamps)} вместо {HOURS_PER_DAY}")
    elif set(timestamps) != expected_hours:
        problems.append(f"time: ожидалось по одной метке каждого часа за {day} UTC")

    for name in HOURLY.split(","):
        values = hourly.get(name, [])
        if len(values) != HOURS_PER_DAY:
            problems.append(f"{name}: значений {len(values)} вместо {HOURS_PER_DAY}")
        # В JSON пропуск приходит как null (None); NaN проверяем на всякий случай
        missing = sum(v is None or (isinstance(v, float) and math.isnan(v)) for v in values)
        if missing:
            problems.append(f"{name}: пропусков {missing}")

    out_of_range = [
        v
        for v in hourly.get("temperature_2m", [])
        if v is not None and not math.isnan(v) and not MIN_TEMP <= v <= MAX_TEMP
    ]
    if out_of_range:
        problems.append(f"temperature_2m вне [{MIN_TEMP}, {MAX_TEMP}]: {out_of_range[:3]}")
    return problems


def hourly_to_rows(city: str, hourly: dict) -> list[tuple]:
    """Превращает массивы Open-Meteo в строки (city, ts, temperature, precipitation, wind_speed)."""
    return [
        # Время приходит в UTC без пояса, поэтому пояс проставляем явно
        (city, datetime.fromisoformat(ts).replace(tzinfo=timezone.utc), temp, prec, wind)
        for ts, temp, prec, wind in zip(
            hourly["time"],
            hourly["temperature_2m"],
            hourly["precipitation"],
            hourly["wind_speed_10m"],
        )
    ]


def get_hourly(city: str, day: str) -> dict:
    """Достаёт из raw_weather почасовые данные города за дату."""
    row = PostgresHook(postgres_conn_id="weather_db").get_first(
        "SELECT payload FROM raw_weather WHERE city = %s AND date = %s",
        parameters=(city, day),
    )
    if row is None:
        raise ValueError(f"{city} {day}: в raw_weather нет строки")
    return row[0]["hourly"]


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
            );
            CREATE TABLE IF NOT EXISTS hourly_weather (
                city          text             NOT NULL,
                ts            timestamptz      NOT NULL,
                temperature   double precision,
                precipitation double precision,
                wind_speed    double precision,
                PRIMARY KEY (city, ts)
            );
            CREATE TABLE IF NOT EXISTS daily_stats (
                city             text             NOT NULL,
                date             date             NOT NULL,
                temp_min         double precision,
                temp_max         double precision,
                temp_avg         double precision,
                precipitation_sum double precision,
                wind_speed_avg   double precision,
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

    @task
    def validate(city: str, data_interval_start=None) -> str:
        # Данные берём из базы по городу и дате (через XCom пришло только имя города)
        day = data_interval_start.strftime("%Y-%m-%d")
        problems = check_hourly(get_hourly(city, day), day)
        if problems:
            # Исключение = задача падает. Retries не нужны: повтор не исправит плохие данные
            raise ValueError(f"{city} {day}: " + "; ".join(problems))
        return city

    @task
    def transform_load(city: str, data_interval_start=None) -> str:
        day = data_interval_start.strftime("%Y-%m-%d")
        rows = hourly_to_rows(city, get_hourly(city, day))
        # replace=True + replace_index даёт INSERT ... ON CONFLICT (city, ts) DO UPDATE:
        # повторный запуск перезапишет строки, а не создаст дубли
        PostgresHook(postgres_conn_id="weather_db").insert_rows(
            table="hourly_weather",
            rows=rows,
            target_fields=["city", "ts", "temperature", "precipitation", "wind_speed"],
            replace=True,
            replace_index=["city", "ts"],
        )
        return city

    @task
    def build_daily_stats(data_interval_start=None, data_interval_end=None) -> int:
        # Одна задача на все города: считаем сводку прямо в Postgres, данные в Python не тянем.
        # Берём сутки run'а: ts в [начало, конец). Upsert по (city, date) — без дублей при перезапуске.
        hook = PostgresHook(postgres_conn_id="weather_db")
        hook.run(
            """
            INSERT INTO daily_stats
                (city, date, temp_min, temp_max, temp_avg, precipitation_sum, wind_speed_avg)
            SELECT city, %(day)s::date,
                   MIN(temperature), MAX(temperature), AVG(temperature),
                   SUM(precipitation), AVG(wind_speed)
            FROM hourly_weather
            WHERE ts >= %(start)s AND ts < %(end)s
            GROUP BY city
            ON CONFLICT (city, date) DO UPDATE SET
                temp_min          = EXCLUDED.temp_min,
                temp_max          = EXCLUDED.temp_max,
                temp_avg          = EXCLUDED.temp_avg,
                precipitation_sum = EXCLUDED.precipitation_sum,
                wind_speed_avg    = EXCLUDED.wind_speed_avg
            """,
            parameters={
                "day": data_interval_start.strftime("%Y-%m-%d"),
                "start": data_interval_start,
                "end": data_interval_end,
            },
        )
        # В XCom — только счётчик: сколько городов попало в сводку
        return hook.get_first(
            "SELECT COUNT(*) FROM daily_stats WHERE date = %s",
            parameters=(data_interval_start.strftime("%Y-%m-%d"),),
        )[0]

    # Сначала таблицы, потом по каждому городу: fetch -> validate -> transform_load,
    # затем одна сводка после всех городов
    fetched = fetch.expand(city=CITIES)
    create_tables() >> fetched
    validated = validate.expand(city=fetched)
    transform_load.expand(city=validated) >> build_daily_stats()


weather_pipeline()

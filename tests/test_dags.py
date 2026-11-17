"""Проверка, что все DAG'и импортируются без ошибок.

Запуск внутри контейнера:
    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests
"""

import sys
from datetime import datetime, timezone

from airflow.models import DagBag

# Чтобы тесты могли импортировать функции из файлов DAG'ов
sys.path.insert(0, "/opt/airflow/dags")


def test_dags_import_without_errors():
    dag_bag = DagBag(dag_folder="/opt/airflow/dags")
    assert dag_bag.import_errors == {}, f"Ошибки импорта: {dag_bag.import_errors}"


def test_hello_dag_structure():
    dag_bag = DagBag(dag_folder="/opt/airflow/dags")
    dag = dag_bag.get_dag("hello_dag")
    assert dag is not None
    assert {t.task_id for t in dag.tasks} == {"make_number", "print_number"}


def _good_hourly():
    # Нормальные сутки: 24 часа, все значения на месте
    return {
        "time": [f"2026-11-01T{h:02d}:00" for h in range(24)],
        "temperature_2m": [5.0] * 24,
        "precipitation": [0.0] * 24,
        "wind_speed_10m": [3.0] * 24,
    }


def test_check_hourly_ok():
    from weather_pipeline import check_hourly

    assert check_hourly(_good_hourly()) == []


def test_check_hourly_wrong_hours_count():
    from weather_pipeline import check_hourly

    hourly = _good_hourly()
    for key in hourly:
        hourly[key] = hourly[key][:23]
    assert check_hourly(hourly) != []


def test_check_hourly_missing_value():
    from weather_pipeline import check_hourly

    hourly = _good_hourly()
    hourly["wind_speed_10m"][5] = None
    assert check_hourly(hourly) != []


def test_check_hourly_nan():
    from weather_pipeline import check_hourly

    hourly = _good_hourly()
    hourly["precipitation"][0] = float("nan")
    assert check_hourly(hourly) != []


def test_check_hourly_temperature_out_of_range():
    from weather_pipeline import check_hourly

    for bad in (-60.1, 50.1):
        hourly = _good_hourly()
        hourly["temperature_2m"][10] = bad
        assert check_hourly(hourly) != []


def test_hourly_to_rows():
    from weather_pipeline import hourly_to_rows

    rows = hourly_to_rows("Kazan", _good_hourly())
    assert len(rows) == 24
    # Порядок: city, ts (UTC), temperature, precipitation, wind_speed
    assert rows[1] == ("Kazan", datetime(2026, 11, 1, 1, 0, tzinfo=timezone.utc), 5.0, 0.0, 3.0)


def test_weather_pipeline_structure():
    dag_bag = DagBag(dag_folder="/opt/airflow/dags")
    dag = dag_bag.get_dag("weather_pipeline")
    assert dag is not None
    assert {t.task_id for t in dag.tasks} == {
        "create_tables",
        "fetch",
        "validate",
        "transform_load",
        "build_daily_stats",
    }
    # Сводка строится только после загрузки всех городов
    assert dag.get_task("build_daily_stats").upstream_task_ids == {"transform_load"}

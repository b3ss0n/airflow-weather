"""Первый учебный DAG: две задачи и передача данных между ними через XCom."""

import pendulum
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    dag_id="hello_dag",
    # Явное расписание с интервалом данных: run за сутки X -> data_interval = [X 00:00, X+1 00:00) UTC.
    # С schedule="@daily" в Airflow 3 start и end интервала совпадали бы.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["learning"],
    doc_md="Минимальный DAG для проверки окружения.",
)
def hello_dag():
    @task
    def make_number() -> int:
        # Возвращаемое значение автоматически кладётся в XCom
        return 42

    @task
    def print_number(number: int, data_interval_start=None) -> None:
        # Airflow сам подставит контекст запуска по имени аргумента
        print(f"Получил число {number}, интервал данных начинается {data_interval_start}")

    # Передача результата первой задачи во вторую задаёт и зависимость между ними
    print_number(make_number())


hello_dag()

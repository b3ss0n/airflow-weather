"""Проверка, что все DAG'и импортируются без ошибок.

Запуск внутри контейнера:
    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests
"""

from airflow.models import DagBag


def test_dags_import_without_errors():
    dag_bag = DagBag(dag_folder="/opt/airflow/dags")
    assert dag_bag.import_errors == {}, f"Ошибки импорта: {dag_bag.import_errors}"


def test_hello_dag_structure():
    dag_bag = DagBag(dag_folder="/opt/airflow/dags")
    dag = dag_bag.get_dag("hello_dag")
    assert dag is not None
    assert {t.task_id for t in dag.tasks} == {"make_number", "print_number"}

# airflow-weather

Учебный пайплайн на Apache Airflow 3: ежедневная загрузка погоды из Open-Meteo в Postgres.

## Запуск

```bash
cp .env.example .env        # затем поменяй пароли и секрет внутри
mkdir -p logs               # на Linux ещё: echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up -d
```

Первый запуск занимает несколько минут (скачиваются образы). UI: http://localhost:8080

## Полезные команды

```bash
docker compose ps                                      # статус сервисов
docker compose logs -f airflow-scheduler               # логи планировщика
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests
docker compose down                                    # остановить (данные сохранятся)
docker compose down -v                                 # остановить и удалить базу
```

## Структура

```
dags/       DAG'и (подхватываются автоматически)
tests/      тесты DAG'ов
postgres/   скрипт создания базы weather
sql/        SQL для таблиц проекта
```

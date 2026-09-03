from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# Идентификатор подключения к PostgreSQL в Airflow Connections
POSTGRES_CONN_ID = 'postgres_conn'

with DAG(
    dag_id='00_init_dwh',
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Запуск только вручную (Manual Trigger)
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql'],  # Путь к папке со скриптом внутри контейнера
    tags=['manual','init']
) as dag:

    # Применение DDL: пересоздание схем, таблиц, ключей и индексов
    apply_full_database_ddl = SQLExecuteQueryOperator(
        task_id='apply_full_database_ddl',
        conn_id=POSTGRES_CONN_ID,
        sql='create_schema.sql'
    )

    apply_full_database_ddl
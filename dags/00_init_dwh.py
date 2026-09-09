from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# Идентификатор подключения к PostgreSQL в Airflow Connections
POSTGRES_CONN_ID = 'postgres_conn'
# Идентификатор подключения к ClickHouse в Airflow Connections (креды не хардкодятся)
CLICKHOUSE_CONN_ID = 'clickhouse_conn'


def get_clickhouse_client():
    from clickhouse_driver import Client

    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    return Client(
        host=conn.host,
        port=conn.port or 9000,
        user=conn.login,
        password=conn.password,
        database=conn.schema or 'dm',
    )


with DAG(
    dag_id='00_init_dwh',
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Запуск только вручную (Manual Trigger)
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql'],  # Путь к папке со скриптом внутри контейнера
    tags=['manual', 'init']
) as dag:

    # Применение DDL в PostgreSQL: пересоздание схем, таблиц, ключей и индексов
    apply_full_database_ddl = SQLExecuteQueryOperator(
        task_id='apply_full_database_ddl',
        conn_id=POSTGRES_CONN_ID,
        sql='create_schema.sql'
    )

    # Создание витрины в ClickHouse (слой dm)
    @task(task_id='init_clickhouse_dm')
    def init_clickhouse_dm():
        client = get_clickhouse_client()
        try:
            client.execute("CREATE DATABASE IF NOT EXISTS dm")
            client.execute(
                """
                CREATE TABLE IF NOT EXISTS dm.monthly_summary
                (
                    report_month        Date,
                    country             String,
                    sum_deposits_usd    Decimal(38, 2),
                    sum_withdrawals_usd Decimal(38, 2),
                    sum_bets_usd        Decimal(38, 2)
                )
                ENGINE = MergeTree
                ORDER BY (report_month, country)
                """
            )
        finally:
            client.disconnect()

    init_dm = init_clickhouse_dm()

    apply_full_database_ddl >> init_dm


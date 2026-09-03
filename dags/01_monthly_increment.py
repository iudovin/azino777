from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

POSTGRES_CONN_ID = 'postgres_conn'
CSV_DIR = '/opt/airflow/dags/csv'
DBT_BIN = '/opt/airflow/dbt_venv/bin/dbt'
DBT_DIR = '/opt/airflow/dags/dbt'

TABLES = [
    'providers_map',
    'games_map',
    'players',
    'currency_rates',
    'deposits',
    'withdrawals',
    'games'
]

with DAG(
    dag_id='01_monthly_increment',
    start_date=datetime(2026, 1, 1),
    schedule='@monthly',
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/dags/sql'],
    tags=['auto','etl','raw','dds','dm']
) as dag:

    # Идемпотентная загрузка статичных CSV-файлов в схему raw
    @task(task_id='load_csv_to_raw')
    def load_csv_to_raw():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for table in TABLES:
                    csv_path = os.path.join(CSV_DIR, f"{table}.csv")
                    if os.path.exists(csv_path):
                        # Полная очистка перед COPY гарантирует отсутствие дубликатов при перезапуске
                        cur.execute(f"TRUNCATE TABLE raw.{table};")
                        copy_sql = f"COPY raw.{table} FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')"
                        with open(csv_path, 'r', encoding='utf-8') as f:
                            cur.copy_expert(sql=copy_sql, file=f)
                        print(f"Таблица raw.{table} загружена из {csv_path}")
                    else:
                        raise FileNotFoundError(f"Файл {csv_path} не найден!")
            conn.commit()

    load_raw = load_csv_to_raw()

    # Трансформация данных через dbt (сборка DDS и витрины dm.monthly_summary)
    dbt_run = BashOperator(
        task_id='dbt_run_transformations',
        bash_command=f"{DBT_BIN} run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    )

    # Проверка качества данных и целостности связей (опционально)
    dbt_test = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=f"{DBT_BIN} test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    )

    load_raw >> dbt_run >> dbt_test
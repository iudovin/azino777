from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import json
import csv
import time

POSTGRES_CONN_ID = 'postgres_conn'
CLICKHOUSE_CONN_ID = 'clickhouse_conn'
CSV_DIR = '/opt/airflow/dags/csv'
DBT_BIN = '/opt/airflow/dbt_venv/bin/dbt'
DBT_DIR = '/opt/airflow/dags/dbt'

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')


def get_clickhouse_client():
    from clickhouse_driver import Client

    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    # Подключаемся к существующей БД 'default'; таблица везде указывается полностью (dm.monthly_summary).
    return Client(
        host=conn.host,
        port=conn.port or 9000,
        user=conn.login,
        password=conn.password,
        database='default',
    )

TABLES = [
    'providers_map',
    'games_map',
    'players',
    'currency_rates',
    'deposits',
    'withdrawals',
    'games'
]

# Маппинг: колонка raw-таблицы -> поле в CSV-заголовке (где имена не совпадают).
# providers_map/games_map в CSV используют `id`, а в raw-слое колонки называются provider_id/game_id.
COLUMN_ALIASES = {
    'provider_id': 'id',
    'game_id': 'id',
}


def kafka_topic(table: str) -> str:
    return f"raw.{table}"


with DAG(
    dag_id='01_monthly_increment',
    start_date=datetime(2026, 1, 1),
    schedule='@monthly',
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/opt/airflow/dags/sql'],
    tags=['auto', 'etl', 'kafka', 'raw', 'dds', 'dm', 'clickhouse']
) as dag:

    # 1. Producer: читает CSV-файлы и публикует каждую строку как JSON в топик raw.<table>
    @task(task_id='publish_csv_to_kafka')
    def publish_csv_to_kafka():
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',
            linger_ms=50,
        )
        try:
            for table in TABLES:
                csv_path = os.path.join(CSV_DIR, f"{table}.csv")
                if not os.path.exists(csv_path):
                    raise FileNotFoundError(f"Файл {csv_path} не найден!")
                topic = kafka_topic(table)
                count = 0
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        producer.send(topic, value={k: ('' if v is None else v) for k, v in row.items()})
                        count += 1
                producer.flush()
                print(f"Топик {topic}: опубликовано {count} записей из {csv_path}")
        finally:
            producer.flush()
            producer.close()

    publish = publish_csv_to_kafka()

    # 2. Consumer: читает топики raw.<table> из Kafka и идемпотентно заливает в raw-слой PostgreSQL
    @task(task_id='consume_kafka_to_raw')
    def consume_kafka_to_raw():
        from kafka import KafkaConsumer

        group_id = f"csv-loader-{int(time.time() * 1000)}"  # свежая группа -> начинаем с earliest
        consumer = KafkaConsumer(
            *[kafka_topic(t) for t in TABLES],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=20000,
        )

        batches = {t: [] for t in TABLES}

        for msg in consumer:
            table = msg.topic.split('.', 1)[1]
            if table not in batches:
                continue
            batches[table].append(msg.value)
        consumer.close()

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for table in TABLES:
                    # Полная очистка перед загрузкой гарантирует отсутствие дубликатов при перезапуске
                    cur.execute(f"TRUNCATE TABLE raw.{table};")
                    if not batches[table]:
                        continue

                    # Реальные колонки raw-таблицы в правильном порядке (соответствие «по позиции»,
                    # как при COPY). Для providers_map/games_map заголовок CSV отличается от схемы.
                    cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'raw' AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                        (table,),
                    )
                    cols = [row[0] for row in cur.fetchall()]
                    placeholders = ', '.join(['%s'] * len(cols))
                    sql = f"INSERT INTO raw.{table} ({', '.join(cols)}) VALUES ({placeholders})"

                    rows = []
                    for record in batches[table]:
                        values = []
                        for col in cols:
                            if col in record:
                                value = record[col]
                            elif col in COLUMN_ALIASES:
                                value = record.get(COLUMN_ALIASES[col], '')
                            else:
                                value = ''
                            # Пустая строка для числовых колонок -> NULL (эквивалент поведения COPY)
                            values.append(value if value != '' else None)
                        rows.append(values)

                    cur.executemany(sql, rows)
                    print(f"raw.{table}: загружено {len(batches[table])} записей")
            conn.commit()

    consume = consume_kafka_to_raw()

    # 3. Трансформация данных через dbt (сборка DDS и витрины dm.monthly_summary в PostgreSQL)
    dbt_run = BashOperator(
        task_id='dbt_run_transformations',
        bash_command=f"{DBT_BIN} run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    )

    # 4. Синхронизация слоя витрин dm.monthly_summary из PostgreSQL в ClickHouse
    @task(task_id='sync_dm_to_clickhouse')
    def sync_dm_to_clickhouse():
        from clickhouse_driver import Client

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT report_month, country,
                           COALESCE(sum_deposits_usd, 0),
                           COALESCE(sum_withdrawals_usd, 0),
                           COALESCE(sum_bets_usd, 0)
                    FROM dm.monthly_summary
                    ORDER BY report_month, country
                    """
                )
                rows = cur.fetchall()

        client = get_clickhouse_client()
        try:
            client.execute("TRUNCATE TABLE dm.monthly_summary")
            if rows:
                client.execute(
                    "INSERT INTO dm.monthly_summary "
                    "(report_month, country, sum_deposits_usd, sum_withdrawals_usd, sum_bets_usd) VALUES",
                    rows,
                )
            print(f"ClickHouse dm.monthly_summary: синхронизировано {len(rows)} строк")
        finally:
            client.disconnect()

    sync_dm = sync_dm_to_clickhouse()

    # 5. Проверка качества данных и целостности связей (опционально)
    dbt_test = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=f"{DBT_BIN} test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    )

    publish >> consume >> dbt_run >> sync_dm >> dbt_test

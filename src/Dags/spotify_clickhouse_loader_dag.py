"""
Airflow DAG: загрузка событий из Postgres-staging в ClickHouse (DWH).
"""

import os
import sys
from datetime import datetime as dt, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

sys.path.append("/opt/airflow/src")
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from spotify_pipeline.clickhouse_repo import load_staging_into_clickhouse


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": dt(2025, 1, 29, tzinfo=timezone.utc),
    "email": ["airflow_admin_8458@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=1),
}


dag = DAG(
    dag_id="spotify_clickhouse_loader_pipeline",
    default_args=default_args,
    description="Load Spotify staging into ClickHouse (multi-tenant, all artists aggregation)",
    schedule_interval=timedelta(minutes=int(os.getenv("SPOTIFY_CLICKHOUSE_SCHEDULE_MINUTES", "10"))),
    catchup=False,
    tags=["spotify", "clickhouse", "etl"],
)


def load_staging_to_clickhouse() -> None:
    clickhouse_batch_limit = int(os.getenv("SPOTIFY_CLICKHOUSE_BATCH_LIMIT", "5000"))
    hook = PostgresHook(postgres_conn_id="postgre_sql")
    connection = hook.get_connection("postgre_sql")

    db_url = (
        f"postgresql+psycopg2://{connection.login}:{connection.password}"
        f"@{connection.host}:{connection.port}/{connection.schema}"
    )
    engine = create_engine(db_url)

    loaded = load_staging_into_clickhouse(staging_engine=engine, limit=clickhouse_batch_limit)
    print(f"ClickHouse load finished. rows_loaded={loaded}")


with dag:
    run_clickhouse_load = PythonOperator(
        task_id="load_staging_into_clickhouse",
        python_callable=load_staging_to_clickhouse,
    )


"""
Airflow DAG: извлечение Spotify "recently played" в Postgres-staging (multi-tenant).

Multi-tenant:
  - учётные данные берутся из SPOTIFY_ACCOUNTS_JSON (или из single-tenant переменных окружения)
  - курсор хранится по паре (tenant_id, spotify_user_id) в таблице stg_pipeline_state

Требование аналитики:
  - события содержат ВСЕХ артистов трека (track.artists), чтобы ClickHouse мог считать по каждому артисту через arrayJoin().
"""

import os
import sys
from datetime import datetime as dt, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

# Контейнер Airflow монтирует код в /opt/airflow/src (см. docker-compose).
sys.path.append("/opt/airflow/src")
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from spotify_pipeline.accounts import load_spotify_accounts
from spotify_pipeline.spotify_client import extract_recently_played
from spotify_pipeline.staging_repo import (
    create_staging_tables,
    get_last_after_unix_ms,
    insert_events_dedup,
    upsert_last_after_unix_ms,
)


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
    dag_id="spotify_staging_pipeline",
    default_args=default_args,
    description="Extract Spotify recently played into Postgres staging (multi-tenant)",
    schedule_interval=timedelta(minutes=int(os.getenv("SPOTIFY_STAGING_SCHEDULE_MINUTES", "50"))),
    catchup=False,
    tags=["spotify", "staging", "postgres"],
)


def extract_and_load_to_staging() -> None:
    print("Starting Spotify staging extraction...")

    overlap_minutes = int(os.getenv("SPOTIFY_CURSOR_OVERLAP_MINUTES", "15"))
    initial_lookback_hours = int(os.getenv("SPOTIFY_INITIAL_LOOKBACK_HOURS", "24"))
    extract_limit = int(os.getenv("SPOTIFY_EXTRACT_LIMIT", "50"))

    hook = PostgresHook(postgres_conn_id="postgre_sql")
    connection = hook.get_connection("postgre_sql")

    db_url = (
        f"postgresql+psycopg2://{connection.login}:{connection.password}"
        f"@{connection.host}:{connection.port}/{connection.schema}"
    )
    engine = create_engine(db_url)

    create_staging_tables(engine)
    accounts = load_spotify_accounts()
    now_ms = int(dt.now(timezone.utc).timestamp() * 1000)

    total_events_attempted = 0
    for acc in accounts:
        last_after = get_last_after_unix_ms(engine, tenant_id=acc.tenant_id, spotify_user_id=acc.spotify_user_id)

        base_after = last_after if last_after is not None else now_ms - (initial_lookback_hours * 3600 * 1000)
        after_unix_ms = max(0, int(base_after - (overlap_minutes * 60 * 1000)))

        print(
            f"[{acc.tenant_id}/{acc.spotify_user_id}] Extract after_unix_ms={after_unix_ms} (last_after={last_after})"
        )

        events, max_played_at_ms = extract_recently_played(
            tenant_id=acc.tenant_id,
            spotify_user_id=acc.spotify_user_id,
            spotify_token=acc.spotify_token,
            after_unix_ms=after_unix_ms,
            limit=extract_limit,
        )
        inserted_attempts = insert_events_dedup(engine, events)
        total_events_attempted += inserted_attempts

        if max_played_at_ms is not None:
            upsert_last_after_unix_ms(
                engine,
                tenant_id=acc.tenant_id,
                spotify_user_id=acc.spotify_user_id,
                last_after_unix_ms=max_played_at_ms,
            )

        print(
            f"[{acc.tenant_id}/{acc.spotify_user_id}] extracted={len(events)} inserted_attempts={inserted_attempts} new_cursor={max_played_at_ms}"
        )

    print(f"Staging extraction finished. accounts={len(accounts)}, inserted_attempts={total_events_attempted}")


with dag:
    run_staging = PythonOperator(
        task_id="extract_and_load_to_staging",
        python_callable=extract_and_load_to_staging,
    )


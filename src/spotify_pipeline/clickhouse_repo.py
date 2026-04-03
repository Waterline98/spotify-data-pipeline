import os
from datetime import datetime
from typing import Any, Iterable, List, Optional, Sequence, Tuple

from dotenv import load_dotenv
from sqlalchemy import text

from clickhouse_driver import Client


load_dotenv()


STG_EVENTS_TABLE = "stg_spotify_recently_played_events"


def _get_clickhouse_client() -> Client:
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DATABASE") or os.getenv("CLICKHOUSE_DB") or "spotify"

    return Client(host=host, port=port, user=user, password=password, database=database)


def create_clickhouse_objects(client: Client) -> None:
    database = os.getenv("CLICKHOUSE_DATABASE") or os.getenv("CLICKHOUSE_DB") or "spotify"
    client.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    client.execute(f"USE {database}")

    client.execute(
        """
        CREATE TABLE IF NOT EXISTS listens (
            tenant_id String,
            spotify_user_id String,
            event_hash String,
            played_at DateTime64(3),
            track_id String,
            track_name String,
            artist_ids Array(String),
            artist_names Array(String),
            ingest_ts DateTime64(3)
        )
        ENGINE = ReplacingMergeTree(ingest_ts)
        ORDER BY (tenant_id, spotify_user_id, event_hash)
        """
    )

    client.execute(
        """
        CREATE TABLE IF NOT EXISTS artist_daily (
            tenant_id String,
            spotify_user_id String,
            day Date,
            artist_id String,
            artist_name String,
            listen_count UInt64
        )
        ENGINE = SummingMergeTree(listen_count)
        ORDER BY (tenant_id, spotify_user_id, day, artist_id, artist_name)
        """
    )

    # Подсчёт по всем артистам: разворачиваем artist_ids+artist_names через ARRAY JOIN.
    # arrayZip сохраняет позиционное соответствие id и имени.
    client.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS artist_daily_mv
        TO artist_daily
        AS
        SELECT
            tenant_id,
            spotify_user_id,
            toDate(played_at) AS day,
            z.1 AS artist_id,
            z.2 AS artist_name,
            count() AS listen_count
        FROM listens
        ARRAY JOIN arrayZip(artist_ids, artist_names) AS z
        GROUP BY tenant_id, spotify_user_id, day, artist_id, artist_name
        """
    )


def load_unloaded_events_from_staging(
    *,
    staging_engine,
    limit: int = 5000,
) -> List[Tuple[Any, ...]]:
    query = text(
        f"""
        SELECT
            tenant_id,
            spotify_user_id,
            event_hash,
            played_at,
            track_id,
            track_name,
            artist_ids,
            artist_names,
            ingest_ts
        FROM {STG_EVENTS_TABLE}
        WHERE clickhouse_loaded_at IS NULL
        ORDER BY ingest_ts ASC
        LIMIT :limit
        """
    )
    with staging_engine.begin() as conn:
        rows = conn.execute(query, {"limit": limit}).fetchall()
    return rows


def mark_events_loaded_in_staging(*, staging_engine, event_hashes: Sequence[str]) -> None:
    if not event_hashes:
        return

    # event_hash — sha256 от (tenant_id, spotify_user_id, ...).
    query = text(
        f"""
        UPDATE {STG_EVENTS_TABLE}
        SET clickhouse_loaded_at = NOW()
        WHERE event_hash = ANY(:event_hashes)
        """
    )
    with staging_engine.begin() as conn:
        conn.execute(query, {"event_hashes": list(event_hashes)})


def insert_events_into_clickhouse(*, client: Client, events: Sequence[Tuple[Any, ...]]) -> None:
    if not events:
        return
    client.execute(
        """
        INSERT INTO listens (
            tenant_id,
            spotify_user_id,
            event_hash,
            played_at,
            track_id,
            track_name,
            artist_ids,
            artist_names,
            ingest_ts
        ) VALUES
        """,
        events,
    )


def load_staging_into_clickhouse(*, staging_engine, limit: int = 5000) -> int:
    client = _get_clickhouse_client()
    create_clickhouse_objects(client)

    rows = load_unloaded_events_from_staging(staging_engine=staging_engine, limit=limit)
    if not rows:
        return 0

    insert_events_into_clickhouse(client=client, events=rows)

    event_hashes = [r[2] for r in rows]  # event_hash is 3rd column
    mark_events_loaded_in_staging(staging_engine=staging_engine, event_hashes=event_hashes)

    return len(rows)


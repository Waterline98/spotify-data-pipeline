import datetime as dt
from typing import Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import text

from spotify_pipeline.spotify_client import SpotifyListenEvent


STG_EVENTS_TABLE = "stg_spotify_recently_played_events"
STG_STATE_TABLE = "stg_pipeline_state"


def create_staging_tables(engine) -> None:
    """
    Создаёт staging-tables в Postgres, если они ещё не существуют.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {STG_STATE_TABLE} (
                    tenant_id TEXT NOT NULL,
                    spotify_user_id TEXT NOT NULL,
                    last_after_unix_ms BIGINT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, spotify_user_id)
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {STG_EVENTS_TABLE} (
                    tenant_id TEXT NOT NULL,
                    spotify_user_id TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    played_at TIMESTAMPTZ NOT NULL,
                    cursor_after BIGINT NOT NULL,
                    track_id TEXT NOT NULL,
                    track_name TEXT NOT NULL,
                    artist_ids TEXT[] NOT NULL,
                    artist_names TEXT[] NOT NULL,
                    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    clickhouse_loaded_at TIMESTAMPTZ NULL,
                    raw_payload_json TEXT NOT NULL,
                    CONSTRAINT stg_events_unique_event UNIQUE (tenant_id, spotify_user_id, event_hash)
                );

                CREATE INDEX IF NOT EXISTS stg_events_clickhouse_ready_idx
                    ON {STG_EVENTS_TABLE} (ingest_ts)
                    WHERE clickhouse_loaded_at IS NULL;
                """
            )
        )


def get_last_after_unix_ms(engine, *, tenant_id: str, spotify_user_id: str) -> Optional[int]:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT last_after_unix_ms
                FROM {STG_STATE_TABLE}
                WHERE tenant_id = :tenant_id AND spotify_user_id = :spotify_user_id
                """
            ),
            {"tenant_id": tenant_id, "spotify_user_id": spotify_user_id},
        ).fetchone()
        if not row:
            return None
        return row[0]


def upsert_last_after_unix_ms(engine, *, tenant_id: str, spotify_user_id: str, last_after_unix_ms: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {STG_STATE_TABLE} (tenant_id, spotify_user_id, last_after_unix_ms, updated_at)
                VALUES (:tenant_id, :spotify_user_id, :last_after_unix_ms, NOW())
                ON CONFLICT (tenant_id, spotify_user_id)
                DO UPDATE SET last_after_unix_ms = EXCLUDED.last_after_unix_ms, updated_at = NOW()
                """
            ),
            {"tenant_id": tenant_id, "spotify_user_id": spotify_user_id, "last_after_unix_ms": last_after_unix_ms},
        )


def insert_events_dedup(engine, events: Sequence[SpotifyListenEvent]) -> int:
    """
    Вставляет события с ON CONFLICT DO NOTHING — задачу можно безопасно переигрывать.
    Возвращает приблизительное количество вставок (по числу попыток); для точного счёта нужен RETURNING.
    """
    if not events:
        return 0

    sql = text(
        f"""
        INSERT INTO {STG_EVENTS_TABLE} (
            tenant_id, spotify_user_id, event_hash,
            played_at, cursor_after,
            track_id, track_name,
            artist_ids, artist_names,
            raw_payload_json
        ) VALUES (
            :tenant_id, :spotify_user_id, :event_hash,
            :played_at, :cursor_after,
            :track_id, :track_name,
            :artist_ids, :artist_names,
            :raw_payload_json
        )
        ON CONFLICT (tenant_id, spotify_user_id, event_hash) DO NOTHING
        """
    )

    inserted_attempts = 0
    with engine.begin() as conn:
        for e in events:
            conn.execute(
                sql,
                {
                    "tenant_id": e.tenant_id,
                    "spotify_user_id": e.spotify_user_id,
                    "event_hash": e.event_hash,
                    "played_at": e.played_at,
                    "cursor_after": e.cursor_after,
                    "track_id": e.track_id,
                    "track_name": e.track_name,
                    "artist_ids": e.artist_ids,
                    "artist_names": e.artist_names,
                    "raw_payload_json": e.raw_payload_json,
                },
            )
            inserted_attempts += 1
    return inserted_attempts


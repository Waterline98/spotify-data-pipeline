
# Spotify Data Pipeline

Multi-tenant ETL-пайплайн: Spotify -> Postgres staging -> ClickHouse (DWH) через Apache Airflow.

В аналитике прослушивания считаются "по всем артистам трека" (`track.artists`), а агрегации считаются в ClickHouse.

## Функционал
* Извлечение `recently-played` из Spotify API (по cursor `after`) для каждого tenant/user.
* Идемпотентная загрузка в Postgres staging через дедупликацию по `event_hash` (без `replace`).
* Загрузка новых событий в ClickHouse и построение дневной агрегации `artist_daily` по всем артистам.

## Настройка
* В Airflow должен быть создан Connection с id `postgre_sql`, указывающий на Postgres staging (где будут созданы `stg_*` таблицы).
* Для multi-tenant укажите `SPOTIFY_ACCOUNTS_JSON` как JSON-массив объектов `{ tenant_id, spotify_user_id, spotify_token }`.
* Либо оставьте fallback single-tenant: `TENANT_ID` (опционально), `SPOTIFY_USER_ID`, `SPOTIFY_TOKEN`.
* Для ClickHouse задайте: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT` (по умолчанию 9000), `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE` (по умолчанию `spotify`).
* Для точной настройки курсора (window): `SPOTIFY_CURSOR_OVERLAP_MINUTES` и `SPOTIFY_INITIAL_LOOKBACK_HOURS`.

## Струтура проекта

Spotify-data-pipeline/
├── .gitignore              
├── requirements.txt        
└── src/                    
    ├── Dags/
    │   ├── spotify_staging_pipeline_dag.py
    │   └── spotify_clickhouse_loader_dag.py
    ├── spotify_pipeline/
    └── docker-compose.yaml

## Требования

* Python 3.8+
* PostgreSQL 13+
* Apache Airflow 2.5.1+
* ClickHouse
* Spotify API-токены для tenant/user



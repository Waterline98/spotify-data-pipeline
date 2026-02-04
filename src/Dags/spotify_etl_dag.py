"""
DAG Airflow для регулярного ETL-процесса:
- Извлечение данных о прослушанных треках из Spotify API.
- Проверка качества данных.
- Загрузка в PostgreSQL.

Зависимости:
- spotify_etl.py (основной ETL-модуль)
- Соединение Airflow 'postgre_sql' (настроено в UI)
"""

from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.utils.dates import days_ago
from spotify_etl import spotify_etl



# Конфигурация DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": dt.datetime(2023, 1, 29),
    "email": ["airflow_admin_8458@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
    "execution_timeout": dt.timedelta(hours=1),
}

dag = DAG(
    dag_id="spotify_etl_pipeline",
    default_args=default_args,
    description="ETL-процесс для загрузки данных Spotify в PostgreSQL",
    schedule_interval=dt.timedelta(minutes=50),
    catchup=False,
    tags=["spotify", "etl", "postgres"],
)



def etl_process():
    """
    Основной ETL-процесс:
    1. Извлечение данных через spotify_etl().
    2. Подключение к PostgreSQL через Airflow Connection.
    3. Загрузка DataFrame в таблицу.

    Raises:
        Exception: При ошибках извлечения или загрузки данных.
    """
    print("Запуск ETL-процесса...")

    # Шаг 1: Извлечение и проверка данных
    try:
        df = spotify_etl()  # Вызов из spotify_etl.py
        if df.empty:
            print("Данные отсутствуют. Пропуск загрузки.")
            return
    except Exception as e:
        print(f"Ошибка при извлечении данных: {e}")
        raise

    # Шаг 2: Подключение к PostgreSQL через Airflow Connection
    try:
        hook = PostgresHook(postgres_conn_id="postgre_sql")
        connection = hook.get_connection("postgre_sql")

        # Формируем URL для SQLAlchemy
        db_url = (
            f"postgresql://{connection.login}:{connection.password}"
            f"@{connection.host}:{connection.port}/{connection.schema}"
        )
        engine = create_engine(db_url)

        # Шаг 3: Загрузка в БД
        df.to_sql(
            name="my_played_tracks",
            con=engine,
            if_exists="replace",  # Заменяем таблицу целиком
            index=False,
            method="multi",  # Ускоряет загрузку больших объёмов
        )
        print(f"Данные успешно загружены. Количество строк: {len(df)}")
    except Exception as e:
        print(f"Ошибка при загрузке в БД: {e}")
        raise



# Определение задач DAG
with dag:
    # Задача 1: Создание таблицы (если не существует)
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgre_sql",
        sql="""
            CREATE TABLE IF NOT EXISTS my_played_tracks (
                song_name VARCHAR(200),
                artist_name VARCHAR(200),
                played_at VARCHAR(200),
                timestamp VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
            );
        """,

        database="demo",
    )

    # Задача 2: Запуск ETL‑процесса
    run_etl = PythonOperator(
        task_id="spotify_etl_final",
        python_callable=etl_process,
        provide_context=True,
    )

    create_table >> run_etl

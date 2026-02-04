import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import Extract
import Transform


load_dotenv()

def get_db_engine():
    """
    Создаёт движок SQLAlchemy для подключения к PostgreSQL на основе переменных из .env.

    Возвращает:
        sqlalchemy.engine.Engine: Движок для работы с БД.
    """
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")

    if not all([db_host, db_port, db_name, db_user, db_pass]):
        raise ValueError("Не все переменные окружения для БД заданы в .env")

    database_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(database_url)



def create_tables(engine):
    """
    Создаёт таблицы в БД, если их ещё нет.

    Параметры:
        engine (sqlalchemy.engine.Engine): Движок SQLAlchemy.
    """
    with engine.connect() as conn:
        # Таблица для прослушанных треков
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS my_played_tracks (
                song_name VARCHAR(200),
                artist_name VARCHAR(200),
                played_at VARCHAR(200),
                timestamp VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
            );
            """
        )

        # Таблица для любимых артистов
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fav_artist (
                timestamp VARCHAR(200),
                ID VARCHAR(200),
                artist_name VARCHAR(200),
                count VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY (ID)
            );
            """
        )
        print("Таблицы созданы (если не существовали).")



def load_data_to_db(engine, load_df, transformed_df):
    """
    Загружает данные в таблицы БД, избегая дубликатов.

    Параметры:
        engine (sqlalchemy.engine.Engine): Движок SQLAlchemy.
        load_df (pd.DataFrame): Данные о прослушанных треках.
        transformed_df (pd.DataFrame): Трансформированные данные о любимых артистах.
    """
    try:
        load_df.to_sql(
            "my_played_tracks",
            engine,
            index=False,
            if_exists="append"
        )
        print("Данные загружены в таблицу my_played_tracks.")
    except Exception as e:
        print(f"Ошибка при загрузке в my_played_tracks: {e}")

    try:
        transformed_df.to_sql(
            "fav_artist",
            engine,
            index=False,
            if_exists="append"
        )
        print("Данные загружены в таблицу fav_artist.")
    except Exception as e:
        print(f"Ошибка при загрузке в fav_artist: {e}")



if __name__ == "__main__":
    try:
        # 1. Извлечение данных
        print("Извлечение данных из Extract...")
        load_df = Extract.return_dataframe()

        # 2. Проверка качества данных
        print("Проверка качества данных...")
        if not Transform.Data_Quality(load_df):
            raise ValueError("Проверка качества данных не пройдена.")

        # 3. Трансформация данных
        print("Трансформация данных...")
        transformed_df = Transform.Transform_df(load_df)

        # 4. Подключение к БД и создание таблиц
        print("Подключение к БД...")
        engine = get_db_engine()
        create_tables(engine)

        # 5. Загрузка данных в БД
        print("Загрузка данных в БД...")
        load_data_to_db(engine, load_df, transformed_df)

        print("Процесс завершён успешно.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")


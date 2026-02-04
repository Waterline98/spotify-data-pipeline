import sqlalchemy
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'my_database'),
    'user': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASSWORD', 'your_password')
}

# URL‑подключения для работы с PostgreSQL через библиотеку SQLAlchemy.
DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
    f"{DB_CONFIG['database']}"
)

# Список таблиц для удаления
TABLES_TO_DROP = [
    'my_played_tracks',
    'fav_artist'
]



def create_engine():
    """
    Создаёт движок SQLAlchemy для работы с PostgreSQL.

    Returns:
        sqlalchemy.engine.Engine: Объект движка для взаимодействия с БД.
    """
    try:
        engine = sqlalchemy.create_engine(DATABASE_URL)
        logger.info("Движок SQLAlchemy создан успешно.")
        return engine
    except Exception as e:
        logger.error(f"Ошибка при создании движка: {e}")
        raise



def drop_tables(engine, tables):
    """
    Удаляет перечисленные таблицы из базы данных.

    Args:
        engine (sqlalchemy.engine.Engine): Движок SQLAlchemy.
        tables (list): Список названий таблиц для удаления.
    """
    try:
        # Создаём сессию
        Session = sessionmaker(bind=engine)
        session = Session()

        # Начинаем транзакцию
        with engine.connect() as conn:
            for table_name in tables:
                # Проверяем существование таблицы
                if engine.dialect.has_table(conn, table_name):
                    logger.info(f"Удаляю таблицу: {table_name}")
                    conn.execute(sqlalchemy.text(f"DROP TABLE {table_name}"))
                else:
                    logger.warning(f"Таблица {table_name} не существует. Пропускаем.")
            
            # Явно завершаем транзакцию
            conn.commit()

        session.close()
        logger.info("Все указанные таблицы удалены успешно.")

    except Exception as e:
        logger.error(f"Ошибка при удалении таблиц: {e}")
        raise
    finally:
        if session:
            session.close()



def main():
    """
    Основная функция скрипта.
    Выполняет подключение к БД и удаление таблиц.
    """
    logger.info("Запуск скрипта удаления таблиц.")

    # Создаём движок
    engine = create_engine()

    # Удаляем таблицы
    drop_tables(engine, TABLES_TO_DROP)

    logger.info("Скрипт завершён.")



if __name__ == "__main__":
    main()

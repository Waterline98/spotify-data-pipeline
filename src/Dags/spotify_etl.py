"""
ETL-модуль для извлечения данных о прослушанных треках из Spotify API.

Функции:
- Извлечение последних 50 треков за последние 24 часа.
- Проверка качества данных (пустота, дубликаты, NULL).
- Трансформация: агрегация по артистам и датам.
- Возврат готового DataFrame.

Требования:
- Пакеты: requests, pandas, python-dotenv.
- Файл .env с переменными USER_ID и TOKEN.
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import os 

load_dotenv()
USER_ID = os.getenv("USER_ID")
TOKEN = os.getenv("TOKEN")

if not USER_ID or not TOKEN:
    raise ValueError("Переменные USER_ID и\или TOKEN не найдены в .env")

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



def return_dataframe() -> pd.DataFrame:
    """
    Извлекает данные о последних прослушанных треках из Spotify API.

    Returns:
        pd.DataFrame: Таблица с колонками:
            - song_name: название трека
            - artist_name: имя артиста
            - played_at: дата и время прослушивания (ISO 8601)
            - timestamp: дата в формате YYYY-MM-DD (без времени)

    Raises:
        requests.exceptions.RequestException: Ошибка HTTP-запроса.
        KeyError: Отсутствие ожидаемых ключей в JSON-ответе.
    """
    logger.info("Начало извлечения данных из Spotify API")

    # Формирование заголовков запроса
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Расчёт временной метки (24 часа назад в миллисекундах)
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_unix_ms = int(yesterday.timestamp() * 1000)

    # URL запроса к Spotify API
    url = (
        "https://api.spotify.com/v1/me/player/recently-played"
        f"?limit=50&after={yesterday_unix_ms}"
    )

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Проверяет статус 2xx
        data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к API: {e}")
        raise

    # Извлечение данных из JSON
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for item in data.get("items", []):
        try:
            song_names.append(item["track"]["name"])
            artist_names.append(item["track"]["album"]["artists"][0]["name"])
            played_at = item["played_at"]
            played_at_list.append(played_at)
            timestamps.append(played_at[:10])  # Дата в формате YYYY-MM-DD
        except KeyError as e:
            logger.warning(f"Пропущен элемент из-за отсутствующего ключа: {e}")

    # Создание DataFrame
    df = pd.DataFrame({
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    })

    logger.info(f"Извлечено {len(df)} треков")
    return df



def data_quality(df: pd.DataFrame) -> bool:
    """
    Проверяет качество данных.

    Args:
        df (pd.DataFrame): Входной DataFrame.

    Returns:
        bool: True, если данные корректны.

    Raises:
        ValueError: Если данные не соответствуют требованиям.
    """
    logger.info("Проверка качества данных")

    if df.empty:
        logger.error("Данные отсутствуют: DataFrame пуст")
        return False

    if not df['played_at'].is_unique:
        raise ValueError("Обнаружены дубликаты в колонке 'played_at'")

    if df.isnull().values.any():
        null_counts = df.isnull().sum()
        raise ValueError(f"Обнаружены NULL-значения: {null_counts.to_dict()}")

    logger.info("Проверка качества данных пройдена успешно")
    return True



def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Трансформирует данные: агрегирует количество прослушиваний по артистам и датам.

    Args:
        df (pd.DataFrame): Входной DataFrame.

    Returns:
        pd.DataFrame: Трансформированный DataFrame с колонками:
            - ID: уникальный ключ (дата-артист)
            - timestamp: дата
            - artist_name: артист
            - count: количество прослушиваний
    """
    logger.info("Начало трансформации данных")

    # Группировка и подсчёт
    transformed = df.groupby(['timestamp', 'artist_name'], as_index=False).size()
    transformed.rename(columns={'size': 'count'}, inplace=True)

    # Создание уникального ID
    transformed['ID'] = transformed['timestamp'] + '-' + transformed['artist_name']

    # Выбор финальных колонок
    result = transformed[['ID', 'timestamp', 'artist_name', 'count']]

    logger.info(f"Трансформация завершена. Получено {len(result)} записей")
    return result



def spotify_etl() -> pd.DataFrame:
    """
    Основной ETL-процесс: извлечение, проверка, трансформация данных.

    Returns:
        pd.DataFrame: Исходный DataFrame с треками (до трансформации).

    Raises:
        Exception: Если на любом этапе произошла ошибка.
    """
    logger.info("Запуск ETL-процесса")

    try:
        # Шаг 1: Извлечение данных
        raw_df = return_dataframe()

        # Шаг 2: Проверка качества
        if not data_quality(raw_df):
            logger.error("ETL прерван из-за некачественных данных")
            return pd.DataFrame()  # Возвращаем пустой DataFrame

        # Шаг 3: Трансформация
        transformed_df = transform_df(raw_df)

        # Логирование результата
        logger.info(f"ETL завершён. Исходные записи: {len(raw_df)}, трансформированные: {len(transformed_df)}")
        print(transformed_df)  # Вывод результата

        return raw_df

    except Exception as e:
        logger.exception(f"Ошибка в ETL-процессе: {e}")
        raise



if __name__ == "__main__":
    """
    Точка входа в скрипт. Запускает ETL-процесс при прямом запуске файла.
    """
    spotify_etl()


import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

SPOTIFY_USER_ID = os.getenv("SPOTIFY_USER_ID")
SPOTIFY_TOKEN = os.getenv("SPOTIFY_TOKEN")

if not SPOTIFY_TOKEN:
    raise ValueError("Токен Spotify не найден в .env. Проверьте переменную SPOTIFY_TOKEN.")



def get_recently_played_tracks(days_back: int = 2) -> pd.DataFrame:
    """
    Запрашивает данные о недавно прослушанных треках из Spotify API и возвращает их в виде DataFrame.

    Параметры:
        days_back (int): количество дней назад, от которых брать данные (по умолчанию 2).

    Возвращает:
        pd.DataFrame: таблица с колонками:
            - song_name (str): название трека.
            - artist_name (str): имя исполнителя.
            - played_at (str): ISO-формат времени воспроизведения.
            - timestamp (str): дата в формате YYYY-MM-DD.
    """
    # Формируем заголовки для запроса
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SPOTIFY_TOKEN}"
    }

    # Вычисляем временную метку (в миллисекундах)
    today = datetime.now()
    past_date = today - timedelta(days=days_back)
    unix_timestamp_ms = int(past_date.timestamp()) * 1000

    # URL запроса к Spotify API
    url = f"https://api.spotify.com/v1/me/player/recently-played?after={unix_timestamp_ms}"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Поднимает исключение для 4xx/5xx статусов

        data = response.json()

        # Проверяем наличие ключа "items"
        if "items" not in data:
            raise ValueError("Ответ API не содержит ключа 'items'.")

        song_names = []
        artist_names = []
        played_at_list = []
        timestamps = []

        for item in data["items"]:
            song_names.append(item["track"]["name"])
            # Берём первого артиста (может быть несколько)
            artist_names.append(item["track"]["album"]["artists"][0]["name"])
            played_at = item["played_at"]
            played_at_list.append(played_at)
            # Извлекаем дату (первые 10 символов ISO-строки)
            timestamps.append(played_at[:10])

        df = pd.DataFrame({
            "song_name": song_names,
            "artist_name": artist_names,
            "played_at": played_at_list,
            "timestamp": timestamps
        })

        return df

    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Ошибка запроса к Spotify API: {e}")
    except KeyError as e:
        raise KeyError(f"Отсутствует ожидаемый ключ в ответе API: {e}")
    except Exception as e:
        raise RuntimeError(f"Неожиданная ошибка: {e}")

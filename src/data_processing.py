from spotify_data_extractor import get_recently_played_tracks
import pandas as pd



def Data_Quality(load_df):
    """
    Проверяет качество входного DataFrame перед обработкой.

    Параметры:
        load_df (pd.DataFrame): входной DataFrame с данными о прослушиваниях.

    Возвращает:
        bool: True, если данные корректны; False, если DataFrame пуст.

    Исключения:
        Exception: если обнаружены дубликаты по первичному ключу или NULL-значения.
    """
    if load_df.empty:
        print('No Songs Extracted')
        return False

    if pd.Series(load_df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Exception, Data Might Contain duplicates")

    if load_df.isnull().values.any():
        raise Exception("Null values found")

    return True



def Transform_df(load_df):
    """
    Трансформирует входной DataFrame: группирует данные по 'timestamp' и 'artist_name',
    подсчитывает количество записей, создаёт уникальный ID.

    Параметры:
        load_df (pd.DataFrame): входной DataFrame с данными о прослушиваниях.

    Возвращает:
        pd.DataFrame: трансформированный DataFrame со столбцами:
            - ID (уникальный ключ);
            - timestamp;
            - artist_name;
            - count (количество прослушиваний).
    """
    # Группировка по 'timestamp' и 'artist_name', подсчёт количества записей
    Transformed_df = load_df.groupby(['timestamp', 'artist_name'], as_index=False).count()

    # Переименование столбца 'played_at' в 'count' (количество прослушиваний)
    Transformed_df.rename(columns={'played_at': 'count'}, inplace=True)

    # Создание уникального ID на основе 'timestamp' и 'artist_name'
    Transformed_df["ID"] = Transformed_df['timestamp'].astype(str) + "-" + Transformed_df["artist_name"]

    # Возврат только необходимых столбцов
    return Transformed_df[['ID', 'timestamp', 'artist_name', 'count']]



if __name__ == "__main__":
    # Импорт DataFrame с данными о прослушиваниях. Fix spotify_etl.py function return_dataframe()!
    load_df = get_recently_played_tracks(days_back=1)

    # Проверка качества данных
    Data_Quality(load_df)

    # Трансформация данных
    Transformed_df = Transform_df(load_df)

    # Вывод результата
    print(Transformed_df)

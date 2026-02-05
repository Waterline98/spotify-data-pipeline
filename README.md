
# Spotify Data Pipeline

ETL‑пайплайн для извлечения данных о прослушанных треках из Spotify API и загрузки в PostgreSQL через Apache Airflow.

## Функционал

* Извлечение данных о последних 50 треках за последние 24 часа из Spotify API.
* Проверка качества данных (пустота, дубликаты, NULL‑значения).
* Трансформация: агрегация по артистам и датам.
* Загрузка в PostgreSQL с использованием Airflow.

## Струтура проекта

Spotify-data-pipeline/
├── .gitignore              
├── requirements.txt        
└── src/                    
    ├── Dags/               
    │   ├── spotify_etl_dag.py  # Главный DAG: запускает ETL‑процесс каждые 50 мин
    │   └── spotify_etl.py     # Вспомогательный модуль для DAG: содержит логику ETL
    ├── data_processing.py    # Обработка и трансформация данных
    ├── docker-compose.yaml  # Конфигурация Docker для локального запуска
    ├── drop_tables.py      # Скрипт для удаления таблиц из PostgreSQL
    ├── load_music_data_to_postgres.py  # Загрузка данных в PostgreSQL
    └── spotify_data_extractor.py  # Извлечение данных из Spotify API

## Требования

* Python 3.8+
* PostgreSQL 13+
* Apache Airflow 2.5.1+
* Spotify API‑токен



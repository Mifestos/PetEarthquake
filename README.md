# PetEarthquake 🌋📊

**PetEarthquake** — это образовательный Data Engineering проект (Pet-проект) для построения сквозного ETL/ELT конвейера данных о землетрясениях. Проект автоматизирует сбор данных из внешних API, их промежуточное хранение, обработку и финальную визуализацию.

---

## 🏗 Архитектура и стек данных

Конвейер данных построен по классической схеме:
1. **Предобработка (Raw Layer):** Извлечение сырых данных из внешнего API землетрясений и их загрузка в объектное хранилище **S3** (`raw_from_api_to_s3.py`).
2. **Слой ODS (Operational Data Store):** Перенос данных из S3 в реляционную базу данных **PostgreSQL** (`raw_from_s3_to_pg.py`).
3. **Витрины данных (Data Marts):** Расчет агрегатов и метрик, таких как средняя магнитуда и количество землетрясений в день (`fct_avg_day_earthquake.py`, `fct_count_day_earthquake.py`).
4. **Визуализация:** Построение дашбордов в BI-системе **Metabase**.

### Инструменты:
* **Оркестрация:** Apache Airflow (задачи описаны с помощью `PythonOperator` и `EmptyOperator`).
* **Инфраструктура:** Docker, Docker Compose (Airflow + PostgreSQL + Metabase).
* **Хранилища данных:** AWS S3 (или аналог MinIO), PostgreSQL.
* **Язык разработки:** Python 3.11+

---

## 📁 Структура репозитория

```text
PetEarthquake/
├── config/               # Конфигурационные файлы
├── dags/                 # Направленные ациклические графы (DAGs) Airflow
│   ├── fct_avg_day_earthquake.py   # Расчет средних показателей за день
│   ├── fct_count_day_earthquake.py # Подсчет количества событий за день
│   ├── raw_from_api_to_s3.py       # Скрипт: API ➔ S3
│   └── raw_from_s3_to_pg.py        # Скрипт: S3 ➔ PostgreSQL (ODS)
├── data/                 # Локальные тестовые данные или кэш
├── logs/                 # Логи выполнения задач Airflow
├── metabase/             # Настройки и метаданные для дашбордов
├── plugins/              # Кастомные плагины для Airflow
├── Dockerfile            # Инструкции для сборки кастомного образа Airflow
├── docker-compose.yaml   # Локальное развертывание всей инфраструктуры
├── cred.py               # Управление секретами и доступами (локально)
└── requirements.txt      # Зависимости Python-окружения
```

---

## 🚀 Быстрый запуск

### Локальное окружение (для разработки)

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com
   cd PetEarthquake
   ```

2. Создайте и активируйте виртуальное окружение:
   ```bash
   python -m venv .venv
   # Для Windows (PowerShell):
   .venv\Scripts\Activate.ps1
   # Для Linux/macOS:
   source .venv/bin/activate
   ```

3. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

### Развертывание инфраструктуры в Docker

Для запуска Airflow, PostgreSQL и Metabase одной командой выполните:

```bash
docker-compose up -d --build
```

После успешного запуска сервисы будут доступны по следующим адресам:
* **Apache Airflow:** `http://localhost:8080`
* **Metabase:** `http://localhost:3000`

---

## ⚙️ Настройка Airflow Connections

Для работы DAG-ов необходимо настроить следующие подключения в веб-интерфейсе Airflow (Admin -> Connections):
* `aws_default` / `s3_conn` — для доступа к вашему S3 бакету.
* `ods_pg` — подключение к базе данных PostgreSQL.

---

## 📄 Лицензия

Проект распространяется под лицензией MIT. Подробнее см. в файле [LICENSE](LICENSE).

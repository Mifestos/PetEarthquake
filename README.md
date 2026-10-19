PetEarthquake
Сквозной ETL/ELT-пайплайн для данных о землетрясениях: от внешнего API до BI-дашбордов

Pet-проект по Data Engineering, в котором собран полный жизненный цикл данных: извлечение из внешнего REST API, загрузка в объектное хранилище (Raw Layer), трансформация в реляционную БД (ODS), построение аналитических витрин (Data Marts) и визуализация в BI. Всё разворачивается одной командой через Docker Compose, а оркестрация задач вынесена в Apache Airflow.

Зачем и для кого этот проект
Проект задумывался как учебный, но по структуре повторяет реальные конвейеры данных, которые встречаются в командах Data Engineering: слоистое хранилище, разделение ответственности между Raw, ODS и Mart-слоями, воспроизводимая инфраструктура. Вместо разрозненных скриптов здесь собран сквозной пайплайн, где каждый слой отвечает за свою задачу, а Airflow управляет зависимостями между ними.

Как это работает на верхнем уровне:

Сбор данных: Извлечение сырых данных о землетрясениях из внешнего API и загрузка в объектное хранилище S3 (MinIO) — слой Raw.

Слой ODS: Перенос структурированных данных из S3 в PostgreSQL — операционный слой.

Витрины данных: Расчёт агрегатов и метрик — средняя магнитуда за день и количество землетрясений в день.

Визуализация: Построение дашбордов в Metabase.

Архитектура и технические решения
1. Слоистая архитектура хранилища
Данные проходят через три уровня, и каждый уровень решает свою задачу:

Слой	Назначение	Технология
Raw	Сырые, неизменяемые данные из API	S3 / MinIO
ODS	Структурированные операционные данные	PostgreSQL
Mart	Агрегаты и метрики для аналитики	PostgreSQL
BI	Дашборды и визуализация	Metabase
Такое разделение позволяет пересчитывать витрины без повторного обращения к внешнему API и упрощает отладку: всегда видно, на каком слое произошла ошибка.

Схема конвейера:

text
Внешний API ──▶ S3 / MinIO ──▶ PostgreSQL ──▶ Data Marts ──▶ Metabase
              (Raw Layer)      (ODS Layer)    (витрины)        (BI)
Оркестрация всего пайплайна — Apache Airflow.

2. Оркестрация через Apache Airflow
Задачи описаны с помощью PythonOperator и EmptyOperator, зависимости между ними выстроены в DAG-и. Это даёт декларативное описание пайплайна: видно, что от чего зависит и в каком порядке выполняется, а не набор ручных запусков скриптов.

DAG-и проекта:

raw_from_api_to_s3.py — API → S3 (Raw Layer)

raw_from_s3_to_pg.py — S3 → PostgreSQL (ODS)

fct_avg_day_earthquake.py — витрина со средней магнитудой за день

fct_count_day_earthquake.py — витрина с количеством событий за день

3. Воспроизводимая инфраструктура
Весь стек — Airflow, PostgreSQL и Metabase — поднимается одной командой docker-compose up -d --build. Окружение изолировано в контейнерах, что убирает расхождения между машинами и делает проект воспроизводимым. Доступы к S3 и PostgreSQL вынесены в Connections Airflow, секреты отделены от кода.

Схема взаимодействия компонентов
text
Airflow (webserver + scheduler)

     ├── DAG: raw_from_api_to_s3 ──▶ S3 / MinIO (Raw)
     ├── DAG: raw_from_s3_to_pg   ──▶ PostgreSQL (ODS)
     ├── DAG: fct_avg_day_earthquake  ──▶ витрина
     └── DAG: fct_count_day_earthquake ──▶ витрина

PostgreSQL ──▶ Metabase (дашборды)
Airflow — оркестрация задач и управление зависимостями.

S3 / MinIO — объектное хранилище для сырых данных.

PostgreSQL — ODS-слой и витрины.

Metabase — BI-дашборды поверх витрин.

Стек
Языки и рантайм
Python 3.11+ — основной язык

Bash — вспомогательные команды для развёртывания

Оркестрация
Apache Airflow — DAG-и на PythonOperator и EmptyOperator, настройка Connections

Data Layer
PostgreSQL — ODS-слой и витрины данных

AWS S3 / MinIO — объектное хранилище для Raw-слоя

BI и визуализация
Metabase — дашборды и визуализация метрик

Инфраструктура
Docker — контейнеризация

Docker Compose — оркестрация многоконтейнерного приложения

Git / GitHub — версионирование

Архитектурные паттерны и практики
Layered Data Architecture — разделение Raw / ODS / Mart

DAG-оркестрация — декларативное описание зависимостей задач

Infrastructure as Code — весь стек поднимается из docker-compose.yaml

Separation of secrets — доступы вынесены в Connections Airflow

Idempotency — повторный запуск DAG-ов не дублирует данные

Локальное развертывание
Для запуска проекта достаточно установленного Docker и Docker Compose.

1. Подготовка окружения
bash
git clone https://github.com/Mifestos/PetEarthquake.git
cd PetEarthquake

python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\Activate.ps1     # Windows

pip install -r requirements.txt
2. Старт контейнеров
bash
docker-compose up -d --build
Адреса запущенных сервисов
Компонент	URL
Apache Airflow	http://localhost:8080
Metabase	http://localhost:3000
3. Настройка Connections в Airflow
В веб-интерфейсе (Admin → Connections) необходимо настроить:

Conn ID	Назначение
aws_default / s3_conn	Доступ к S3-бакету
ods_pg	Подключение к PostgreSQL
После настройки DAG-и готовы к запуску.

Структура репозитория
text
PetEarthquake/
├── config/                              # Конфигурационные файлы
├── dags/
│   ├── raw_from_api_to_s3.py            # API → S3 (Raw Layer)
│   ├── raw_from_s3_to_pg.py             # S3 → PostgreSQL (ODS)
│   ├── fct_avg_day_earthquake.py        # Витрина: средняя магнитуда за день
│   └── fct_count_day_earthquake.py      # Витрина: количество событий за день
├── data/                                # Тестовые данные / кэш
├── logs/                                # Логи Airflow
├── metabase/                            # Настройки и метаданные дашбордов
├── plugins/                             # Кастомные плагины Airflow
├── Dockerfile                           # Сборка кастомного образа Airflow
├── docker-compose.yaml                  # Локальное развёртывание инфраструктуры
├── cred.py                              # Управление секретами (локально)
└── requirements.txt                     # Зависимости Python-окружения
Инженерные заметки
Слоистость хранилища снижает связанность: изменение логики витрины не затрагивает Raw-слой, а повторный расчёт метрик не требует обращения к внешнему API.

Секреты отделены от кода: доступы к S3 и PostgreSQL хранятся в Connections Airflow, а не в репозитории.

Воспроизводимость окружения: весь стек поднимается из docker-compose.yaml, что исключает расхождения между машинами разработчиков.

Roadmap
Добавление тестов данных (Great Expectations / dbt tests)

Переход на dbt для трансформаций Mart-слоя

CI/CD (GitHub Actions) для линта и деплоя DAG-ов

Мониторинг и алертинг пайплайна

Лицензия: MIT
Автор: Mifestos
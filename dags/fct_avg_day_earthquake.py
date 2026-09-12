import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "Mifestos"
DAG_ID = "fct_avg_day_earthquake"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_avg_day_earthquake"

PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Расчет витрины данных: Средняя магнитуда землетрясений по дням

### Архитектура процесса (Пайплайн ODS -> STG -> DM):
1. **Сенсор контроля:** `ExternalTaskSensor` контролирует, чтобы расчет не начинался, пока слой `ods` (`raw_from_s3_to_pg`) не обновится за аналогичный операционный день.
2. **Идемпотентность (Очистка STG):** Предотвращает конфликты имен, дропая временную таблицу от возможных прошлых упавших сессий.
3. **Расчет метрики (STG):** Создает временную таблицу в схеме `stg`, фильтрует записи из `ods.fct_earthquake` за текущую дату Airflow (`{{ data_interval_start }}`), приводит текстовое поле магнитуды к числовому виду `mag::float` и вычисляет среднее арифметическое (`avg`).
4. **Защита от дубликатов (DM):** Очищает целевую витрину за текущий день (`drop_from_target_table`), гарантируя атомарность операции при перезапусках.
5. **Фиксация (DM):** Выполняет финальный инсерт рассчитанного агрегата в целевую аналитическую таблицу **`dm.fct_avg_day_earthquake`**.
6. **Очистка окружения:** Безвозвратно удаляет временные таблицы из схемы `stg`.

*Слой назначения:* `Data Mart (DM)`  
*Метрика:* `avg(mag::float)`  
*Автор:* Mifestos
"""

SHORT_DESCRIPTION = "Расчет аналитической витрины средней магнитуды землетрясений по дням в слое DM"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 8, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["dm", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT
            time::date AS date,
            avg(mag::float)
        FROM
            ods.fct_earthquake
        WHERE
            time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY 1
        """,
    )

    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN
        (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        )
        """,
    )

    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
            start >>
            sensor_on_raw_layer >>
            drop_stg_table_before >>
            create_stg_table >>
            drop_from_target_table >>
            insert_into_target_table >>
            drop_stg_table_after >>
            end
    )
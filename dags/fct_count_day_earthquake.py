import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "Mifestos"
DAG_ID = "fct_count_day_earthquake"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_count_day_earthquake"

PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Расчет витрины данных: Количество землетрясений по дням

### Архитектура процесса (Пайплайн ODS -> STG -> DM):
1. **Сенсор контроля:** Ожидает успешного завершения предыдущего шага `raw_from_s3_to_pg` за аналогичный операционный день.
2. **Идемпотентность (Очистка STG):** Удаляет старую временную таблицу, если прошлый запуск упал (`drop_stg_table_before`).
3. **Агрегация (STG):** Создает партиционированную временную таблицу в схеме `stg`, фильтруя данные из `ods.fct_earthquake` строго за текущую дату Airflow (`{{ data_interval_start }}`) и рассчитывая `count(*)`.
4. **Защита от дубликатов (DM):** Удаляет существующие записи за этот день из целевой витрины (`drop_from_target_table`), гарантируя перезапускаемость без задвоения метрик.
5. **Фиксация (DM):** Переносит готовый агрегированный расчет в финальную витрину **`dm.fct_count_day_earthquake`**.
6. **Очистка хвостов:** Удаляет временную `stg`-таблицу после успешного инсерта.

*Слой назначения:* `Data Mart (DM)`  
*Используемое подключение:* `postgres_dwh`  
*Автор:* Mifestos
"""

SHORT_DESCRIPTION = "Расчет аналитической витрины количества землетрясений по дням в слое DM"


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
        timeout=360000,  
        poke_interval=60,  
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
            count(*)
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
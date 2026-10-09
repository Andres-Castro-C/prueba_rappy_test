from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def task_success_slack_alert(context):
    slack_msg = f"""
    :large_green_circle: *Proceso Completado con Éxito*
    *DAG*: {context.get('task_instance').dag_id}
    *Task*: {context.get('task_instance').task_id}
    *Execution Time*: {context.get('execution_date')}
    """
    success_alert = SlackWebhookOperator(
        task_id='slack_success_alert',
        slack_webhook_conn_id='slack_connection',
        message=slack_msg,
        username='airflow',
    )
    return success_alert.execute(context=context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': task_success_slack_alert,
}

with DAG(
    'meetup_incremental_etl',
    default_args=default_args,
    description='A DAG to automate incremental ETL every 15 mins for Meetup Data',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['meetup'],
) as dag:

    # 1. Simulación de nuevos datos (El punto "creativo").
    # Actualizamos el conteo de RSVPs de algunos eventos y creamos uno nuevo.
    simulate_new_data = SnowflakeOperator(
        task_id='simulate_new_data',
        snowflake_conn_id='snowflake_default',
        split_statements=True,
        sql=[
            """
            -- Simular actualización de RSVPs para 5 eventos aleatorios
            UPDATE RAW_DATA.EVENTS
            SET yes_rsvp_count = (COALESCE(TRY_TO_NUMBER(yes_rsvp_count), 0) + UNIFORM(1, 10, RANDOM()))::VARCHAR,
                updated = CURRENT_TIMESTAMP()::VARCHAR
            WHERE event_id IN (
                SELECT event_id FROM RAW_DATA.EVENTS SAMPLE (1) LIMIT 5
            );
            """,
            """
            -- Simular la inserción de 1 nuevo evento en un grupo aleatorio
            INSERT INTO RAW_DATA.EVENTS (
                event_id, group_id, created, event_time, updated,
                event_status, yes_rsvp_count, maybe_rsvp_count, event_name
            )
            SELECT
                UUID_STRING(),
                group_id,
                CURRENT_TIMESTAMP()::VARCHAR,
                DATEADD(day, UNIFORM(1, 30, RANDOM()), CURRENT_TIMESTAMP())::VARCHAR,
                CURRENT_TIMESTAMP()::VARCHAR,
                'upcoming',
                UNIFORM(1, 100, RANDOM())::VARCHAR,
                UNIFORM(0, 20, RANDOM())::VARCHAR,
                'Generated Event ' || UUID_STRING()
            FROM RAW_DATA.GROUPS SAMPLE (1) LIMIT 1;
            """
        ]
    )

    # 2. Refrescar STAGING.EVENTS_CLEAN usando MERGE para datos incrementales
    refresh_staging_events = SnowflakeOperator(
        task_id='refresh_staging_events',
        snowflake_conn_id='snowflake_default',
        sql="""
        MERGE INTO STAGING.EVENTS_CLEAN tgt
        USING (
            SELECT
                NULLIF(TRIM(event_id), '') AS event_id,
                TRY_TO_NUMBER(NULLIF(TRIM(group_id), ''))::NUMBER(38, 0) AS group_id,
                TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(created), ''), 'YYYY-MM-DD HH24:MI:SS') AS created_at,
                TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(event_time), ''), 'YYYY-MM-DD HH24:MI:SS') AS event_at,
                TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(updated), ''), 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
                NULLIF(TRIM(event_status), '') AS event_status,
                TRY_TO_NUMBER(NULLIF(TRIM(yes_rsvp_count), ''))::NUMBER(38, 0) AS yes_rsvp_count,
                TRY_TO_NUMBER(NULLIF(TRIM(maybe_rsvp_count), ''))::NUMBER(38, 0) AS maybe_rsvp_count,
                TRY_TO_DECIMAL(NULLIF(TRIM("rating.average"), ''), 5, 2) AS rating_average,
                TRY_TO_NUMBER(NULLIF(TRIM("rating.count"), ''))::NUMBER(38, 0) AS rating_count,
                NULLIF(TRIM(event_name), '') AS event_name
            FROM RAW_DATA.EVENTS
            WHERE NULLIF(TRIM(event_id), '') IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY NULLIF(TRIM(event_id), '')
              ORDER BY TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(updated), ''), 'YYYY-MM-DD HH24:MI:SS') DESC NULLS LAST
            ) = 1
        ) src
        ON tgt.event_id = src.event_id
        WHEN MATCHED AND (tgt.updated_at IS NULL OR tgt.updated_at < src.updated_at) THEN
            UPDATE SET
                tgt.group_id = src.group_id,
                tgt.event_at = src.event_at,
                tgt.updated_at = src.updated_at,
                tgt.event_status = src.event_status,
                tgt.yes_rsvp_count = src.yes_rsvp_count,
                tgt.maybe_rsvp_count = src.maybe_rsvp_count,
                tgt.rating_average = src.rating_average,
                tgt.rating_count = src.rating_count,
                tgt.event_name = src.event_name
        WHEN NOT MATCHED THEN
            INSERT (event_id, group_id, created_at, event_at, updated_at, event_status, yes_rsvp_count, maybe_rsvp_count, rating_average, rating_count, event_name)
            VALUES (src.event_id, src.group_id, src.created_at, src.event_at, src.updated_at, src.event_status, src.yes_rsvp_count, src.maybe_rsvp_count, src.rating_average, src.rating_count, src.event_name);
        """
    )

    # 3. Actualizar la tabla auxiliar AUX.GROUPS_BY_CITY_CATEGORY
    # Usaremos CREATE OR REPLACE para consolidar el paso de agregación.
    refresh_aux_table = SnowflakeOperator(
        task_id='refresh_aux_table',
        snowflake_conn_id='snowflake_default',
        sql="""
        CREATE OR REPLACE TABLE AUX.GROUPS_BY_CITY_CATEGORY AS
        WITH event_metrics AS (
          SELECT
            group_id,
            COUNT(*)::NUMBER(38, 0) AS event_count,
            SUM(COALESCE(yes_rsvp_count, 0))::NUMBER(38, 0) AS yes_rsvp_across_events,
            MAX(event_at) AS latest_event_at
          FROM STAGING.EVENTS_CLEAN
          WHERE group_id IS NOT NULL
          GROUP BY group_id
        )
        SELECT
          g.city_id,
          g.city_name,
          g.country_code,
          g.state_code,
          g.category_id,
          g.category_name,
          COUNT(*)::NUMBER(38, 0) AS group_count,
          SUM(COALESCE(g.member_count, 0))::NUMBER(38, 0) AS members_across_groups,
          ROUND(AVG(g.rating), 2)::NUMBER(5, 2) AS average_group_rating,
          SUM(COALESCE(e.event_count, 0))::NUMBER(38, 0) AS event_count,
          SUM(COALESCE(e.yes_rsvp_across_events, 0))::NUMBER(38, 0) AS yes_rsvp_across_events,
          MAX(e.latest_event_at) AS latest_event_at,
          CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS refreshed_at
        FROM STAGING.GROUPS_CLEAN AS g
        LEFT JOIN event_metrics AS e
          ON g.group_id = e.group_id
        GROUP BY
          g.city_id,
          g.city_name,
          g.country_code,
          g.state_code,
          g.category_id,
          g.category_name;
        """
    )

    simulate_new_data >> refresh_staging_events >> refresh_aux_table

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.eczachly.poke_glue_partition import poke_glue_partition
from include.eczachly.trino_queries import execute_trino_query
import os
from airflow.models import Variable

aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
aws_region = Variable.get("AWS_GLUE_REGION")

local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')

# TODO make sure to rename this if you're testing this dag out!
schema = os.environ.get('STUDENT_SCHEMA', 'zachwilson')
@dag(
    description="A dag that aggregates data from Iceberg into metrics",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2025, 1, 9),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 1, 9),
    max_active_runs=1,
    schedule="@daily",
    catchup=True,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def cumulate_web_events_dag():
    upstream_table = f'{schema}.user_web_events_daily'
    production_table = f'{schema}.user_web_events_cumulated'
    wait_for_web_events = PythonOperator(
        task_id='wait_for_web_events',
        depends_on_past=True,
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": upstream_table,
            "partition": 'ds={{ ds }}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )

    wait_for_yesterday_data = PythonOperator(
        task_id='wait_for_yesterday_data',
        depends_on_past=True,
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": production_table,
            "partition": 'ds_day={{ yesterday_ds }}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )

    create_step = PythonOperator(
        task_id="create_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
             CREATE TABLE IF NOT EXISTS {production_table} (
                user_id INTEGER,
                academy_id INTEGER,
                event_count_array ARRAY(INTEGER),
                event_count_last_7d INTEGER,
                event_count_lifetime INTEGER,
                ds DATE
             ) WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(ds)']
             )
             """
        }
    )

    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
               DELETE FROM {production_table} 
               WHERE ds = DATE('{ds}')
               """
        }
    )

    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                 INSERT INTO {production_table}
                 WITH yesterday AS (
                    SELECT * FROM {production_table}
                    WHERE ds = DATE('{ yesterday_ds }')
                    AND academy_id = 2
                 ),
                 today AS (
                    SELECT user_id, academy_id, MAX(event_count) as event_count
                    FROM {upstream_table}
                    WHERE ds = DATE('{ds}')
                    AND academy_id = 2
                    GROUP BY user_id, academy_id
                 ),
                 event_arrays AS (
                 SELECT 
                        COALESCE(t.user_id, y.user_id) as user_id,
                        COALESCE(t.academy_id, y.academy_id) as academy_id,
                        CASE 
                            WHEN y.user_id IS NULL THEN ARRAY[t.event_count]
                            WHEN t.user_id IS NULL THEN ARRAY[0] || y.event_count_array
                            ELSE ARRAY[t.event_count] || y.event_count_array
                        END as event_count_array,
                        COALESCE(y.event_count_lifetime,0) + COALESCE(t.event_count, 0) as event_count_lifetime
                    FROM today t 
                    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id 
                ) 
                
                SELECT user_id, 
                        academy_id, 
                        event_count_array,
                        REDUCE(
                            slice(event_count_array, 1, 7), 
                            0, 
                            (acc, x) -> acc + coalesce(x, 0),
                            acc -> acc
                        ) AS event_count_last_7d, 
                        event_count_lifetime + ELEMENT_AT(event_count_array, 1)  as event_count_lifetime,
                        DATE('{ds}') as ds 
                FROM event_arrays   
                 """
        }
    )

    wait_for_web_events >> wait_for_yesterday_data >> create_step >> clear_step >> cumulate_step


cumulate_web_events_dag()

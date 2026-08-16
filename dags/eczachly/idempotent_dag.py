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


@dag(
    description="A dag that aggregates data from Iceberg into metrics",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2024, 10, 18),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2024, 10, 18),
    max_active_runs=10,
    schedule="15 * * * *",
    catchup=True,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def idempotent_dag():
    # TODO make sure to rename this if you're testing this dag out!
    upstream_table = 'bootcamp.user_web_events_daily'
    summary_table = 'bootcamp.academy_web_events_summary_monthly'
    ds = '{{ ds }}'
    thirty_days_ago = "{{ macros.ds_add(ds, -30) }}"

    wait_for_web_events_daily = PythonOperator(
        task_id='wait_for_web_events_daily',
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": upstream_table,
            "partition": 'ds={{ ds }}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )

    academy_summary_create = PythonOperator(
        task_id="academy_summary_create",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {summary_table} (
                   academy_id INTEGER,
                   monthly_active_users INTEGER,
                   ds DATE
                ) WITH (
                   format = 'PARQUET',
                   partitioning = ARRAY['day(ds)']
                )
                """
        }
    )
    clear_summary_step = PythonOperator(
        task_id="clear_summary_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                       DELETE FROM {summary_table} 
                       WHERE ds = DATE('{ds}')  
            """
        }
    )
    insert_academy_summary = PythonOperator(
        task_id="insert_academy_summary",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                    INSERT INTO {summary_table}  
                    SELECT 
                        academy_id, 
                        COUNT(DISTINCT user_id) as monthly_active_users,
                        DATE('{ds}') as ds
                    FROM {upstream_table}
                    WHERE ds BETWEEN DATE('{thirty_days_ago}') AND DATE('{ds}')
                    GROUP BY academy_id
                    """
        }
    )
    wait_for_web_events_daily >> academy_summary_create >> clear_summary_step >> insert_academy_summary


idempotent_dag()

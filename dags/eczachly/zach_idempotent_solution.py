from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.eczachly.poke_glue_partition import poke_glue_partition
from include.eczachly.trino_queries import execute_trino_query
import os
from airflow.models import Variable
from include.eczachly.glue_job_submission import create_glue_job

local_script_path = os.path.join("include", 'eczachly/scripts/user_cumulated_academy_example.py')

s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
kafka_credentials = Variable.get("KAFKA_CREDENTIALS")

# This DAG is not idempotent. It is missing so many things
# Bad query logic
# Deletes
@dag(
    description="A dag that aggregates data from Iceberg into metrics",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2025, 4, 15),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 4, 15),
    max_active_runs=1,
    schedule="@daily",
    catchup=True,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def non_idempotent_dag_v2():
    # TODO make sure to rename this if you're testing this dag out!
    schema = os.environ.get('STUDENT_SCHEMA', 'zachwilson')
    upstream_table = f'{schema}.user_web_events_daily'
    summary_table = f'{schema}.academy_web_events_summary_monthly'
    cumulated_table = f'{schema}.user_web_events_cumulated_master'
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

    wait_for_cumulated_data = PythonOperator(
        task_id='wait_for_cumulated_yesterday',
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": cumulated_table,
            "partition": 'ds={{ yesterday_ds }}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )

    users_cumulated_table = PythonOperator(
        task_id="user_cumulate_Table_create",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                  CREATE TABLE IF NOT EXISTS {cumulated_table} (
                     user_id INTEGER,
                     is_daily_active BOOLEAN,
                     activity_array_last_30d ARRAY<INTEGER>,
                     activity_map_last_30d MAP<INTEGER, ARRAY<INTEGER>>,
                     ds DATE
                  ) WITH (
                     format = 'PARQUET',
                     partitioning = ARRAY['ds']
                  )
                  """
        }
    )

    start_glue_job_task = PythonOperator(
        task_id='start_glue_job',
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "user_cumulated_job_{{ ds }}",
            "script_path": local_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "s3_bucket": s3_bucket,
            "aws_region": aws_region,
            "kafka_credentials": kafka_credentials,
            "description": "User Cumulated with Glue",
            "arguments": {
                "--ds": "{{ ds }}",
                "--yesterday_ds": "{{ yesterday_ds }}",
                "--output_table": cumulated_table
            },
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
    clear_academy_summary = PythonOperator(
        task_id="clear_academy_summary",
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
    wait_for_web_events_daily >> users_cumulated_table >> wait_for_cumulated_data >> start_glue_job_task >> academy_summary_create >> clear_academy_summary >> insert_academy_summary


non_idempotent_dag_v2()

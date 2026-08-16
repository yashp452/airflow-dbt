from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.eczachly.poke_glue_partition import poke_glue_partition
from include.eczachly.trino_queries import execute_trino_query, run_trino_query_dq_check
import os
from airflow.models import Variable

aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
aws_region = Variable.get("AWS_GLUE_REGION")

schema = os.environ.get('STUDENT_SCHEMA', 'zachwilson')

@dag(
    description="Growth accounting DAG for tracking user state transitions",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2024, 6, 3),
        "retries": 1,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2024, 6, 3),
    max_active_runs=1,
    schedule="@daily",
    catchup=True,
    tags=["growth_accounting"],
)
def growth_accounting_dag():
    ds = '{{ ds }}'
    yesterday_ds = '{{ macros.ds_add(ds, -1) }}'
    ds_nodash = '{{ ds_nodash }}'
    
    production_table = f'{schema}.growth_accounting_tracking'
    staging_table = f'{schema}.growth_accounting_tracking_{ds_nodash}'

    # Wait for upstream datasets
    wait_for_platform_users = PythonOperator(
        task_id='wait_for_platform_users',
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": f'{schema}.platform_users',
            "partition": f'ds={ds}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )

    wait_for_growth_accounting = PythonOperator(
        task_id='wait_for_growth_accounting',
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": f'{schema}.growth_accounting_tracking',
            "partition": f'ds={yesterday_ds}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )

    wait_for_web_site_events = PythonOperator(
        task_id='wait_for_web_site_events',
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": f'{schema}.web_site_events_prod',
            "partition": f'ds={ds}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )

    create_production_table = PythonOperator(
        task_id="create_production_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {production_table} (
                  user_id BIGINT,
                  first_action_timestamp TIMESTAMP,
                  last_action_timestamp TIMESTAMP,
                  state_transition VARCHAR,
                  pages_visited INTEGER,
                  hosts_visited ARRAY(VARCHAR),
                  ds VARCHAR
                )
                WITH (
                  partitioning = ARRAY['ds']
                )
            """
        }
    )

    create_staging_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {staging_table} (
                  user_id BIGINT,
                  first_action_timestamp TIMESTAMP,
                  last_action_timestamp TIMESTAMP,
                  state_transition VARCHAR,
                  pages_visited INTEGER,
                  hosts_visited ARRAY(VARCHAR),
                  ds VARCHAR
                )
            """
        }
    )

    insert_into_staging = PythonOperator(
        task_id="insert_into_staging",
        python_callable=execute_trino_query,
        depends_on_past=True,
        op_kwargs={
            'query': f"""
                INSERT INTO {staging_table}
                WITH yesterday AS (
                    SELECT * FROM {production_table}
                    WHERE ds = '{yesterday_ds}'
                ),
                today AS (
                    SELECT * FROM {schema}.platform_users
                    WHERE ds = '{ds}'
                ),
                activity_today AS (
                    SELECT
                        user_id,
                        COUNT(1) as pages_visited,
                        MIN(event_time) as first_action_timestamp,
                        MAX(event_time) as last_action_timestamp,
                        ARRAY_AGG(DISTINCT host) as hosts_visited
                    FROM {schema}.web_site_events_prod
                    WHERE ds = '{ds}'
                    GROUP BY user_id
                ),
                combined AS (
                    SELECT
                        COALESCE(t.user_id, y.user_id) as user_id,
                        t.created_at,
                        y.user_id IS NULL as is_new,
                        y.pages_visited as pages_visited_yesterday,
                        y.last_action_timestamp as last_action_timestamp_yesterday
                    FROM today t
                    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id
                )
                SELECT
                    c.user_id,
                    COALESCE(c.created_at, t.first_action_timestamp) as first_action_timestamp,
                    COALESCE(t.last_action_timestamp, c.last_action_timestamp_yesterday) as last_action_timestamp,
                    CASE
                        WHEN is_new THEN 'New'
                        WHEN t.pages_visited > 0 AND c.pages_visited_yesterday > 0 THEN 'Retained'
                        WHEN t.pages_visited > 0 AND c.pages_visited_yesterday = 0 THEN 'Resurrected'
                        WHEN c.pages_visited_yesterday > 0 AND t.pages_visited IS NULL THEN 'Churned'
                        ELSE 'Stale'
                    END as state_transition,
                    COALESCE(t.pages_visited, 0) as pages_visited,
                    COALESCE(t.hosts_visited, ARRAY[]) as hosts_visited,
                    '{ds}' as ds
                FROM combined c
                LEFT JOIN activity_today t ON c.user_id = t.user_id
            """
        }
    )

    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                WITH stats AS (
                    SELECT 
                        COUNT(1) as row_count,
                        COUNT(DISTINCT user_id) as distinct_users
                    FROM {staging_table}
                )
                SELECT 
                    row_count = distinct_users as no_duplicates,
                    row_count > 0 as has_data,
                    (row_count = distinct_users AND row_count > 0) as all_checks
                FROM stats
            """
        }
    )

    clear_data = PythonOperator(
        task_id="clear_data",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                 DELETE FROM {production_table} WHERE ds = '{ds}'
             """
        }
    )

    exchange_data = PythonOperator(
        task_id="exchange_data",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {production_table}
                SELECT * FROM {staging_table}
            """
        }
    )

    drop_staging = PythonOperator(
        task_id="drop_staging",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"DROP TABLE {staging_table}"
        }
    )

    [wait_for_platform_users, wait_for_web_site_events, wait_for_growth_accounting] >> create_staging_table >> create_production_table >> insert_into_staging >> run_dq_check >> clear_data >> exchange_data >> drop_staging

growth_accounting_dag()

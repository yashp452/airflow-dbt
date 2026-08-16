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

schema = os.environ.get('STUDENT_SCHEMA', 'zachwilson')
local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')


@dag(
    description="A dag that aggregates data from Iceberg into metrics",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2025, 5, 14),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 5, 14),
    max_active_runs=10,
    schedule="@daily",
    catchup=False,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def program_master_dag():
    ds = '{{ ds }}'
    wait_fors = {}
    tables = [
        f"{schema}.user_programs",
        f"{schema}.programs",
        f"{schema}.course_content",
        f"{schema}.program_modules",
        f"{schema}.modules",
        f"{schema}.module_courses",
    ]

    for table in tables:
        wait_fors[table] = PythonOperator(
            task_id=f'wait_for_{table}',
            python_callable=poke_glue_partition,
            op_kwargs={
                "table": table,
                "partition": 'ds={{ ds }}',
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
                "aws_region": aws_region
            }
        )

    wait_fors['web_events_production'] = PythonOperator(
        task_id='wait_for_web_events',
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": 'bootcamp.web_events_production',
            "partition": 'event_time_day={{ ds }}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )

    program_create_step = PythonOperator(
        task_id="program_create_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
               CREATE TABLE IF NOT EXISTS {schema}.dim_programs (
                program_id INTEGER,
                academy_id INTEGER,
                dim_program_name VARCHAR,
                dim_is_active boolean,
                dim_is_subscription BOOLEAN,
                dim_one_time_price REAL,
                dim_monthly_price REAL,
                dim_annual_price REAL,
                dim_minutes_for_certification INTEGER,
                dim_required_course_ids ARRAY(INTEGER),
                dim_course_ids ARRAY(INTEGER),
                m_num_users INTEGER,
                m_num_active_users INTEGER,
                ds DATE
            ) WITH (
               partitioning = ARRAY['ds']
            )
                """
        }
    )

    program_insert_step = PythonOperator(
        task_id="program_insert_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                 INSERT INTO {schema}.dim_programs
                    SELECT
                       p.program_id,
                       MAX(p.academy_id) as academy_id,
                       MAX(p.name) as dim_program_name,
                       MAX(p.is_active) as dim_is_active,
                       MAX(p.is_subscription) as dim_is_subscription,
                       MAX(p.price) as dim_one_time_price,
                       MAX(p.monthly_price) as dim_monthly_price,
                       MAX(p.annual_price) as dim_annual_price,
                       MAX(p.required_minutes_for_certification) as dim_minutes_for_certification,
                       FILTER(ARRAY_AGG(DISTINCT CASE WHEN c.attendance_required THEN c.course_id END), x -> x is not null)
                           as dim_required_course_ids,
                       ARRAY_AGG(DISTINCT c.course_id) as dim_course_ids,
                       COUNT(DISTINCT up.user_id) as m_num_users,
                       COUNT(DISTINCT CASE WHEN up.is_active THEN up.user_id END) as m_num_active_users,
                       DATE('{ds}') as ds
                   FROM {schema}.programs p
                            LEFT OUTER JOIN {schema}.program_modules pm ON p.program_id = pm.program_id
                            LEFT OUTER JOIN {schema}.module_courses mc ON mc.module_id = pm.module_id
                            LEFT OUTER JOIN {schema}.modules m ON m.module_id = pm.module_id
                            LEFT OUTER JOIN {schema}.course_content c on mc.course_id = c.course_id
                            LEFT OUTER JOIN {schema}.user_programs up ON up.program_id = p.program_id
                    WHERE p.ds = DATE('{ds}')
                    AND pm.ds =  DATE('{ds}')
                        AND mc.ds =  DATE('{ds}')
                        AND c.ds =  DATE('{ds}')
                    GROUP BY p.program_id
                   """
        }
    )
    upstream_tasks = list(wait_fors.values())
    upstream_tasks.append(program_create_step)
    program_insert_step.set_upstream(upstream_tasks)

program_master_dag()

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
def user_master_dag_v2():
    ds = '{{ ds }}'
    ds_nodash = '{{ ds_nodash }}'
    seven_days_ago = "{{ macros.ds_add(ds, -7) }}"

    wait_fors = {}
    tables = [
        f"{schema}.blog_users",
        f"{schema}.video_intervals",
        f"{schema}.user_programs",
        f"{schema}.dim_programs",
        f"{schema}.user_sql_queries",
        f"{schema}.course_view_time"
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
    production_table = f'{schema}.dim_users'
    staging_table = f'{schema}.dim_users_{ds_nodash}'

    create_tables = {
        'production': production_table,
        'staging': staging_table
    }
    create_steps = {}
    for step_name, table in create_tables.items():
        create_steps[step_name] = PythonOperator(
            task_id="user_create_step_" + step_name,
            python_callable=execute_trino_query,
            op_kwargs={
                'query': f"""
                   CREATE TABLE IF NOT EXISTS {table} (
                    user_id INTEGER,
                    dim_certified_programs MAP(INTEGER, ROW(is_certified BOOLEAN, program_name VARCHAR, minutes_watched BIGINT,
                        minutes_for_certification BIGINT)),
                    m_num_web_hits_last_7d INTEGER,
                    m_num_dashboard_hits_last_7d INTEGER,
                    latest_activity_date TIMESTAMP,
                    m_queries_ran_last_7d INTEGER,
                    m_queries_ran_total INTEGER,
                    ds DATE
                )
                WITH (
                    partitioning = ARRAY['ds']
                )
            """
            }
        )

    user_insert_staging_step = PythonOperator(
        task_id="user_insert_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                 INSERT INTO {staging_table}
        WITH users AS (
                    SELECT *
                       FROM {schema}.blog_users
                       WHERE ds = DATE ('{ds}')
            ),
            user_programs AS (
                    SELECT *
                       FROM {schema}.user_programs
                       WHERE ds = DATE ('{ds}')
        
            ),
                           query_summary AS (
        
                        SELECT user_id,
                            COUNT(CASE WHEN start_time BETWEEN DATE('{seven_days_ago}')
                        AND DATE('{ds}') THEN 1 END) as queries_ran_last_7d,
                             COUNT(1) as queries_ran_last_total
                       FROM {schema}.user_sql_queries
                       WHERE ds = DATE ('{ds}')
                           GROUP BY user_id
            ),
                           user_web_events_summary AS (
                               SELECT user_id,
                                   COUNT(1) as m_num_web_hits_last_7d,
                                   COUNT(CASE WHEN url = '/dashboard' THEN 1 END) as num_dashboard_hits_last_7d,
                                   MAX(event_time) as latest_activity_date
                       FROM bootcamp.web_events_production
                       WHERE event_time BETWEEN DATE ('{seven_days_ago}')  AND DATE('{ds}')
                           GROUP BY user_id
            ),
                           course_view_time AS (
                            SELECT *
                       FROM {schema}.course_view_time
                       WHERE ds = DATE ('{ds}')
            ),
                           programs_with_courses AS (
        
                           SELECT
                               *
                           FROM {schema}.dim_programs
                            WHERE ds = DATE('{ds}')
            ),
            programs_with_users AS (
        
                         SELECT pwc.program_id, u.user_id,
                             MAX(pwc.dim_program_name) as name,
                MAX(pwc.dim_minutes_for_certification) as required_minutes_for_certification,
                SUM(cvt.seconds_watched)/60 as total_minutes_watched
                FROM users u
                JOIN user_programs up ON u.user_id = up.user_id
                JOIN programs_with_courses pwc ON pwc.program_id = up.program_id
                JOIN course_view_time cvt ON u.user_id = cvt.user_id
                                                 AND CONTAINS(pwc.dim_required_course_ids, cvt.course_id)
        GROUP BY pwc.program_id, u.user_id
            ),
            certified_users AS (
        
        SELECT user_id,
            MAP_AGG(program_id, CAST(ROW (
                    total_minutes_watched > required_minutes_for_certification
                    , name, total_minutes_watched, required_minutes_for_certification
            ) AS ROW(
                            is_certified BOOLEAN,
                            program_name VARCHAR,
                            minutes_watched BIGINT,
                            minutes_for_certification BIGINT
                        ))
        
            )
            as certified_programs
        
        FROM programs_with_users
        GROUP BY user_id
            )
        
        SELECT u.user_id,
               COALESCE(cu.certified_programs, MAP()) as dim_certified_programs,
               COALESCE(m_num_web_hits_last_7d, 0) as m_num_web_hits_last_7d,
               COALESCE(num_dashboard_hits_last_7d, 0) as m_num_dashboard_hits_last_7d,
               latest_activity_date,
               COALESCE(queries_ran_last_7d, 0) as m_queries_ran_last_7d,
               COALESCE(queries_ran_last_total, 0) as m_queries_ran_total,
               DATE('{ds}')
            FROM users u
                LEFT JOIN certified_users cu ON cu.user_id = u.user_id
                LEFT JOIN user_web_events_summary uwes ON u.user_id = uwes.user_id
                LEFT JOIN query_summary qs on qs.user_id = u.user_id

                   """
        }
    )

    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                    WITH last_week AS (
                        SELECT 
                            COUNT(CASE WHEN m_num_web_hits_last_7d > 0 THEN 1 END) as weekly_active_users,
                            COUNT(CASE WHEN m_queries_ran_last_7d > 0 THEN 1 END) as weekly_active_query_users,
                            COUNT(1) as num_users
                        FROM {production_table}
                        WHERE ds = DATE('{seven_days_ago}')
                    ),
                    this_week AS (
                        SELECT 
                            COUNT(1) as num_users,
                            COUNT(1) = COUNT(DISTINCT user_id)  as no_duplicates,
                            COUNT(CASE WHEN m_num_web_hits_last_7d > 0 THEN 1 END) as weekly_active_users,
                            COUNT(CASE WHEN m_queries_ran_last_7d > 0 THEN 1 END) as weekly_active_query_users
                        FROM {staging_table}
                    ),
                    checks AS (
                    
                     SELECT 
                        tw.weekly_active_users/CASE WHEN lw.weekly_active_users = 0 THEN 1 ELSE lw.weekly_active_users END  > .9 as weekly_growth_decline_less_than_10pct,
                        tw.num_users/CASE WHEN lw.num_users = 0 THEN 1 ELSE lw.num_users END >= 1 as num_users_is_growing,
                        tw.no_duplicates
                    
                     FROM this_week tw CROSS JOIN last_week lw
                    )
                    
                    SELECT *,
                    weekly_growth_decline_less_than_10pct 
                    AND no_duplicates 
                    AND num_users_is_growing
                    AS all_checks
                    FROM checks 
            
                 """
        }
    )

    exchange_data_from_staging = PythonOperator(
        task_id="exchange_data_from_staging",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                     INSERT INTO {production_table}
                     SELECT * FROM {staging_table} 
                     WHERE ds = DATE('{ds}')
            """
        }
    )

    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""DROP TABLE {staging_table}"""
        }
    )

    upstream_tasks = list(wait_fors.values())
    upstream_tasks.extend(list(create_steps.values()))
    user_insert_staging_step.set_upstream(upstream_tasks)
    run_dq_check.set_upstream([user_insert_staging_step])
    exchange_data_from_staging.set_upstream([run_dq_check])
    drop_staging_table.set_upstream([exchange_data_from_staging])





user_master_dag_v2()

import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


airflow_home=os.environ['AIRFLOW_HOME']

# Define paths to the DBT project and virtual environment
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/dbt'
PATH_TO_RSA_KEY = os.path.join(airflow_home, 'rsa_key.p8')
dbt_env = {**os.environ, 'PRIVATE_KEY_PATH': PATH_TO_RSA_KEY}

default_args = {
  "owner": "Bruno de Lima",
  "retries": 0,
  "execution_timeout": timedelta(hours=1),
}

@dag(
    start_date=datetime(2024, 5, 9),
    schedule='@once',
    catchup=False,
    default_args=default_args,
)
def dbt_dag_bash():
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command=f'{PATH_TO_DBT_VENV} build -s +fact_orders',
        cwd=PATH_TO_DBT_PROJECT,
        env=dbt_env,
    )

    # Post DBT workflow task
    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", trigger_rule="all_done")

    # Define task dependencies
    pre_dbt_workflow >> dbt_build >> post_dbt_workflow

dbt_dag_bash()
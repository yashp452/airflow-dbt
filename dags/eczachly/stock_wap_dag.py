from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.eczachly.trino_queries import execute_trino_query, run_trino_query_dq_check
import os
from airflow.models import Variable
from include.eczachly.glue_job_submission import create_glue_job

stock_scrape_script_path = os.path.join("include", 'eczachly/scripts/iceberg_branching_example.py')
promote_to_production_path = os.path.join("include", 'eczachly/scripts/iceberg_fast_forward_example.py')

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
def stock_wap_dag():
    stock_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.branching_stock_tickers"
    ds = '{{ ds }}'
    yesterday_ds = '{{ yesterday_ds }}'
    thirty_days_ago = "{{ macros.ds_add(ds, -30) }}"


    # TODO add initial create table statement to make sure the main branch exists (run just the Spark job DDL without branch in context)

    start_glue_job_task = PythonOperator(
        task_id='start_glue_job',
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "stock_ticker_job{{ ds }}",
            "script_path": stock_scrape_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "s3_bucket": s3_bucket,
            "aws_region": aws_region,
            "kafka_credentials": kafka_credentials,
            "description": "Scrape Polygon API for Tickers",
            "arguments": {
                "--ds": "{{ ds }}",
                "--yesterday_ds": "{{ yesterday_ds }}",
                "--output_table": stock_table,
                '--branch': 'switch_branch'
            },
        }
    )

    run_stock_data_quality_checks = PythonOperator(
        task_id="run_stock_data_quality_checks",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                WITH checks as (
                SELECT 
                  COUNT(1) > 0 as there_is_data,
                  COUNT(1) = COUNT(DISTINCT ticker) as is_deduplicated,            
                  COUNT(CASE WHEN locale = 'us' AND currency_name <> 'USD' THEN 1 END) = 0 as all_us_locales_in_us_currency,
                  COUNT(CASE WHEN market = 'stocks' AND primary_exchange IS NULL THEN 1 END) = 0 as all_stocks_have_exchange,
                  COUNT(CASE WHEN market = 'otc' AND primary_exchange IS NOT NULL THEN 1 END) = 0 as all_otc_not_on_exchange,
                  COUNT(CASE WHEN market = 'stocks' AND name IS NULL THEN 1 END) = 0 as all_stocks_have_name,
                  
                  COUNT(1) / (SELECT COUNT(1) FROM {stock_table} WHERE date = DATE('{yesterday_ds}')) >= .95 AS tickers_are_growing
                  
                FROM {stock_table} FOR VERSION AS OF 'audit_branch'
                WHERE date = DATE('{ds}')
                )
                
                SELECT 
                   *,
                  there_is_data AND is_deduplicated AND all_us_locales_in_us_currency AND all_stocks_have_exchange AND all_otc_not_on_exchange AND all_stocks_have_name AND tickers_are_growing AS all_checks
                  FROM checks
                """
        }
    )

    promote_to_production = PythonOperator(
        task_id='promote_to_production',
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "stock_ticker_job{{ ds }}",
            "script_path": promote_to_production_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "s3_bucket": s3_bucket,
            "aws_region": aws_region,
            "kafka_credentials": kafka_credentials,
            "description": "Scrape Polygon API for Tickers",
            "arguments": {
                "--ds": "{{ ds }}",
                "--yesterday_ds": "{{ yesterday_ds }}",
                "--output_table": stock_table,
                '--branch': 'switch_branch'
            },
        }
    )

    start_glue_job_task >> run_stock_data_quality_checks >> promote_to_production


stock_wap_dag()

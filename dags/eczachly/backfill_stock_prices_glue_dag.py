from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from include.eczachly.glue_job_submission import create_glue_job
import os

# Get AWS credentials from Airflow Variables
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
aws_region = Variable.get("AWS_GLUE_REGION")
s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")

# Get Massive.com credentials from Airflow Variables
massive_access_key = Variable.get("MASSIVE_ACCESS_KEY", default_var="214ceb02-b057-4567-b406-1111868ddf6d")
massive_secret_key = Variable.get("MASSIVE_SECRET_KEY", default_var="Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q")





script_path = "include/eczachly/scripts/backfill_stock_prices_glue.py"

@dag(
    description="Backfill stock prices from Massive.com S3 endpoint using Glue/Spark",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2025, 1, 1),
        "retries": 1,
        "execution_timeout": timedelta(hours=4),
    },
    start_date=datetime(2025, 1, 1),
    max_active_runs=8,
    schedule="@daily",
    catchup=True,
    tags=["community", "backfill", "massive", "iceberg", "stock-prices", "glue", "spark"],
)
def backfill_stock_prices_glue_dag():
    output_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.stock_prices_history_backfill"
    
    # Massive.com credentials as JSON string for Glue job
    massive_credentials = {
        "AWS_ACCESS_KEY_ID": massive_access_key,
        "AWS_SECRET_ACCESS_KEY": massive_secret_key
    }
    
    run_job = PythonOperator(
        task_id="backfill_stock_prices_glue",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "backfill_stock_prices_job_glue",
            "script_path": script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "s3_bucket": s3_bucket,
            "aws_region": aws_region,
            "description": "Backfill stock prices from Massive.com using Spark",
            "arguments": {
                "--date": "{{ ds }}",
                "--output_table": output_table,
                "--massive_credentials": str(massive_credentials)
            },
        },
    )
    
    run_job

backfill_stock_prices_glue_dag()

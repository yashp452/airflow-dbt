from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from include.eczachly.scripts.backfill_stock_prices import backfill_stock_prices_from_massive
import os

# Get AWS credentials from Airflow Variables
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
aws_region = Variable.get("AWS_GLUE_REGION")
s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")

# Get Massive.com credentials from Airflow Variables
massive_access_key = Variable.get("MASSIVE_ACCESS_KEY", default_var="214ceb02-b057-4567-b406-1111868ddf6d")
massive_secret_key = Variable.get("MASSIVE_SECRET_KEY", default_var="Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q")

def run_backfill(**context):
    """Execute the backfill function with AWS and Massive credentials"""
    # Get execution date from Airflow context
    execution_date = context['ds']  # YYYY-MM-DD format
    output_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.stock_prices_history"
    
    result = backfill_stock_prices_from_massive(
        date=execution_date,
        output_table=output_table,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        s3_bucket=s3_bucket,
        massive_access_key=massive_access_key,
        massive_secret_key=massive_secret_key
    )
    
    print(result)
    return result

@dag(
    description="Backfill stock prices from Massive.com S3 endpoint using Python/boto3",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2025, 1, 1),
        "retries": 1,
        "execution_timeout": timedelta(hours=4),
    },
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    catchup=True,
    tags=["community", "backfill", "massive", "iceberg", "stock-prices"],
)
def backfill_stock_prices_dag():
    backfill_task = PythonOperator(
        task_id='backfill_stock_prices',
        python_callable=run_backfill
    )
    
    backfill_task

backfill_stock_prices_dag()

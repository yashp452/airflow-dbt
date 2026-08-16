from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from include.eczachly.poke_glue_partition import poke_glue_partition
from include.eczachly.scripts.polygon_stock_prices import fetch_stock_prices
import os

# Get AWS credentials from Airflow Variables
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
aws_region = Variable.get("AWS_GLUE_REGION")
s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")

def run_stock_prices_script(**context):
    """Execute the stock prices fetching function with AWS credentials"""
    run_date = context['ds']
    schema = os.environ.get('STUDENT_SCHEMA', 'zachwilson')
    tickers_table = f"{schema}.stock_tickers"
    output_table = f"{schema}.stock_prices"
    
    result = fetch_stock_prices(
        run_date=run_date,
        tickers_table=tickers_table,
        output_table=output_table,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        s3_bucket=s3_bucket
    )

    return result

@dag(
    description="A DAG that waits for Polygon ticker data then fetches stock prices and creates Snowflake Iceberg table",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2026, 1, 1),
        "retries": 1,
        "execution_timeout": timedelta(hours=2),
    },
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
    tags=["community", "polygon", "iceberg", "stock-prices", "snowflake"],
)
def massive_stock_prices_dag():
    schema = os.environ.get('STUDENT_SCHEMA', 'zachwilson')
    upstream_table = f'{schema}.polygon_tickers'
    
    wait_for_tickers = PythonOperator(
        task_id='wait_for_polygon_tickers',
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": upstream_table,
            "partition": 'date_day={{ ds }}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        }
    )
    
    fetch_prices = PythonOperator(
        task_id='fetch_stock_prices',
        python_callable=run_stock_prices_script
    )

    wait_for_tickers >> fetch_prices

massive_stock_prices_dag()

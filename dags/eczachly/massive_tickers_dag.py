from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from include.eczachly.scripts.pyiceberg_api_example import fetch_polygon_tickers
from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query
import os

# Get AWS credentials from Airflow Variables
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
aws_region = Variable.get("AWS_GLUE_REGION")
s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")

def run_pyiceberg_script(**context):
    """Execute the PyIceberg API example function with AWS credentials"""
    run_date = context['ds']
    output_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.massive_tickers_lab"
    
    result = fetch_polygon_tickers(
        run_date=run_date,
        output_table=output_table,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        s3_bucket=s3_bucket
    )

    return result

@dag(
    description="A DAG that fetches Polygon ticker data and writes to Iceberg table",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2026, 1, 1),
        "retries": 1,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
    tags=["community", "polygon", "iceberg"],
)
def massive_tickers_dag():
    schema = os.environ.get('STUDENT_SCHEMA', 'zachwilson')
    
    fetch_tickers = PythonOperator(
        task_id='fetch_massive_tickers',
        python_callable=run_pyiceberg_script
    )

    create_table = f"""
    CREATE TABLE {schema}.massive_tickers_lab_production
    ( active boolean, cik varchar,
      composite_figi varchar, 
      currency_name varchar,
      last_updated_utc varchar, 
      locale varchar, 
      market varchar, name varchar, primary_exchange varchar, share_class_figi varchar, ticker varchar, type varchar, date date ) 
        WITH ( format = 'PARQUET', partitioning = ARRAY['day(date)'])
    """

    create_step = PythonOperator(
        task_id="create_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query':create_table
        }
    )


    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f""""
                SELECT 
                  COUNT(DISTINCT ticker) = COUNT(ticker) as no_duplicates,
                  COUNT(CASE WHEN locale = 'us' AND currency_name <> 'USD' THEN 1 END) = 0 as no_stocks_in_us_trading_not_in_usd,
                  COUNT(CASE WHEN locale = 'us' AND currency_name IS NULL and market <> 'indices' THEN 1 END) = 0 as only_indices_in_us_can_have_no_currency,
                  COUNT(CASE WHEN market = 'otc' AND primary_exchange <> 'OTC Link' THEN 1 END) = 0 as otc_stock_should_not_be_on_an_exchange,
                  COUNT(CASE WHEN primary_exchange IS NULL AND market NOT IN ('crypto', 'indices', 'fx') THEN 1 END) = 0 as primary_exchange_should_not_be_null,
                  COUNT(CASE WHEN market IN ('crypto', 'indices', 'fx') AND primary_exchange IS NOT NULL THEN 1 END) = 0 as crypto_fx_and_indices_should_not_have_a_primary_exchange,
                  COUNT(CASE WHEN ticker IS NULL THEN 1 END) = 0 as ticker_should_not_be_null,
                  COUNT(CASE WHEN locale IS NULL THEN 1 END) = 0 as locale_should_not_be_null
                
                  FROM {schema}.massive_tickers_lab
                WHERE date = '{{ ds }}'
            """
        }
    )

    exchange_data_to_production = PythonOperator(
        task_id="exchange_to_production",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f""""
                  INSERT INTO {schema}.massive_tickers_lab_production
                  SELECT *
                  FROM {schema}.massive_tickers_lab
                  WHERE date = '{{ ds }}'
              """
        }
    )
    
    create_step >> fetch_tickers >> run_dq_check >> exchange_data_to_production

massive_tickers_dag()

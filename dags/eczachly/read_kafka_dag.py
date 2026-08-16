from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.eczachly.glue_job_submission import create_glue_job
import os
from airflow.models import Variable
local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')

s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
kafka_credentials = Variable.get("KAFKA_CREDENTIALS")

@dag(
    description="A dag that reads from the Kafka queue and dumps the data to Iceberg",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2026, 1, 1),
        "retries": 1,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2026, 1, 1),
    max_active_runs=15,
    schedule="@daily",
    catchup=True,
    template_searchpath='include/eczachly',
    tags=["community", 'bootcamp'],
)
def read_kafka_dag():
    start_glue_job_task = PythonOperator(
        task_id='start_glue_job',
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "read_web_events_kafka_{{ ds }}",
            "script_path": local_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "s3_bucket": s3_bucket,
            "aws_region": aws_region,
            "kafka_credentials": kafka_credentials,
            "description": "Testing Job Spark",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": 'bootcamp.web_events_production'
            },
        }
    )

read_kafka_dag()

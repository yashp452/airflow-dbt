import sys
from datetime import datetime
import requests
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    StringType,
    DateType,
    NestedField
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import os
import boto3
from dotenv import load_dotenv


def fetch_polygon_tickers(
    run_date: str,
    output_table: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_region: str = None,
    s3_bucket: str = None
):
    """
    Fetch Polygon ticker data and write to Iceberg table.
    
    Args:
        run_date: Date string in YYYY-MM-DD format
        output_table: Target Iceberg table (namespace.table_name)
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        aws_region: AWS region
        s3_bucket: S3 bucket for Iceberg warehouse
    """
    # Set AWS credentials if provided
    if aws_access_key_id:
        os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    if aws_secret_access_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    if aws_region:
        os.environ['AWS_REGION'] = aws_region
        os.environ['AWS_DEFAULT_REGION'] = aws_region
    if s3_bucket:
        os.environ['AWS_S3_BUCKET_TABULAR'] = s3_bucket
    
    # Get AWS region from environment variable or default to us-west-2
    aws_region = os.environ.get('AWS_REGION', 'us-west-2')
    # Set default output_table if not provided
    if output_table is None:
        output_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.polygon_tickers"
    
    # Get S3 bucket from environment
    s3_bucket = os.environ.get('AWS_S3_BUCKET_TABULAR', 'zachwilsonsorganization-522')
    if not s3_bucket:
        raise ValueError("AWS_S3_BUCKET_TABULAR environment variable is required")
    
    # Set the default region for boto3 to avoid NoRegionError
    boto3.setup_default_session(region_name=aws_region)
    
    # Configure PyIceberg to use AWS Glue catalog
    catalog = load_catalog(
        name="glue_catalog",
        **{
            "type": "glue",
            "region": aws_region,
            "warehouse": f"s3://{s3_bucket}/iceberg-warehouse/",
        }
    )
    
    # Fetch data from Polygon API
    api_key = 'Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q'
    starter_url = f'https://api.polygon.io/v3/reference/tickers?limit=1000&apiKey={api_key}'
    response = requests.get(starter_url)
    tickers = []
    data = response.json()
    tickers.extend(data['results'])
    
    # Collect all results
    while data['status'] == 'OK' and 'next_url' in data:
        tickers.extend(data['results'])
        print(data['next_url'] + '&apiKey=' + api_key)
        response = requests.get(data['next_url'] + '&apiKey=' + api_key)
        data = response.json()
    
    # Define PyIceberg schema
    schema = Schema(
        NestedField(1, "active", BooleanType(), required=False),
        NestedField(2, "cik", StringType(), required=False),
        NestedField(3, "composite_figi", StringType(), required=False),
        NestedField(4, "currency_name", StringType(), required=False),
        NestedField(5, "last_updated_utc", StringType(), required=False),
        NestedField(6, "locale", StringType(), required=False),
        NestedField(7, "market", StringType(), required=False),
        NestedField(8, "name", StringType(), required=False),
        NestedField(9, "primary_exchange", StringType(), required=False),
        NestedField(10, "share_class_figi", StringType(), required=False),
        NestedField(11, "ticker", StringType(), required=False),
        NestedField(12, "type", StringType(), required=False),
        NestedField(13, "date", DateType(), required=False)
    )
    
    # Parse run date
    date = datetime.strptime(run_date, "%Y-%m-%d").date()
    
    # Convert tickers to PyArrow table
    # Add the date column to each record

    filtered_tickers = []
    tickers_dict = {}

    for ticker in tickers:
        if ticker['ticker'] not in tickers_dict:
            ticker['date'] = date
            ticker['currency_name'] = ticker['currency_name'].upper() if 'currency_name' in ticker else None
            if 'primary_exchange' in ticker and ticker['primary_exchange'] == 'Grey Market' and ticker['market'] == 'otc':
                ticker['primary_exchange'] = 'OTC Link'
            filtered_tickers.append(ticker)
            tickers_dict[ticker['ticker']] = 1

    
    # Create PyArrow schema
    pa_schema = pa.schema([
        ('active', pa.bool_()),
        ('cik', pa.string()),
        ('composite_figi', pa.string()),
        ('currency_name', pa.string()),
        ('last_updated_utc', pa.string()),
        ('locale', pa.string()),
        ('market', pa.string()),
        ('name', pa.string()),
        ('primary_exchange', pa.string()),
        ('share_class_figi', pa.string()),
        ('ticker', pa.string()),
        ('type', pa.string()),
        ('date', pa.date32())
    ])
    
    # Convert to PyArrow table
    pa_table = pa.Table.from_pylist(filtered_tickers, schema=pa_schema)
    
    # Create or load table
    namespace, table_name = output_table.split('.')
    try:
        table = catalog.load_table(output_table)
        print(f"Table {output_table} already exists")
    except Exception:
        # Create table if it doesn't exist
        partition_spec = PartitionSpec(
            PartitionField(source_id=13, field_id=1000, transform=DayTransform(), name="date_day")
        )
        
        table = catalog.create_table(
            identifier=output_table,
            schema=schema,
            partition_spec=partition_spec
        )
        print(f"Created table {output_table}")
    
    # Overwrite partition for the run_date
    table.overwrite(pa_table)
    
    print(f"Successfully wrote {len(tickers)} tickers to {output_table} for date {date}")
    return f"Successfully wrote {len(tickers)} tickers to {output_table} for date {date}"


# Script execution mode (when run directly)
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Get arguments from command line
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    output_table = sys.argv[2] if len(sys.argv) > 2 else f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.massive_tickers_lab"
    
    fetch_polygon_tickers(run_date, output_table)

import sys
from datetime import datetime, timedelta
import boto3
import gzip
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    TimestampType,
    DoubleType,
    LongType,
    NestedField
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import os
from dotenv import load_dotenv


def backfill_stock_prices_from_massive(
    date: str,
    output_table: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_region: str = None,
    s3_bucket: str = None,
    massive_access_key: str = None,
    massive_secret_key: str = None
):
    """
    Backfill stock prices from Massive.com S3 endpoint for a single date.
    
    Args:
        date: Date in YYYY-MM-DD format
        output_table: Target Iceberg table
        aws_access_key_id: AWS access key ID for Glue catalog
        aws_secret_access_key: AWS secret access key for Glue catalog
        aws_region: AWS region
        s3_bucket: S3 bucket for Iceberg warehouse
        massive_access_key: Massive.com S3 access key
        massive_secret_key: Massive.com S3 secret key
    """

    # Set AWS credentials for Glue catalog if provided
    if aws_access_key_id:
        os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    if aws_secret_access_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    if aws_region:
        os.environ['AWS_REGION'] = aws_region
        os.environ['AWS_DEFAULT_REGION'] = aws_region
    if s3_bucket:
        os.environ['AWS_S3_BUCKET_TABULAR'] = s3_bucket
    
    # Set default output_table if not provided
    if output_table is None:
        output_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.stock_prices_backfill"
    
    # Get AWS region from environment variable or default
    aws_region = os.environ.get('AWS_REGION', 'us-west-2')
    s3_bucket = os.environ.get('AWS_S3_BUCKET_TABULAR', 'zachwilsonsorganization-522')
    if not s3_bucket:
        raise ValueError("AWS_S3_BUCKET_TABULAR environment variable is required")
    
    # Get Massive.com credentials
    massive_access_key = massive_access_key or os.environ.get('MASSIVE_ACCESS_KEY', '214ceb02-b057-4567-b406-1111868ddf6d')
    massive_secret_key = massive_secret_key or os.environ.get('MASSIVE_SECRET_KEY', 'Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q')
    
    # Configure PyIceberg to use AWS Glue catalog
    catalog = load_catalog(
        name="glue_catalog",
        **{
            "type": "glue",
            "region": aws_region,
            "warehouse": f"s3://{s3_bucket}/iceberg-warehouse/",
        }
    )
    
    # Create S3 client for Massive.com endpoint
    s3_client = boto3.client(
        's3',
        endpoint_url='https://files.massive.com',
        aws_access_key_id=massive_access_key,
        aws_secret_access_key=massive_secret_key
    )
    
    # Parse date
    current_date = datetime.strptime(date, "%Y-%m-%d").date()
    
    # Define PyIceberg schema for stock prices
    schema = Schema(
        NestedField(1, "ticker", StringType(), required=False),
        NestedField(2, "window_start", LongType(), required=False),
        NestedField(3, "open", DoubleType(), required=False),
        NestedField(4, "high", DoubleType(), required=False),
        NestedField(5, "low", DoubleType(), required=False),
        NestedField(6, "close", DoubleType(), required=False),
        NestedField(7, "volume", LongType(), required=False),
        NestedField(8, "vwap", DoubleType(), required=False),
        NestedField(9, "transactions", LongType(), required=False),
        NestedField(10, "date", TimestampType(), required=False)
    )
    
    # Create or load table
    namespace, table_name = output_table.split('.')
    try:
        table = catalog.load_table(output_table)
        print(f"Table {output_table} already exists")
    except Exception:
        # Create table if it doesn't exist
        partition_spec = PartitionSpec(
            PartitionField(source_id=10, field_id=1000, transform=DayTransform(), name="date_day")
        )
        
        table = catalog.create_table(
            identifier=output_table,
            schema=schema,
            partition_spec=partition_spec
        )
        print(f"Created table {output_table}")
    
    # Process the single date
    date_str = current_date.strftime("%Y-%m-%d")
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    
    # Construct S3 key for the date
    s3_key = f"us_stocks_sip/minute_aggs_v1/{year}/{month}/{date_str}.csv.gz"
    
    print(f"Processing {date_str}...")
    
    try:
        # Download and decompress the file
        response = s3_client.get_object(Bucket='flatfiles', Key=s3_key)
        
        # Read gzipped CSV
        with gzip.GzipFile(fileobj=response['Body']) as gzipped_file:
            # Read CSV into pandas DataFrame
            df = pd.read_csv(gzipped_file)
        
        # Assuming CSV columns: ticker, timestamp, open, high, low, close, volume, vwap, transactions
        # Add date column
        df['date'] = pd.to_datetime(current_date)
        
        # Convert timestamp to datetime if it's not already
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Create PyArrow schema
        pa_schema = pa.schema([
            ('ticker', pa.string()),
            ('open', pa.float64()),
            ('high', pa.float64()),
            ('window_start', pa.int64()),
            ('low', pa.float64()),
            ('close', pa.float64()),
            ('volume', pa.int64()),
            ('transactions', pa.int64()),
            ('date', pa.timestamp('us'))
        ])
        
        # Convert to PyArrow table
        pa_table = pa.Table.from_pandas(df, schema=pa_schema)
        
        # Append to Iceberg table
        table.overwrite(pa_table)
        
        records_count = len(df)
        print(f"Loaded {records_count} records for {date_str}")
        
        return f"Backfill complete: {records_count} records loaded for {date_str}"
        
    except Exception as e:
        error_msg = f"Error processing {date_str}: {str(e)}"
        print(error_msg)


# Script execution mode (when run directly)
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Get date argument from command line
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    output_table = sys.argv[2] if len(sys.argv) > 2 else f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.stock_prices_history"
    
    backfill_stock_prices_from_massive(date, output_table)

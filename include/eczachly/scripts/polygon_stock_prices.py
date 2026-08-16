import sys
from datetime import datetime, timedelta
import requests
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
import boto3
from dotenv import load_dotenv
import time


def fetch_stock_prices(
    run_date: str,
    tickers_table: str = None,
    output_table: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    aws_region: str = None,
    s3_bucket: str = None
):
    """
    Fetch stock prices from Polygon API for tickers in the tickers table.
    
    Args:
        run_date: Date string in YYYY-MM-DD format
        tickers_table: Source Iceberg table with ticker list
        output_table: Target Iceberg table for stock prices
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
    
    # Set default tables if not provided
    if tickers_table is None:
        tickers_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.polygon_tickers"
    if output_table is None:
        output_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.polygon_stock_prices"
    
    # Get AWS region from environment variable or default to us-west-2
    aws_region = os.environ.get('AWS_REGION', 'us-west-2')
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
    
    # Parse run date
    date = datetime.strptime(run_date, "%Y-%m-%d").date()
    
    # Load tickers from the tickers table for the given date
    print(f"Loading tickers from {tickers_table} for date {date}")
    tickers_tbl = catalog.load_table(tickers_table)
    
    # Query tickers for the specific date
    df = tickers_tbl.scan(
        row_filter=f"date = '{date}'"
    ).to_pandas()
    
    if df.empty:
        raise ValueError(f"No tickers found for date {date}")
    
    ticker_symbols = df['ticker'].dropna().unique().tolist()
    print(f"Found {len(ticker_symbols)} tickers to fetch prices for")
    
    # Fetch stock prices from Polygon API
    api_key = os.getenv("MASSIVE_API_KEY")
    stock_prices = []
    
    for ticker in ticker_symbols:
        # Get aggregates (OHLCV) for the ticker on the run_date
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{run_date}/{run_date}?apiKey={api_key}"
        
        try:
            response = requests.get(url)
            data = response.json()
            
            if data.get('status') == 'OK' and data.get('results'):
                for result in data['results']:
                    stock_prices.append({
                        'ticker': ticker,
                        'timestamp': datetime.fromtimestamp(result['t'] / 1000),
                        'open': result.get('o'),
                        'high': result.get('h'),
                        'low': result.get('l'),
                        'close': result.get('c'),
                        'volume': result.get('v'),
                        'vwap': result.get('vw'),
                        'transactions': result.get('n'),
                        'date': date
                    })
            
            # Rate limiting - Polygon free tier allows 5 requests per minute
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Error fetching data for {ticker}: {str(e)}")
            continue
    
    if not stock_prices:
        raise ValueError(f"No stock prices fetched for date {date}")
    
    print(f"Fetched {len(stock_prices)} stock price records")
    
    # Define PyIceberg schema for stock prices
    schema = Schema(
        NestedField(1, "ticker", StringType(), required=False),
        NestedField(2, "timestamp", TimestampType(), required=False),
        NestedField(3, "open", DoubleType(), required=False),
        NestedField(4, "high", DoubleType(), required=False),
        NestedField(5, "low", DoubleType(), required=False),
        NestedField(6, "close", DoubleType(), required=False),
        NestedField(7, "volume", LongType(), required=False),
        NestedField(8, "vwap", DoubleType(), required=False),
        NestedField(9, "transactions", LongType(), required=False),
        NestedField(10, "date", TimestampType(), required=False)
    )
    
    # Create PyArrow schema
    pa_schema = pa.schema([
        ('ticker', pa.string()),
        ('timestamp', pa.timestamp('us')),
        ('open', pa.float64()),
        ('high', pa.float64()),
        ('low', pa.float64()),
        ('close', pa.float64()),
        ('volume', pa.int64()),
        ('vwap', pa.float64()),
        ('transactions', pa.int64()),
        ('date', pa.timestamp('us'))
    ])
    
    # Convert to PyArrow table
    pa_table = pa.Table.from_pylist(stock_prices, schema=pa_schema)
    
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
    
    # Overwrite partition for the run_date
    table.overwrite(pa_table)
    
    print(f"Successfully wrote {len(stock_prices)} stock price records to {output_table} for date {date}")
    return f"Successfully wrote {len(stock_prices)} stock price records to {output_table} for date {date}"


# Script execution mode (when run directly)
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Get arguments from command line
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    tickers_table = sys.argv[2] if len(sys.argv) > 2 else f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.polygon_tickers"
    output_table = sys.argv[3] if len(sys.argv) > 3 else f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.polygon_stock_prices"
    
    fetch_stock_prices(run_date, tickers_table, output_table)

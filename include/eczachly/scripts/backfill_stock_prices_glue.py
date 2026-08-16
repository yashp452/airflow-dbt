import sys
import ast
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, lit, col, to_timestamp

# Get job arguments
args = getResolvedOptions(
    sys.argv, 
    ["JOB_NAME", "date", "output_table", "massive_credentials"]
)

date = args['date']
output_table = args['output_table']
massive_credentials = ast.literal_eval(args['massive_credentials'])

# Create SparkSession
spark = SparkSession.builder \
    .appName("BackfillStockPrices") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Parse date for S3 path
current_date = datetime.strptime(date, "%Y-%m-%d")
year = current_date.strftime("%Y")
month = current_date.strftime("%m")

# Configure S3 access for Massive.com endpoint
massive_access_key = massive_credentials['AWS_ACCESS_KEY_ID']
massive_secret_key = massive_credentials['AWS_SECRET_ACCESS_KEY']

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", massive_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", massive_secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://files.massive.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Define S3 path for the date
s3_bucket = "s3a://flatfiles"
file_path = f"{s3_bucket}/us_stocks_sip/minute_aggs_v1/{year}/{month}/{date}.csv.gz"

print(f"Reading data from: {file_path}")

try:
    # Read the gzipped CSV file from S3
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)

    # Create or replace the Iceberg table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {output_table} (
        ticker STRING,
        window_start BIGINT,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        transactions BIGINT,
        date TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(date))
    """)

    # Add date column and write to Iceberg table
    df_with_date = df.withColumn('date', lit(date).cast('timestamp'))

    # Write the DataFrame to the Iceberg table with partition overwrite
    (
        df_with_date
        .sortWithinPartitions("ticker")
        .write
        .format("iceberg")
        .mode("append")
        .option("write.spark.fanout.enabled", "true")
        .save(output_table)
    )

    records_count = df.count()
    print(f"Loaded {records_count} records for {date}")

    # Show schema and sample
    df_with_date.printSchema()
    print(f"Sample records:")
    df_with_date.show(5)
except Exception as e:
    print(f"Error processing {date}: {str(e)}")


job = Job(glueContext)
job.init(args["JOB_NAME"], args)

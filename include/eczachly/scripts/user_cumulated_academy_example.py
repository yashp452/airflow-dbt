import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize Spark session
spark = (SparkSession.builder
         .getOrCreate())

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", "yesterday_ds", 'output_table'])
run_date = args['ds']
yesterday_ds = args['yesterday_ds']
output_table = args['output_table']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
df = spark.sql(f"""
    WITH today AS (
        SELECT 
            user_id,
            SUM(event_count) as total_events,
            null as academy_map
        FROM {os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.user_web_events_daily 
        WHERE ds = DATE('{run_date}')
        GROUP BY user_id 
    ),
    yesterday AS (
        SELECT * FROM {os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.user_web_events_cumulated_master 
        WHERE ds = DATE('{yesterday_ds}')
    )
    
    SELECT 
        COALESCE(t.user_id, yesterday.user_id) as user_id,
        t.user_id IS NOT NULL as is_daily_active,
        CONCAT(
            ARRAY(CASE WHEN t.user_id IS NOT NULL THEN t.total_events ELSE 0 END),
            COALESCE(yesterday.activity_array_last_30d, ARRAY())
        ) as activity_array_last_30d,
        NULL as activity_map_last_30d,
        DATE('{run_date}') AS ds
    FROM today t FULL OUTER JOIN yesterday ON t.user_id = yesterday.user_id
""")

df.writeTo(f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.user_web_events_cumulated_master") \
    .tableProperty("write.spark.fanout.enabled", "true") \
    .overwritePartitions()

# Initialize and run Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize Spark session
spark = (SparkSession.builder
         .getOrCreate())

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'branch', 'output_table'])

branch = args['branch'] or 'audit_branch'
run_date = args['ds']
output_table = args['output_table']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Uncomment for fast-forwarding
spark.sql(f""" CALL system.fast_forward('{output_table}', 'main', '{branch}') """)
spark.sql(f""" ALTER TABLE {output_table} DROP BRANCH {branch}""")

# Initialize and run Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

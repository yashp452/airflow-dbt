from pyspark.sql import SparkSession


# events -  user_id, event_type, event_timestamp, ds (partition key)
# users - user_id, age, country, ds (partition key)
# aggregate output -  age, country, post_event_count, like_event_count, total_event_count, ds
#testing cicd workflow
def do_event_agg_job_transformation(spark, user_dataframe, event_dataframe, ds):
    query = f"""
    SELECT
         u.age,
         u.country,
         COUNT(CASE WHEN e.event_type = 'post' THEN 1 END) post_event_count,
         COUNT(CASE WHEN e.event_type = 'like' THEN 1 END) like_event_count,
         COUNT(CASE WHEN e.event_type = 'comment' THEN 1 END) comment_event_count,
         COUNT(e.event_type) total_event_count,
         e.ds 
    FROM events e JOIN users u ON e.user_id = u.user_id
    WHERE e.ds = '{ds}' AND u.ds = '{ds}'
    GROUP BY  u.age, u.country, e.ds
    """
    event_dataframe.createOrReplaceTempView("events")
    user_dataframe.createOrReplaceTempView("users")
    return spark.sql(query)


def main():
    ds = '2023-01-01'
    spark = SparkSession.builder \
      .master("local") \
      .appName("event_agg") \
      .getOrCreate()
    output_df = do_event_agg_job_transformation(spark, spark.table('users'), spark.table("events"), ds)
    output_df.write.mode("overwrite").insertInto("daily_event_aggregate")
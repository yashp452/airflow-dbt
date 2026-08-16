from pyspark.sql import SparkSession


# events -  user_id, event_type, event_timestamp, ds (partition key)
# users - user_id, age, country, ds (partition key)
# aggregate output -  age, country, post_event_count, like_event_count, total_event_count, ds
def do_course_consumption_agg_transformation(spark, courses, users, view_events, ds):
    query = f"""
    SELECT
         u.user_id,
         COUNT(DISTINCT c.course_id) as courses_viewed,
         SUM(v.duration_seconds) as total_view_time,
         COUNT(1) as total_view_events,
         v.ds 
    FROM view_events v  
    JOIN courses c ON v.course_id = c.course_id
    JOIN users u ON v.user_id = u.user_id
    WHERE v.ds = '{ds}' AND u.ds = '{ds}' AND c.ds = '{ds}'
    GROUP BY u.user_id
    """
    courses.createOrReplaceTempView("courses")
    users.createOrReplaceTempView("users")
    view_events.createOrReplaceTempView("view_events")
    return spark.sql(query)


def main():
    ds = '2023-01-01'
    spark = SparkSession.builder \
      .master("local") \
      .appName("event_agg") \
      .getOrCreate()
    output_df = do_course_consumption_agg_transformation(spark, spark.table('users'), spark.table("events"), ds)
    output_df.write.mode("overwrite").insertInto("daily_event_aggregate")
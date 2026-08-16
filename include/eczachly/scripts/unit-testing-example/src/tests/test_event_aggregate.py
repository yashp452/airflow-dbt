from pyspark.testing import assertDataFrameEqual
from ..jobs.event_agg_job import do_event_agg_job_transformation
from collections import namedtuple

import pytest


User = namedtuple("User",  "user_id age country ds")
Event = namedtuple("Event",  "user_id event_type event_timestamp ds")
AggregatedEvent = namedtuple("AggregatedEvent",  "age country post_event_count like_event_count comment_event_count total_event_count ds")



# 1. Create the input User data
users = [
    User(user_id="u1", age=25, country="US", ds="2023-01-01"),
    User(user_id="u2", age=30, country="US", ds="2023-01-01"),
    User(user_id="u3", age=40, country="UK", ds="2023-01-01"),
    User(user_id="u4", age=40, country="UK", ds="2023-01-01"),
]

# 2. Create the input Event data
events = [
    Event(user_id="u1", event_type="post", event_timestamp="2023-01-01 09:00:00", ds="2023-01-01"),
    Event(user_id="u1", event_type="like", event_timestamp="2023-01-01 09:05:00", ds="2023-01-01"),
    Event(user_id="u2", event_type="post", event_timestamp="2023-01-01 10:00:00", ds="2023-01-01"),
    Event(user_id="u2", event_type="like", event_timestamp="2023-01-01 10:10:00", ds="2023-01-01"),
    Event(user_id="u2", event_type="post", event_timestamp="2023-01-01 10:15:00", ds="2023-01-01"),
    Event(user_id="u3", event_type="like", event_timestamp="2023-01-01 11:00:00", ds="2023-01-01"),
    Event(user_id="u3", event_type="like", event_timestamp="2023-01-01 11:02:00", ds="2023-01-01"),
    Event(user_id="u3", event_type="comment", event_timestamp="2023-01-01 11:02:00", ds="2023-01-01"),
]

aggregated_events = [
    AggregatedEvent(age=25, country="US", post_event_count=1, like_event_count=1, comment_event_count=0,  total_event_count=2, ds="2023-01-01"),
    AggregatedEvent(age=30, country="US", post_event_count=2, like_event_count=1, comment_event_count=0, 
     total_event_count=3, ds="2023-01-01"),
    AggregatedEvent(age=40, country="UK", post_event_count=0, like_event_count=2, comment_event_count=1, total_event_count=3, ds="2023-01-01"),
]

def test_event_aggregate(spark):
    users_df = spark.createDataFrame(users)
    events_df = spark.createDataFrame(events)
    expected_df = spark.createDataFrame(aggregated_events)
    actual_df = do_event_agg_job_transformation(spark, users_df, events_df, "2023-01-01")
    assertDataFrameEqual(actual_df, expected_df)

# events -  user_id, event_type, event_timestamp, ds (partition key)
# users - user_id, age, country, ds (partition key)
# aggregate output -  age, country, post_event_count, like_event_count, total_event_count, ds
# def do_event_agg_job_transformation(spark, user_dataframe, event_dataframe, ds):
#     query = f"""
#     SELECT
#          u.age,
#          u.country,
#          COUNT(CASE WHEN e.event_type = 'post' THEN 1 END) post_event_count,
#          COUNT(CASE WHEN e.event_type = 'like' THEN 1 END) like_event_count,
#          COUNT(e.event_type) total_event_count,
#          e.ds 
#     FROM events e JOIN users u ON e.user_id = u.user_id
#     WHERE e.ds = '{ds}' AND u.ds = '{ds}'
#     GROUP BY  u.age, u.country, e.ds
#     """
#     event_dataframe.createOrReplaceTempView("events")
#     user_dataframe.createOrReplaceTempView("users")
#     return spark.sql(query)
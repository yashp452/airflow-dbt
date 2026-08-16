from pyspark.testing import assertDataFrameEqual
from ..jobs.monthly_user_site_hits_job import do_monthly_user_site_hits_transformation
from collections import namedtuple



import pytest



User = namedtuple("User",  "user_id country date_partition")
MonthlySiteHit = namedtuple("MonthlySiteHit",  "month_start user_id hit_array date_partition")
MonthlySiteHitsAgg = namedtuple("MonthlySiteHitsAgg",  "month_start country num_hits_first_day num_hits_second_day num_hits_third_day")


def test_monthly_site_hits(spark):
    ds = "2023-03-01"
    new_month_start = "2023-04-01"
    input_data = [
        # Make sure basic case is handled gracefully
        MonthlySiteHit(
            month_start=ds,
            user_id=2,
            hit_array=[0, 1, 3],
            date_partition=ds
        ),
        MonthlySiteHit(
            month_start=ds,
            user_id=1,
            hit_array=[1, 2, 3],
            date_partition=ds
        ),
        #  Make sure empty array is handled gracefully
        MonthlySiteHit(
            month_start=new_month_start,
            user_id=2,
            hit_array=[],
            date_partition=ds
        ),
        # Make sure other partitions get filtered
        MonthlySiteHit(
            month_start=new_month_start,
            user_id=2,
            hit_array=[],
            date_partition=""
        )
    ]

    users = [
        User(
            user_id=1,
            country='India',
            date_partition=ds
        ),
        User(
            user_id=2,
            country='United States',
            date_partition=ds
        ),
    ]

    source_df = spark.createDataFrame(input_data)
    user_df = spark.createDataFrame(users)
    actual_df = do_monthly_user_site_hits_transformation(spark, user_df, source_df, ds)

    expected_values = [
        MonthlySiteHitsAgg(
            month_start=ds,
            country='India',
            num_hits_first_day=1,
            num_hits_second_day=2,
            num_hits_third_day=3
        ),
        MonthlySiteHitsAgg(
            month_start=ds,
            country='United States',
            num_hits_first_day=0,
            num_hits_second_day=1,
            num_hits_third_day=3
        ),
        MonthlySiteHitsAgg(
            month_start=new_month_start,
            country='United States',
            num_hits_first_day=0,
            num_hits_second_day=0,
            num_hits_third_day=0
        )
    ]
    expected_df = spark.createDataFrame(expected_values)
    assertDataFrameEqual(actual_df, expected_df)


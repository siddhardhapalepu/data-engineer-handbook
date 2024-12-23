from chispa.dataframe_comparer import *
from ..jobs.users_cumulated_job import do_users_cumulated_transformation
from collections import namedtuple
from datetime import date

Events = namedtuple("Events", "url referrer user_id device_id host event_time")
UsersCumulated = namedtuple("UsersCumulated", "user_id dates_active date")


def test_users_cumulated(spark):
    source_data = [
        Events("/robots.txt", "", "10060569187331700000", "10598707916011500000", "www.zachwilson.tech", "2023-01-01 03:03:34.304000"),
        Events("/robots.txt", "", "10060569187331700000", "10598707916011500000", "www.zachwilson.tech", "2023-01-01 03:03:34.304000"),
        Events("/about", "", "10945278839921100000", "16554774340712500000", "www.eczachly.com", "2023-01-01 12:07:12.949000"),
        Events("/about", "", "10945278839921100000", "16554774340712500000", "www.eczachly.com", "2023-01-01 12:07:12.949000"),
        Events("/about", "", "11355648956477300000", "8453901392618209000", "www.eczachly.com", "2023-01-01 12:07:08.829000"),
        Events("/about", "", "11355648956477300000", "8453901392618209000", "www.eczachly.com", "2023-01-01 12:07:08.829000"),
        Events("/about", "", "11355648956477300000", "16554774340712500000", "www.eczachly.com", "2023-01-01 12:08:01.848000"),
        Events("/about", "", "11355648956477300000", "16554774340712500000", "www.eczachly.com", "2023-01-01 12:08:01.848000"),
        Events("/about", "", "11355648956477300000", "8453901392618209000", "www.eczachly.com", "2023-01-01 23:58:01.266000"),
        Events("/about", "", "11355648956477300000", "8453901392618209000", "www.eczachly.com", "2023-01-01 23:58:01.266000"),
        Events("/about", "", "11355648956477300000", "8453901392618209000", "www.eczachly.com", "2023-01-01 23:58:02.442000"),
        Events("/about", "", "11355648956477300000", "8453901392618209000", "www.eczachly.com", "2023-01-01 23:58:02.442000")
    ]
    source_df = spark.createDataFrame(source_data)
    
    actual_df = do_users_cumulated_transformation(spark, source_df)
    expected_data = [
        UsersCumulated("11355648956477300000",[date(2023, 1, 1)],date(2023, 1, 1)),
        UsersCumulated("10060569187331700000",[date(2023, 1, 1)],date(2023, 1, 1)),
        UsersCumulated("10945278839921100000",[date(2023, 1, 1)],date(2023, 1, 1))
        
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DateType, ArrayType


query = """
WITH yesterday AS (
    SELECT * FROM users_cumulated uc 
    WHERE date = '2022-12-31'
),

today AS (
    SELECT 
        CAST(user_id AS STRING) AS user_id,
        TO_DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM
        events 
    WHERE TO_DATE(CAST(event_time AS TIMESTAMP)) = '2023-01-01'
        AND user_id IS NOT NULL 
    GROUP BY user_id, date_active
)

SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE 
        WHEN y.dates_active IS NULL THEN ARRAY(t.date_active)
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE CONCAT(y.dates_active, ARRAY(t.date_active))
    END AS dates_active,
    COALESCE(t.date_active, DATE_ADD(y.date, 1)) AS date
FROM today t 
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
"""

def do_users_cumulated_transformation(spark, dataframe):
    users_cumulated_schema = StructType([
    StructField("user_id", LongType(), False),  # False for nullable implies it's a part of the primary key
    StructField("dates_active", ArrayType(DateType()), True),  # An array of dates
    StructField("date", DateType(), False)  # Part of the primary key
    ])
    users_cumulated_df = spark.createDataFrame(data=[], schema=users_cumulated_schema)
    users_cumulated_df.createOrReplaceTempView("users_cumulated")
    dataframe.createOrReplaceTempView("events")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("users_cumulated") \
      .getOrCreate()
    output_df = do_users_cumulated_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("users_cumulated")
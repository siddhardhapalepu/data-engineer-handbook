from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, ArrayType


query = """
-- Define the "yesterday" CTE
WITH yesterday AS (
  SELECT *
  FROM actors af
  WHERE current_year = 1969
),

-- Define the "today" CTE
today AS (
  SELECT 
    actor, 
    actorid, 
    year,
    collect_list(named_struct(
      'film', film,
      'votes', CAST(votes as INT),
      'rating', CAST(rating as DOUBLE),
      'filmid', filmid
    )) AS films,
    ROUND(AVG(rating), 0) AS avg_rating
  FROM actor_films af
  WHERE year = 1970
  GROUP BY actor, actorid, year
)

-- Main query
SELECT 
  COALESCE(y.actor, t.actor) AS actor,
  COALESCE(y.actorid, t.actorid) AS actorid,
  CASE 
    WHEN y.films IS NULL THEN t.films
    WHEN t.films IS NOT NULL THEN concat(y.films, t.films)
    ELSE y.films
  END AS films,
  CASE 
    WHEN t.films IS NOT NULL THEN
      CASE 
        WHEN t.avg_rating > 8 THEN 'star'
        WHEN t.avg_rating > 7 AND t.avg_rating <= 8 THEN 'good'
        WHEN t.avg_rating > 6 AND t.avg_rating <= 7 THEN 'average'
        WHEN t.avg_rating <= 6 THEN 'bad'
      END
    ELSE y.quality_class
  END AS quality_class,
  CASE 
    WHEN t.films IS NULL THEN CAST(y.year_last_active AS INT)
    ELSE CAST(t.year AS INT)
  END AS year_last_active,
  1970 AS current_year,
  CASE 
    WHEN t.films IS NULL THEN false 
    ELSE true 
  END AS is_active
FROM today t
FULL OUTER JOIN yesterday y
ON t.actorid = y.actorid
"""
def do_actors_scd_transformation(spark, dataframe):
    film_schema = StructType([
    StructField("film", StringType(), True),
    StructField("votes", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("filmid", StringType(), True)
    ])
    
    actors_schema = StructType([
    StructField("actor", StringType(), True),
    StructField("actorid", StringType(), True),
    StructField("films", ArrayType(film_schema), True),  # Assuming films are stored as a string (e.g., JSON)
    StructField("quality_class", StringType(), True),
    StructField("year_last_active", IntegerType(), True),
    StructField("current_year", IntegerType(), True),
    StructField("is_active", BooleanType(), True)
])
    actors_df = spark.createDataFrame(data=[], schema = actors_schema)
    actors_df.createOrReplaceTempView("actors")
    dataframe.createOrReplaceTempView("actor_films")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()
    output_df = do_actors_scd_transformation(spark, spark.table("actor_films"))
    output_df.write.mode("overwrite").insertInto("players_scd")
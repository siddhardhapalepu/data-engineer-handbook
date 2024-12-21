from chispa.dataframe_comparer import *
from ..jobs.actors_cumulative_job import do_actors_scd_transformation
from collections import namedtuple
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, FloatType, ArrayType, Row, BooleanType

ActorFilms = namedtuple("ActorFilms", "actor actorid film year votes rating filmid")
Actors = namedtuple("Actors", "actor actorid films quality_class year_last_active current_year is_active")

def test_actors_scd_generation(spark):
    source_data = [
        ActorFilms("Alain Delon","nm0001128","Le Cercle Rouge","1970","22796","8.0","tt0065531"),
        ActorFilms("Alain Delon","nm0001128","Borsalino","1970","3578","7.0","tt0065486"),
        ActorFilms("Alain Delon","nm0001128","The Love Mates","1970","236","5.8","tt0242632"),
        ActorFilms("Alan Alda","nm0000257","The Moonshine War","1970","468","5.9","tt0066096"),
        ActorFilms("Alan Alda","nm0000257","Jenny","1970","137","5.5","tt0064510")
    ]
    source_df = spark.createDataFrame(source_data)
    actual_df = do_actors_scd_transformation(spark, source_df)
    """
    expected_data = [
        Actors("Alain Delon","nm0001128","{""(The Love Mates,236,5.8,tt0242632)"",""(Le Cercle Rouge,22796,8.0,tt0065531)"",(Borsalino,3578,7.0,tt0065486)}","average",1970,1970,True),
        Actors("Alan Alda","nm0000257","{""(The Moonshine War,468,5.9,tt0066096)"",(Jenny,137,5.5,tt0064510)}","bad",1970,1970,True)
    ]
    """
    # Film schema definition
    film_schema = StructType([
        StructField("film", StringType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("filmid", StringType(), True)
    ])

    # Actor schema definition
    actor_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actorid", StringType(), True),
        StructField("films", ArrayType(film_schema), True),
        StructField("quality_class", StringType(), True),
        StructField("year_last_active", IntegerType(), True),
        StructField("current_year", IntegerType(), True),
        StructField("is_active", BooleanType(), True)
    ])

    expected_data = [
    Row(
        actor="Alain Delon",
        actorid="nm0001128",
        films=[
            Row(film="Le Cercle Rouge", votes=22796, rating=8.0, filmid="tt0065531"),
            Row(film="Borsalino", votes=3578, rating=7.0, filmid="tt0065486"),
            Row(film="The Love Mates", votes=236, rating=5.8, filmid="tt0242632")
        ],
        quality_class="average",
        year_last_active=1970,
        current_year=1970,
        is_active=True
    ),
    Row(
        actor="Alan Alda",
        actorid="nm0000257",
        films=[
            Row(film="The Moonshine War", votes=468, rating=5.9, filmid="tt0066096"),
            Row(film="Jenny", votes=137, rating=5.5, filmid="tt0064510")
        ],
        quality_class="bad",
        year_last_active=1970,
        current_year=1970,
        is_active=True
    )
    ]
    expected_df = spark.createDataFrame(expected_data, actor_schema)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
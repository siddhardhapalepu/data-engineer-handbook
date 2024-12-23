from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# setting broadcast join off
spark = SparkSession.builder.appName('BroadcastJoin').getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Creating dataframes of all the datasets
medals_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
                                                       
maps_df = spark.read.option('header', 'true').option("inferSchema", "true").csv('/home/iceberg/data/maps.csv')

matches_df = spark.read.option('header', 'true').option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")

matchDetails_df =  spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")

medal_match_df =  spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")

# Creating matches bucketed table
matches_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    mapid STRING,
    playlist_id STRING,
    is_match_over BOOLEAN,
    is_team_game BOOLEAN,
    completion_date TIMESTAMP
)
USING iceberg
PARTITIONED BY (completion_date, bucket(16, match_id))
"""

# Execute the DDL statement
spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
spark.sql(matches_bucketed_ddl)

matches_df.select(
     F.col("match_id"), F.col("mapid"),F.col("playlist_id"),\
       F.col("is_match_over"), F.col("is_team_game"),  F.col("completion_date")
    ).writeTo("bootcamp.matches_bucketed") \
 .option("write.distribution-mode", "hash") \
 .createOrReplace()

# Creating matches_details bucketed table

match_details_bucketed_DDL = """
 CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")
spark.sql(match_details_bucketed_DDL)

matchDetails_df.select(
     F.col("match_id"), F.col("player_gamertag"),F.col("player_total_kills"),\
       F.col("player_total_deaths")
    ).writeTo("bootcamp.match_details_bucketed") \
 .option("write.distribution-mode", "hash") \
 .createOrReplace()
 
 
# Creating medal_matches bucketed table
medals_matches_players_bucketed_ddl = """
 CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
     match_id STRING,
     player_gamertag STRING,
     medal_id LONG,
     count INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """

spark.sql("""DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed""")
spark.sql(medals_matches_players_bucketed_ddl)
medal_match_df.select(
     F.col("match_id"), F.col("player_gamertag"),F.col("medal_id"),\
       F.col("count")
    ).writeTo("bootcamp.medals_matches_players_bucketed") \
 .option("write.distribution-mode", "hash") \
 .createOrReplace()
 
 #----------------------------------------------#
 # Bucket join matches, match_details and medals_matches
matches_df = spark.table("bootcamp.matches_bucketed")
match_details_df = spark.table("bootcamp.match_details_bucketed")
medals_matches_df = spark.table("bootcamp.medals_matches_players_bucketed")

query = "select mb.match_id, mb.mapid, mb.playlist_id, mb.completion_date, mdb.player_gamertag, mmpd.medal_id, mmpd.count \
    from bootcamp.matches_bucketed mb join bootcamp.match_details_bucketed mdb \
    on mb.match_id = mdb.match_id\
    join bootcamp.medals_matches_players_bucketed mmpd on mb.match_id = mmpd.match_id"
result_df = spark.sql(query)
result_df.show()
#------------------------------------------------#
# Which player averages the most kills per game?
query = """
SELECT 
    player_gamertag,
    SUM(player_total_kills) AS total_kills,
    COUNT(DISTINCT match_id) AS total_games,
    SUM(player_total_kills) / COUNT(DISTINCT match_id) AS avg_kills_per_game
FROM bootcamp.match_details_bucketed
GROUP BY player_gamertag
ORDER BY avg_kills_per_game DESC
LIMIT 1
"""

# Execute the query
most_kills_player = spark.sql(query)

# Show the result
most_kills_player.show()

#------------------------------------------------#
# Which playlist gets played the most
query = """
SELECT 
    playlist_id,
    COUNT(match_id) AS total_matches
FROM bootcamp.matches_bucketed
GROUP BY playlist_id
ORDER BY total_matches DESC
LIMIT 1
"""

# Execute the query
most_played_playlist = spark.sql(query)

# Show the result
most_played_playlist.show()

#------------------------------------------------#
#Which map gets played the most?
# Broadcast join - maps being broadcasted


matches_bucketed_df = spark.table("bootcamp.matches_bucketed")

# Perform a broadcast join between matches and maps
result_df = matches_bucketed_df.join(
    broadcast(maps_df.alias("maps")),
    matches_bucketed_df.mapid == maps_df.mapid, 
    "inner"
)

# Aggregate to find the most played map
most_played_map = result_df.groupBy("maps.mapid").count().orderBy("count", ascending=False).limit(1)

# Show the result
most_played_map.show()

#------------------------------------------------#
#Which map do players get the most Killing Spree medals on?

medals_matches_bucketed_df = spark.table("bootcamp.medals_matches_players_bucketed")

killing_spree_medals_df = medals_df.filter(F.col("classification") == "KillingSpree")


# Perform a broadcast join between medals_matches and medals
killing_spree_matches_df = medals_matches_bucketed_df.join(
    broadcast(killing_spree_medals_df.alias("medals")),
    medals_matches_bucketed_df.medal_id == medals_df.medal_id, 
    "inner"
)

# join with matches_df to get the map_id
killing_spree_map_df = killing_spree_matches_df.join(
    matches_df,
    on="match_id",
    how="inner"
)

# join with maps to get map_name
killing_spree_map_df = killing_spree_map_df.join(
    broadcast(maps_df.alias("maps")),
    killing_spree_map_df.mapid == maps_df.mapid,
    "inner")

killing_spree_count_df = killing_spree_map_df.groupBy("maps.mapid", "maps.name") \
    .count() \
    .orderBy(F.col("count").desc())

killing_spree_count_df.show(1)


# applying sortWithinPartitions
# joining matches, match_details and medals_matches_players



matches_df = spark.table("bootcamp.matches_bucketed")
match_details_df = spark.table("bootcamp.match_details_bucketed")
medals_matches_df = spark.table("bootcamp.medals_matches_players_bucketed")

query = "select mb.match_id, mb.mapid, mb.playlist_id, mb.completion_date, mdb.player_gamertag, mmpd.medal_id, mmpd.count \
    from bootcamp.matches_bucketed mb join bootcamp.match_details_bucketed mdb \
    on mb.match_id = mdb.match_id\
    join bootcamp.medals_matches_players_bucketed mmpd on mb.match_id = mmpd.match_id"
result_df = spark.sql(query)
#result_df.show()

start_df = result_df.repartition(4, F.col("completion_date"))
sort_df = start_df.sortWithinPartitions(F.col("playlist_id"))

start_df.write.mode("overwrite").saveAsTable("bootcamp.result_unsorted")
sort_df.write.mode("overwrite").saveAsTable("bootcamp.result_sorted")
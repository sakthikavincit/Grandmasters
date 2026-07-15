# Databricks notebook source
# DBTITLE 1,Configuration
# ============================================================
# CHESS HISTORIC GAMES - SILVER LAYER CONFIGURATION
# ============================================================

BRONZE_TABLE = "grandmasters.bronze.historic_feed"

SILVER_GAMES = "grandmasters.silver.historic_games"

DEVELOPMENT_MODE = True

print(f"Bronze source : {BRONZE_TABLE}")
print(f"Silver target : {SILVER_GAMES}")
print(f"Dev mode      : {DEVELOPMENT_MODE}")

# COMMAND ----------

# DBTITLE 1,Read Bronze
# ============================================================
# READ BRONZE
# ============================================================

bronze_df = spark.table(BRONZE_TABLE)

print(f"Total Bronze Records: {bronze_df.count()}")
bronze_df.groupBy("player_tier").count().show()

display(bronze_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Parse JSON to Silver Columns
# ============================================================
# PARSE RAW JSON -> SILVER COLUMNS
#
# Extract all fields needed for the 6 Gold use cases:
# 1. Player Retention: player, utc_date, rating_diff
# 2. Opening Meta Trends: opening, eco, utc_date
# 3. Matchmaking Health: event, white_elo, black_elo
# 4. Engagement Timezones: elo, utc_time, utc_date
# 5. Game Speed / Server Load: time_control, event
# 6. Upset Rate: result, white_elo, black_elo
# ============================================================

from pyspark.sql.functions import (
    col, from_json, get_json_object,
    to_timestamp, date_format,
    expr, lit
)
from pyspark.sql.types import LongType, IntegerType

silver_df = (
    bronze_df
    .withColumn("game_id", col("game_id"))
    .withColumn("player_tier", col("player_tier"))

    # Players
    .withColumn("white_player", get_json_object("raw_event", "$.players.white.user.name"))
    .withColumn("black_player", get_json_object("raw_event", "$.players.black.user.name"))

    # Elo Ratings
    .withColumn("white_elo", get_json_object("raw_event", "$.players.white.rating").cast(IntegerType()))
    .withColumn("black_elo", get_json_object("raw_event", "$.players.black.rating").cast(IntegerType()))

    # Rating Diff (how much rating changed after this game)
    .withColumn("white_rating_diff", get_json_object("raw_event", "$.players.white.ratingDiff").cast(IntegerType()))
    .withColumn("black_rating_diff", get_json_object("raw_event", "$.players.black.ratingDiff").cast(IntegerType()))

    # Timestamps - Lichess uses epoch milliseconds
    .withColumn("created_at_ms", get_json_object("raw_event", "$.createdAt").cast(LongType()))
    .withColumn("utc_datetime", (col("created_at_ms") / 1000).cast("timestamp"))
    .withColumn("utc_date", date_format("utc_datetime", "yyyy-MM-dd").cast("date"))
    .withColumn("utc_time", date_format("utc_datetime", "HH:mm:ss"))
    .withColumn("utc_hour", date_format("utc_datetime", "HH").cast(IntegerType()))

    # Opening
    .withColumn("opening", get_json_object("raw_event", "$.opening.name"))
    .withColumn("eco", get_json_object("raw_event", "$.opening.eco"))

    # Time Control
    .withColumn("time_control_initial", get_json_object("raw_event", "$.clock.initial").cast(IntegerType()))
    .withColumn("time_control_increment", get_json_object("raw_event", "$.clock.increment").cast(IntegerType()))
    .withColumn("speed", get_json_object("raw_event", "$.speed"))

    # Result & Game Info
    .withColumn("result", get_json_object("raw_event", "$.winner"))  # white/black/null(draw)
    .withColumn("status", get_json_object("raw_event", "$.status"))  # resign/mate/draw/timeout
    .withColumn("variant", get_json_object("raw_event", "$.variant"))
    .withColumn("rated", get_json_object("raw_event", "$.rated"))

    # Drop intermediate columns
    .drop("raw_event", "event_type", "source", "ingested_at", "created_at_ms", "utc_datetime")
)

print(f"Silver records: {silver_df.count()}")

# COMMAND ----------

# DBTITLE 1,Silver Data Validation
# ============================================================
# SILVER DATA VALIDATION
# ============================================================

print("Schema")
silver_df.printSchema()

print(f"\nTotal Records: {silver_df.count()}")

print("\nSpeed Distribution (Game Speed vs Server Load)")
silver_df.groupBy("speed").count().orderBy("count", ascending=False).show()

print("\nPlayer Tier Distribution")
silver_df.groupBy("player_tier").count().show()

print("\nElo Rating Ranges by Tier")
from pyspark.sql.functions import min as _min, max as _max, avg as _avg, round as _round
silver_df.groupBy("player_tier").agg(
    _round(_avg("white_elo"), 0).alias("avg_white_elo"),
    _min("white_elo").alias("min_elo"),
    _max("white_elo").alias("max_elo")
).show()

print("\nSample Records")
display(silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Null & Quality Check
# ============================================================
# NULL & QUALITY CHECK
# ============================================================

from pyspark.sql.functions import sum as _sum, count as _count

print("Null counts per critical column:")
silver_df.select(
    _sum(col("game_id").isNull().cast("int")).alias("null_game_id"),
    _sum(col("white_player").isNull().cast("int")).alias("null_white_player"),
    _sum(col("black_player").isNull().cast("int")).alias("null_black_player"),
    _sum(col("white_elo").isNull().cast("int")).alias("null_white_elo"),
    _sum(col("black_elo").isNull().cast("int")).alias("null_black_elo"),
    _sum(col("opening").isNull().cast("int")).alias("null_opening"),
    _sum(col("eco").isNull().cast("int")).alias("null_eco"),
    _sum(col("utc_date").isNull().cast("int")).alias("null_utc_date"),
    _sum(col("result").isNull().cast("int")).alias("null_result_means_draw"),
).show(truncate=False)

print("\nResult distribution (null = draw):")
silver_df.groupBy("result").count().show()

print("\nTop 10 Openings (Opening Meta Trends):")
silver_df.groupBy("eco", "opening").count().orderBy("count", ascending=False).show(10, truncate=40)

# COMMAND ----------

# DBTITLE 1,Write Silver Delta Table
# ============================================================
# WRITE SILVER DELTA TABLE
# ============================================================

spark.sql("CREATE DATABASE IF NOT EXISTS grandmasters.silver")

if DEVELOPMENT_MODE:
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_GAMES}")

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite" if DEVELOPMENT_MODE else "append")
    .saveAsTable(SILVER_GAMES)
)

print(f"Successfully wrote {silver_df.count()} records to {SILVER_GAMES}")

# COMMAND ----------

# DBTITLE 1,Final Verification
# ============================================================
# FINAL VERIFICATION - Read from Delta table
# ============================================================

verify_df = spark.table(SILVER_GAMES)

print(f"Table: {SILVER_GAMES}")
print(f"Total rows: {verify_df.count()}")
print(f"Columns: {verify_df.columns}")

print("\n--- Fields mapped to Gold Use Cases ---")
print("\n1. Player Retention: white_player, utc_date, white_rating_diff")
verify_df.select("white_player", "utc_date", "white_rating_diff").show(3)

print("2. Opening Meta Trends: opening, eco, utc_date")
verify_df.select("opening", "eco", "utc_date").show(3)

print("3. Matchmaking Health: speed, white_elo, black_elo")
verify_df.select("speed", "white_elo", "black_elo").show(3)

print("4. Engagement Timezones: white_elo, utc_time, utc_hour, utc_date")
verify_df.select("white_elo", "utc_time", "utc_hour", "utc_date").show(3)

print("5. Game Speed vs Server Load: time_control_initial, speed")
verify_df.select("time_control_initial", "time_control_increment", "speed").show(3)

print("6. Upset Rate: result, white_elo, black_elo")
verify_df.select("result", "white_elo", "black_elo").show(3)

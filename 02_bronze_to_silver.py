# Databricks notebook source
# ============================================================
# CHESS LIVE ANALYTICS PROJECT
# SILVER LAYER CONFIGURATION
# ============================================================

BRONZE_TABLE = "bronze.tv_feed"

SILVER_GAMES = "silver.game_events"
SILVER_MOVES = "silver.move_events"
SILVER_PLAYER_METRICS = "silver.player_metrics"

DEVELOPMENT_MODE = True

# COMMAND ----------

# ============================================================
# READ BRONZE
# ============================================================

bronze_df = spark.table(BRONZE_TABLE)

display(bronze_df.limit(10))

# COMMAND ----------

print("Total Bronze Records:", bronze_df.count())

bronze_df.groupBy("event_type").count().show()

bronze_df.select("game_id").distinct().show()

# COMMAND ----------

# ============================================================
# EXTRACT GAME EVENTS
#
# Purpose:
# Parse 'featured' events from Bronze into structured columns.
# One record represents one chess game.
# ============================================================

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

featured_schema = StructType([
    StructField("t", StringType()),
    StructField("d", StructType([
        StructField("id", StringType()),
        StructField("orientation", StringType()),
        StructField("fen", StringType()),
        StructField("players", ArrayType(
            StructType([
                StructField("color", StringType()),
                StructField("rating", IntegerType()),
                StructField("seconds", IntegerType()),
                StructField("user", StructType([
                    StructField("name", StringType()),
                    StructField("id", StringType())
                ]))
            ])
        ))
    ]))
])

game_events_df = (
    bronze_df
    .filter(col("event_type") == "featured")
    .withColumn(
        "json",
        from_json(col("raw_event"), featured_schema)
    )
    .select(
        col("game_id"),
        col("ingested_at"),

        col("json.d.players")[0]["user"]["name"].alias("white_player"),
        col("json.d.players")[1]["user"]["name"].alias("black_player"),

        col("json.d.players")[0]["rating"].alias("white_rating"),
        col("json.d.players")[1]["rating"].alias("black_rating"),

        col("json.d.players")[0]["seconds"].alias("time_control_seconds"),

        col("json.d.fen").alias("starting_fen")
    )
)

display(game_events_df)

# COMMAND ----------

# ============================================================
# VALIDATE GAME EVENTS
# ============================================================

from pyspark.sql.functions import countDistinct, col, sum as Fsum, col as Fcol

print("Total Games")
print(game_events_df.count())

print("\nDistinct Game IDs")
print(game_events_df.select(countDistinct("game_id")).first()[0])

print("\nDuplicate Games")

display(
    game_events_df
        .groupBy("game_id")
        .count()
        .filter("count > 1")
)

print("\nMissing Values")

game_events_df.select(
    *[
        Fsum(Fcol(c).isNull().cast("int")).alias(c)
        for c in game_events_df.columns
    ]
).display()

# COMMAND ----------

# ============================================================
# GAME EVENTS DATA QUALITY
#
# Business Rule:
# Keep only the latest record for each game.
# ============================================================

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

latest_game_window = (
    Window
    .partitionBy("game_id")
    .orderBy(desc("ingested_at"))
)

game_events_df = (
    game_events_df
    .withColumn("rn", row_number().over(latest_game_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

# Run this to clean silver data
# %sql
# DROP TABLE IF EXISTS silver.game_events;
# DROP TABLE IF EXISTS silver.move_events;
# DROP TABLE IF EXISTS silver.player_metrics;

# COMMAND ----------

# ============================================================
# WRITE SILVER GAME EVENTS
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

(
    game_events_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_GAMES)
)

print(f"Successfully wrote {game_events_df.count()} rows to {SILVER_GAMES}")

# COMMAND ----------

display(spark.table(SILVER_GAMES))

# COMMAND ----------

spark.table(SILVER_GAMES).printSchema()

# COMMAND ----------

# ============================================================
# EXTRACT MOVE EVENTS
#
# Purpose:
# Parse every FEN event into a structured move timeline.
# One row represents one move in a chess game.
# ============================================================

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

fen_schema = StructType([
    StructField("t", StringType()),
    StructField("d", StructType([
        StructField("fen", StringType()),
        StructField("lm", StringType()),
        StructField("wc", IntegerType()),
        StructField("bc", IntegerType())
    ]))
])

move_events_df = (
    bronze_df
    .filter(col("event_type") == "fen")
    .withColumn(
        "json",
        from_json(col("raw_event"), fen_schema)
    )
    .select(
        "game_id",
        "move_seq",
        "ingested_at",

        col("json.d.fen").alias("fen"),
        col("json.d.lm").alias("last_move"),

        col("json.d.wc").alias("white_clock_secs"),
        col("json.d.bc").alias("black_clock_secs")
    )
)

display(move_events_df)

# COMMAND ----------

# ============================================================
# VALIDATE MOVE EVENTS
# ============================================================

from pyspark.sql import functions as F

print(f"Total Moves : {move_events_df.count()}")

print("\nDuplicate (game_id, move_seq) Check")

display(
    move_events_df
    .groupBy("game_id", "move_seq")
    .count()
    .filter("count > 1")
)

print("\nMissing Values")

display(
    move_events_df.select(
        *[
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in move_events_df.columns
        ]
    )
)

print("\nMoves Per Game")

display(
    move_events_df
    .groupBy("game_id")
    .agg(
        F.max("move_seq").alias("max_move_seq"),
        F.count("*").alias("rows")
    )
)

# COMMAND ----------

# ============================================================
# WRITE SILVER MOVE EVENTS
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

if DEVELOPMENT_MODE:
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_MOVES}")

(
    move_events_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_MOVES)
)

print(f"Successfully wrote {move_events_df.count()} rows to {SILVER_MOVES}")

# COMMAND ----------

# DBTITLE 1,Read Silver Tables
# ============================================================
# READ SILVER TABLES
#
# Purpose:
# Read validated Silver tables to build player-level metrics.
# ============================================================

games_df = spark.table(SILVER_GAMES)

moves_df = spark.table(SILVER_MOVES)

display(games_df)

display(moves_df.limit(10))

# COMMAND ----------

# ============================================================
# PLAYER MOVE METRICS
#
# Purpose:
# Calculate move-level metrics such as think time and clock loss.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

move_window = (
    Window
    .partitionBy("game_id")
    .orderBy("move_seq")
)

player_moves_df = (

    moves_df

    .withColumn(
        "previous_white_clock",
        F.lag("white_clock_secs").over(move_window)
    )

    .withColumn(
        "previous_black_clock",
        F.lag("black_clock_secs").over(move_window)
    )

    .withColumn(
        "player",
        F.when(
            F.col("move_seq") % 2 == 1,
            "White"
        ).otherwise("Black")
    )

    .withColumn(
        "remaining_clock_secs",
        F.when(
            F.col("player") == "White",
            F.col("white_clock_secs")
        ).otherwise(
            F.col("black_clock_secs")
        )
    )

    .withColumn(
        "think_time_secs",
        F.when(
            F.col("player") == "White",
            F.col("previous_white_clock") - F.col("white_clock_secs")
        ).otherwise(
            F.col("previous_black_clock") - F.col("black_clock_secs")
        )
    )

    .withColumn(
        "clock_loss_secs",
        F.col("think_time_secs")
    )

)

display(player_moves_df)

# COMMAND ----------

# DBTITLE 1,Cell 16
# ============================================================
# PLAYER METRICS
#
# 
# ============================================================

from pyspark.sql.functions import when, col

games_df = spark.table(SILVER_GAMES)
moves_df = spark.table(SILVER_MOVES)

moves_with_players = (
    moves_df.join(
        games_df.select(
            "game_id",
            "white_player",
            "black_player"
        ),
        on="game_id",
        how="left"
    )
    .withColumn(
        "player",
        when(col("move_seq") % 2 == 1, "White").otherwise("Black")
    )
    .withColumn(
        "player_name",
        when(
            col("player") == "White",
            col("white_player")
        ).otherwise(
            col("black_player")
        )
    )
)

display(moves_with_players)

# COMMAND ----------

# DBTITLE 1,Cell 17
from pyspark.sql import functions as F

player_metrics_df = (
    player_moves_df
    .join(games_df.select("game_id", "white_player", "black_player"), on="game_id", how="left")
    .withColumn("player_name", F.when(F.col("player") == "White", F.col("white_player")).otherwise(F.col("black_player")))
    .groupBy("player_name")
    .agg(
        F.count("*").alias("total_moves"),
        F.countDistinct("game_id").alias("games_played"),
        F.avg("think_time_secs").alias("avg_think_time_secs"),
        F.max("think_time_secs").alias("max_think_time_secs"),
        F.sum("clock_loss_secs").alias("total_clock_loss_secs"),
        F.avg("remaining_clock_secs").alias("avg_remaining_clock_secs")
    )
)

display(player_metrics_df)

# COMMAND ----------

player_metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(SILVER_PLAYER_METRICS)

print(f"Written to {SILVER_PLAYER_METRICS}")

# COMMAND ----------

games_df = spark.table(SILVER_GAMES)

moves_df = spark.table(SILVER_MOVES)

metrics_df = spark.table(SILVER_PLAYER_METRICS)

display(games_df)

display(moves_df.limit(10))

display(metrics_df)

# COMMAND ----------



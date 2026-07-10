# Databricks notebook source
# ============================================================
# CHESS LIVE ANALYTICS PROJECT
# BRONZE LAYER CONFIGURATION
# ============================================================

# Lichess TV Streaming API
LICHESS_TV_URL = "https://lichess.org/api/tv/feed"

# Delta Tables
BRONZE_TABLE = "bronze.tv_feed"

# Number of streaming events to capture
MAX_EVENTS = 300

# Development flag
# True  -> Clear Bronze before every run
# False -> Keep appending to Bronze (stream simulation)
DEVELOPMENT_MODE = True

print(LICHESS_TV_URL)
print(BRONZE_TABLE)
print(MAX_EVENTS)
print(DEVELOPMENT_MODE)

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone


def read_lichess_tv(url: str, max_events: int) -> list:
    """
    Reads live events from the Lichess TV stream.

    Each event is stored exactly as received along with
    minimal metadata required for Bronze ingestion.
    """

    events = []

    current_game = None
    move_seq = 0

    headers = {
        "Accept": "application/x-ndjson",
        "User-Agent": "chess-live-analytics/1.0"
    }

    print(f"Connecting to {url}...")

    with requests.get(
        url,
        headers=headers,
        stream=True,
        timeout=300
    ) as response:

        response.raise_for_status()

        print("Connected successfully.\n")

        for line in response.iter_lines():

            if not line:
                continue

            raw_event = line.decode("utf-8")
            event = json.loads(raw_event)

            event_type = event.get("t")

            if event_type == "featured":

                current_game = event["d"]["id"]
                move_seq = 0

            elif event_type == "fen":

                move_seq += 1

            events.append({

                "raw_event": raw_event,

                "event_type": event_type,

                "game_id": current_game,

                "move_seq": move_seq if event_type == "fen" else None,

                "ingested_at": datetime.now(timezone.utc)

            })

            if len(events) >= max_events:
                break

    return events


events = read_lichess_tv(
    LICHESS_TV_URL,
    MAX_EVENTS
)

print(f"Total events collected : {len(events)}")

featured = sum(e["event_type"] == "featured" for e in events)
fen = sum(e["event_type"] == "fen" for e in events)

print(f"Featured events : {featured}")
print(f"Move events     : {fen}")

print("\nFirst event:")
print(events[0])

# COMMAND ----------

# ============================================================
# INGESTION VALIDATION
# ============================================================

print(f"Total events collected : {len(events)}")

featured = sum(e["event_type"] == "featured" for e in events)
fen = sum(e["event_type"] == "fen" for e in events)

print(f"Featured events : {featured}")
print(f"Move events     : {fen}")

# Number of unique games captured
games = {
    e["game_id"]
    for e in events
    if e["game_id"] is not None
}

print(f"\nUnique games captured : {len(games)}")

print("\nGame IDs:")
for game in sorted(games):
    print(f" - {game}")

print("\nFirst event:")
print(events[0])

print("\nLast event:")
print(events[-1])

assert featured > 0, "No featured games received."
assert fen > 0, "No move events received."

print("\n✅ Stream ingestion validation passed.")

# COMMAND ----------

# ============================================================
# BRONZE DATAFRAME
#
# Purpose:
# Convert raw Python events into a typed Spark DataFrame.
# No transformations or cleaning are performed.
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

bronze_schema = StructType([

    StructField("raw_event", StringType(), False),

    StructField("event_type", StringType(), False),

    StructField("game_id", StringType(), True),

    StructField("move_seq", IntegerType(), True),

    StructField("ingested_at", TimestampType(), False)

])

bronze_df = spark.createDataFrame(
    events,
    schema=bronze_schema
)

# COMMAND ----------

# ============================================================
# BRONZE DATA VALIDATION
# ============================================================

print("Schema")
bronze_df.printSchema()

print(f"\nTotal Records : {bronze_df.count()}")

print("\nEvent Type Distribution")
bronze_df.groupBy("event_type").count().show()

print("\nUnique Games")
bronze_df.select("game_id").distinct().show(truncate=False)

print("\nSample Records")
display(bronze_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col


(
    bronze_df
    .filter(col("event_type") == "featured")
    .groupBy("game_id")
    .count()
    .show()
)

# COMMAND ----------

# DBTITLE 1,Cell 7
import pyspark.sql.functions as F

print("Move sequence check")

(
    bronze_df
    .filter(col("event_type") == "fen")
    .groupBy("game_id")
    .agg(F.max("move_seq").alias("total_moves"))
    .show()
)

# COMMAND ----------

(
    bronze_df
    .select(
        F.sum(F.col("raw_event").isNull().cast("int")).alias("null_raw_event"),
        F.sum(F.col("game_id").isNull().cast("int")).alias("null_game_id")
    )
    .show()
)

# COMMAND ----------

# ============================================================
# WRITE BRONZE DELTA TABLE
# ============================================================

if DEVELOPMENT_MODE:
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")

(
    bronze_df
    .write
    .format("delta")
    .mode("overwrite" if DEVELOPMENT_MODE else "append")
    .saveAsTable(BRONZE_TABLE)
)

print(f"Successfully wrote {bronze_df.count()} records to {BRONZE_TABLE}")

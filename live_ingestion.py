# Databricks notebook source
# MAGIC %md
# MAGIC # Chess Intelligence System — Lichess TV Feed Ingestion
# MAGIC
# MAGIC **Source:** `GET https://lichess.org/api/tv/feed` (no auth required)
# MAGIC
# MAGIC **What this does:**
# MAGIC - Connects to Lichess TV: a live stream of the highest-rated ongoing game on Lichess
# MAGIC - Reads NDJSON events line by line: one `featured` event when a game starts, one `fen` event per move
# MAGIC - Writes raw events to **Bronze** Delta table (untouched)
# MAGIC - Parses into two **Silver** tables: `silver_game_events` and `silver_move_events`
# MAGIC - Sanity check: computes time spent per move using window functions
# MAGIC
# MAGIC **Two event types explained:**
# MAGIC
# MAGIC | Event type | When it fires | Key fields |
# MAGIC |---|---|---|
# MAGIC | `featured` | New game starts on TV | game_id, player names, ratings, starting position |
# MAGIC | `fen` | Every move made | board position (FEN), last move (UCI), clocks remaining |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Imports & Config

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────────────
LICHESS_TV_URL = "https://lichess.org/api/tv/feed"

BRONZE_TABLE = "bronze.tv_feed"        # Raw events — never alter the raw_event column
SILVER_GAMES = "silver.game_events"    # One row per game (from 'featured' events)
SILVER_MOVES = "silver.move_events"    # One row per move (from 'fen' events)

MAX_EVENTS = 300  # Roughly 1-2 full games (~30 moves each = 60 fen events + featured events)
MAX_GAMES = 5  # Max number of games to process at once

# COMMAND ----------

# ============================================================
# DEVELOPMENT ONLY
# Reset Bronze before ingesting a fresh stream
# ============================================================

spark.sql("TRUNCATE TABLE bronze.tv_feed")

print("Bronze table cleared.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Stream Reader
# MAGIC
# MAGIC Connects to Lichess TV and collects NDJSON events into a Python list.
# MAGIC
# MAGIC **Why collect first, then write?**
# MAGIC `requests` streaming is a long-running Python process — not native Spark Structured Streaming.
# MAGIC For a learning notebook this is the simplest pattern. In production you'd put Kafka in the middle
# MAGIC and read with Spark Structured Streaming instead.
# MAGIC
# MAGIC **game_id tracking:** `fen` events don't include a game_id — only `featured` events do.
# MAGIC So we track the current game_id from the last `featured` event seen and stamp it onto every move.

# COMMAND ----------

def read_lichess_tv(url: str, max_events: int) -> list:
    """
    Reads NDJSON events from the Lichess TV stream.

    Returns a list of dicts with these fields:
      raw_event   : original JSON string — stored as-is in Bronze, never altered
      event_type  : 'featured' (new game) or 'fen' (move made)
      game_id     : id of the current game (tracked from featured events)
      move_seq    : move number within the game (None for featured events)
      ingested_at : UTC timestamp when this line was read
    """
    events       = []
    current_game = None
    move_seq     = 0
    headers      = {
        "Accept"    : "application/x-ndjson",
        "User-Agent": "chess-de-project/1.0"
    }

    print(f"Connecting to {url} ...")

    with requests.get(url, stream=True, headers=headers, timeout=300) as r:
        r.raise_for_status()
        print(f"Connected — HTTP {r.status_code}. Reading up to {max_events} events...\n")

        for line in r.iter_lines():
            if not line:
                continue

            raw    = line.decode("utf-8") if isinstance(line, bytes) else line
            parsed = json.loads(raw)
            etype  = parsed.get("t")

            if etype == "featured":
                # New game started on Lichess TV
                current_game = parsed["d"]["id"]
                move_seq     = 0
                players      = parsed["d"]["players"]
                white        = f"{players[0]['user']['name']} ({players[0]['rating']})"
                black        = f"{players[1]['user']['name']} ({players[1]['rating']})"
                print(f"  New game on TV: {current_game}")
                print(f"  White: {white}  vs  Black: {black}\n")

            elif etype == "fen":
                # A move was made — increment move counter
                move_seq += 1

            events.append({
                "raw_event"  : raw,
                "event_type" : etype,
                "game_id"    : current_game,
                "move_seq"   : move_seq if etype == "fen" else None,
                "ingested_at": datetime.now(timezone.utc).isoformat()
            })

            if len(events) % 50 == 0:
                print(f"  {len(events)} events collected...")

            if len(events) >= max_events:
                break

    featured_count = sum(1 for e in events if e["event_type"] == "featured")
    fen_count      = sum(1 for e in events if e["event_type"] == "fen")
    print(f"\nDone. {len(events)} total events — {featured_count} games, {fen_count} moves.")
    return events


events = read_lichess_tv(LICHESS_TV_URL, MAX_EVENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Write to Bronze Delta Table
# MAGIC
# MAGIC **Bronze rule:** store raw data exactly as received. No transforms, no parsing.
# MAGIC If the Silver parsing logic breaks or schema changes, Bronze is the source of truth to replay from.
# MAGIC
# MAGIC We do add three metadata columns that aren't in the raw payload:
# MAGIC - `event_type` — extracted from the raw JSON for easy filtering downstream
# MAGIC - `game_id`    — tracked from featured events (not in fen events)
# MAGIC - `move_seq`   — move counter within a game
# MAGIC - `ingested_at`— when we read this line

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

bronze_schema = StructType([
    StructField("raw_event",   StringType(),  False),   # Original JSON — never alter this
    StructField("event_type",  StringType(),  True),
    StructField("game_id",     StringType(),  True),
    StructField("move_seq",    IntegerType(), True),
    StructField("ingested_at", StringType(),  False),
])

bronze_df = spark.createDataFrame(events, schema=bronze_schema)

# Quick check before writing
print("Event type breakdown:")
bronze_df.groupBy("event_type").count().show()

print("Sample raw events:")
bronze_df.select("event_type", "game_id", "move_seq", "raw_event").show(5, truncate=80)

# Append to Bronze — use append so re-runs accumulate (good for streaming simulation)
(bronze_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable('bronze.tv_feed'))

print(f"Written {bronze_df.count()} rows to {BRONZE_TABLE}")

# COMMAND ----------

spark.table(BRONZE_TABLE).groupBy("game_id").count().orderBy("count", ascending=False).display()

# COMMAND ----------

# ============================================================
# SILVER LAYER - GAME EVENTS
#
# Purpose:
# Extract structured game metadata from Bronze.
# One record represents one featured game.
# ============================================================

from pyspark.sql.functions import col, get_json_object

game_events_df = (
    spark.table(BRONZE_TABLE)
    .filter(col("event_type") == "featured")
    .select(
        col("game_id"),
        col("ingested_at"),

        get_json_object("raw_event", "$.d.players[0].user.name").alias("white_player"),
        get_json_object("raw_event", "$.d.players[1].user.name").alias("black_player"),

        get_json_object("raw_event", "$.d.players[0].rating").cast("int").alias("white_rating"),
        get_json_object("raw_event", "$.d.players[1].rating").cast("int").alias("black_rating"),

        get_json_object("raw_event", "$.d.players[0].seconds").cast("int").alias("time_control_seconds"),

        get_json_object("raw_event", "$.d.fen").alias("starting_fen")
    )
)

display(game_events_df)

# COMMAND ----------

# ============================================================
# SILVER DATA QUALITY
#
# Purpose:
# Ensure Silver contains one clean record per game.
#
# Rules:
# 1. Keep only the latest featured event.
# 2. Remove duplicate games.
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
    .withColumn(
        "rn",
        row_number().over(latest_game_window)
    )
    .filter(col("rn") == 1)
    .drop("rn")
)

print("Duplicate Check")

display(
    game_events_df
    .groupBy("game_id")
    .count()
    .filter("count > 1")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4a — Silver: Game Events
# MAGIC
# MAGIC One row per game. Parses the `featured` event JSON into structured columns.
# MAGIC
# MAGIC **`starting_fen`** — the board position at game start.
# MAGIC FEN (Forsyth-Edwards Notation) encodes the entire board in one string:
# MAGIC `rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR`
# MAGIC - 8 ranks separated by `/` (rank 8 at left = Black's back rank, rank 1 = White's back rank)
# MAGIC - Letters = pieces: `r`ook `n`knight `b`ishop `q`ueen `k`ing `p`awn
# MAGIC - Uppercase = White, lowercase = Black
# MAGIC - Numbers = consecutive empty squares
# MAGIC
# MAGIC The starting FEN is always the same — it's useful in Silver to confirm the game started normally
# MAGIC (not from a custom position).

# COMMAND ----------

# from pyspark.sql.functions import col, get_json_object

# game_events_df = (
#     spark.table(BRONZE_TABLE)
#     .filter(col("event_type") == "featured")
#     .select(
#         col("game_id"),
#         col("ingested_at"),

#         # Player info — note: players[0] = White, players[1] = Black
#         get_json_object("raw_event", "$.d.players[0].user.name").alias("white_player"),
#         get_json_object("raw_event", "$.d.players[1].user.name").alias("black_player"),
#         get_json_object("raw_event", "$.d.players[0].rating").cast("int").alias("white_rating"),
#         get_json_object("raw_event", "$.d.players[1].rating").cast("int").alias("black_rating"),

#         # time_control_seconds: how many seconds each player starts with
#         # e.g. 300 = 5 minutes (Blitz), 60 = 1 minute (Bullet)
#         get_json_object("raw_event", "$.d.players[0].seconds").cast("int").alias("time_control_seconds"),

#         # Starting board position (always standard unless it's a Chess960 game)
#         get_json_object("raw_event", "$.d.fen").alias("starting_fen"),
#     )
# )

# print("Silver — Game Events:")
# game_events_df.show(truncate=False)

# game_events_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_GAMES)
# print(f"Written to {SILVER_GAMES}")
print("Silver — Game Events:")
game_events_df.show(truncate=False)

game_events_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(SILVER_GAMES)

print(f"Written to {SILVER_GAMES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4b — Silver: Move Events
# MAGIC
# MAGIC One row per move. Parses each `fen` event into structured columns.
# MAGIC
# MAGIC **Fields explained:**
# MAGIC
# MAGIC | Column | Source field | What it means |
# MAGIC |---|---|---|
# MAGIC | `fen` | `$.d.fen` | Board position after this move (see FEN explanation above) |
# MAGIC | `last_move` | `$.d.lm` | Move in UCI format: `e2e4` = piece moved from e2 to e4 |
# MAGIC | `white_clock_secs` | `$.d.wc / 1000` | Seconds White has left on their clock |
# MAGIC | `black_clock_secs` | `$.d.bc / 1000` | Seconds Black has left on their clock |
# MAGIC | `move_seq` | tracked in reader | Move number within this game (1, 2, 3...) |
# MAGIC
# MAGIC **Whose turn is it?**
# MAGIC Odd move_seq = White just moved. Even move_seq = Black just moved.
# MAGIC (White always goes first in chess)

# COMMAND ----------

# Delete existing table to resolve schema conflict, then write fresh
# spark.sql(f"DROP TABLE IF EXISTS {SILVER_MOVES}")

move_events_df = (
    spark.table(BRONZE_TABLE)
    .filter(col("event_type") == "fen")
    .select(
        col("game_id"),
        col("move_seq"),
        col("ingested_at"),

        get_json_object("raw_event", "$.d.fen").alias("fen"),

        # last_move in UCI format: from-square + to-square, e.g. "e2e4", "g1f3", "e1g1" (castling)
        get_json_object("raw_event", "$.d.lm").alias("last_move"),

        # Clocks: wc and bc are already in seconds, no division needed
        get_json_object("raw_event", "$.d.wc").cast("int").alias("white_clock_secs"),
        get_json_object("raw_event", "$.d.bc").cast("int").alias("black_clock_secs"),
    )
)

print("Silver — Move Events (first 20 rows):")
move_events_df.show(20, truncate=60)

move_events_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_MOVES)
print(f"Written to {SILVER_MOVES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Sanity Check: Time Spent Per Move
# MAGIC
# MAGIC How many seconds did each player think before making their move?
# MAGIC
# MAGIC We compute this using a **window function**: compare each row's clock to the previous row's clock.
# MAGIC The difference = time spent on that move (minus the increment added back, if any).
# MAGIC
# MAGIC `lag()` — looks at the previous row's value within the same game, ordered by move_seq.

# COMMAND ----------

from pyspark.sql.functions import lag, round as spark_round, when, avg
from pyspark.sql.window import Window

# Window: per game, ordered by move number
w = Window.partitionBy("game_id").orderBy("move_seq")

clock_df = (
    move_events_df
    # Get the clock value from the previous move
    .withColumn("prev_white_clock", lag("white_clock_secs").over(w))
    .withColumn("prev_black_clock", lag("black_clock_secs").over(w))

    # Time spent = previous clock - current clock
    # Odd move_seq = White moved, Even = Black moved
    .withColumn(
        "time_spent_secs",
        when(
            col("move_seq") % 2 == 1,  # White's move (odd)
            spark_round(col("prev_white_clock") - col("white_clock_secs"), 1)
        ).otherwise(
            spark_round(col("prev_black_clock") - col("black_clock_secs"), 1)  # Black's move (even)
        )
    )
    .withColumn("player", when(col("move_seq") % 2 == 1, "White").otherwise("Black"))
    .select("game_id", "move_seq", "player", "last_move", "time_spent_secs", "white_clock_secs", "black_clock_secs")
    .filter(col("move_seq") > 1)  # Skip move 1 — no previous row to diff against
)

print("Time spent per move:")
clock_df.show(30, truncate=False)

# Summary: average think time per player
print("Average think time per player:")
clock_df.groupBy("player").agg(
    spark_round(avg("time_spent_secs"), 2).alias("avg_seconds_per_move"),
).show()

# COMMAND ----------

# ============================================================
# GOLD LAYER - LIVE TOURNAMENT LEADERBOARD
#
# Purpose:
# Create a business-ready Gold table by combining Silver game
# metadata with live move events and derived analytics.
#
# Output:
# sandbox.gold.live_tournament_leaderboard
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws

# ============================================================
# Load Silver Tables
# ============================================================

games = spark.table(SILVER_GAMES)
move_df = spark.table(SILVER_MOVES)

# ============================================================
# Step 1: Get the latest move for every active game
#
# Business Purpose:
# Each game has multiple move events. The dashboard should
# always display the latest board state, remaining clocks
# and last move.
# ============================================================

latest_window = Window.partitionBy("game_id").orderBy(F.desc("move_seq"))

latest_move = (
    move_df
    .withColumn("rn", F.row_number().over(latest_window))
    .filter(col("rn") == 1)
    .select(
        "game_id",
        col("move_seq").alias("move_count"),
        "last_move",
        col("white_clock_secs").alias("white_remaining_clock_secs"),
        col("black_clock_secs").alias("black_remaining_clock_secs"),
        "ingested_at"
    )
)

# ============================================================
# Step 2: Calculate average think time
#
# Business Purpose:
# Estimate the average time spent by each player per move.
# This metric will later be useful for decision quality and
# fatigue analysis.
# ============================================================

avg_think = (
    clock_df
    .groupBy("game_id", "player")
    .agg(
        F.round(
            F.avg("time_spent_secs"),
            2
        ).alias("avg_think_secs")
    )
)

avg_think_pivot = (
    avg_think
    .groupBy("game_id")
    .pivot("player", ["White", "Black"])
    .agg(F.first("avg_think_secs"))
    .withColumnRenamed("White", "white_avg_think_secs")
    .withColumnRenamed("Black", "black_avg_think_secs")
)

# ============================================================
# Step 3: Join Game Metadata + Latest Move + Think Time
# ============================================================

leaderboard = (
    games.alias("g")
    .join(latest_move.alias("m"), "game_id")
    .join(avg_think_pivot.alias("a"), "game_id", "left")
)

# ============================================================
# Step 4: Calculate Game Duration
#
# Business Purpose:
# Time elapsed between game creation and latest received move.
# ============================================================

leaderboard = leaderboard.withColumn(
    "game_duration_secs",
    F.unix_timestamp(F.expr("try_to_timestamp(m.ingested_at)"))
    - F.unix_timestamp(F.expr("try_to_timestamp(g.ingested_at)"))
)

# ============================================================
# Step 5: Compute Win Probability (Rule-Based MVP)
#
# Current Implementation:
# - Rating Difference
# - Remaining Clock Difference
#
# Future Enhancement:
# Replace with ML model trained using historical games and
# Stockfish evaluations.
# ============================================================

leaderboard = (
    leaderboard
    .withColumn(
        "rating_difference",
        F.abs(col("white_rating") - col("black_rating"))
    )
    .withColumn(
        "rating_diff",
        col("white_rating") - col("black_rating")
    )
    .withColumn(
        "clock_diff",
        col("white_remaining_clock_secs")
        - col("black_remaining_clock_secs")
    )
)

leaderboard = leaderboard.withColumn(
    "white_score",
    F.lit(50)
    + (col("rating_diff") * 0.05)
    + (col("clock_diff") * 0.20)
)

leaderboard = (
    leaderboard
    .withColumn(
        "white_win_probability",
        F.when(col("white_score") < 0, 0)
         .when(col("white_score") > 100, 100)
         .otherwise(F.round(col("white_score"), 1))
    )
    .withColumn(
        "black_win_probability",
        100 - col("white_win_probability")
    )
)

# ============================================================
# Step 6: Additional Dashboard Columns
# ============================================================

leaderboard = (
    leaderboard
    .withColumn("game_status", F.lit("In Progress"))
    .withColumn(
        "match_name",
        concat_ws(
            " vs ",
            col("white_player"),
            col("black_player")
        )
    )
)

# ============================================================
# Step 7: Final Gold Dataset
#
# This dataset powers the Live Tournament Dashboard.
# ============================================================

gold_live_leaderboard = (
    leaderboard.select(
        "game_id",
        "match_name",
        "white_player",
        "black_player",
        "white_rating",
        "black_rating",
        "rating_difference",
        "white_remaining_clock_secs",
        "black_remaining_clock_secs",
        "white_avg_think_secs",
        "black_avg_think_secs",
        "last_move",
        "move_count",
        "game_duration_secs",
        "white_win_probability",
        "black_win_probability",
        "game_status"
    )
    .fillna({
        "white_avg_think_secs": 0,
        "black_avg_think_secs": 0
    })
)

# ============================================================
# Step 8: Save Gold Table
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS sandbox.gold")

(
    gold_live_leaderboard.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("sandbox.gold.live_tournament_leaderboard")
)

print("Gold table written successfully:")
print("sandbox.gold.live_tournament_leaderboard")

display(gold_live_leaderboard)

# COMMAND ----------

# ============================================================
# DASHBOARD TILE 2 - LIVE GAMES KPI
#
# Purpose:
# Displays the total number of active live games currently
# being tracked from the Lichess stream.
# ============================================================

live_games_df = (
    gold_live_leaderboard
    .selectExpr("count(*) as Live_Games")
)

display(live_games_df)

# COMMAND ----------

# ============================================================
# DASHBOARD TILE 3 - LOWEST REMAINING CLOCK
#
# Purpose:
# Displays the lowest remaining clock among all active players.
# Useful to identify games that are entering time pressure.
# ============================================================

from pyspark.sql.functions import least, min

lowest_clock_df = (
    gold_live_leaderboard
    .select(
        least(
            "white_remaining_clock_secs",
            "black_remaining_clock_secs"
        ).alias("Lowest Remaining Clock (sec)")
    )
    .agg(
        min("Lowest Remaining Clock (sec)")
        .alias("Lowest Remaining Clock (sec)")
    )
)

display(lowest_clock_df)

# COMMAND ----------

gold_live_leaderboard.groupBy("game_id").count().display()

# COMMAND ----------

# ============================================================
# DASHBOARD TILE 4 - REMAINING CLOCK COMPARISON
#
# Purpose:
# Compare the remaining clock of White and Black players
# for every live game.
# ============================================================

from pyspark.sql.functions import col, lit

white_clock_df = (
    gold_live_leaderboard
    .select(
        "match_name",
        col("white_player").alias("player"),
        col("white_remaining_clock_secs").alias("remaining_clock_secs")
    )
    .withColumn("color", lit("White"))
)

black_clock_df = (
    gold_live_leaderboard
    .select(
        "match_name",
        col("black_player").alias("player"),
        col("black_remaining_clock_secs").alias("remaining_clock_secs")
    )
    .withColumn("color", lit("Black"))
)

clock_comparison_df = white_clock_df.unionByName(black_clock_df)

display(clock_comparison_df)



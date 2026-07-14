# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Chess Intelligence System — Kafka Consumer (Spark Structured Streaming)
# MAGIC
# MAGIC **Role:** Reads events from the Confluent Kafka topic using Spark Structured Streaming
# MAGIC and writes them continuously to the Bronze Delta table.
# MAGIC
# MAGIC **Run this notebook second** — after the producer is publishing events.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How Spark Structured Streaming works (vs regular Spark)
# MAGIC
# MAGIC | Regular Spark (batch) | Spark Structured Streaming |
# MAGIC |---|---|
# MAGIC | Reads a fixed dataset | Reads an infinite, continuously growing dataset |
# MAGIC | `spark.read` | `spark.readStream` |
# MAGIC | `df.write` | `df.writeStream` |
# MAGIC | Finishes and stops | Runs forever until you stop it |
# MAGIC | Returns a DataFrame | Returns a StreamingQuery (a running job) |
# MAGIC
# MAGIC Under the hood, Structured Streaming processes data in **micro-batches**: every N seconds
# MAGIC it pulls new messages from Kafka, transforms them, and appends to Delta.
# MAGIC The `trigger(processingTime="5 seconds")` setting controls this interval.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Config
# MAGIC
# MAGIC Same Confluent credentials as the producer notebook.
# MAGIC **Checkpoint location** is critical for Structured Streaming — it records which Kafka
# MAGIC offsets have been processed so the stream can resume correctly after a restart.

# COMMAND ----------

# ── Confluent Cloud credentials (same as producer) ───────────────────────────
BOOTSTRAP_SERVERS = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
API_KEY           = "Z2DLXTSRMR3JXMAX"
API_SECRET        = "cfltMorayXn5QEmOFSKYwuh4gdbDuyTRZa4cQZQ775OEqpYkKw1b61I5MOUN3V3w"

# ── Kafka topic to consume from ───────────────────────────────────────────────
KAFKA_TOPIC = "lichess.tv.event"

# ── Output Delta tables ───────────────────────────────────────────────────────
BRONZE_TABLE = "grandmasters.bronze.tv_feed"

# ── Checkpoint: where Spark tracks which Kafka offsets have been processed ────
# Must be a persistent location — DBFS or cloud storage. Never delete this while
# the stream is running or you'll reprocess from the beginning.
CHECKPOINT_LOCATION = "dbfs:/chess/checkpoints/bronze_tv_feed"

# ── Streaming config ──────────────────────────────────────────────────────────
TRIGGER_INTERVAL = "5 seconds"   # How often Spark pulls a micro-batch from Kafka

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Read Stream from Kafka
# MAGIC
# MAGIC `spark.readStream.format("kafka")` creates a **streaming DataFrame** — it represents
# MAGIC an infinite table that keeps growing as new messages arrive on the topic.
# MAGIC
# MAGIC **Kafka message columns Spark exposes:**
# MAGIC
# MAGIC | Column | Type | What it is |
# MAGIC |---|---|---|
# MAGIC | `key` | binary | Message key — we set this to game_id in the producer |
# MAGIC | `value` | binary | Message payload — our enriched JSON |
# MAGIC | `topic` | string | Topic name |
# MAGIC | `partition` | int | Which Kafka partition this message came from |
# MAGIC | `offset` | long | Position of this message within the partition |
# MAGIC | `timestamp` | timestamp | When Kafka received the message |
# MAGIC
# MAGIC `startingOffsets = "latest"` → only process new messages from now.
# MAGIC Use `"earliest"` to reprocess everything from the beginning of the topic.

# COMMAND ----------

from pyspark.sql.functions import col, get_json_object, current_timestamp

# SASL config — how Spark authenticates with Confluent Cloud
# SASL = Simple Authentication and Security Layer (industry standard for Kafka auth)
sasl_config = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{API_KEY}" password="{API_SECRET}";'
)

kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",  BOOTSTRAP_SERVERS)
    .option("subscribe",                KAFKA_TOPIC)           # Topic to consume
    .option("startingOffsets",          "latest")              # Start from new messages only
    .option("kafka.security.protocol",  "SASL_SSL")            # Encrypted + authenticated
    .option("kafka.sasl.mechanism",     "PLAIN")
    .option("kafka.sasl.jaas.config",   sasl_config)
    # Limit messages per micro-batch to avoid overwhelming the cluster on large backlogs
    .option("maxOffsetsPerTrigger",     "1000")
    .load()
)

print("Streaming DataFrame schema (raw Kafka columns):")
kafka_stream_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Parse and Build Bronze DataFrame
# MAGIC
# MAGIC Kafka delivers `value` as raw bytes. We need to:
# MAGIC 1. Cast `value` bytes → string (our JSON)
# MAGIC 2. Extract fields using `get_json_object` (same function as the non-streaming notebook)
# MAGIC
# MAGIC **Bronze rule still applies:** keep `raw_event` (the original JSON string) untouched.
# MAGIC We add metadata columns but never alter the source payload.
# MAGIC
# MAGIC We also carry over Kafka metadata columns (`partition`, `offset`, `kafka_timestamp`)
# MAGIC — useful for debugging and reprocessing.

# COMMAND ----------

bronze_stream_df = (
    kafka_stream_df
    .select(
        # ── Raw payload — never alter this ────────────────────────────────────
        col("value").cast("string").alias("raw_event"),

        # ── Fields extracted from JSON for easy filtering ─────────────────────
        get_json_object(col("value").cast("string"), "$.t").alias("event_type"),
        get_json_object(col("value").cast("string"), "$.game_id").alias("game_id"),
        get_json_object(col("value").cast("string"), "$.move_seq").cast("int").alias("move_seq"),
        get_json_object(col("value").cast("string"), "$.ingested_at").alias("ingested_at"),

        # ── Kafka metadata — helpful for debugging ────────────────────────────
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),    # When Kafka received this message

        # ── Processing timestamp — when Spark processed this micro-batch ──────
        current_timestamp().alias("processed_at"),
    )
)

print("Bronze streaming DataFrame schema (after parsing):")
bronze_stream_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Write Stream to Bronze Delta Table
# MAGIC
# MAGIC `writeStream` is the streaming equivalent of `write`. Key differences:
# MAGIC
# MAGIC - **`.outputMode("append")`** — only new rows are written each micro-batch. Correct for Bronze.
# MAGIC   (Other modes: `complete` rewrites the whole table, `update` only rewrites changed rows —
# MAGIC   those are for aggregations, not raw ingestion.)
# MAGIC
# MAGIC - **`.trigger(processingTime="5 seconds")`** — Spark waits 5 seconds, pulls all new Kafka
# MAGIC   messages since last batch, writes them to Delta, waits 5 seconds, repeats.
# MAGIC
# MAGIC - **`.option("checkpointLocation", ...)`** — Spark writes its progress here after every
# MAGIC   micro-batch. If the cluster restarts, it reads this to know where to resume.
# MAGIC   Without this, every restart reprocesses from `startingOffsets`.
# MAGIC
# MAGIC - **`.toTable(...)`** — writes to a named Delta table (registered in Unity Catalog/Hive Metastore).
# MAGIC   Equivalent to `saveAsTable` for streaming.
# MAGIC
# MAGIC `query` is a `StreamingQuery` object — the running stream. The cell won't return
# MAGIC until you stop the stream (interrupt the cluster or call `query.stop()`).

# COMMAND ----------

query = (
    bronze_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("mergeSchema", "true")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable(BRONZE_TABLE)
)

print(f"Stream started. Writing to {BRONZE_TABLE} every {TRIGGER_INTERVAL}.")
print("Run the cells below in a separate notebook or after stopping this stream.\n")
print(f"Stream ID  : {query.id}")
print(f"Stream name: {query.name}")

# Block — keeps the stream alive. Interrupt the cell to stop.
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Monitor the Stream (run in a separate cell while stream is active)
# MAGIC
# MAGIC While Cell 4 is running, open a new cell and run this to see stream progress.

# COMMAND ----------

# Check stream status
for q in spark.streams.active:
    print(f"Stream: {q.name or q.id}")
    print(f"  Status       : {q.status}")
    print(f"  Recent progress:")
    if q.recentProgress:
        latest = q.recentProgress[-1]
        print(f"    Input rows/sec : {latest.get('inputRowsPerSecond', 0):.1f}")
        print(f"    Processed rows : {latest.get('numInputRows', 0)}")
        print(f"    Batch duration : {latest.get('durationMs', {}).get('triggerExecution', 0)}ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Verify Bronze Table (run after a few micro-batches have written)

# COMMAND ----------

# Row count and event type breakdown
bronze_df = spark.table(BRONZE_TABLE)

print(f"Total rows in Bronze table: {bronze_df.count()}\n")

print("Breakdown by event type:")
bronze_df.groupBy("event_type").count().orderBy("count", ascending=False).show()

print("Latest 10 events:")
bronze_df.orderBy("kafka_timestamp", ascending=False).show(10, truncate=80)

# Databricks notebook source
# MAGIC %md
# MAGIC # Chess Intelligence System — Kafka Producer
# MAGIC
# MAGIC **Role:** Reads live events from Lichess TV API and publishes them to a Confluent Cloud Kafka topic.
# MAGIC
# MAGIC **Run this notebook first — keep it running while the consumer notebook streams.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## One-time Confluent Cloud setup (do this before running)
# MAGIC
# MAGIC 1. Go to https://confluent.cloud and create a free account
# MAGIC 2. Create a cluster → choose **Basic** (free tier)
# MAGIC 3. Go to **Topics** → Create topic → name it `lichess.tv.event`, 1 partition
# MAGIC 4. Go to **API Keys** → Create key scoped to your cluster → copy the Key and Secret
# MAGIC 5. Go to **Cluster Settings** → copy the **Bootstrap server** URL
# MAGIC 6. Paste all three values into the Config cell below

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Install Confluent Kafka Library
# MAGIC
# MAGIC Databricks doesn't include `confluent-kafka` by default. `%pip install` runs on the cluster.
# MAGIC The cluster restarts after this — normal behaviour.

# COMMAND ----------

# MAGIC %pip install confluent-kafka

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Config
# MAGIC
# MAGIC **Security note:** In a real project, store credentials in Databricks Secrets:
# MAGIC `dbutils.secrets.get(scope="confluent", key="api_key")`
# MAGIC For this learning project, pasting values directly is fine.

# COMMAND ----------

# ── Confluent Cloud credentials ──────────────────────────────────────────────
BOOTSTRAP_SERVERS = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"  # from Cluster Settings
API_KEY           = "Z2DLXTSRMR3JXMAX"
API_SECRET        = "cfltMorayXn5QEmOFSKYwuh4gdbDuyTRZa4cQZQ775OEqpYkKw1b61I5MOUN3V3w"

# ── Kafka topic ───────────────────────────────────────────────────────────────
KAFKA_TOPIC = "lichess.tv.event"

# ── Lichess ───────────────────────────────────────────────────────────────────
LICHESS_TV_URL = "https://lichess.org/api/tv/feed"
MAX_EVENTS     = 500   # increase for longer runs; set to -1 to run forever

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Build the Kafka Producer
# MAGIC
# MAGIC `confluent_kafka.Producer` is a thin client that publishes messages to a Kafka topic.
# MAGIC
# MAGIC Each message has two parts:
# MAGIC - **key** — used by Kafka to route the message to a partition. We use `game_id`
# MAGIC   so all moves from the same game land on the same partition (preserving order).
# MAGIC - **value** — the actual payload. We send the enriched JSON string (UTF-8 bytes).
# MAGIC
# MAGIC `delivery_report` is a callback Kafka calls after each message is confirmed written.
# MAGIC If it errors, we log it — we don't crash the producer.

# COMMAND ----------

import json
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer

def build_producer(bootstrap_servers: str, api_key: str, api_secret: str) -> Producer:
    conf = {
        "bootstrap.servers"  : bootstrap_servers,
        "security.protocol"  : "SASL_SSL",
        "sasl.mechanisms"    : "PLAIN",
        "sasl.username"      : api_key,
        "sasl.password"      : api_secret,
        # How many ms to wait before sending a batch of messages (reduces requests)
        "linger.ms"          : 100,
        # Retry on transient failures
        "retries"            : 3,
    }
    return Producer(conf)


def delivery_report(err, msg):
    """Called by Kafka after each message is acknowledged (or fails)."""
    if err:
        print(f"  [ERROR] Message delivery failed: {err}")
    # Uncomment to log every successful delivery (verbose):
    # else:
    #     print(f"  [OK] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


producer = build_producer(BOOTSTRAP_SERVERS, API_KEY, API_SECRET)
print("Kafka producer ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Stream Reader + Publisher
# MAGIC
# MAGIC This cell does two things in one loop:
# MAGIC 1. Reads NDJSON lines from the Lichess TV stream
# MAGIC 2. Immediately publishes each line to Kafka
# MAGIC
# MAGIC **Message format published to Kafka:**
# MAGIC We enrich the raw Lichess payload with two extra top-level fields before publishing:
# MAGIC - `game_id` — tracked from `featured` events (not present in `fen` events natively)
# MAGIC - `ingested_at` — UTC timestamp so the consumer knows when we read this line
# MAGIC
# MAGIC **`producer.flush()`** — Kafka batches messages in memory before sending.
# MAGIC `flush()` forces all pending messages to be sent immediately.
# MAGIC We call it every 10 events so the consumer sees data quickly during demo runs.

# COMMAND ----------

def stream_and_publish(url: str, topic: str, producer: Producer, max_events: int):
    """
    Reads Lichess TV NDJSON stream and publishes each event to a Kafka topic.

    Enriches each message with:
      game_id     : carried forward from the last 'featured' event (fen events don't include it)
      ingested_at : UTC timestamp of when we read this line
    """
    current_game = None
    move_seq     = 0
    count        = 0
    headers      = {
        "Accept"    : "application/x-ndjson",
        "User-Agent": "chess-de-project/1.0"
    }

    print(f"Connecting to {url} ...")

    with requests.get(url, stream=True, headers=headers, timeout=600) as r:
        r.raise_for_status()
        print(f"Connected — HTTP {r.status_code}. Publishing to topic: {topic}\n")

        for line in r.iter_lines():
            if not line:
                continue

            raw    = line.decode("utf-8") if isinstance(line, bytes) else line
            parsed = json.loads(raw)
            etype  = parsed.get("t")

            # Track game_id from featured events
            if etype == "featured":
                current_game = parsed["d"]["id"]
                move_seq     = 0
                players      = parsed["d"]["players"]
                w = f"{players[0]['user']['name']} ({players[0]['rating']})"
                b = f"{players[1]['user']['name']} ({players[1]['rating']})"
                print(f"  New game: {current_game} | {w} vs {b}")

            elif etype == "fen":
                move_seq += 1

            # Enrich: add game_id and ingested_at at the top level
            enriched = {
                **parsed,
                "game_id"    : current_game,
                "move_seq"   : move_seq if etype == "fen" else None,
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }

            # Publish to Kafka
            # key   = game_id (bytes) — routes all moves of a game to the same partition
            # value = enriched JSON   (bytes)
            producer.produce(
                topic    = topic,
                key      = (current_game or "unknown").encode("utf-8"),
                value    = json.dumps(enriched).encode("utf-8"),
                callback = delivery_report
            )

            count += 1
            if count % 10 == 0:
                producer.flush()   # Push buffered messages to Confluent
                print(f"  {count} events published...")

            if max_events > 0 and count >= max_events:
                break

    # Final flush — send any remaining buffered messages
    producer.flush()
    print(f"\nDone. {count} events published to topic '{topic}'.")


# ── Run ───────────────────────────────────────────────────────────────────────
stream_and_publish(LICHESS_TV_URL, KAFKA_TOPIC, producer, MAX_EVENTS)

# COMMAND ----------

spark.version

# COMMAND ----------



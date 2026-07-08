# Databricks notebook source
# MAGIC %pip install requests
# MAGIC
# MAGIC import requests
# MAGIC
# MAGIC r = requests.get("https://lichess.org")
# MAGIC
# MAGIC print(r.status_code)

# COMMAND ----------

import requests

r = requests.get("https://lichess.org")

print(r.status_code)

# COMMAND ----------

import requests

r = requests.get(
    "https://lichess.org/api/tv/feed",
    headers={"Accept": "application/x-ndjson"},
    stream=True
)

print(r.status_code)
print(r.headers.get("Content-Type"))

# COMMAND ----------

import requests
import json

r = requests.get(
    "https://lichess.org/api/tv/feed",
    headers={"Accept": "application/x-ndjson"},
    stream=True
)

line = next(r.iter_lines())

event = json.loads(line.decode())

print(event)

# COMMAND ----------

import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, MapType

r = requests.get(
    "https://lichess.org/api/tv/feed",
    headers={"Accept": "application/x-ndjson"},
    stream=True
)

events = []

for i, line in enumerate(r.iter_lines()):
    if line:
        event = json.loads(line.decode())
        # Ensure 'd' is a dict of string keys and string values
        if 'd' in event and isinstance(event['d'], dict):
            event['d'] = {str(k): str(v) for k, v in event['d'].items()}
        events.append(event)

    if len(events) == 5:
        break

schema = StructType([
    StructField("t", StringType(), True),
    StructField("d", MapType(StringType(), StringType()), True)
])

df = spark.createDataFrame(events, schema=schema)

display(df)
df.printSchema()

# COMMAND ----------

# Bronze layer

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
import json

bronze_rows = []

for event in events:
    bronze_rows.append(
        Row(
            event_type=event["t"],
            raw_json=json.dumps(event)
        )
    )

bronze_df = spark.createDataFrame(bronze_rows)

bronze_df = bronze_df.withColumn(
    "ingestion_time",
    current_timestamp()
)

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Cell 7
spark.sql("CREATE SCHEMA IF NOT EXISTS sandbox.bronze")# save as delta table


bronze_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("sandbox.bronze.lichess_events")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sandbox.bronze.lichess_events
# MAGIC LIMIT 10;
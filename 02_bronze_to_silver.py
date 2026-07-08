# Databricks notebook source
bronze_df = spark.table("sandbox.bronze.lichess_events")

display(bronze_df)

# COMMAND ----------

from pyspark.sql.functions import from_json,col
bronze_df.select("raw_json").show(1,False)
sample = bronze_df.select("raw_json").first()[0]

print(sample)

# COMMAND ----------


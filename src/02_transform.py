# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2: Transform - Build Flight Sessions

dbutils.widgets.text("catalog", "xe_training_catalog")
dbutils.widgets.text("schema", "databricks_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

from pyspark.sql.functions import col, min, max, count, last, collect_list, to_timestamp

raw = spark.table("raw_flight_events")
raw = raw.withColumn("event_time_ts", to_timestamp("event_time"))

sessions = (
    raw.groupBy("flight_id", "airport")
    .agg(
        min("event_time_ts").alias("session_start"),
        max("event_time_ts").alias("session_end"),
        count("*").alias("event_count"),
        max("delay_minutes").alias("max_delay"),
        last("event_type").alias("final_status")
    )
)

sessions.write.mode("overwrite").saveAsTable("flight_sessions")
print(f"✓ Created {sessions.count()} flight sessions in {catalog}.{schema}.flight_sessions")

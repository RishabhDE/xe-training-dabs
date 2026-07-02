# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1: Ingest Flight Events

dbutils.widgets.text("catalog", "xe_training_catalog")
dbutils.widgets.text("schema", "databricks_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

# Ingest raw flight events
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *

raw_data = [
    ("FL001", "DEL", "delayed", 15, "2025-07-01T08:00:00"),
    ("FL001", "DEL", "delayed", 30, "2025-07-01T08:30:00"),
    ("FL001", "DEL", "departed", 0, "2025-07-01T09:00:00"),
    ("FL002", "BOM", "delayed", 45, "2025-07-01T10:00:00"),
    ("FL002", "BOM", "cancelled", 0, "2025-07-01T11:00:00"),
    ("FL003", "BLR", "on_time", 0, "2025-07-01T07:00:00"),
    ("FL003", "BLR", "departed", 0, "2025-07-01T07:30:00"),
]

schema_def = StructType([
    StructField("flight_id", StringType()),
    StructField("airport", StringType()),
    StructField("event_type", StringType()),
    StructField("delay_minutes", IntegerType()),
    StructField("event_time", StringType())
])

df = spark.createDataFrame(raw_data, schema_def)
df = df.withColumn("ingested_at", current_timestamp())

df.write.mode("overwrite").saveAsTable("raw_flight_events")
print(f"✓ Ingested {df.count()} events into {catalog}.{schema}.raw_flight_events")

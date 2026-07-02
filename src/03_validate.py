# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3: Validate Data Quality

dbutils.widgets.text("catalog", "xe_training_catalog")
dbutils.widgets.text("schema", "databricks_dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Quality checks
sessions = spark.table("flight_sessions")
raw = spark.table("raw_flight_events")

checks = []

# Check 1: No null flight_ids
null_count = sessions.filter("flight_id IS NULL").count()
checks.append(("no_null_flight_ids", null_count == 0, null_count))

# Check 2: All flights from raw are in sessions
raw_flights = raw.select("flight_id").distinct().count()
session_flights = sessions.select("flight_id").distinct().count()
checks.append(("all_flights_covered", raw_flights == session_flights, f"{session_flights}/{raw_flights}"))

# Check 3: event_count > 0 for all sessions
zero_events = sessions.filter("event_count <= 0").count()
checks.append(("positive_event_count", zero_events == 0, zero_events))

# Check 4: session_end >= session_start
invalid_times = sessions.filter("session_end < session_start").count()
checks.append(("valid_time_range", invalid_times == 0, invalid_times))

# Report
print("=" * 50)
print("DATA QUALITY REPORT")
print("=" * 50)
all_passed = True
for name, passed, detail in checks:
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"  {status} | {name} | detail: {detail}")
    if not passed:
        all_passed = False

print("=" * 50)
if all_passed:
    print("✓ ALL QUALITY CHECKS PASSED")
else:
    raise Exception("Quality checks failed — pipeline halted")

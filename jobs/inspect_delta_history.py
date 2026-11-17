from delta.tables import DeltaTable

from jobs.delta_spark_session import create_delta_spark_session


spark = create_delta_spark_session()

table_path = "data/delta/nyc_taxi_trips"

delta_table = DeltaTable.forPath(
    spark,
    table_path
)

print("=== DELTA TABLE HISTORY ===")

history = delta_table.history()

history.select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
    "operationMetrics"
).show(truncate=False)

print("=== CURRENT TABLE VERSION ===")

print(
    delta_table.history(1)
    .select("version")
    .first()["version"]
)

spark.stop()
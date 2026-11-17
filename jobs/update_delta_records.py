from delta.tables import DeltaTable

from jobs.delta_spark_session import create_delta_spark_session


spark = create_delta_spark_session()

table_path = "data/delta/nyc_taxi_trips"

delta_table = DeltaTable.forPath(
    spark,
    table_path
)

condition = """
    VendorID = 2
    AND pickup_datetime = '2017-10-11 12:34:00'
    AND trip_distance = 12.3
"""

print("=== RECORD BEFORE UPDATE ===")

delta_table.toDF().filter(condition).select(
    "VendorID",
    "pickup_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount"
).show(truncate=False)

print("=== PERFORMING UPDATE ===")

delta_table.update(
    condition=condition,
    set={
        "fare_amount": "69.25"
    }
)

print("=== RECORD AFTER UPDATE ===")

delta_table.toDF().filter(condition).select(
    "VendorID",
    "pickup_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount"
).show(truncate=False)

print("=== CURRENT VERSION ===")

delta_table.history(1).select(
    "version",
    "operation",
    "operationMetrics"
).show(truncate=False)

spark.stop()

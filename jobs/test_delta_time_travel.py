from jobs.delta_spark_session import create_delta_spark_session

spark = create_delta_spark_session()

table_path = "data/delta/nyc_taxi_trips"

condition = """
    VendorID = 2
    AND pickup_datetime = '2017-10-11 12:34:00'
    AND trip_distance = 12.3
"""

print("=== VERSION 0: ORIGINAL DATA ===")

version_0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(table_path)
)

version_0.filter(condition).select(
    "fare_amount"
).show()

print("=== VERSION 1: UPDATED DATA ===")

version_1 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .load(table_path)
)

version_1.filter(condition).select(
    "fare_amount"
).show()

spark.stop()

from jobs.delta_spark_session import create_delta_spark_session

spark = create_delta_spark_session()

table_path = "data/delta/nyc_taxi_trips"

df = spark.read.format("delta").load(table_path)

print("=== SAMPLE TAXI RECORDS ===")

df.select(
    "VendorID",
    "pickup_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "pickup_year",
    "pickup_month"
).show(10, truncate=False)

spark.stop()

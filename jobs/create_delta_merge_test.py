from jobs.delta_spark_session import create_delta_spark_session

spark = create_delta_spark_session()

source_path = "data/delta/nyc_taxi_trips"
test_path = "data/delta/merge_test"

print("=== CREATING MERGE TEST TABLE ===")

source_df = (
    spark.read
    .format("delta")
    .load(source_path)
)

sample_df = source_df.limit(10)

(
    sample_df.write
    .format("delta")
    .mode("overwrite")
    .save(test_path)
)

test_df = (
    spark.read
    .format("delta")
    .load(test_path)
)

print("Test table created.")
print("Records:", test_df.count())

test_df.select(
    "VendorID",
    "pickup_datetime",
    "trip_distance",
    "fare_amount"
).show(truncate=False)

spark.stop()

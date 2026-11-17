from jobs.delta_spark_session import create_delta_spark_session

spark = create_delta_spark_session()

source_path = "data/delta/nyc_taxi_trips"
test_path = "data/delta/delete_test"

print("=== CREATING DELETE TEST TABLE ===")

# Read a small sample from the main Delta table
source_df = (
    spark.read
    .format("delta")
    .load(source_path)
)

sample_df = source_df.limit(10)

# Write sample into a separate Delta table
(
    sample_df.write
    .format("delta")
    .mode("overwrite")
    .save(test_path)
)

# Validate the new table
test_df = (
    spark.read
    .format("delta")
    .load(test_path)
)

print("Test table created.")
print("Record count:", test_df.count())

test_df.select(
    "VendorID",
    "pickup_datetime",
    "trip_distance",
    "fare_amount"
).show(truncate=False)

spark.stop()

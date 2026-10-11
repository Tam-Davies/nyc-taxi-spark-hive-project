from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("NYCTaxiTransformation")
    .master("local[*]")
    .getOrCreate()
)


# ============================================================
# 2. READ RAW DATA
# ============================================================

input_path = "data/raw/2017.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("\n=== RAW SCHEMA ===")
df.printSchema()


# ============================================================
# 3. REMOVE UNNECESSARY COLUMN
# ============================================================

# _c0 is an extra index-like column from the CSV.
# It is not part of the taxi trip information.

df = df.drop("_c0")


# ============================================================
# 4. CONVERT DATETIME COLUMNS
# ============================================================

df = (
    df.withColumn(
        "pickup_datetime",
        to_timestamp(
            "tpep_pickup_datetime",
            "M/d/yyyy H:mm"
        )
    )
    .withColumn(
        "dropoff_datetime",
        to_timestamp(
            "tpep_dropoff_datetime",
            "M/d/yyyy H:mm"
        )
    )
)


# ============================================================
# 5. SELECT ANALYTICAL COLUMNS
# ============================================================

df = df.select(
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount"
)


# ============================================================
# 6. DATA QUALITY PROFILING
# ============================================================

print("\n=== DATA QUALITY SUMMARY ===")

null_pickup = df.filter(
    col("pickup_datetime").isNull()
).count()

null_dropoff = df.filter(
    col("dropoff_datetime").isNull()
).count()

invalid_distance = df.filter(
    col("trip_distance") <= 0
).count()

invalid_passengers = df.filter(
    col("passenger_count") <= 0
).count()

invalid_fare = df.filter(
    col("fare_amount") <= 0
).count()

invalid_datetime_order = df.filter(
    col("dropoff_datetime") < col("pickup_datetime")
).count()


print("Null pickup timestamps:", null_pickup)
print("Null dropoff timestamps:", null_dropoff)
print("Zero or negative trip distance:", invalid_distance)
print("Zero or negative passenger count:", invalid_passengers)
print("Zero or negative fare:", invalid_fare)
print("Dropoff before pickup:", invalid_datetime_order)


# ============================================================
# 7. DEFINE WHAT COUNTS AS A VALID TRIP
# ============================================================

# A valid analytical trip must satisfy all of these rules:
#
# 1. Pickup timestamp exists
# 2. Dropoff timestamp exists
# 3. Trip distance is greater than zero
# 4. Passenger count is greater than zero
# 5. Fare amount is greater than zero
# 6. Dropoff occurs at or after pickup

valid_trip = (
    col("pickup_datetime").isNotNull()
    & col("dropoff_datetime").isNotNull()
    & (col("trip_distance") > 0)
    & (col("passenger_count") > 0)
    & (col("fare_amount") > 0)
    & (col("dropoff_datetime") >= col("pickup_datetime"))
)


# ============================================================
# 8. CREATE CLEAN DATASET
# ============================================================

clean_df = df.filter(valid_trip)


# ============================================================
# 9. COMPARE BEFORE AND AFTER CLEANING
# ============================================================

raw_count = df.count()
clean_count = clean_df.count()
removed_count = raw_count - clean_count

print("\n=== CLEANING RESULTS ===")
print("Records before cleaning:", raw_count)
print("Records after cleaning:", clean_count)
print("Records removed:", removed_count)


# ============================================================
# 10. SHOW CLEAN DATA
# ============================================================

print("\n=== CLEAN DATA SAMPLE ===")

clean_df.show(
    10,
    truncate=False
)


# ============================================================
# 11. SHOW CLEAN DATA SCHEMA
# ============================================================

print("\n=== CLEAN DATA SCHEMA ===")

clean_df.printSchema()


# ============================================================
# 12. WRITE CLEAN DATA AS PARQUET
# ============================================================

output_path = Path(
    "data/processed/clean_trips"
).resolve()

print("\n=== WRITING PARQUET ===")
print("Output path:", output_path)

clean_df.write \
    .mode("overwrite") \
    .parquet(str(output_path))


print("\n=== PARQUET WRITE COMPLETE ===")


# ============================================================
# 13. STOP SPARK
# ============================================================

spark.stop()

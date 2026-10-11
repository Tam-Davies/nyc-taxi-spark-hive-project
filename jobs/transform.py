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


print("=== RAW SCHEMA ===")
df.printSchema()


# ============================================================
# 3. REMOVE UNNECESSARY COLUMN
# ============================================================

# The original CSV contains an extra index-like column called _c0.
# It is not part of the actual taxi data.

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
# 5. SELECT COLUMNS FOR THE CLEAN DATASET
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
# 6. DATA QUALITY CHECKS
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
# 7. INSPECT INVALID TRIP DISTANCES
# ============================================================

print("\n=== RECORDS WITH ZERO OR NEGATIVE TRIP DISTANCE ===")

df.filter(
    col("trip_distance") <= 0
).select(
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount"
).show(20, truncate=False)


# ============================================================
# 8. INSPECT INVALID PASSENGER COUNTS
# ============================================================

print("\n=== RECORDS WITH ZERO OR NEGATIVE PASSENGER COUNT ===")

df.filter(
    col("passenger_count") <= 0
).select(
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount"
).show(20, truncate=False)


# ============================================================
# 9. INSPECT INVALID FARES
# ============================================================

print("\n=== RECORDS WITH ZERO OR NEGATIVE FARE ===")

df.filter(
    col("fare_amount") <= 0
).select(
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount"
).show(20, truncate=False)


# ============================================================
# 10. INSPECT INVALID DATETIME ORDER
# ============================================================

print("\n=== RECORDS WHERE DROPOFF IS BEFORE PICKUP ===")

df.filter(
    col("dropoff_datetime") < col("pickup_datetime")
).select(
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount"
).show(20, truncate=False)


# ============================================================
# 11. DISPLAY TRANSFORMED SCHEMA
# ============================================================

print("\n=== TRANSFORMED SCHEMA ===")
df.printSchema()


# ============================================================
# 12. DISPLAY SAMPLE TRANSFORMED DATA
# ============================================================

print("\n=== TRANSFORMED DATA ===")

df.show(
    10,
    truncate=False
)


# ============================================================
# 13. RECORD COUNT
# ============================================================

print("\n=== RECORD COUNT ===")

print(df.count())


# ============================================================
# 14. STOP SPARK
# ============================================================

spark.stop()

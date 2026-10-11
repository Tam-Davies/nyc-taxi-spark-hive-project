from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

spark = (
    SparkSession.builder
    .appName("NYCTaxiTransformation")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/raw/2017.csv"

# ============================================================
# 1. READ RAW DATA
# ============================================================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("=== RAW SCHEMA ===")
df.printSchema()


# ============================================================
# 2. REMOVE UNNECESSARY COLUMN
# ============================================================

df = df.drop("_c0")


# ============================================================
# 3. CONVERT DATETIME COLUMNS
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
# 4. SELECT COLUMNS FOR THE CLEAN DATASET
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
# 5. DATA QUALITY CHECKS
# ============================================================

print("=== DATA QUALITY CHECKS ===")

print("Null pickup timestamps:")
print(
    df.filter(
        col("pickup_datetime").isNull()
    ).count()
)

print("Null dropoff timestamps:")
print(
    df.filter(
        col("dropoff_datetime").isNull()
    ).count()
)

print("Zero or negative trip distance:")
print(
    df.filter(
        col("trip_distance") <= 0
    ).count()
)

print("Zero or negative passenger count:")
print(
    df.filter(
        col("passenger_count") <= 0
    ).count()
)

print("Zero or negative fare:")
print(
    df.filter(
        col("fare_amount") <= 0
    ).count()
)

print("Dropoff before pickup:")
print(
    df.filter(
        col("dropoff_datetime") < col("pickup_datetime")
    ).count()
)


# ============================================================
# 6. TRANSFORMED DATASET INSPECTION
# ============================================================

print("=== TRANSFORMED SCHEMA ===")
df.printSchema()

print("=== TRANSFORMED DATA ===")
df.show(10, truncate=False)

print("=== RECORD COUNT ===")
print(df.count())


# ============================================================
# 7. STOP SPARK
# ============================================================

spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

spark = (
    SparkSession.builder
    .appName("NYCTaxiTransformation")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/raw/2017.csv"

# Read raw data
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("=== RAW SCHEMA ===")
df.printSchema()

# Remove the unnamed index column
df = df.drop("_c0")

# Convert datetime strings into timestamps
df = (
    df.withColumn(
        "pickup_datetime",
        to_timestamp("tpep_pickup_datetime", "M/d/yyyy H:mm")
    )
    .withColumn(
        "dropoff_datetime",
        to_timestamp("tpep_dropoff_datetime", "M/d/yyyy H:mm")
    )
)

# Select the columns we need
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

print("=== TRANSFORMED SCHEMA ===")
df.printSchema()

print("=== TRANSFORMED DATA ===")
df.show(10, truncate=False)

print("=== RECORD COUNT ===")
print(df.count())

spark.stop()

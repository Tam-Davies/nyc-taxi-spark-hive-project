from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

spark = (
    SparkSession.builder
    .appName("NYCTaxiPartitioning")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/processed/clean_trips"

df = spark.read.parquet(input_path)

print("=== INPUT DATA ===")
print("Records:", df.count())
print("Current Spark partitions:", df.rdd.getNumPartitions())

# Create partition columns
df = (
    df
    .withColumn("pickup_year", year("pickup_datetime"))
    .withColumn("pickup_month", month("pickup_datetime"))
)

print("\n=== PARTITION COLUMNS ===")
df.select(
    "pickup_datetime",
    "pickup_year",
    "pickup_month"
).show(10, truncate=False)

output_path = Path(
    "data/processed/partitioned_trips"
).resolve()

print("\n=== WRITING PARTITIONED PARQUET ===")
print("Output:", output_path)

(
    df.write
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .parquet(str(output_path))
)

print("\n=== WRITE COMPLETE ===")

spark.stop()

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round
)

spark = (
    SparkSession.builder
    .appName("NYCTaxiAggregation")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.table("taxi.partitioned_trips")

monthly_summary = (
    df.groupBy(
        "pickup_year",
        "pickup_month",
        "VendorID"
    )
    .agg(
        count("*").alias("total_trips"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        round(avg("fare_amount"), 2).alias("avg_fare")
    )
    .orderBy(
        "pickup_year",
        "pickup_month",
        "VendorID"
    )
)

print("=== MONTHLY SUMMARY ===")
monthly_summary.show(50, truncate=False)

output_path = Path(
    "data/processed/monthly_summary"
).resolve()

(
    monthly_summary.write
    .mode("overwrite")
    .parquet(str(output_path))
)

print("=== OUTPUT ===")
print(output_path)

spark.stop()

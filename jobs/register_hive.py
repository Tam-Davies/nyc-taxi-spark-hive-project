from pathlib import Path

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RegisterTaxiHiveTable")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

data_path = Path(
    "data/processed/partitioned_trips"
).resolve()

spark.sql("CREATE DATABASE IF NOT EXISTS taxi")

spark.sql("DROP TABLE IF EXISTS taxi.partitioned_trips")

# Read the existing partitioned Parquet dataset.
df = spark.read.parquet(str(data_path))

print("=== DETECTED SCHEMA ===")
df.printSchema()

print("=== SAMPLE DATA ===")
df.show(5, truncate=False)

# Register the existing Parquet dataset as a Hive table.
df.createOrReplaceTempView("partitioned_trips_view")

spark.sql("""
    CREATE TABLE taxi.partitioned_trips
    USING PARQUET
    AS
    SELECT *
    FROM partitioned_trips_view
""")

print("=== TABLES IN TAXI DATABASE ===")
spark.sql("SHOW TABLES IN taxi").show()

print("=== TABLE DESCRIPTION ===")
spark.sql("""
    DESCRIBE taxi.partitioned_trips
""").show(50, truncate=False)

spark.stop()
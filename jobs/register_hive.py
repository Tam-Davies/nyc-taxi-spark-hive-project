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

spark.sql(f"""
    CREATE TABLE taxi.partitioned_trips
    USING PARQUET
    LOCATION '{data_path}'
""")

# Explicitly discover partitions from the directory structure.
spark.sql("""
    ALTER TABLE taxi.partitioned_trips
    RECOVER PARTITIONS
""")

print("=== PARTITIONS DISCOVERED ===")

spark.sql("""
    SHOW PARTITIONS taxi.partitioned_trips
""").show(50, truncate=False)

print("=== TABLE DESCRIPTION ===")

spark.sql("""
    DESCRIBE taxi.partitioned_trips
""").show(50, truncate=False)

spark.stop()
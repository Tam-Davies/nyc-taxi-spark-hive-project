from pathlib import Path

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ParquetHiveTest")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

# Create sample data
data = [
    ("2026-10-01", "Alice", 2.5, 12.50),
    ("2026-10-01", "Bob", 5.2, 22.00),
    ("2026-10-02", "Charlie", 3.1, 15.75),
]

columns = ["trip_date", "passenger", "distance", "fare"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

# Use an absolute path for the Parquet dataset
output_path = Path("data/processed/test_trips").resolve()

# Write DataFrame to Parquet
df.write.mode("overwrite").parquet(str(output_path))

print("Parquet files written to:")
print(output_path)

# Create Hive database
spark.sql("CREATE DATABASE IF NOT EXISTS taxi")

# Remove the test table if it already exists
spark.sql("DROP TABLE IF EXISTS taxi.test_trips")

# Register the existing Parquet dataset as a Hive table
spark.sql(f"""
    CREATE TABLE taxi.test_trips
    USING PARQUET
    LOCATION '{output_path}'
""")

print("Hive table:")
spark.sql("SHOW TABLES IN taxi").show()

print("Querying Hive table:")
spark.sql("""
    SELECT *
    FROM taxi.test_trips
""").show()

spark.stop()
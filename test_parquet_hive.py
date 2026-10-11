from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ParquetHiveTest")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

data = [
    ("2026-10-01", "Alice", 2.5, 12.50),
    ("2026-10-01", "Bob", 5.2, 22.00),
    ("2026-10-02", "Charlie", 3.1, 15.75),
]

columns = ["trip_date", "passenger", "distance", "fare"]

df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

# Write the actual data as Parquet
output_path = "data/processed/test_trips"

df.write.mode("overwrite").parquet(output_path)

print("Parquet files written to:")
print(output_path)

# Register the Parquet data as a Hive table
spark.sql("CREATE DATABASE IF NOT EXISTS taxi")

spark.sql("""
    CREATE TABLE IF NOT EXISTS taxi.test_trips
    USING PARQUET
    LOCATION 'data/processed/test_trips'
""")

print("Hive table:")
spark.sql("SHOW TABLES IN taxi").show()

print("Querying Hive table:")
spark.sql("""
    SELECT *
    FROM taxi.test_trips
""").show()

spark.stop()

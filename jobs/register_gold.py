from pathlib import Path

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RegisterGoldHiveTable")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

data_path = Path(
    "data/processed/monthly_summary"
).resolve()

spark.sql("CREATE DATABASE IF NOT EXISTS taxi")

spark.sql("DROP TABLE IF EXISTS taxi.monthly_summary")

spark.sql(f"""
    CREATE TABLE taxi.monthly_summary
    USING PARQUET
    LOCATION '{data_path}'
""")

print("=== GOLD TABLE ===")

spark.sql("""
    SHOW TABLES IN taxi
""").show()

print("=== GOLD TABLE DATA ===")

spark.sql("""
    SELECT *
    FROM taxi.monthly_summary
    ORDER BY pickup_month, VendorID
""").show(50, truncate=False)

spark.stop()

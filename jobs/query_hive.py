from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("QueryTaxiHive")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

print("=== JULY 2017 TRIPS ===")

spark.sql("""
    SELECT COUNT(*) AS trip_count
    FROM taxi.partitioned_trips
    WHERE pickup_year = 2017
      AND pickup_month = 7
""").show()

print("=== QUERY PLAN ===")

spark.sql("""
    SELECT *
    FROM taxi.partitioned_trips
    WHERE pickup_year = 2017
      AND pickup_month = 7
""").explain(True)

spark.stop()

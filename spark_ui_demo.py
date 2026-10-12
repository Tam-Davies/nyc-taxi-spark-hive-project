from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkUIPractice")
    .master("local[*]")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)

df = spark.read.parquet("data/processed/partitioned_trips")

result = (
    df.groupBy("VendorID")
      .count()
)

result.explain(True)
result.show()

input("Press Enter to stop Spark...")

spark.stop()

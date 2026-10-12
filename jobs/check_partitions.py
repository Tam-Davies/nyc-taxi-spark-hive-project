from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckPartitions")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.read
    .parquet("data/processed/clean_trips")
)

print("Number of Spark partitions:", df.rdd.getNumPartitions())

spark.stop()

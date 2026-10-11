from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HiveTest")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

print("Catalog implementation:",
      spark.conf.get("spark.sql.catalogImplementation"))

spark.sql("CREATE DATABASE IF NOT EXISTS taxi")

spark.sql("SHOW DATABASES").show()

spark.stop()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYCTaxiIngestion")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/raw/2017.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("=== SCHEMA ===")
df.printSchema()

print("=== SAMPLE DATA ===")
df.show(10, truncate=False)

print("=== RECORD COUNT ===")
print(df.count())

spark.stop()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkTest")
    .master("local[*]")
    .getOrCreate()
)

data = [
    ("Alice", 25),
    ("Bob", 30),
    ("Charlie", 35)
]

df = spark.createDataFrame(data, ["name", "age"])

df.show()

print("Number of rows:", df.count())

spark.stop()

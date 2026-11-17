from jobs.delta_spark_session import create_delta_spark_session

spark = create_delta_spark_session()

print("Spark version:", spark.version)
print("Application:", spark.sparkContext.appName)

print("\n=== DELTA CONFIGURATION ===")
print(
    spark.conf.get("spark.sql.extensions")
)
print(
    spark.conf.get("spark.sql.catalog.spark_catalog")
)

print("\n=== TEST DELTA WRITE ===")

test_data = [
    (1, "NYC", 25.50),
    (2, "Lagos", 30.75),
    (3, "London", 42.00)
]

columns = ["id", "city", "amount"]

df = spark.createDataFrame(test_data, columns)

output_path = "data/delta/test_table"

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(output_path)
)

print("Delta table created successfully.")

spark.read.format("delta").load(output_path).show()

spark.stop()

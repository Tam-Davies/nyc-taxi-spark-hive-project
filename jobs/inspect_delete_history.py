from jobs.delta_spark_session import create_delta_spark_session

spark = create_delta_spark_session()

table_path = "data/delta/delete_test"

print("=== VERSION 0: ORIGINAL RECORD COUNT ===")

version_0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(table_path)
)

print("Records:", version_0.count())

condition = """
    VendorID = 1
    AND pickup_datetime = '2017-10-26 19:43:00'
"""

version_0.filter(condition).show(truncate=False)

print("=== VERSION 1: AFTER DELETE ===")

version_1 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .load(table_path)
)

print("Records:", version_1.count())

version_1.filter(condition).show(truncate=False)

spark.stop()

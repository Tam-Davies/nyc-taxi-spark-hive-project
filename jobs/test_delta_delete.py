from delta.tables import DeltaTable

from jobs.delta_spark_session import create_delta_spark_session

spark = create_delta_spark_session()

table_path = "data/delta/delete_test"

delta_table = DeltaTable.forPath(
    spark,
    table_path
)

condition = """
    VendorID = 1
    AND pickup_datetime = '2017-10-26 19:43:00'
"""

print("=== BEFORE DELETE ===")
print("Records:", delta_table.toDF().count())

delta_table.toDF().filter(condition).show(
    truncate=False
)

print("=== PERFORMING DELETE ===")

delta_table.delete(condition)

print("=== AFTER DELETE ===")
print("Records:", delta_table.toDF().count())

delta_table.toDF().filter(condition).show(
    truncate=False
)

print("=== TRANSACTION HISTORY ===")

delta_table.history(1).select(
    "version",
    "operation",
    "operationMetrics"
).show(truncate=False)

spark.stop()

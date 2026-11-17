
from delta.tables import DeltaTable

from jobs.delta_spark_session import create_delta_spark_session

spark = create_delta_spark_session()

table_path = "data/delta/test_table"

delta_table = DeltaTable.forPath(spark, table_path)

print("=== ORIGINAL TABLE ===")
delta_table.toDF().show()

print("=== UPDATE OPERATION ===")

delta_table.update(
    condition="city = 'Lagos'",
    set={"amount": "35.00"}
)

print("=== UPDATED TABLE ===")
delta_table.toDF().show()

print("=== TRANSACTION HISTORY ===")
delta_table.history().select(
    "version",
    "operation",
    "operationMetrics"
).show(truncate=False)

print("=== TIME TRAVEL: VERSION 0 ===")

historical_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(table_path)
)

historical_df.show()

spark.stop()

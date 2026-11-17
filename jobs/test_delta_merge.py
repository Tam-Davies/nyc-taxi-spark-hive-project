from delta.tables import DeltaTable

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    TimestampType,
    DoubleType
)

from datetime import datetime

from jobs.delta_spark_session import create_delta_spark_session


spark = create_delta_spark_session()

table_path = "data/delta/merge_test"

delta_table = DeltaTable.forPath(
    spark,
    table_path
)

# Incoming batch
schema = StructType([
    StructField("VendorID", IntegerType(), False),
    StructField("pickup_datetime", TimestampType(), False),
    StructField("trip_distance", DoubleType(), False),
    StructField("fare_amount", DoubleType(), False)
])

source_data = [
    (2, datetime(2017, 10, 11, 12, 34), 12.3, 75.00),
    (2, datetime(2017, 10, 30, 10, 15), 4.5, 22.50)
]

source_df = spark.createDataFrame(
    source_data,
    schema
)

print("=== INCOMING DATA ===")
source_df.show(truncate=False)

print("=== MERGE OPERATION ===")

(
    delta_table.alias("target")
    .merge(
        source_df.alias("source"),
        """
        target.VendorID = source.VendorID
        AND target.pickup_datetime = source.pickup_datetime
        AND target.trip_distance = source.trip_distance
        """
    )
    .whenMatchedUpdate(
        set={
            "fare_amount": "source.fare_amount"
        }
    )
    .whenNotMatchedInsert(
        values={
            "VendorID": "source.VendorID",
            "pickup_datetime": "source.pickup_datetime",
            "trip_distance": "source.trip_distance",
            "fare_amount": "source.fare_amount"
        }
    )
    .execute()
)

print("=== MERGE RESULT ===")

delta_table.toDF().select(
    "VendorID",
    "pickup_datetime",
    "trip_distance",
    "fare_amount"
).show(truncate=False)

print("=== TABLE RECORD COUNT ===")
print(delta_table.toDF().count())

print("=== MERGE TRANSACTION HISTORY ===")

delta_table.history(1).select(
    "version",
    "operation",
    "operationMetrics"
).show(truncate=False)

spark.stop()

from pathlib import Path

from jobs.delta_spark_session import create_delta_spark_session


def main():

    spark = create_delta_spark_session()

    # Existing Parquet source
    source_path = "data/processed/partitioned_trips"

    # New Delta destination
    destination_path = "data/delta/nyc_taxi_trips"

    print("\n=== PARQUET TO DELTA CONVERSION ===")

    # Confirm source exists
    if not Path(source_path).exists():
        raise FileNotFoundError(
            f"Parquet source not found: {source_path}"
        )

    # Read existing Parquet dataset
    print("\nReading partitioned Parquet dataset...")

    parquet_df = spark.read.parquet(source_path)

    source_count = parquet_df.count()

    print(f"Parquet records: {source_count:,}")

    # Write to a separate Delta location
    print("\nWriting Delta table...")

    (
        parquet_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .save(destination_path)
    )

    print("\nDelta conversion completed.")

    # Read Delta table for validation
    print("\n=== DELTA VALIDATION ===")

    delta_df = (
        spark.read
        .format("delta")
        .load(destination_path)
    )

    delta_count = delta_df.count()

    print(f"Delta records: {delta_count:,}")

    print("\n=== SCHEMA ===")
    delta_df.printSchema()

    print("\n=== PARTITION DISTRIBUTION ===")

    (
        delta_df.groupBy(
            "pickup_year",
            "pickup_month"
        )
        .count()
        .orderBy(
            "pickup_year",
            "pickup_month"
        )
        .show(20)
    )

    # Validate record counts
    if source_count != delta_count:
        raise ValueError(
            "Record count mismatch between Parquet and Delta!"
        )

    print("\nSUCCESS: Parquet and Delta record counts match.")

    spark.stop()


if __name__ == "__main__":
    main()

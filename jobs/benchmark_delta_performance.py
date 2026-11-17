
import time

from jobs.delta_spark_session import create_delta_spark_session


# =====================================================
# CONFIGURATION
# =====================================================

PARQUET_PATH = "data/processed/partitioned_trips"
DELTA_PATH = "data/delta/nyc_taxi_trips"

spark = create_delta_spark_session()


# =====================================================
# BENCHMARK FUNCTIONS
# =====================================================

def benchmark_full_read(label, path, format_name):

    print(f"\n=== {label} ===")

    start = time.perf_counter()

    df = (
        spark.read
        .format(format_name)
        .load(path)
    )

    record_count = df.count()
    elapsed = time.perf_counter() - start

    print(f"Records: {record_count:,}")
    print(f"Read time: {elapsed:.4f} seconds")

    return elapsed, record_count


def benchmark_filtered_query(label, path, format_name):

    print(f"\n=== {label} ===")

    start = time.perf_counter()

    df = (
        spark.read
        .format(format_name)
        .load(path)
    )

    result = df.filter(
        (df.pickup_year == 2017)
        & (df.pickup_month == 10)
    )

    record_count = result.count()
    elapsed = time.perf_counter() - start

    print(f"October records: {record_count:,}")
    print(f"Query time: {elapsed:.4f} seconds")

    return elapsed, record_count


# =====================================================
# MAIN EXECUTION
# =====================================================

try:

    # FULL TABLE READ BENCHMARK

    print("\n" + "=" * 55)
    print("FULL TABLE READ BENCHMARK")
    print("=" * 55)

    parquet_read_time, parquet_count = benchmark_full_read(
        "PARQUET FULL READ",
        PARQUET_PATH,
        "parquet"
    )

    delta_read_time, delta_count = benchmark_full_read(
        "DELTA FULL READ",
        DELTA_PATH,
        "delta"
    )

    if parquet_count != delta_count:
        raise ValueError("Record count mismatch!")


    # FILTERED QUERY BENCHMARK

    print("\n" + "=" * 55)
    print("FILTERED QUERY BENCHMARK")
    print("=" * 55)

    parquet_filter_time, parquet_october_count = (
        benchmark_filtered_query(
            "PARQUET OCTOBER FILTER",
            PARQUET_PATH,
            "parquet"
        )
    )

    delta_filter_time, delta_october_count = (
        benchmark_filtered_query(
            "DELTA OCTOBER FILTER",
            DELTA_PATH,
            "delta"
        )
    )

    if parquet_october_count != delta_october_count:
        raise ValueError("Filtered record count mismatch!")


    # FINAL SUMMARY

    print("\n" + "=" * 55)
    print("FINAL BENCHMARK SUMMARY")
    print("=" * 55)

    print("\nFULL TABLE READ")
    print(f"Parquet: {parquet_read_time:.4f} seconds")
    print(f"Delta:   {delta_read_time:.4f} seconds")

    print("\nFILTERED QUERY")
    print(f"Parquet: {parquet_filter_time:.4f} seconds")
    print(f"Delta:   {delta_filter_time:.4f} seconds")

    print("\nRECORD VALIDATION")
    print(f"Full records: {parquet_count:,}")
    print(f"October records: {parquet_october_count:,}")


    # PARTITION PRUNING: PARQUET

    print("\n" + "=" * 55)
    print("PARQUET QUERY EXECUTION PLAN")
    print("=" * 55)

    parquet_df = spark.read.parquet(PARQUET_PATH)

    parquet_df.filter(
        (parquet_df.pickup_year == 2017)
        & (parquet_df.pickup_month == 10)
    ).explain("formatted")


    # PARTITION PRUNING: DELTA

    print("\n" + "=" * 55)
    print("DELTA QUERY EXECUTION PLAN")
    print("=" * 55)

    delta_df = (
        spark.read
        .format("delta")
        .load(DELTA_PATH)
    )

    delta_df.filter(
        (delta_df.pickup_year == 2017)
        & (delta_df.pickup_month == 10)
    ).explain("formatted")


    print("\nBENCHMARK COMPLETED SUCCESSFULLY")


finally:
    spark.stop()

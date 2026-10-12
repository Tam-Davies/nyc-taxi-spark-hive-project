from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def get_spark():
    return (
        SparkSession.builder
        .appName("NYCTaxiTests")
        .master("local[2]")
        .getOrCreate()
    )


def test_clean_data_exists():
    spark = get_spark()

    df = spark.read.parquet("data/processed/clean_trips")

    assert df.count() == 22502
    assert "_c0" not in df.columns
    assert "pickup_datetime" in df.columns
    assert "dropoff_datetime" in df.columns

    spark.stop()


def test_clean_data_quality():
    spark = get_spark()

    df = spark.read.parquet("data/processed/clean_trips")

    assert df.filter(col("trip_distance") <= 0).count() == 0
    assert df.filter(col("passenger_count") <= 0).count() == 0
    assert df.filter(col("fare_amount") <= 0).count() == 0
    assert df.filter(
        col("dropoff_datetime") < col("pickup_datetime")
    ).count() == 0

    spark.stop()


def test_partitioned_data():
    spark = get_spark()

    df = spark.read.parquet("data/processed/partitioned_trips")

    assert "pickup_year" in df.columns
    assert "pickup_month" in df.columns
    assert df.count() == 22502

    spark.stop()


def test_gold_data():
    spark = get_spark()

    df = spark.read.parquet("data/processed/monthly_summary")

    assert df.count() == 24
    assert set(["pickup_year", "pickup_month", "VendorID"]).issubset(
        df.columns
    )
    assert df.select("total_trips").rdd.map(lambda r: r[0]).sum() == 22502

    spark.stop()

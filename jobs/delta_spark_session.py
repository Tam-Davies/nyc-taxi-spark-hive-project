
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_delta_spark_session():

    builder = (
        SparkSession.builder
        .appName("NYCTaxiDeltaLake")
        .master("local[*]")

        # Delta Lake configuration
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # WSL networking configuration
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")

        # Resource configuration
        .config("spark.driver.memory", "2g")
    )

    spark = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    return spark

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYCTaxiAnalysis")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
)

queries = [
    """
    SELECT
        SUM(total_trips) AS total_trips,
        ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM taxi.monthly_summary
    """,
    """
    SELECT
        pickup_month,
        SUM(total_trips) AS total_trips,
        ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM taxi.monthly_summary
    GROUP BY pickup_month
    ORDER BY pickup_month
    """,
    """
    SELECT
        VendorID,
        SUM(total_trips) AS total_trips,
        ROUND(SUM(total_revenue), 2) AS total_revenue,
        ROUND(AVG(avg_fare), 2) AS avg_fare
    FROM taxi.monthly_summary
    GROUP BY VendorID
    ORDER BY total_trips DESC
    """,
    """
    SELECT
        pickup_month,
        ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM taxi.monthly_summary
    GROUP BY pickup_month
    ORDER BY total_revenue DESC
    LIMIT 1
    """
]

for i, query in enumerate(queries, 1):
    print(f"\n=== QUERY {i} ===")
    spark.sql(query).show(20, truncate=False)

spark.stop()

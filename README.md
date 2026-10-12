# NYC Taxi Spark + Hive Data Engineering Pipeline

An end-to-end local Big Data engineering project demonstrating data ingestion, cleaning, transformation, partitioning, aggregation, Hive Metastore integration, Spark SQL analysis, and automated testing using Apache Spark and Parquet.

## Project Overview

NYC Taxi CSV
↓
Apache Spark
├── Ingestion
├── Data Quality Checks
├── Cleaning
├── Transformation
└── Aggregation
↓
Partitioned Parquet
↓
Hive Metastore / Catalog
↓
Spark SQL
↓
Analytical Results

## Technologies

- Python 3.14.4
- Apache Spark 4.2.0
- PySpark
- Hive catalog/metastore support
- Spark SQL
- Parquet
- Pytest 9.1.1
- WSL Ubuntu
- Java / OpenJDK

## Project Structure

```text
nyc-taxi-spark/
├── data/
│   ├── raw/
│   │   └── 2017.csv
│   └── processed/
│       ├── clean_trips/
│       ├── partitioned_trips/
│       ├── monthly_summary/
│       └── test_trips/
├── jobs/
│   ├── ingest.py
│   ├── transform.py
│   ├── partition.py
│   ├── register_hive.py
│   ├── query_hive.py
│   ├── aggregate.py
│   ├── register_gold.py
│   ├── run_analysis.py
│   └── spark_ui_demo.py
├── sql/
│   └── analysis.sql
├── tests/
│   └── test_pipeline.py
├── .venv/
├── requirements.txt
└── README.md
```

## Dataset

The project uses a NYC Taxi dataset containing 2017 taxi trip records.

- Raw records: **22,699**
- Raw columns: **18**

Important fields include `VendorID`, pickup/dropoff timestamps, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount`, `tip_amount`, `tolls_amount`, and `total_amount`.

The `_c0` column was identified as an unnecessary index-like column and removed during transformation.

## Data Cleaning and Transformation

The transformation stage:

- Removes `_c0`
- Converts pickup and dropoff timestamps
- Validates trip measurements
- Removes invalid records

A valid trip satisfies:

```text
pickup_datetime IS NOT NULL
dropoff_datetime IS NOT NULL
trip_distance > 0
passenger_count > 0
fare_amount > 0
dropoff_datetime >= pickup_datetime
```

Raw data quality findings:

- 148 records with zero/negative trip distance
- 33 records with zero/negative passenger count
- 20 records with zero/negative fare
- 1 record where dropoff occurred before pickup

These categories overlap.

Final dataset:

```text
Raw records:      22,699
Clean records:    22,502
Records removed:     197
```

Clean data is stored in:

```text
data/processed/clean_trips
```

## Parquet Storage

The cleaned CSV data is converted to Parquet:

```text
CSV → Spark DataFrame → Parquet
```

Parquet provides columnar storage suitable for analytical workloads.

## Data Partitioning

The cleaned dataset is partitioned by:

```text
pickup_year
pickup_month
```

Directory structure:

```text
partitioned_trips/
└── pickup_year=2017/
    ├── pickup_month=1/
    ├── pickup_month=2/
    ├── ...
    └── pickup_month=12/
```

The partitioned dataset contains **22,502 records** across **12 monthly partitions**.

## Hive Metastore Integration

Spark is configured with Hive support:

```python
SparkSession.builder     .enableHiveSupport()     .getOrCreate()
```

This project uses Spark's local Hive catalog/metastore support rather than a standalone Hive server.

The partitioned Parquet dataset is registered as:

```text
taxi.partitioned_trips
```

Partitions are recovered using:

```sql
ALTER TABLE taxi.partitioned_trips
RECOVER PARTITIONS;
```

The Hive catalog recognizes all 12 monthly partitions.

## Partition Pruning

Example query:

```sql
SELECT COUNT(*)
FROM taxi.partitioned_trips
WHERE pickup_year = 2017
  AND pickup_month = 7;
```

Result:

```text
1,680 trips
```

The Spark physical plan contains partition filters for `pickup_year = 2017` and `pickup_month = 7`, demonstrating partition pruning.

## Spark Aggregation

The pipeline creates a monthly vendor-level summary containing:

- Total trips
- Total revenue
- Average trip distance
- Average fare

Grouped by:

```text
pickup_year
pickup_month
VendorID
```

The Gold dataset contains **24 rows**:

```text
12 months × 2 vendors = 24 groups
```

Stored at:

```text
data/processed/monthly_summary
```

## Gold Hive Table

The aggregated Parquet dataset is registered as:

```text
taxi.monthly_summary
```

Main Hive tables:

```text
taxi.partitioned_trips
taxi.monthly_summary
```

## Spark SQL Analysis

### Total trips and revenue

```sql
SELECT
    SUM(total_trips) AS total_trips,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM taxi.monthly_summary;
```

Result:

```text
Total trips:    22,502
Total revenue:  365,786.00
```

### Vendor comparison

```text
Vendor 2
Trips:          12,547
Revenue:        203,602.74
Average fare:       12.94

Vendor 1
Trips:           9,955
Revenue:        162,183.26
Average fare:       13.02
```

### Highest-revenue month

```text
May
Revenue: 33,629.89
```

## Spark Execution Concepts Demonstrated

The project demonstrates:

### Lazy Evaluation

Transformations build an execution plan without immediately executing computation.

Examples:

```text
filter
select
withColumn
groupBy
```

Actions trigger execution:

```text
count
show
write
```

### Narrow Transformations

Examples:

```text
filter
select
withColumn
```

These generally do not require a shuffle.

### Wide Transformations

Examples:

```text
groupBy
join
distinct
orderBy
```

These can require data shuffling between Spark partitions.

The monthly aggregation demonstrates a shuffle-producing `groupBy`.

## Testing

The Pytest suite validates the main pipeline outputs.

It checks:

- Clean dataset record count
- Required columns
- Removal of `_c0`
- Data quality rules
- Partition columns
- Partitioned dataset record count
- Gold dataset row count
- Gold dataset columns
- Reconciliation of total trips

Run:

```bash
pytest -v
```

Current result:

```text
4 passed in 66.39s
```

## Running the Pipeline

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Run the pipeline stages:

```bash
python jobs/ingest.py
python jobs/transform.py
python jobs/partition.py
python jobs/register_hive.py
python jobs/query_hive.py
python jobs/aggregate.py
python jobs/register_gold.py
python jobs/run_analysis.py
```

Run tests:

```bash
pytest -v
```

## End-to-End Workflow

```text
Raw NYC Taxi CSV
        ↓
Spark Ingestion
        ↓
Schema Inference
        ↓
Data Quality Checks
        ↓
Data Cleaning
        ↓
Clean Parquet
        ↓
Year/Month Partitioning
        ↓
Partitioned Parquet
        ↓
Hive Table Registration
        ↓
Partition Recovery
        ↓
Spark SQL
        ↓
Monthly Vendor Aggregation
        ↓
Gold Parquet
        ↓
Gold Hive Table
        ↓
Analytical Queries
        ↓
Pytest Validation
```

## Key Learning Outcomes

This project demonstrates practical experience with:

- Apache Spark
- PySpark
- Spark DataFrames
- Spark SQL
- Hive catalog/metastore integration
- Hive table registration
- Parquet
- Data cleaning
- Data quality validation
- Data partitioning
- Partition recovery
- Partition pruning
- Aggregations
- Shuffle concepts
- Spark execution model
- Automated data pipeline testing with Pytest

## Project Status

**Status: COMPLETE — Current Scope**

Final statistics:

```text
Raw records:       22,699
Clean records:     22,502
Records removed:      197
Hive partitions:       12
Monthly groups:        24
Tests passed:            4
```

Final pipeline:

```text
CSV
 ↓
Apache Spark
 ↓
Cleaning & Transformation
 ↓
Parquet
 ↓
Partitioning
 ↓
Hive Metastore
 ↓
Spark SQL
 ↓
Aggregation
 ↓
Gold Parquet
 ↓
Gold Hive Table
 ↓
Analytics
 ↓
Automated Tests
```

This project is intentionally scoped as a local Big Data engineering project. Cloud deployment and performance optimization are outside the current scope.

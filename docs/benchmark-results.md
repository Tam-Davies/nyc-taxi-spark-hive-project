
# Delta Lake vs Parquet Performance Benchmark

## 1. Objective

This benchmark evaluates the storage overhead and read performance of a partitioned Parquet dataset against its Delta Lake equivalent.

Both datasets originate from the NYC Taxi 2017 dataset and contain 22,502 records.

The objective is to examine:
- Storage requirements.
- Full-table read performance.
- Filtered query performance.
- Partition pruning.
- The additional capabilities provided by Delta Lake.

## 2. Dataset Configuration

| Property | Value |
|---|---|
| Dataset | NYC Taxi 2017 |
| Total records | 22,502 |
| Partition column 1 | pickup_year |
| Partition column 2 | pickup_month |
| Number of monthly partitions | 12 |
| Spark version | 4.2.0 |
| Delta Lake version | 4.4.1 |
| Execution environment | Local WSL Ubuntu |

## 3. Storage Comparison

| Dataset | Storage Size |
|---|---:|
| Partitioned Parquet | 784 KB |
| Delta Lake | 924 KB |
| Delta transaction log | 76 KB |

Delta Lake occupies approximately 17.9% more storage than the original partitioned Parquet dataset.

This additional storage is associated with the Delta table's transaction and metadata management, alongside differences in the physical data files.

## 4. Full-Table Read Benchmark

Two benchmark runs were performed.

| Run | Parquet (seconds) | Delta (seconds) |
|---|---:|---:|
| 1 | 7.4951 | 16.3268 |
| 2 | 9.3680 | 21.2295 |

Both formats returned exactly 22,502 records.

## 5. Filtered Query Benchmark

The benchmark filtered records using:

```sql
pickup_year = 2017
AND pickup_month = 10
```

Results:

| Run | Parquet (seconds) | Delta (seconds) |
|---|---:|---:|
| 1 | 1.6900 | 4.3352 |
| 2 | 2.1276 | 5.3321 |

Both formats returned 2,007 October records.

## 6. Partition Pruning Analysis

Physical execution plans were inspected using Spark's:

```python
.explain("formatted")
```

Both Parquet and Delta execution plans contained the following partition filters:

```text
PartitionFilters:
[
    isnotnull(pickup_year),
    isnotnull(pickup_month),
    pickup_year = 2017,
    pickup_month = 10
]
```

This confirms that Spark applies partition pruning to both datasets.

The Parquet scan uses `InMemoryFileIndex`, while the Delta scan uses `PreparedDeltaFileIndex`.

Both ultimately scan Parquet data files.

## 7. Interpretation

The initial local benchmark measurements show that Parquet performed faster than Delta Lake for both full-table reads and filtered queries.

This is not evidence that Delta Lake is universally slower.

Delta Lake introduces transaction-log and metadata processing overhead. This overhead can be more noticeable with relatively small datasets.

However, Delta Lake provides additional capabilities that are not available through ordinary Parquet storage alone, including:

- ACID transactions.
- Transaction history.
- UPDATE operations.
- DELETE operations.
- MERGE operations.
- Time travel.

Both formats also successfully demonstrated partition pruning.

## 8. Benchmark Limitations

The benchmark was conducted using a local WSL environment and a relatively small dataset.

The measurements are initial observations rather than statistically conclusive performance results.

Potential influences include:

- Spark startup overhead.
- Local filesystem caching.
- Execution order.
- JVM and Spark runtime overhead.
- Dataset size.

Further benchmarking with repeated controlled runs and larger datasets would provide stronger performance evidence.

## 9. Conclusion

The experiment successfully converted an existing partitioned Parquet dataset into Delta Lake while preserving all 22,502 records and its 12 monthly partitions.

The benchmark demonstrated that ordinary Parquet reads were faster in this local experiment, while Delta Lake provided transactional capabilities, data modification operations, transaction history and time travel.

The results demonstrate the distinction between a conventional columnar data lake and a transactional lakehouse storage layer.

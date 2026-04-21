# BigQuery to S3 Distributed Export (Spark)

Enterprise-grade Spark job to export large-scale datasets from BigQuery to Amazon S3 using distributed processing.

## Designed for:

* Billion-scale tables
* Recovery execution mode
* Date-range filtering
* Partitioned output (~1M rows per logical partition)
* Full observability and structured logging
* No local disk dependency
* Direct distributed write to S3 (s3a)

## Architecture Overview

This job runs as a single Spark application that:

1. Receives runtime parameters
2. Dynamically builds the SQL query
3. Reads data from BigQuery in distributed mode
4. Applies partitioning strategy
5. Writes directly to S3 using distributed writers
6. Emits structured logs and execution metrics

## Execution Flow

flowchart TD
    A[Start Job] --> B[Parse Arguments]
    B --> C{Recovery Mode?}
    
    C -- Yes --> D[Build Query using updated_at >= recovery_date]
    C -- No --> E[Build Query using settlement_date BETWEEN start_date AND end_date]
    
    D --> F[Read BigQuery via Spark Connector]
    E --> F
    
    F --> G[Count Rows & Emit Metrics]
    G --> H[Apply Row-Based Partition Strategy]
    
    H --> I[Repartition by file_partition]
    I --> J[Write Parquet to S3 via s3a]
    
    J --> K[Log Completion]
    K --> L[Stop Spark Session]

## Data Model (Example Table)

BigQuery Table:

fintech_prod.analytics.transactions_v2

Representative schema:

* transaction_id (STRING)
* customer_id (STRING)
* merchant_id (STRING)
* transaction_timestamp (TIMESTAMP)
* settlement_date (DATE)
* amount (NUMERIC)
* currency (STRING)
* status (STRING)
* payment_method (STRING)
* risk_score (FLOAT)
* fraud_flag (BOOLEAN)
* device_metadata (STRUCT)
* installment_plan (STRUCT)
* updated_at (TIMESTAMP)

Recovery logic is based on:

updated_at

Normal execution is based on:

settlement_date

## Execution Modes

### Normal Mode

Used for standard batch exports.

Filter:

settlement_date BETWEEN start_date AND end_date

Example parameters:

--start_date=2026-01-01
--end_date=2026-01-31
--recovery=false

### Recovery Mode

Used when reprocessing data after failure or correction.

Filter:

updated_at >= recovery_date

Example parameters:

--recovery=true
--recovery_date=2026-01-15T00:00:00

This allows partial reprocessing without re-exporting the full historical dataset.

## Partitioning Strategy

The job applies a logical partitioning mechanism targeting approximately 1,000,000 rows per output group.

Steps:

1. Generate row_number ordered by transaction_timestamp
2. Divide row_number by 1,000,000
3. Create file_partition column
4. Repartition Spark DataFrame by file_partition
5. Write partitioned Parquet to S3

Output structure:

s3://bucket/prefix/file_partition=0/
s3://bucket/prefix/file_partition=1/
...

Note:
For ultra-large datasets (multi-billion rows), a hash-based partitioning strategy may be preferable to reduce shuffle pressure.

## Storage Format

Output format:

* Parquet
* Distributed write
* Compression (Snappy recommended in production)
* Direct write via s3a connector

No local intermediate files are created.

## Data flow:

BigQuery → Spark Executors → S3

## Logging and Observability

The job includes:

* Structured logging
* Execution mode logging
* Row count metrics
* Failure visibility
* Explicit Spark job naming

## Recommended production integrations:

* Cloud Monitoring
* Datadog
* Prometheus
* Centralized log aggregation

## Scalability Characteristics

Component	Behavior
BigQuery Read	Distributed
Transformations	Distributed
Write to S3	Distributed
Memory Usage	Executor-bound
Horizontal Scale	Supported

The job does not depend on local disk capacity.
Scaling depends on Spark cluster configuration.

## Deployment Options

This script can run on:

* Google Dataproc
* Amazon EMR
* Kubernetes Spark Operator
* Standalone Spark Cluster
* Dataflow (if Spark runner is configured)

## Example Execution

spark-submit \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2 \
  job.py \
  --start_date=2026-01-01 \
  --end_date=2026-01-31 \
  --recovery=false \
  --s3_bucket=my-bucket \
  --s3_prefix=exports/january

## Failure and Recovery Strategy

If the job fails:

1. Identify last successful export date
2. Re-run in recovery mode
3. Provide recovery_date aligned with last checkpoint

Example:

--recovery=true
--recovery_date=2026-01-20T15:00:00

This ensures controlled and deterministic reprocessing.

## Design Principles

* Deterministic query building
* Explicit execution modes
* Clear operational semantics
* No hidden state
* Cluster-first architecture
* Production observability
* Separation of configuration and logic

## Future Improvements

For enterprise-scale evolution:

* Add watermark tracking
* Add idempotent S3 versioned paths
* Add checksum validation
* Add partition pruning strategy
* Add dynamic partition sizing based on row count
* Add structured JSON logs
* Add failure alert hooks

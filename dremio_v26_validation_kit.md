
# Dremio v26 Validation Kit for Storage Vendors

## Overview

Use this guide to validate your S3-compatible storage with Dremio v26 using the TPC-DS 1TB SF1000 benchmark. Execute the full 99 TPC-DS queries on a clustered Iceberg dataset stored in your object store. Submit results for certification.

## Prerequisites

Prepare a Kubernetes environment with Dremio v26 (Software Edition).
Use 1 coordinator and 8 executors.
Ensure each executor has at least 32 vCPUs and 120 GB RAM.
Provision high-throughput local storage on each executor.
Configure fast network access to your S3-compatible storage.

## Set Up Dremio

Deploy Dremio v26 using Helm or your preferred method.
Expose the UI via port 9047 and the JDBC/ODBC endpoint via port 31010.
Access the UI and create an admin user.

## Connect S3-Compatible Storage

In Dremio, add your S3-compatible storage as a source.
Set access credentials and custom endpoint.
Enable compatibility mode.
Set the following connection properties:

fs.s3a.endpoint = <your-endpoint>
fs.s3a.path.style.access = true
fs.s3a.connection.maximum = 200

## Prepare TPC-DS Dataset

Obtain the 1TB TPC-DS dataset in Parquet format.
Use the following script to upload each Parquet file into your S3 bucket, placing each file in a subdirectory named after the table.

```python
import os
import boto3

s3 = boto3.client("s3")
bucket_name = "your-bucket-name"
source_dir = "/path/to/parquet/files"

for filename in os.listdir(source_dir):
    if filename.endswith(".parquet"):
        table_name = filename.replace(".parquet", "")
        local_path = os.path.join(source_dir, filename)
        s3_path = f"{table_name}/{filename}"
        s3.upload_file(local_path, bucket_name, s3_path)
        print(f"Uploaded {filename} to s3://{bucket_name}/{s3_path}")
```

## Ingest into Dremio with Clustering

In Dremio, execute a CREATE TABLE AS SELECT statement for each table.
Cluster each table by the following columns:

store_sales: ss_sold_date_sk
web_sales: ws_sold_date_sk
catalog_sales: cs_sold_date_sk
inventory: inv_date_sk
store_returns: sr_returned_date_sk
web_returns: wr_returned_date_sk
catalog_returns: cr_returned_date_sk

For example:

```sql
CREATE TABLE s3source.tpcds_clustered.store_sales
CLUSTER BY (ss_sold_date_sk) AS
SELECT * FROM s3source.parquet_stage.store_sales;
```

Repeat for each table.

## Optimize Tables

After table creation, run OPTIMIZE TABLE on each clustered table.

```sql
OPTIMIZE TABLE s3source.tpcds_clustered.store_sales;
OPTIMIZE TABLE s3source.tpcds_clustered.web_sales;
-- Repeat for other clustered tables
```

## Check Clustering Depth

Run the following query to check clustering depth.

```sql
SELECT * FROM TABLE(clustering_information('s3source.tpcds_clustered.store_sales'));
```

Repeat for each clustered table.

## Run Benchmark

Run all 99 TPC-DS queries sequentially in a single session.
Capture the total wall-clock time.
Run the benchmark on cold cache (first run).
Optionally run three more times and record average of second to fourth runs.

## Submit Results

Submit the following:

Cluster configuration (node type, count, CPU, RAM)
Storage type and configuration
Total execution time (cold run)
Average execution time (warm runs)
Clustering keys used
Clustering depth per table


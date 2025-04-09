
# Dremio v26 Storage Vendor Validation Kit

## Overview
Use this guide to validate S3-compatible storage with Dremio v26 using the TPC-DS 1TB SF1000 benchmark. Follow each step to stage data, configure the environment, ingest data with clustering, run benchmarks, and report results.

## Prerequisites
Prepare a Kubernetes environment for deploying Dremio v26.
Use one coordinator and eight executor nodes.
Ensure each executor has a minimum of 32 vCPUs and 120 GB of RAM.
Provision high-throughput local storage and fast networking to your S3-compatible object store.

## Setting Up Dremio
Deploy Dremio v26 using Helm or other supported methods.
Expose port 9047 for the UI and 31010 for JDBC/ODBC.
Access the UI and create an admin user.

## Setting Up the Polaris Catalog
Navigate to the Dremio UI.
Open the "Sources" tab and add a new source.
Select "Nessie" or "Polaris" as the source type.
Configure the Polaris catalog with a name and default branch.
Confirm connectivity and save the source.

## Connecting S3-Compatible Storage
Add your object storage as a source in Dremio.
Use access credentials and custom endpoint if needed.
Set the following properties:
fs.s3a.endpoint = <your-endpoint>
fs.s3a.path.style.access = true
fs.s3a.connection.maximum = 200

## Preparing the TPC-DS Dataset
Download the provided 1TB TPC-DS dataset in Parquet format.
Use the following script to upload each file into its own subdirectory, named after the table.

```python
import os
import boto3

s3 = boto3.client("s3")
bucket = "your-bucket-name"
local_dir = "/path/to/parquet/files"

for file in os.listdir(local_dir):
    if file.endswith(".parquet"):
        table = file.replace(".parquet", "")
        local_path = os.path.join(local_dir, file)
        s3_path = f"{table}/{file}"
        s3.upload_file(local_path, bucket, s3_path)
        print(f"Uploaded {file} to s3://{bucket}/{s3_path}")
```

## Ingesting Tables with Clustering
Ingest the TPC-DS tables into Iceberg format in Dremio using clustering only.

```sql
CREATE TABLE s3source.tpcds_clustered.store_sales CLUSTER BY (ss_sold_date_sk) AS SELECT * FROM s3source.raw.store_sales;
CREATE TABLE s3source.tpcds_clustered.web_sales CLUSTER BY (ws_sold_date_sk) AS SELECT * FROM s3source.raw.web_sales;
CREATE TABLE s3source.tpcds_clustered.catalog_sales CLUSTER BY (cs_sold_date_sk) AS SELECT * FROM s3source.raw.catalog_sales;
CREATE TABLE s3source.tpcds_clustered.inventory CLUSTER BY (inv_date_sk) AS SELECT * FROM s3source.raw.inventory;
```

Create the remaining tables without clustering.

```sql
CREATE TABLE s3source.tpcds_clustered.customer AS SELECT * FROM s3source.raw.customer;
-- Repeat for remaining small tables
```

## Optimizing Clustered Tables
Run OPTIMIZE TABLE to compact and sort files by the clustering key.

```sql
OPTIMIZE TABLE s3source.tpcds_clustered.store_sales;
OPTIMIZE TABLE s3source.tpcds_clustered.web_sales;
OPTIMIZE TABLE s3source.tpcds_clustered.catalog_sales;
OPTIMIZE TABLE s3source.tpcds_clustered.inventory;
```

## Checking Clustering Depth
Use the clustering_information table function to verify clustering metrics.

```sql
SELECT * FROM TABLE(clustering_information('s3source.tpcds_clustered.store_sales'));
```

Repeat for each clustered table.

## Running the TPC-DS Benchmark
Run the 99 TPC-DS queries against the clustered dataset.
Use a provided script or run queries sequentially.
Measure total wall-clock execution time.
Run at least four iterations and use the average of runs 2–4.
Record any failures or outliers.

## Submitting Results
Submit the following information:
Environment specifications including node types, count, CPU, RAM
Ingestion method used (clustering)
Clustering keys used per table
Clustering depth outputs
Average query execution time (warm runs)
Total query execution time
Notable observations or anomalies


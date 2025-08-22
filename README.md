# TPC-DS Benchmarking with Dremio v26

This repository supports benchmarking Dremio v26 using the TPC-DS benchmark at 1TB scale. It includes tooling to:

- Generate TPC-DS test data in `.dat` format (1 file per table)
- Convert that data to Parquet
- Create Apache Iceberg tables
- Execute benchmark queries and capture performance metrics

The framework is designed to be S3-compatible and flexible across environments for real-world performance testing.

---

## **Features**
1. **Data Generation**: Generate TPC-DS raw `.dat` files at a specified scale factor.
2. **Parquet Conversion and Upload**: Convert `.dat` files to `.parquet` format and upload them to an S3 bucket.
3. **Cleanup**: Remove `.dat` and `.parquet` files to free up disk space.

---

## **Workflow**
The workflow is managed through the `tpcds` script, which provides three main commands: `generate`, `upload`, and `cleanup`.

### **1. Data Generation**
Generate raw `.dat` files using the `generate` command.

#### **Usage**
```bash
python tpcds generate --scale <scale_factor>
```

#### **Arguments**
- `--scale`: The scale factor for data generation (e.g., `1` for 1GB, `10` for 10GB).

#### **Example**
```bash
python tpcds generate --scale 10
```
This will generate approximately 10GB of raw `.dat` files in the `tpcds-kit/test_data/raw_files` directory.

---

### **2. Parquet Conversion and Upload**
Convert `.dat` files to `.parquet` format and upload them to an S3 bucket using the `upload` command.

#### **Usage**
```bash
python tpcds upload [--test] [--spark]
```

#### **Arguments**
- `--test`: Run in test mode to simulate the upload process. Outputs the source and target paths without performing the actual upload.
- `--spark`: Use Spark-based Parquet transformation instead of the default method.

#### **Example**
1. **Test Mode**:
   ```bash
   python tpcds upload --test
   ```
   Output:
   ```
   TEST MODE: Listing source and target paths
   Source: tpcds-kit/test_data/parquet/customer.parquet Target: s3://tpcds/customer/customer.parquet
   Source: tpcds-kit/test_data/parquet/orders.parquet Target: s3://tpcds/orders/orders.parquet
   ```

2. **Full Execution (Default Method)**:
   ```bash
   python tpcds upload
   ```
   This will:
   - Convert `.dat` files in `tpcds-kit/test_data/raw_files` to `.parquet` files in `tpcds-kit/test_data/parquet`.
   - Upload the `.parquet` files to the S3 bucket specified in the `.env` file.

3. **Full Execution (Spark Method)**:
   ```bash
   python tpcds upload --spark
   ```
   This will use Spark to convert and upload the data.

---

### **3. Cleanup**
Remove all `.dat` and `.parquet` files using the `cleanup` command.

#### **Usage**
```bash
python tpcds cleanup
```

#### **Example**
```bash
python tpcds cleanup
```
Output:
```
WARNING: This will delete all .dat files in 'tpcds-kit/test_data/raw_files' and all .parquet files in 'tpcds-kit/test_data/parquet'.
Do you want to proceed? (y/n): y
Deleted all .dat files in 'tpcds-kit/test_data/raw_files'.
Deleted all .parquet files in 'tpcds-kit/test_data/parquet'.
```

---

### **Test Plans**
The `query_file_test.csv` file, which contains test queries, is now located at:
```
root/perf_testing/benchmark-kit/test_plans/query_file_test.csv
```

Benchmark results and debug logs will be stored in:
```
root/perf_testing/results/small_sample/
```

---

## **Iceberg Lakehouse Kit**

The `iceberg.py` script provides commands to deploy and clean up tables in Dremio.

### **1. Deploy Tables**
Deploy tables to Dremio using the `deploy` command.

#### **Usage**
```bash
python iceberg.py deploy
```

#### **Example**
```bash
python iceberg.py deploy
```
This will deploy tables to Dremio. A warning will be displayed, and you must confirm to proceed.

---

### **2. Cleanup Tables**
Drop tables in Dremio using the `cleanup` command.

#### **Usage**
```bash
python iceberg.py cleanup
```

#### **Example**
```bash
python iceberg.py cleanup
```
This will drop tables in Dremio. A warning will be displayed, and you must confirm to proceed.

---

## **Environment Configuration**
The toolkit uses environment variables for S3 configuration. These variables are stored in a `.env` file.

### **Example .env File**
```properties
# S3 Configuration
S3_BUCKET_NAME=tpcds
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY=admin
S3_SECRET_KEY=admin123
```
The `.env` file is located in the root of the repository.

---

## **Directory Structure**
The toolkit organizes files into the following directories:
```
tpcds-kit/
├── tpcds               # Main script to manage the workflow
├── data_generator.py     # Script for generating raw TPC-DS data
├── data_to_parquet.py    # Script for converting .dat files to .parquet
├── upload_parquet.py     # Script for uploading .parquet files to S3
├── iceberg.py            # Script for managing Iceberg tables in Dremio
├── test_data/
│   ├── raw_files/        # Directory for raw .dat files
│   └── parquet/          # Directory for .parquet files
├── .env                  # Environment variables for S3 configuration
└── .gitignore            # Git ignore rules
```

---

## **Best Practices**
1. **Test Before Uploading**:
   Use the `--test` flag with the `upload` command to verify source and target paths before performing the upload.

2. **Cleanup Regularly**:
   Run the `cleanup` command after completing the workflow to free up disk space.

3. **Environment Variables**:
   Ensure the `.env` file is correctly configured with your S3 credentials and endpoint.

---

## **Contributing**
Feel free to contribute to the TPC-DS Test Kit by submitting issues or pull requests. For major changes, please open an issue first to discuss what you would like to change.

---

## **License**
This project is licensed under the TPC Benchmark License. See the `LICENSE` file for details.

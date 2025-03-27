# TPC-DS Benchmarking with Dremio v26

This repository supports benchmarking Dremio v26 OSS using the TPC-DS benchmark at 1TB scale. It includes tooling to:

- Generate TPC-DS test data in `.dat` format (1 file per table)
- Convert that data to Parquet
- Create Apache Iceberg tables
- Execute benchmark queries and capture performance metrics

The framework is designed to be S3-compatible and flexible across environments for real-world performance testing.

---

## License Notice

This project includes the open-source `tpcds-kit` from Databricks.

- **Original source**: [https://github.com/databricks/tpcds-kit](https://github.com/databricks/tpcds-kit)
- **License**: Apache License 2.0

A local copy is included in this repository under the `tools/` directory.

---

## Prerequisites

Before proceeding, make sure the following dependencies are installed on your system:

- **Python 3.x**
- **GCC / C development tools**
- **Make**
- **Xcode Command Line Tools** (macOS): Install using:
  ```bash
  xcode-select --install
  ```
- **Homebrew** (macOS): [https://brew.sh](https://brew.sh)
- **Bison** (required to compile the query generator): Install using:
  ```bash
  brew install bison
  ```

These are required to compile the TPC-DS generator and process the generated data.

---

## Generating TPC-DS Data Files (1TB)

1. Navigate to the `tpcds-kit` directory:
   ```bash
   cd tools
   ```

2. Create a directory to hold the raw data files:
   ```bash
   mkdir test_data
   ```

3. Build the data generator:
   ```bash
   make OS=MACOS
   ```

4. Generate the data (Scale Factor 1000 = ~1TB total):
   ```bash
   ./dsdgen -SCALE 1000 -DIR test_data -FORCE
   ```

This will generate one `.dat` file per table in the `test_data` folder. Do not commit this folder to version control—it should be ignored using `.gitignore`.

---

## Next Steps

### Convert to Parquet

(Coming Soon)

### Create Iceberg Tables

(Coming Soon)

### Execute Queries and Benchmark

(Coming Soon)

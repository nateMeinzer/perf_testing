import os
import glob
import pandas as pd
import json
from termcolor import colored
import sys

# Load schema definitions
schema_path = os.path.join(os.path.dirname(__file__), "tpcds_schema.json")
with open(schema_path, 'r') as f:
    TABLE_SCHEMAS = json.load(f)

# Resolve paths relative to the script's location
script_dir = os.path.dirname(__file__)
input_dir = os.path.join(script_dir, "test_data/raw_files")
output_dir = os.path.join(script_dir, "test_data/parquet")

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Get list of all .dat files in the input directory
dat_files = glob.glob(os.path.join(input_dir, "*.dat"))

def handle_error(message):
    """Handle errors by printing in red and stopping the process."""
    print(colored(f"ERROR: {message}", "red"))
    print("Please use the cleanup tool before beginning again.")
    sys.exit(1)

if not dat_files:
    print("No .dat files found in the input directory.")
else:
    for file_path in dat_files:
        try:
            # Read the .dat file; TPC-DS files are pipe-delimited with no header
            df = pd.read_csv(file_path, sep="|", header=None, encoding="latin1", engine="python")

            # Sometimes the .dat files have a trailing empty column due to the delimiter at the end
            if df.columns[-1] == df.columns[-1] and df[df.columns[-1]].isnull().all():
                df = df.iloc[:, :-1]

            # Get table name from file name (remove .dat extension)
            table_name = os.path.splitext(os.path.basename(file_path))[0]

            # Apply schema if available
            if table_name in TABLE_SCHEMAS:
                expected_columns = TABLE_SCHEMAS[table_name]["columns"]
                if len(df.columns) == len(expected_columns):
                    df.columns = expected_columns
                    print(f"Applied schema for table {table_name}")
                else:
                    print(f"Warning: Column count mismatch for {table_name}. Expected {len(expected_columns)}, got {len(df.columns)}")
            else:
                print(f"Warning: No schema found for table {table_name}")

            # Write DataFrame to Parquet using pyarrow
            output_file = os.path.join(output_dir, f"{table_name}.parquet")
            df.to_parquet(output_file, engine="pyarrow", index=False)
            print(f"Successfully converted {file_path} to {output_file}")
        except Exception as e:
            handle_error(f"Failed to process {file_path}: {e}")
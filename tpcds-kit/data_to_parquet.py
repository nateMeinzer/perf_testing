import os
import glob
import pandas as pd

# Resolve paths relative to the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, "tpcds-kit/test_data/raw_data")
output_dir = os.path.join(script_dir, "tpcds-kit/test_data/parquet")

# Debug: Print full paths
print(f"Input directory: {input_dir}")
print(f"Output directory: {output_dir}")

# Ensure the input directory exists
if not os.path.exists(input_dir):
    raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Get list of all .dat files in the input directory
dat_files = glob.glob(os.path.join(input_dir, "*.dat"))

# Debug: Print discovered .dat files
print(f"Found {len(dat_files)} .dat files: {dat_files}")

for file_path in dat_files:
    # Debug: Print the file being processed
    print(f"Processing file: {file_path}")
    
    # Read the .dat file; TPC-DS files are pipe-delimited with no header
    df = pd.read_csv(file_path, sep="|", header=None, engine="python")
    
    # Sometimes the .dat files have a trailing empty column due to the delimiter at the end
    if df.columns[-1] == df.columns[-1] and df[df.columns[-1]].isnull().all():
        df = df.iloc[:, :-1]
    
    # Construct output file path (same base name, .parquet extension)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_file = os.path.join(output_dir, base_name + ".parquet")
    
    # Write DataFrame to Parquet using pyarrow
    df.to_parquet(output_file, engine="pyarrow", index=False)
    print(f"Converted {file_path} to {output_file}")
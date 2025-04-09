import glob
import pandas as pd
import os

# Resolve input and output directories relative to the script's location
script_dir = os.path.dirname(__file__)
input_dir = os.path.join(script_dir, "test_data/raw_files")
output_dir = os.path.join(script_dir, "test_data/parquet")

# Get list of all .dat files in the input directory
dat_files = glob.glob(input_dir + "/*.dat")

if not dat_files:
    print("No .dat files found in the input directory.")
else:
    for file_path in dat_files:
        try:
            # Read the .dat file; TPC-DS files are pipe-delimited with no header
            df = pd.read_csv(file_path, sep="|", header=None, engine="python")

            # Sometimes the .dat files have a trailing empty column due to the delimiter at the end
            if df.columns[-1] == df.columns[-1] and df[df.columns[-1]].isnull().all():
                df = df.iloc[:, :-1]

            # Construct output file path (same base name, .parquet extension)
            base_name = file_path.split("/")[-1].split(".")[0]
            output_file = os.path.join(output_dir, base_name + ".parquet")

            # Write DataFrame to Parquet using pyarrow
            df.to_parquet(output_file, engine="pyarrow", index=False)
            print(f"Successfully converted {file_path} to {output_file}")
        except Exception as e:
            print(f"Failed to process {file_path}: {e}")
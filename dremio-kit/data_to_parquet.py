import os
import glob
import pandas as pd

# Set directory paths
input_dir = "test_data"
output_dir = os.path.join(input_dir, "parquet")

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Get list of all .dat files in the test_data folder
dat_files = glob.glob(os.path.join(input_dir, "*.dat"))

for file_path in dat_files:
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
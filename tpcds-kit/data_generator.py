import os
import subprocess
import argparse

# Path to the dsdgen executable
DSDGEN_PATH = os.path.join(os.path.dirname(__file__), "tools", "dsdgen")  # Adjusted path to tools folder
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "test_data", "raw_files")  # Updated output directory

# Create the output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Compile the dsdgen tool
def compile_dsdgen():
    dsdgen_dir = os.path.join(os.path.dirname(__file__), "tools")
    print("Compiling the dsdgen tool...")
    try:
        subprocess.run(["make"], cwd=dsdgen_dir, check=True)
        print("Compilation complete.")
    except subprocess.CalledProcessError as e:
        print("Failed to compile dsdgen. Ensure you have 'make' and necessary dependencies installed.")
        exit(1)

# Generate data using dsdgen
def generate_data(scale_factor):
    print(f"Generating TPC-DS data with scale factor {scale_factor}...")
    tools_dir = os.path.join(os.path.dirname(__file__), "tools")
    command = [
        DSDGEN_PATH,
        "-SCALE", str(scale_factor),
        "-FORCE",
        "-DIR", OUTPUT_DIR  # Specify output directory
    ]
    process = subprocess.Popen(command, cwd=tools_dir, stdout=subprocess.PIPE, text=True)
    for line in process.stdout:
        print(line.strip())  # Print dsdgen output for visibility
    process.wait()
    if process.returncode != 0:
        print("Data generation failed.")
        exit(1)
    print(f"Data generation complete. Files are located in {OUTPUT_DIR}.")

# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate TPC-DS data.")
    parser.add_argument(
        "--scale-factor", 
        type=int, 
        required=True, 
        help="Scale factor for data generation (e.g., 1 for 1GB, 10 for 10GB)."
    )
    args = parser.parse_args()

    scale_factor = args.scale_factor

    compile_dsdgen()
    generate_data(scale_factor)
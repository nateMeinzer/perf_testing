import os
import subprocess
import boto3
import argparse

# S3 configuration
S3_BUCKET = os.getenv("S3_BUCKET", "default-bucket")  # Use environment variable or default
S3_PREFIX = "dat_files"  # Folder in the bucket to store .dat files
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")  # Use environment variable or default
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")  # Use environment variable
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")  # Use environment variable

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

# Path to the dsdgen executable
DSDGEN_PATH = os.path.join(os.path.dirname(__file__), "tools", "dsdgen")  # Adjusted path to tools folder

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

# Delete .dat files from the tools directory
def delete_dat_files():
    tools_dir = os.path.join(os.path.dirname(__file__), "tools")
    print("Deleting .dat files from the tools directory...")
    for file_name in os.listdir(tools_dir):
        if file_name.endswith(".dat"):
            file_path = os.path.join(tools_dir, file_name)
            os.remove(file_path)
            print(f"Deleted {file_name}")

# Generate data using dsdgen
def generate_data(scale_factor):
    print(f"Generating TPC-DS data with scale factor {scale_factor}...")
    tools_dir = os.path.join(os.path.dirname(__file__), "tools")
    command = [
        DSDGEN_PATH,
        "-SCALE", str(scale_factor),
        "-FORCE"
    ]
    process = subprocess.Popen(command, cwd=tools_dir, stdout=subprocess.PIPE, text=True)
    for line in process.stdout:
        if line.strip().endswith(".dat"):
            file_name = line.strip()
            file_path = os.path.join(tools_dir, file_name)
            s3_key = f"{S3_PREFIX}/{file_name}"
            print(f"Uploading {file_name} to s3://{S3_BUCKET}/{s3_key}...")
            s3_client.upload_file(file_path, S3_BUCKET, s3_key)
            print(f"Uploaded {file_name} successfully.")
    process.wait()
    if process.returncode != 0:
        print("Data generation failed.")
        exit(1)
    print("Data generation and upload complete.")
    delete_dat_files()

# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate TPC-DS data and upload to S3.")
    parser.add_argument(
        "--scale-factor", 
        type=int, 
        required=True, 
        help="Scale factor for data generation (e.g., 1 for 1GB, 10 for 10GB)."
    )
    args = parser.parse_args()

    scale_factor = args.scale_factor
    print(f"You are about to generate approximately {scale_factor}GB of data.")
    confirmation = input("Do you want to proceed? (y/n): ").strip().lower()
    if confirmation != "y":
        print("Operation cancelled.")
        exit(1)

    compile_dsdgen()
    generate_data(scale_factor)
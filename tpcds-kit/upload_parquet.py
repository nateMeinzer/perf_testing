import os
import argparse
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def upload_parquet_files(parquet_dir, test_mode):
    # Retrieve S3 credentials and configuration from environment variables
    s3_bucket = os.environ.get("S3_BUCKET_NAME")
    s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
    s3_access_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret_key = os.environ.get("S3_SECRET_KEY")

    if not s3_bucket or not s3_endpoint or not s3_access_key or not s3_secret_key:
        print("Environment variables S3_BUCKET_NAME, S3_ENDPOINT_URL, S3_ACCESS_KEY, and S3_SECRET_KEY must be set.")
        return

    # Create an S3 client using environment variables
    s3 = boto3.client(
        's3',
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )

    # Iterate through each file in the parquet directory
    for filename in os.listdir(parquet_dir):
        if filename.endswith(".parquet"):
            file_path = os.path.join(parquet_dir, filename)
            # Use the base name (without extension) as the table name
            table_name = os.path.splitext(filename)[0]
            # Define the S3 key as "table_name/filename.parquet"
            key = f"{table_name}/{filename}"

            if test_mode:
                print(f"TEST MODE: Source: {file_path}, Target: s3://{s3_bucket}/{key}")
            else:
                print(f"Uploading {file_path} to s3://{s3_bucket}/{key}")
                try:
                    s3.upload_file(file_path, s3_bucket, key)
                    print("Upload successful")
                except NoCredentialsError:
                    print("Credentials not available. Please ensure S3_ACCESS_KEY and S3_SECRET_KEY are set.")
                    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Parquet files to S3-compatible storage")
    parser.add_argument("--directory", default="test_data/parquet", help="Local directory containing Parquet files (default: test_data/parquet)")
    parser.add_argument("--test", action="store_true", help="Run in test mode to output source and target paths without uploading")

    args = parser.parse_args()
    upload_parquet_files(args.directory, args.test)
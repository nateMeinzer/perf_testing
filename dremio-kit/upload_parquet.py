import os
import argparse
import boto3
from botocore.exceptions import NoCredentialsError

def upload_parquet_files(endpoint, bucket, parquet_dir):
    # Retrieve AWS credentials from environment variables
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")  # Optional
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    
    # Create an S3 client using environment credentials
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region
    )
    
    # Iterate through each file in the parquet directory
    for filename in os.listdir(parquet_dir):
        if filename.endswith(".parquet"):
            file_path = os.path.join(parquet_dir, filename)
            # Use the base name (without extension) as the subdirectory name
            base_name = os.path.splitext(filename)[0]
            # Define the S3 key as "base_name/filename"
            key = f"{base_name}/{filename}"
            print(f"Uploading {file_path} to s3://{bucket}/{key}")
            try:
                s3.upload_file(file_path, bucket, key)
                print("Upload successful")
            except NoCredentialsError:
                print("Credentials not available. Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
                return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Parquet files to S3-compatible storage")
    parser.add_argument("--endpoint", required=True, help="S3 endpoint URL (e.g., https://s3.amazonaws.com or a custom endpoint)")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--directory", default="test_data/parquet", help="Local directory containing Parquet files (default: test_data/parquet)")
    
    args = parser.parse_args()
    upload_parquet_files(args.endpoint, args.bucket, args.directory)
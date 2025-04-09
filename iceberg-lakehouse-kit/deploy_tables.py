import os
import requests
from dotenv import load_dotenv
import boto3

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials and configuration from environment variables
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DREMIO_URL = os.getenv("DREMIO_URL")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

if not DREMIO_USERNAME or not DREMIO_PASSWORD or not DREMIO_URL:
    raise ValueError("DREMIO_USERNAME, DREMIO_PASSWORD, and DREMIO_URL must be set in the environment variables.")

if not S3_BUCKET_NAME or not S3_ENDPOINT_URL or not S3_ACCESS_KEY or not S3_SECRET_KEY:
    raise ValueError("S3_BUCKET_NAME, S3_ENDPOINT_URL, S3_ACCESS_KEY, and S3_SECRET_KEY must be set in the environment variables.")

# Initialize S3 client
s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# Define the schema for each table
TABLE_SCHEMAS = {
    "customer": ["c_customer_sk", "c_customer_id", "c_first_name", "c_last_name", "c_birth_country"],
    "orders": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"],
    # Add schemas for other tables as needed
}

# Fetch table names from S3 bucket
def fetch_table_names(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        table_names = [prefix['Prefix'].strip('/') for prefix in response.get('CommonPrefixes', [])]
        print("Fetched table names from S3:")
        for table_name in table_names:
            print(f"- {table_name}")
            if table_name in TABLE_SCHEMAS:
                print(f"  Schema: {TABLE_SCHEMAS[table_name]}")
            else:
                print("  Schema: Not defined")
        return table_names
    except Exception as e:
        print(f"Error fetching table names: {e}")
        return []

if __name__ == "__main__":
    # Fetch and print table names from S3
    table_names = fetch_table_names(S3_BUCKET_NAME)

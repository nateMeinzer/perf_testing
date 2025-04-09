import os
import json
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

def get_auth_header():
    """Get authentication header for Dremio API calls"""
    auth_payload = {
        "userName": DREMIO_USERNAME,
        "password": DREMIO_PASSWORD
    }
    try:
        response = requests.post(f"{DREMIO_URL}/apiv2/login", json=auth_payload)
        response.raise_for_status()
        token = response.json()["token"]
        return {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}
    except requests.exceptions.RequestException as e:
        print(f"Authentication failed: {e}")
        return None

def create_storage_source():
    """Create object storage source in Dremio"""
    headers = get_auth_header()
    if not headers:
        return False
    
    payload = {
        "entityType": "source",
        "name": "storage",
        "type": "S3",
        "config": {
            "accessKey": S3_ACCESS_KEY,
            "accessSecret": S3_SECRET_KEY,
            "secure": False,
            "rootPath": f"/{S3_BUCKET_NAME}",
            "whitelistedBuckets": [S3_BUCKET_NAME],
            "defaultCtasFormat": "PARQUET",
            "enableAsync": True,
            "compatibilityMode": True,
            "isCachingEnabled": True,
            "maxCacheSpacePct": 100,
            "requesterPays": False,
            "enableFileStatusCheck": True,
            "credentialType": "ACCESS_KEY",
            "propertyList": [
                {
                    "name": "fs.s3a.endpoint",
                    "value": S3_ENDPOINT_URL
                },
                {
                    "name": "fs.s3a.path.style.access",
                    "value": True
                }
            ]
        }
    }

    try:
        response = requests.post(
            f"{DREMIO_URL}/api/v3/catalog/source",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        print("Successfully created storage source")
        return True
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response.status_code == 409:
            print("Storage source already exists")
            return True
        print(f"Failed to create storage source: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return False

def create_parquet_dataset(table_name):
    """Create a Parquet dataset in Dremio for a specific table"""
    headers = get_auth_header()
    if not headers:
        return False
    
    payload = {
        "entityType": "dataset",
        "type": "PHYSICAL_DATASET",
        "path": [table_name],
        "format": {
            "type": "Parquet"
        },
        "physicalDataset": {
            "type": "DATASET_CONFIG",
            "path": f"/{table_name}/*.parquet",
            "origin": {
                "type": "OBJECT_STORE"
            }
        }
    }

    try:
        response = requests.post(
            f"{DREMIO_URL}/api/v3/catalog/storage/{table_name}",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        print(f"Successfully created Parquet dataset for table: {table_name}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to create dataset for {table_name}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return False

def fetch_table_names(bucket_name):
    """Fetch table names from S3/MinIO"""
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        return [prefix['Prefix'].strip('/') for prefix in response.get('CommonPrefixes', [])]
    except Exception as e:
        print(f"Error fetching table names: {e}")
        return []

def main():
    # Create storage source first
    if not create_storage_source():
        print("Failed to create or verify storage source")
        return

    # Fetch table names from storage
    table_names = fetch_table_names(S3_BUCKET_NAME)
    if not table_names:
        print("No tables found in storage")
        return

    # Create Parquet datasets for each table
    for table_name in table_names:
        print(f"Processing table: {table_name}")
        create_parquet_dataset(table_name)

if __name__ == "__main__":
    main()

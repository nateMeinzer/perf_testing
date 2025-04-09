import os
import json
import requests
from dotenv import load_dotenv
import boto3
import urllib.parse

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials and configuration from environment variables
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DREMIO_URL = os.getenv("DREMIO_URL")
DREMIO_SOURCE_NAME = os.getenv("DREMIO_SOURCE_NAME")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

ICEBERG_BUCKET_NAME = os.getenv("ICEBERG_BUCKET_NAME")
S3_OBJECT_STORE = os.getenv("S3_OBJECT_STORE")

if not DREMIO_USERNAME or not DREMIO_PASSWORD or not DREMIO_URL or not DREMIO_SOURCE_NAME:
    raise ValueError("DREMIO_USERNAME, DREMIO_PASSWORD, DREMIO_URL, and DREMIO_SOURCE_NAME must be set in the environment variables.")

if not S3_BUCKET_NAME or not S3_ENDPOINT_URL or not S3_ACCESS_KEY or not S3_SECRET_KEY:
    raise ValueError("S3_BUCKET_NAME, S3_ENDPOINT_URL, S3_ACCESS_KEY, and S3_SECRET_KEY must be set in the environment variables.")

if not ICEBERG_BUCKET_NAME or not S3_OBJECT_STORE:
    raise ValueError("ICEBERG_BUCKET_NAME and S3_OBJECT_STORE must be set in the environment variables.")

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

def execute_query(query):
    """Execute a SQL query in Dremio"""
    headers = get_auth_header()
    if not headers:
        return False

    payload = {
        "sql": query
    }

    try:
        print(f"Executing SQL query: {query}")  # Debug log
        response = requests.post(
            f"{DREMIO_URL}/api/v3/sql",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        print("Query executed successfully")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to execute query: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return False

def get_folder_contents(folder_path):
    """Retrieve folder contents using the Catalog API"""
    headers = get_auth_header()
    if not headers:
        return None

    # Ensure the path is properly encoded without escapes
    encoded_path = "/".join(urllib.parse.quote(part) for part in folder_path.split("/"))

    try:
        response = requests.get(
            f"{DREMIO_URL}/api/v3/catalog/by-path/{encoded_path}",
            headers=headers
        )
        response.raise_for_status()
        return response.json().get("children", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve folder contents for {folder_path}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return []

def format_file_as_table(file_path):
    """Format a file as a table in Dremio using path-based API"""
    headers = get_auth_header()
    if not headers:
        return False

    table_name = file_path[-2]  # Use the folder name as the table name
    path_parts = [DREMIO_SOURCE_NAME, S3_BUCKET_NAME, table_name]
    encoded_path = "/".join(urllib.parse.quote(part) for part in path_parts)

    try:
        # First, check if the dataset already exists
        response = requests.get(
            f"{DREMIO_URL}/api/v3/catalog/by-path/{encoded_path}",
            headers=headers
        )
        
        if response.status_code == 200:
            print(f"Table already exists: {table_name}")
            return True

        payload = {
            "entityType": "dataset",
            "path": path_parts,
            "type": "PHYSICAL_DATASET",
            "format": {
                "type": "Parquet"
            }
        }

        response = requests.put(
            f"{DREMIO_URL}/api/v3/catalog/by-path/{encoded_path}",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        print(f"Successfully formatted file as table: {table_name}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to format file as table: {table_name}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return False

def create_iceberg_table(file_path):
    print(f"Starting to get the table paths for iceberg table creation.")
    """Create an Iceberg table for a given file"""
    table_name = file_path[-2]  # Use the folder name as the table name

    query = f"""
    CREATE TABLE {ICEBERG_BUCKET_NAME}.{table_name} AS 
    SELECT * FROM {S3_OBJECT_STORE}.{S3_BUCKET_NAME}.{table_name};
    """

    print(f"Executing query to create Iceberg table: {table_name}")  # Log before execution
    success = execute_query(query)
    if success:
        print(f"Successfully created Iceberg table: {table_name}")  # Log success
    else:
        print(f"Failed to create Iceberg table: {table_name}")  # Log failure

def process_folder(folder_path):
    """Recursively process a folder and its contents"""
    print(f"\nProcessing folder: {folder_path}")
    children = get_folder_contents(folder_path)

    if not children:
        print(f"No files or subdirectories found in folder: {folder_path}")
        return

    print(f"Found {len(children)} children in folder {folder_path}")
    for child in children:
        print(f"Child type: {child['type']}, Path: {child['path']}")
        if child["type"] == "DATASET":
            print(f"Processing file: {child['path'][-1]}")
            format_file_as_table(child["path"])
            create_iceberg_table(child["path"])
        elif child["type"] == "CONTAINER":
            print(f"Entering folder: {child['path'][-1]}")
            process_folder("/".join(child["path"]))

def main():
    # Start processing from the root folder
    root_folder_path = f"{DREMIO_SOURCE_NAME}/{S3_BUCKET_NAME}"
    process_folder(root_folder_path)

if __name__ == "__main__":
    main()

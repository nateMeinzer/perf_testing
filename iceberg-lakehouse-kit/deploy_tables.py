import os
import json
import requests
from dotenv import load_dotenv
import boto3
import urllib.parse
import time  # Ensure 'time' is imported for sleep functionality

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

# Load table configuration
with open("tables.json", "r") as f:
    TABLES = json.load(f)

formatted_parquet_files = []
iceberg_tables_created = []

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
    """Execute a SQL query in Dremio and wait for results"""
    headers = get_auth_header()
    if not headers:
        return False

    # Step 1: Submit the query
    submit_payload = {
        "sql": query
    }

    try:
        print(f"Submitting SQL query: {query}")
        response = requests.post(
            f"{DREMIO_URL}/api/v3/sql",
            headers=headers,
            json=submit_payload
        )
        response.raise_for_status()
        job_id = response.json().get('id')
        
        if not job_id:
            print("No job ID returned from query submission")
            return False

        # Step 2: Poll for query status
        while True:
            status_response = requests.get(
                f"{DREMIO_URL}/api/v3/job/{job_id}",
                headers=headers
            )
            status_response.raise_for_status()
            status = status_response.json()
            
            if status.get('jobState') == 'COMPLETED':
                print("Query completed successfully")
                return True
            elif status.get('jobState') in ['FAILED', 'CANCELED', 'INVALID']:
                print(f"Query failed with state: {status.get('jobState')}")
                print(f"Error: {status.get('errorMessage')}")
                return False
            
            time.sleep(1)  # Wait before polling again

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
        formatted_parquet_files.append(table_name)  # Track formatted files
        print(f"Successfully formatted file as table: {table_name}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to format file as table: {table_name}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return False

def create_partitioned_iceberg_table(file_path, partition_key):
    """Create a partitioned Iceberg table for a given file"""
    print(f"Starting to create a partitioned Iceberg table.")

    # Extract the table name by removing the .parquet extension from the last item
    file_name = file_path[-1]  # e.g., "store_sales.parquet"
    table_name = file_name.replace(".parquet", "")  # e.g., "store_sales"

    query = f"""
    CREATE TABLE {ICEBERG_BUCKET_NAME}.{table_name} PARTITIONED BY ({partition_key}) AS 
    SELECT * FROM {S3_OBJECT_STORE}.{S3_BUCKET_NAME}.{table_name}."{file_name}";
    """

    print(f"Executing query to create partitioned Iceberg table: {table_name}")  # Log before execution
    success = execute_query(query)
    if success:
        print(f"Successfully created partitioned Iceberg table: {table_name}")  # Log success
    else:
        print(f"Failed to create partitioned Iceberg table: {table_name}")  # Log failure

def create_iceberg_table(file_path):
    """Create an Iceberg table for a given file"""
    file_name = file_path[-1]
    if not file_name.endswith(".parquet"):
        file_name = f"{file_name}.parquet"

    table_name = file_name.replace(".parquet", "")

    partition_clause = ""
    if table_name in TABLES["partitioned_tables"]:
        partition_key = TABLES["partitioned_tables"][table_name]
        partition_clause = f"PARTITION BY ({partition_key})"

    query = f"""
    CREATE TABLE IF NOT EXISTS {ICEBERG_BUCKET_NAME}.{table_name}
    {partition_clause}
    AS SELECT * FROM {S3_OBJECT_STORE}.{S3_BUCKET_NAME}.{table_name}."{file_name}";
    """

    print(f"Executing query to create Iceberg table: {table_name}")
    success = execute_query(query)
    if success:
        iceberg_tables_created.append(table_name)  # Track created tables
        print(f"Successfully created Iceberg table: {table_name}")
    else:
        print(f"Failed to create Iceberg table: {table_name}")

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

    print("\nSummary of Operations:")
    print(f"Parquet files Formatted: {formatted_parquet_files}")
    print(f"Total Count: {len(formatted_parquet_files)}")
    print(f"Iceberg tables Created: {iceberg_tables_created}")
    print(f"Total Count: {len(iceberg_tables_created)}")

if __name__ == "__main__":
    main()

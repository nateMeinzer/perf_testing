import os
import json
import requests
from dotenv import load_dotenv
import boto3
import urllib.parse
import time  # Ensure 'time' is imported for sleep functionality
import argparse  # Add argparse for command-line arguments

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

ICEBERG_BUCKET_NAME = os.getenv("ICEBERG_BUCKET_NAME")
ICEBERG_FOLDER_NAME = os.getenv("ICEBERG_FOLDER_NAME")
ICEBERG_SUBFOLDER = os.getenv("ICEBERG_SUBFOLDER")  # Default to empty if not set
S3_OBJECT_STORE = os.getenv("S3_BUCKET_NAME")
S3_FOLDER_NAME = os.getenv("S3_FOLDER_NAME")


if not DREMIO_USERNAME or not DREMIO_PASSWORD or not DREMIO_URL or not ICEBERG_BUCKET_NAME:
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

def process_table(table_name, partition_column=None, localsort_column=None):
    """Query the object and create the Iceberg table."""
    # Step 1: Query the object with LIMIT 1
    query = f"""
    SELECT * FROM tpcds.tpcds.tpcds.sample."{table_name}" LIMIT 1
    """
    print(f"Querying object for table: {table_name}")
    execute_query(query)  # Execute the query regardless of the outcome

    # Step 2: Create the Iceberg table

    
    if partition_column:
        localsort_clause = f" LOCALSORT BY ({localsort_column})"
        create_query = f"""
        CREATE TABLE "{ICEBERG_BUCKET_NAME}"."{ICEBERG_FOLDER_NAME}"."{ICEBERG_SUBFOLDER}"."{table_name}" 
        PARTITION BY ({partition_column}){localsort_clause} AS 
        SELECT * FROM tpcds.tpcds.tpcds.sample."{table_name}";
        """
    else:
        create_query = f"""
        CREATE TABLE  "{ICEBERG_BUCKET_NAME}"."{ICEBERG_FOLDER_NAME}"."{ICEBERG_SUBFOLDER}"."{table_name}" AS 
        SELECT * FROM tpcds.tpcds.tpcds.sample."{table_name}";
        """
    print(f"Creating Iceberg table for: {table_name}")
    execute_query(create_query)

def main():
    parser = argparse.ArgumentParser(description="Deploy Iceberg tables from S3 objects")
    parser.add_argument("--table", help="Specific table name to process (without fetching the list)")
    args = parser.parse_args()

    if args.table:
        # Process only the specified table
        table_name = args.table
        print(f"Processing single table: {table_name}")
        # Assuming partition_column and localsort_column are provided for single table processing
        process_table(table_name, partition_column="partition_column", localsort_column="localsort_column")
    else:
        # Load tables.json
        tables_file = "tables.json"
        if not os.path.exists(tables_file):
            print(f"Error: {tables_file} not found. Ensure the file exists in the current directory: {os.getcwd()}")
            return

        try:
            with open(tables_file, "r") as f:
                tables = json.load(f)
                # Debug: Print the contents of tables.json
                print("Contents of tables.json:")
                print(json.dumps(tables, indent=4))
        except json.JSONDecodeError as e:
            print(f"Error: Failed to parse {tables_file}. Ensure it contains valid JSON. {e}")
            return

        # Process partitioned tables
        for table_name, config in tables["partitioned_tables"].items():
            # Debug: Print table_name and config to verify structure
            print(f"Processing partitioned table: {table_name}")
            print(f"Config for {table_name}: {config}")
            
            # Ensure config contains the expected keys
            if "partition_by" not in config or "localsort_by" not in config:
                print(f"Error: Missing 'partition_by' or 'localsort_by' in config for table {table_name}")
                continue
            
            partition_column = config["partition_by"]  # Use "partition_by" instead of "partition_column"
            localsort_column = config["localsort_by"]  # Use "localsort_by" instead of "localsort_column"
            process_table(table_name, partition_column, localsort_column)

        # Process non-partitioned tables
        for table_name in tables["non_partitioned_tables"]:  # Iterate directly over the list
            process_table(table_name)

if __name__ == "__main__":
    main()

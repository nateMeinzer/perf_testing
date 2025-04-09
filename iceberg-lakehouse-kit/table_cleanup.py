import os
import json
import requests
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials and configuration from environment variables
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DREMIO_URL = os.getenv("DREMIO_URL")
ICEBERG_BUCKET_NAME = os.getenv("ICEBERG_BUCKET_NAME")

if not DREMIO_USERNAME or not DREMIO_PASSWORD or not DREMIO_URL or not ICEBERG_BUCKET_NAME:
    raise ValueError("DREMIO_USERNAME, DREMIO_PASSWORD, DREMIO_URL, and ICEBERG_BUCKET_NAME must be set in the environment variables.")

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

            time.sleep(1)

    except requests.exceptions.RequestException as e:
        print(f"Failed to execute query: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return False

def cleanup_tables():
    """Loop through table names and delete them"""
    with open("tables.json", "r") as f:  # Updated to use tables.json
        tables = json.load(f)

    # Fetch all table names from both partitioned and non-partitioned sections
    table_names = list(tables["partitioned_tables"].keys()) + tables["non_partitioned_tables"]

    for table_name in table_names:
        query = f"DROP TABLE {ICEBERG_BUCKET_NAME}.{table_name};"
        print(f"Executing cleanup for table: {table_name}")
        success = execute_query(query)
        if success:
            print(f"Successfully dropped table: {table_name}")
        else:
            print(f"Failed to drop table: {table_name}")

if __name__ == "__main__":
    cleanup_tables()
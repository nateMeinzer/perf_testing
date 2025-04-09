import os
import json
import requests
from dotenv import load_dotenv
import urllib.parse

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials and configuration from environment variables
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DREMIO_URL = os.getenv("DREMIO_URL")
DREMIO_SOURCE_NAME = os.getenv("DREMIO_SOURCE_NAME")

if not DREMIO_USERNAME or not DREMIO_PASSWORD or not DREMIO_URL or not DREMIO_SOURCE_NAME:
    raise ValueError("DREMIO_USERNAME, DREMIO_PASSWORD, DREMIO_URL, and DREMIO_SOURCE_NAME must be set in the environment variables.")

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

def fetch_file_paths(tables):
    """Fetch file paths for each table in the tables list"""
    all_file_paths = {}
    for table_name in tables:
        print(f"Processing table: {table_name}")
        folder_path = f"{DREMIO_SOURCE_NAME}/Samples/tpcds_sf1000/{table_name}"
        children = get_folder_contents(folder_path)

        if not children:
            print(f"No files or subdirectories found for table: {table_name}")
            continue

        table_files = []
        for child in children:
            if child["type"] == "CONTAINER" and child["path"][-1] != "metadata":
                uuid_folder = "/".join(child["path"])
                print(f"Found UUID folder: {uuid_folder}")
                uuid_children = get_folder_contents(uuid_folder)
                for uuid_child in uuid_children:
                    if uuid_child["type"] == "FILE" and uuid_child["path"][-1].endswith(".parquet"):
                        table_files.append("/".join(uuid_child["path"]))
        all_file_paths[table_name] = table_files
    return all_file_paths

def main():
    # Load table names from tables.json
    tables_file = os.path.join(os.path.dirname(__file__), "tables.json")
    with open(tables_file, 'r') as f:
        tables = json.load(f)

    # Fetch file paths for each table
    file_paths = fetch_file_paths(tables)
    print("\nFetched file paths:")
    for table, paths in file_paths.items():
        print(f"Table: {table}")
        for path in paths:
            print(f"  - {path}")

if __name__ == "__main__":
    main()

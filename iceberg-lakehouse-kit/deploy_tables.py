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

if not DREMIO_USERNAME or not DREMIO_PASSWORD or not DREMIO_URL or not DREMIO_SOURCE_NAME:
    raise ValueError("DREMIO_USERNAME, DREMIO_PASSWORD, DREMIO_URL, and DREMIO_SOURCE_NAME must be set in the environment variables.")

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

def get_folder_contents(folder_path):
    """Retrieve folder contents using the Catalog API"""
    headers = get_auth_header()
    if not headers:
        return None

    try:
        response = requests.get(
            f"{DREMIO_URL}/api/v3/catalog/by-path/{folder_path}",
            headers=headers
        )
        response.raise_for_status()
        return response.json().get("children", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve folder contents for {folder_path}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return []

def format_file_as_table(file_id, file_path):
    """Format a file as a table in Dremio"""
    headers = get_auth_header()
    if not headers:
        return False

    # URL-encode the file ID
    encoded_file_id = urllib.parse.quote(file_id, safe='')

    payload = {
        "entityType": "dataset",
        "path": file_path,
        "type": "PHYSICAL_DATASET",
        "format": {
            "type": "Parquet"
        }
    }

    try:
        response = requests.post(
            f"{DREMIO_URL}/api/v3/catalog/{encoded_file_id}",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        print(f"Successfully formatted file as table: {file_path[-1]}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to format file as table: {file_path[-1]}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return False

def process_folder(folder_path):
    """Recursively process a folder and its contents"""
    children = get_folder_contents(folder_path)

    if not children:
        print(f"No files or subdirectories found in folder: {folder_path}")
        return

    for child in children:
        if child["type"] == "FILE":
            print(f"Processing file: {child['path'][-1]}")
            format_file_as_table(child["id"], child["path"])
        elif child["type"] == "CONTAINER":
            print(f"Entering folder: {child['path'][-1]}")
            process_folder("/".join(child["path"]))

def main():
    # Start processing from the root folder
    root_folder_path = f"{DREMIO_SOURCE_NAME}/{S3_BUCKET_NAME}"
    process_folder(root_folder_path)

if __name__ == "__main__":
    main()

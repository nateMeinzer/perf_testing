import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DREMIO_URL = "http://localhost:9047"

if not DREMIO_USERNAME or not DREMIO_PASSWORD:
    raise ValueError("DREMIO_USERNAME and DREMIO_PASSWORD must be set in the environment variables.")

# Authenticate and retrieve token
def authenticate():
    try:
        response = requests.post(
            f"{DREMIO_URL}/apiv2/login",
            json={"userName": DREMIO_USERNAME, "password": DREMIO_PASSWORD},
        )
        response.raise_for_status()
        return response.json().get("token")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Authentication failed: {e}")

# Create a space in Dremio
def create_space(token, space_name):
    headers = {"Authorization": f"_dremio{token}"}
    payload = {
        "entityType": "space",
        "name": space_name,
    }
    try:
        print("Sending request to create space...")
        print("Headers:", headers)
        print("Payload:", payload)
        response = requests.post(
            f"{DREMIO_URL}/api/v3/catalog", json=payload, headers=headers
        )
        print("Response status code:", response.status_code)
        print("Response text:", response.text)
        response.raise_for_status()
        print(f"Space '{space_name}' created successfully.")
        print("Response JSON:", response.json())
    except requests.exceptions.RequestException as e:
        if e.response is not None and e.response.status_code == 409:
            print(f"Space '{space_name}' already exists. Continuing...")
        else:
            print(f"Failed to create space: {e}")
            if e.response is not None:
                print("Error Response:", e.response.text)
            raise

# Create a folder in a space in Dremio
def create_folder(token, space_name, folder_name):
    headers = {"Authorization": f"_dremio{token}"}
    payload = {
        "entityType": "folder",
        "name": folder_name,
        "path": [space_name, folder_name],
    }
    try:
        print("Sending request to create folder...")
        print("Headers:", headers)
        print("Payload:", payload)
        response = requests.post(
            f"{DREMIO_URL}/api/v3/catalog", json=payload, headers=headers
        )
        print("Response status code:", response.status_code)
        print("Response text:", response.text)
        response.raise_for_status()
        print(f"Folder '{folder_name}' created successfully in space '{space_name}'.")
        print("Response JSON:", response.json())
    except requests.exceptions.RequestException as e:
        if e.response is not None and e.response.status_code == 409:
            print(f"Folder '{folder_name}' already exists in space '{space_name}'. Continuing...")
        else:
            print(f"Failed to create folder: {e}")
            if e.response is not None:
                print("Error Response:", e.response.text)
            raise

# List folders in a data source
def list_folders(token, path):
    headers = {"Authorization": f"_dremio{token}"}
    try:
        print(f"Listing folders at path: {path}")
        response = requests.get(
            f"{DREMIO_URL}/api/v3/catalog/by-path/{'/'.join(path)}", headers=headers
        )
        print("Response status code:", response.status_code)
        print("Response text:", response.text)
        response.raise_for_status()
        # Parse the response into a JSON dictionary
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to list folders: {e}")
        if e.response is not None:
            print("Error Response:", e.response.text)
        return {}

# Create a table in Dremio
def create_table(token, source_path, target_space, table_name):
    headers = {"Authorization": f"_dremio{token}"}
    # Adjusted query to include the folder "dataset" in the target space
    sql = f"CREATE TABLE {target_space}.dataset.{table_name} AS SELECT * FROM {'.'.join(source_path)}"
    payload = {"sql": sql}
    try:
        print(f"Executing SQL to create table '{table_name}' in 'tpcds.dataset': {sql}")
        response = requests.post(
            f"{DREMIO_URL}/api/v3/sql", json=payload, headers=headers
        )
        print("Response status code:", response.status_code)
        print("Response text:", response.text)
        response.raise_for_status()
        print(f"Table '{table_name}' created successfully in 'tpcds.dataset'.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to create table '{table_name}': {e}")
        if e.response is not None:
            print("Error Response:", e.response.text)

if __name__ == "__main__":
    # Test connection
    token = authenticate()
    print("Authentication successful.")

    # Create a space named "tpcds"
    space_name = "tpcds"
    create_space(token, space_name)

    # Create a folder named "dataset" in the "tpcds" space
    folder_name = "dataset"
    create_folder(token, space_name, folder_name)

    # Define source and target paths
    source_path = ["Samples", "samples.dremio.com", "tpcds_sf1000"]
    target_space = "tpcds"

    # List folders in the source path
    payload = list_folders(token, source_path)

    if not payload:
        print("No folders found in the source path.")
    else:
        # Ensure the payload ID matches the required ID
        if payload.get("id") == "dremio:/Samples/samples.dremio.com/tpcds_sf1000":
            print(f"Processing children for ID: {payload['id']}")

            # Iterate through the children
            children = payload.get("children", [])
            for child in children:
                # Concatenate the path to form the table name
                path_list = child.get("path", [])
                source_table_name = ".".join(path_list)

                # Use the last part of the path as the table name in the target space
                table_name = path_list[-1] if path_list else None

                if table_name:
                    print(f"Creating table '{table_name}' from source '{source_table_name}'")
                    create_table(token, path_list, target_space, table_name)
                else:
                    print("Skipping child with no valid path.")
        else:
            print(f"Payload ID does not match the required ID: {payload.get('id')}")

import os
import requests
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DREMIO_URL = os.getenv("DREMIO_URL")

if not DREMIO_USERNAME or not DREMIO_PASSWORD:
    raise ValueError("DREMIO_USERNAME and DREMIO_PASSWORD must be set in the environment variables.")

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

def main():
    print("🔄 Starting main execution...")
    queries_folder = os.path.join(os.path.dirname(__file__), "queries")
    if not os.path.exists(queries_folder):
        print(f"❌ Error: Queries folder not found at {queries_folder}")
        return

    print("🔄 Looking for query files...")
    query_files = [f for f in os.listdir(queries_folder) if f.endswith(".sql")]
    if not query_files:
        print("❌ No query files found in the queries folder.")
        return

    first_query_file = query_files[0]
    query_path = os.path.join(queries_folder, first_query_file)
    try:
        print(f"🔄 Reading query file: {first_query_file}")
        with open(query_path, "r") as f:
            query = f.read().strip()
            print(f"📋 Query content: {query}")
    except Exception as e:
        print(f"❌ Error reading query file {first_query_file}: {e}")
        return

    print(f"🔄 Submitting query: {first_query_file}")
    job_id = execute_query(query)
    if not job_id:
        print(f"❌ Query execution failed. Please inspect SQL logs.")
        return

    print(f"🔄 Waiting for job {job_id} to complete...")
    wait_for_job_completion(job_id)

if __name__ == "__main__":
    main()
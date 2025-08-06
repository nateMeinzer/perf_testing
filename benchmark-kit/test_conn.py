import os
import requests
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DREMIO_PAT = os.getenv("DREMIO_PAT")
DREMIO_URL = os.getenv("DREMIO_URL")

if not DREMIO_PAT or not DREMIO_URL:
    raise ValueError("DREMIO_PAT and DREMIO_URL must be set in the environment variables.")

def get_auth_header():
    return {
        "Authorization": f"Bearer {DREMIO_PAT}",
        "Content-Type": "application/json"
    }

def execute_query(query):
    headers = get_auth_header()
    payload = {"sql": query, "context": ["icerberg", "test-dremio", "sample"]}
    endpoint = f"{DREMIO_URL}/sql"

    try:
        print(f"Executing query: {query}")
        response = requests.post(endpoint, headers=headers, json=payload)
        print(f"Response: {response.status_code} - {response.text}")
        response.raise_for_status()
        job_id = response.json().get("id")
        print(f"Job ID: {job_id}")
        return job_id
    except requests.exceptions.RequestException as e:
        print(f"Failed to execute query: {e}")
        return None

def wait_for_job_completion(job_id):
    headers = get_auth_header()
    url = f"{DREMIO_URL}/job/{job_id}"

    print(f"Waiting for job {job_id} to complete...")
    while True:
        try:
            res = requests.get(url, headers=headers)
            print(f"Job status response: {res.status_code} - {res.text}")
            res.raise_for_status()
            state = res.json().get("jobState")
            print(f"Job {job_id} status: {state}")

            if state == "COMPLETED":
                print(f"✅ Job {job_id} completed successfully.")
                return True
            elif state in ["FAILED", "CANCELED"]:
                print(f"❌ Job {job_id} failed with state: {state}.")
                return False
        except requests.RequestException as e:
            print(f"Error checking job status: {e}")
        time.sleep(5)

def main():
    queries_folder = os.path.join(os.path.dirname(__file__), "queries")
    if not os.path.exists(queries_folder):
        print(f"Error: Queries folder not found at {queries_folder}")
        return

    query_files = [f for f in os.listdir(queries_folder) if f.endswith(".sql")]
    if not query_files:
        print("No query files found in the queries folder.")
        return

    first_query_file = query_files[0]
    query_path = os.path.join(queries_folder, first_query_file)
    try:
        with open(query_path, "r") as f:
            query = f.read().strip()
            print(f"Submitting first query: {first_query_file}")
            print(f"Query content: {query}")
    except Exception as e:
        print(f"Error reading query file {first_query_file}: {e}")
        return

    job_id = execute_query(query)
    if not job_id:
        print(f"❌ Query execution failed. Please inspect SQL logs.")
        return

    wait_for_job_completion(job_id)

if __name__ == "__main__":
    main()
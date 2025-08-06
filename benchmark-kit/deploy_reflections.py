import os
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DREMIO_PAT = os.getenv("DREMIO_PAT")
DREMIO_URL = os.getenv("DREMIO_URL")
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")

if not DREMIO_PAT or not DREMIO_URL or not DREMIO_USERNAME or not DREMIO_PASSWORD:
    raise ValueError("DREMIO_PAT, DREMIO_URL, DREMIO_USERNAME, and DREMIO_PASSWORD must be set in the environment variables.")

def get_auth_header():
    """Get authentication header using DREMIO_PAT"""
    if not DREMIO_PAT:
        print("Error: DREMIO_PAT is not set in the environment variables.")
        return None
    return {"Authorization": f"Bearer {DREMIO_PAT}", "Content-Type": "application/json"}

def execute_query(query):
    """Execute a SQL query in Dremio and wait for results"""
    headers = get_auth_header()
    if not headers:
        return False

    submit_payload = {
        "sql": query,
        "context": ["icerberg", "test-dremio", "sample"]
    }

    try:
        print(f"Submitting SQL query: {query}")
        response = requests.post(
            f"{DREMIO_URL}/api/v3/sql",
            headers=headers,
            json=submit_payload
        )
        print(f"Response status code: {response.status_code}")
        print(response.text)  # Debug response content
        try:
            job_id = response.json().get('id')
        except ValueError:
            print("Error: Response is not JSON. Debugging response content:")
            print(response.text)
            return False
        
        if not job_id:
            print("No job ID returned from query submission")
            return False

        while True:
            print(f"Polling for job status: {job_id}")
            status_response = requests.get(
                f"{DREMIO_URL}/api/v3/job/{job_id}",
                headers=headers
            )
            try:
                status = status_response.json()
            except ValueError:
                print("Error: Status response is not JSON. Debugging response content:")
                print(status_response.text)
                return False
            
            if status.get('jobState') == 'COMPLETED':
                print("Query completed successfully")
                return job_id
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

def wait_for_job_completion(job_id):
    headers = get_auth_header()
    url = f"{DREMIO_URL}/api/v3/job/{job_id}"
    elapsed_time = 0

    print(f"Waiting for job {job_id} to complete...")
    for attempt in range(5):
        try:
            res = requests.get(url, headers=headers)
            print(f"Attempt {attempt + 1}: Response: {res.status_code} - {res.text}")
            if res.status_code == 404:
                time.sleep(10)
                continue
            res.raise_for_status()
            break
        except requests.RequestException as e:
            print(f"Error checking job status: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Error response: {e.response.text}")
            time.sleep(10)
    else:
        print(f"Error: Job {job_id} not found after multiple attempts.")
        return False

    last_state = None
    while True:
        try:
            res = requests.get(url, headers=headers)
            print(f"Job status response: {res.status_code}")
            try:
                state = res.json().get("jobState")
            except ValueError:
                print("Error: Job status response is not JSON. Debugging response content:")
                print(res.text)
                return False
        except requests.RequestException as e:
            print(f"Error polling job status: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Error response: {e.response.text}")
            return False

        if state != last_state:
            print(f"Job {job_id} status: {state}")
            last_state = state
        elif state == last_state and state not in ["COMPLETED", "FAILED", "CANCELED"]:
            elapsed_time += 1
            if elapsed_time % 30 == 0:
                print(".", end="", flush=True)

        if state == "COMPLETED":
            print(f"✅ Job {job_id} completed successfully.")
            return True
        elif state in ["FAILED", "CANCELED"]:
            print(f"\n❌ Job {job_id} failed with state: {state}. Please inspect SQL logs.")
            return False
        time.sleep(1)

def request_recommendations(job_id):
    headers = get_auth_header()
    endpoint = f"{DREMIO_URL}/api/v3/reflection/recommendations"
    payload = {"jobIds": [job_id]}

    print(f"Requesting recommendations for job {job_id}...")
    try:
        res = requests.post(endpoint, headers=headers, json=payload)
        print(f"Recommendations response: {res.status_code} - {res.text}")
        res.raise_for_status()
        return res.json()
    except requests.RequestException as e:
        print(f"Error getting recommendations for job {job_id}: {e}")
        return None

def create_reflection_from_recommendation(recommendation):
    headers = get_auth_header()

    print(f"Creating view from recommendation: {recommendation}")
    view_payload = recommendation["viewRequestBody"]
    catalog_url = f"{DREMIO_URL}/api/v3/catalog"
    view_response = requests.post(catalog_url, headers=headers, json=view_payload)
    print(f"View creation response: {view_response.status_code} - {view_response.text}")
    view_response.raise_for_status()
    view_id = view_response.json()["id"]

    print(f"Creating reflection for view ID: {view_id}")
    reflection_payload = recommendation["reflectionRequestBody"]
    reflection_payload["datasetId"] = view_id
    reflection_url = f"{DREMIO_URL}/api/v3/reflection"
    reflection_response = requests.post(reflection_url, headers=headers, json=reflection_payload)
    print(f"Reflection creation response: {reflection_response.status_code} - {reflection_response.text}")
    reflection_response.raise_for_status()
    print("✅ Recommended reflection created.")

def process_queries():
    queries_folder = os.path.join(os.path.dirname(__file__), "queries")
    if not os.path.exists(queries_folder):
        print(f"Error: Queries folder not found at {queries_folder}")
        return

    query_files = [f for f in os.listdir(queries_folder) if f.endswith(".sql")]
    if not query_files:
        print("No query files found in the queries folder.")
        return

    for query_file in query_files:
        query_path = os.path.join(queries_folder, query_file)
        try:
            with open(query_path, "r") as f:
                query = f.read().strip()
                print(f"Processing query: {query_file}")
                print(f"Query content: {query}")
        except Exception as e:
            print(f"Error reading query file {query_file}: {e}")
            continue

        job_id = execute_query(query)
        if not job_id:
            print(f"❌ Skipping query {query_file} due to execution failure. Please inspect SQL logs.")
            continue

        if not wait_for_job_completion(job_id):
            print(f"❌ Skipping query {query_file} due to job failure. Please inspect SQL logs.")
            continue

        recommendations = request_recommendations(job_id)
        if recommendations and "data" in recommendations and recommendations["data"]:
            for rec in recommendations["data"]:
                create_reflection_from_recommendation(rec)
        else:
            print(f"No reflection recommendations for query {query_file}. Skipping reflection creation.")

def main():
    process_queries()

if __name__ == "__main__":
    main()
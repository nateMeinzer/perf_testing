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
    return {
        "Authorization": f"Bearer {DREMIO_PAT}",
        "Content-Type": "application/json"
    }

def execute_query(query):
    headers = get_auth_header()
    payload = {"sql": query, "context": ["iceberg", "sample"]}
    endpoint = f"{DREMIO_URL}/sql"

    try:
        response = requests.post(endpoint, headers=headers, json=payload)
        response.raise_for_status()
        job_id = response.json().get("id")
        return job_id
    except requests.exceptions.RequestException as e:
        print(f"Failed to execute query: {e}")
        return None

def wait_for_job_completion(job_id):
    headers = get_auth_header()
    url = f"{DREMIO_URL}/job/{job_id}"
    elapsed_time = 0

    for attempt in range(5):
        try:
            res = requests.get(url, headers=headers)
            if res.status_code == 404:
                time.sleep(10)
                continue
            res.raise_for_status()
            break
        except requests.RequestException:
            time.sleep(10)
    else:
        print(f"Error: Job {job_id} not found after multiple attempts.")
        return False

    last_state = None
    while True:
        res = requests.get(url, headers=headers)
        state = res.json().get("jobState")
        if state != last_state:
            print(f"Job {job_id} status: {state}")
            last_state = state
        elif state == last_state and state not in ["COMPLETED", "FAILED", "CANCELED"]:
            elapsed_time += 1
            if elapsed_time % 30 == 0:
                print(".", end="", flush=True)

        if state == "COMPLETED":
            return True  # Skip repeating the success message
        elif state in ["FAILED", "CANCELED"]:
            print(f"\n❌ Job {job_id} failed with state: {state}. Please inspect SQL logs.")
            return False
        time.sleep(1)

def request_recommendations(job_id):
    headers = get_auth_header()
    endpoint = f"{DREMIO_URL}/reflection/recommendations"
    payload = {"jobIds": [job_id]}

    try:
        res = requests.post(endpoint, headers=headers, json=payload)
        res.raise_for_status()
        return res.json()
    except requests.RequestException as e:
        print(f"Error getting recommendations for job {job_id}: {e}")
        return None

def create_reflection_from_recommendation(recommendation):
    headers = get_auth_header()

    view_payload = recommendation["viewRequestBody"]
    catalog_url = f"{DREMIO_URL}/catalog"
    view_response = requests.post(catalog_url, headers=headers, json=view_payload)
    view_response.raise_for_status()
    view_id = view_response.json()["id"]

    reflection_payload = recommendation["reflectionRequestBody"]
    reflection_payload["datasetId"] = view_id
    reflection_url = f"{DREMIO_URL}/reflection"
    reflection_response = requests.post(reflection_url, headers=headers, json=reflection_payload)
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
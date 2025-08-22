import os
import requests
import csv
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Env vars
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DREMIO_URL = os.getenv("DREMIO_URL")
CATALOG = os.getenv("ICEBERG_BUCKET_NAME")  # now dynamic!

if not DREMIO_USERNAME or not DREMIO_PASSWORD or not DREMIO_URL or not CATALOG:
    raise ValueError("Missing one or more required environment variables.")

# Config
queries_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "queries"))
results_file = os.path.join(os.path.dirname(__file__), "execution_results.csv")

def get_auth_header():
    """Authenticate with Dremio and return the authorization header."""
    auth_payload = {"userName": DREMIO_USERNAME, "password": DREMIO_PASSWORD}
    response = requests.post(f"{DREMIO_URL}/apiv2/login", json=auth_payload)
    response.raise_for_status()
    token = response.json()["token"]
    return {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}

def execute_query(query):
    """Execute a SQL query in Dremio and return the results."""
    headers = get_auth_header()
    payload = {
        "sql": query,
        "context": [CATALOG]
    }
    try:
        response = requests.post(f"{DREMIO_URL}/api/v3/sql", headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        print(f"✅ Executed query:\n{query[:100]}...\n")
        return {"query": query, "status": "success", "data": data}
    except requests.exceptions.HTTPError:
        error_message = response.json().get("errorMessage", "Unknown error")
        print(f"❌ Query failed: {error_message}\n")
        return {"query": query, "status": "failed", "error": error_message}
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}\n")
        return {"query": query, "status": "error", "error": str(e)}

def execute_queries():
    """Iterate through queries in the queries folder and log results to a CSV file."""
    with open(results_file, mode="w", newline="") as csvfile:
        fieldnames = ["filename", "status"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for filename in sorted(os.listdir(queries_dir)):
            if not filename.endswith(".sql"):
                continue
            file_path = os.path.join(queries_dir, filename)
            with open(file_path, "r") as f:
                query_text = f.read().strip().rstrip(";")
            result = execute_query(query_text)
            writer.writerow({
                "filename": filename,
                "status": result["status"]
            })

if __name__ == "__main__":
    execute_queries()
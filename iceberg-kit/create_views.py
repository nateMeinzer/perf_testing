import os
import requests
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
view_folder = "views"
queries_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tpcds-kit", "tools", "queries"))

def get_auth_header():
    """Authenticate with Dremio and return the authorization header."""
    auth_payload = {"userName": DREMIO_USERNAME, "password": DREMIO_PASSWORD}
    response = requests.post(f"{DREMIO_URL}/apiv2/login", json=auth_payload)
    response.raise_for_status()
    token = response.json()["token"]
    return {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}

def execute_query(query):
    """Execute a SQL query in Dremio."""
    headers = get_auth_header()
    payload = {
        "sql": query,
        "context": [CATALOG]
    }
    try:
        response = requests.post(f"{DREMIO_URL}/api/v3/sql", headers=headers, json=payload)
        response.raise_for_status()
        print(f"✅ Executed query:\n{query[:100]}...\n")
    except requests.exceptions.HTTPError:
        error_message = response.json().get("errorMessage", "Unknown error")
        print(f"❌ Query failed: {error_message}\n")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}\n")

def create_views():
    for filename in sorted(os.listdir(queries_dir)):
        if not filename.endswith(".sql") or filename.lower() == "query_0.sql":
            continue
        view_name = filename[:-4]  # strip .sql
        file_path = os.path.join(queries_dir, filename)
        with open(file_path, "r") as f:
            query_text = f.read().strip().rstrip(";")
        create_query = f"CREATE OR REPLACE VIEW {CATALOG}.{view_folder}.{view_name} AS {query_text};"
        execute_query(create_query)

if __name__ == "__main__":
    create_views()
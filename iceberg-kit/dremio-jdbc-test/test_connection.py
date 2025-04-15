import requests
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Dremio API credentials and configuration
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DREMIO_URL = os.getenv("DREMIO_URL")
CATALOG = "iceberg"

if not DREMIO_USERNAME or not DREMIO_PASSWORD or not DREMIO_URL:
    raise ValueError("DREMIO_USERNAME, DREMIO_PASSWORD, and DREMIO_URL must be set in the environment variables.")

def get_auth_header():
    """Authenticate with Dremio and return the authorization header."""
    auth_payload = {"userName": DREMIO_USERNAME, "password": DREMIO_PASSWORD}
    try:
        response = requests.post(f"{DREMIO_URL}/apiv2/login", json=auth_payload)
        response.raise_for_status()
        token = response.json()["token"]
        return {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Authentication failed: {e}")

def execute_query(query):
    """Execute a SQL query in Dremio."""
    headers = get_auth_header()
    payload = {
        "sql": query,
        "context": [CATALOG]  # Specify the catalog context
    }
    try:
        response = requests.post(f"{DREMIO_URL}/api/v3/sql", headers=headers, json=payload)
        response.raise_for_status()
        print("✅ Query executed successfully. Results:")
        print(response.json())
    except requests.exceptions.HTTPError as e:
        error_message = response.json().get("errorMessage", "Unknown error")
        print(f"❌ Failed to execute query: {error_message}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to execute query: {e}")

if __name__ == "__main__":
    query = "CREATE VIEW iceberg.views.call_center as select * from call_center limit 1;"
    execute_query(query)

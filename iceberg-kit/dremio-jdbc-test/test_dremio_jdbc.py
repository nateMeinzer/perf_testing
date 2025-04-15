import jaydebeapi
import os
from dotenv import load_dotenv


load_dotenv()

DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DRIVER_PATH = os.getenv("DREMIO_JDBC_JAR")
DREMIO_HOST = os.getenv("DREMIO_HOST", "localhost")
DREMIO_PORT = os.getenv("DREMIO_PORT", "32010")

JDBC_URL = f"jdbc:dremio:direct={DREMIO_HOST}:{DREMIO_PORT};disableTLS=true;schema=object_store.tpcds"


print("🔍 Environment:")

print(f"  HOST: {DREMIO_HOST}")
print(f"  PORT: {DREMIO_PORT}")
print(f"  JAR: {DRIVER_PATH}")
print(f" JDBC_URL: {JDBC_URL}")


query = "SELECT 1"

try:
    conn = jaydebeapi.connect(
        "com.dremio.jdbc.Driver",
        JDBC_URL,
        [DREMIO_USERNAME, DREMIO_PASSWORD],
        DRIVER_PATH
    )
    curs = conn.cursor()
    curs.execute(query)
    rows = curs.fetchall()
    print("✅ Query results:")
    for row in rows:
        print(row)
except Exception as e:
    print(f"❌ JDBC query failed: {e}")
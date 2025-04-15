import jaydebeapi
import os
from dotenv import load_dotenv


print("🚀 Starting Flight JDBC test...")

load_dotenv()

DREMIO_USERNAME = os.getenv("DREMIO_USERNAME")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD")
DRIVER_PATH = os.getenv("DREMIO_JDBC_JAR")
DREMIO_HOST = os.getenv("DREMIO_HOST", "localhost")
DREMIO_PORT = os.getenv("DREMIO_PORT", "32010")
JDBC_URL = (
    f"jdbc:arrow-flight-sql://{DREMIO_HOST}:{DREMIO_PORT}/?useEncryption=false"
)

print("🔍 Environment:")

print(f"  HOST: {DREMIO_HOST}")
print(f"  PORT: {DREMIO_PORT}")
print(f"  JAR: {DRIVER_PATH}")
print(f" JDBC_URL: {JDBC_URL}")


def test_connection():
    try:
        conn = jaydebeapi.connect(
            "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver",
            JDBC_URL,
            [DREMIO_USERNAME, DREMIO_PASSWORD],
            DRIVER_PATH,
        )
        curs = conn.cursor()
        curs.execute("SELECT 1")
        rows = curs.fetchall()
        print("✅ JDBC Flight SQL results:")
        for row in rows:
            print(row)
        conn.close()
    except Exception as e:
        print(f"❌ Flight SQL connection failed: {e}")

if __name__ == "__main__":
    test_connection()
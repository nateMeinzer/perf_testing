import sys
import subprocess

def execute_script(script_name):
    """Execute a Python script from the iceberg-lakehouse-kit folder."""
    script_path = f"./iceberg-kit/{script_name}"
    try:
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to execute {script_name}. {e}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python iceberg.py <flag>")
        print("Flags: tables, views, cleanup")
        sys.exit(1)

    flag = sys.argv[1].lower()

    if flag == "tables":
        execute_script("deploy_tables.py")
    elif flag == "views":
        execute_script("create_views.py")
    elif flag == "cleanup":
        execute_script("table_cleanup.py")
    else:
        print(f"Error: Unknown flag '{flag}'. Valid flags are: tables, views, cleanup")
        sys.exit(1)

if __name__ == "__main__":
    main()

import os
import chardet

def check_file_encodings(directory):
    """Check the encoding of all .parquet files in the specified directory."""
    if not os.path.exists(directory):
        print(f"Directory does not exist: {directory}")
        return

    parquet_files = [f for f in os.listdir(directory) if f.endswith(".dat")]

    if not parquet_files:
        print(f"No .parquet files found in directory: {directory}")
        return

    for file_name in parquet_files:
        file_path = os.path.join(directory, file_name)
        with open(file_path, "rb") as f:
            raw_data = f.read(10000)
            detected_encoding = chardet.detect(raw_data)["encoding"]
            print(f"File: {file_name}, Encoding: {detected_encoding}")

if __name__ == "__main__":
    directory = "test_data/raw_files"
    check_file_encodings(directory)

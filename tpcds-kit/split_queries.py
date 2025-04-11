import os

# Input file containing all queries
input_file = "/Users/nathan.meinzer/Repos/perf_testing/tpcds-kit/tools/queries/query_0.sql"

# Output directory for individual query files
output_dir = "/Users/nathan.meinzer/Repos/perf_testing/tpcds-kit/tools/queries"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the input file
with open(input_file, "r") as file:
    content = file.read()

# Split queries by the semi-colon delimiter
queries = content.split(";")

# Write each query to a separate file
for i, query in enumerate(queries, start=1):
    query = query.strip()
    if query:  # Skip empty queries
        output_file = os.path.join(output_dir, f"query_{i}.sql")
        with open(output_file, "w") as out_file:
            out_file.write(query + ";\n")

print(f"Split {len(queries)} queries into individual files in {output_dir}")

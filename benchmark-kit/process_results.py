import pandas as pd
import time  # Import time module for Unix timestamp

# Read the source CSV file
input_file = "results/full_results.csv"

# Generate output filename with Unix timestamp
timestamp = int(time.time())
output_file = f"results/validated_results-{timestamp}.csv"

# Load the CSV into a DataFrame
df = pd.read_csv(input_file)

# Map the label column directly to jobID
job_parts = df['label'].str.extract(r'(run)(?:-noref)?-(\d{8}-\d{6})')
df['jobID'] = job_parts[0] + '-' + job_parts[1]
df['reflections'] = df['label'].str.contains('wref')  # True if 'wref', False otherwise
df['query'] = df['label'].str.extract(r'(query_\d+)')  # Extract query number

# Select and rename columns
df = df[['jobID', 'reflections', 'query', 'elapsed', 'success', 'bytes', 'sentBytes']]
df.rename(columns={
    'elapsed': 'elapsed_time',
    'success': 'success',
    'bytes': 'bytes',
    'sent_bytes': 'sent_bytes'
}, inplace=True)

# Validation logic
unique_jobids_with_reflections = df[df['reflections'] == True]['jobID'].nunique()
unique_jobids_without_reflections = df[df['reflections'] == False]['jobID'].nunique()
queries_per_jobid = df.groupby('jobID')['query'].nunique().unique()
all_queries_successful = df['success'].all()  # Check if all queries are successful

# Check validation conditions
if (unique_jobids_with_reflections == 495 and 
    unique_jobids_without_reflections == 495 and 
    len(queries_per_jobid) == 1 and queries_per_jobid[0] == 99 and 
    all_queries_successful):
    print("Validation passed. Generating validated results file.")
    df.to_csv(output_file, index=False)
else:
    print(f"Validation failed:")
    print(f" - Unique jobIDs with reflections: {unique_jobids_with_reflections}")
    print(f" - Unique jobIDs without reflections: {unique_jobids_without_reflections}")
    print(f" - Queries per jobID: {queries_per_jobid}")
    print(f" - All queries successful: {all_queries_successful}")
    invalid_output_file = "results/invalid_results.csv"
    df.to_csv(invalid_output_file, index=False)

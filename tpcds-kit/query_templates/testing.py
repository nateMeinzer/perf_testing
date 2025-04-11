import os
import re

def extract_queries(content):
    # Remove comments and DEFINE macros
    content = re.sub(r"--.*", "", content)
    content = re.sub(r"define\s.*?;", "", content, flags=re.IGNORECASE)
    content = re.sub(r"\s+", " ", content)
    # Split on semicolons and keep only blocks containing 'select'
    return [q.strip() for q in content.split(';') if re.search(r'\bselect\b', q, re.IGNORECASE)]

for filename in os.listdir("."):
    if filename.endswith(".tpl"):
        try:
            with open(filename, "r", encoding="utf-8") as file:
                content = file.read()
                queries = extract_queries(content)
                if len(queries) > 1:
                    print(f"{filename} — {len(queries)} queries found")
        except Exception as e:
            print(f"Error reading {filename}: {e}")
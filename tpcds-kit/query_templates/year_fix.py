import os
import re

def fix_year_column(text):
    # 1. as year,
    text = re.sub(r'\bas\s+year\b(?=\s*,)', 'as "year"', text, flags=re.IGNORECASE)

    # 2. as year (no comma)
    text = re.sub(r'\bas\s+year\b(?!\s*,)', 'as "year"', text, flags=re.IGNORECASE)

    # 3. function(year) → function("year")
    text = re.sub(r'(\b\w+\s*\()\s*year\s*(\))', r'\1"year"\2', text)

    # 4. .year = → ."year" =
    text = re.sub(r'\.year\b\s*=', r'."year" =', text)

    # 5. ,year → ,"year"
    text = re.sub(r',\s*year\b', r',"year"', text)

    # 6. AS year (case insensitive)
    text = re.sub(r'\bAS\s+year\b', 'AS "year"', text, flags=re.IGNORECASE)

    return text

for filename in os.listdir("."):
    if filename.endswith(".tpl"):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                original = f.read()
            updated = fix_year_column(original)
            if original != updated:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(updated)
                print(f"Updated: {filename}")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
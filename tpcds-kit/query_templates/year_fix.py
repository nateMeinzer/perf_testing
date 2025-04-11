import os
import re

def fix_year_column(text):
    # 1. as year,
    text = re.sub(r'\bas\s+year\b(?=\s*,)', 'as "year"', text)
    # 2. as year (no comma)
    text = re.sub(r'\bas\s+year\b(?!\s*,)', 'as "year"', text)
    # 3. function(year) → function("year")
    text = re.sub(r'(\w+)\(\s*year\s*\)', r'\1("year")', text)
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
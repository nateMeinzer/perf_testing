import os
import re

def fix_returns(text):
    # 1. Match: as returns,
    text = re.sub(r'\bas\s+returns\b(?=\s*,)', 'as "returns"', text)
    # 2. Match: as returns (no comma after)
    text = re.sub(r'\bas\s+returns\b(?!\s*[,])', 'as "returns"', text)
    # 3. Match: (returns) or ( returns )
    text = re.sub(r'\(\s*returns\s*\)', '("returns")', text)
    return text

for filename in os.listdir("."):
    if filename.endswith(".tpl"):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                original = f.read()
            updated = fix_returns(original)
            if original != updated:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(updated)
                print(f"Updated: {filename}")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
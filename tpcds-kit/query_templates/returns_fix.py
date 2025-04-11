import os
import re

def fix_returns(text):
    # 1. as returns,
    text = re.sub(r'\bas\s+returns\b(?=\s*,)', 'as "returns"', text, flags=re.IGNORECASE)
    # 2. as returns (no comma)
    text = re.sub(r'\bas\s+returns\b(?!\s*,)', 'as "returns"', text, flags=re.IGNORECASE)
    # 3. function(returns) → function("returns")
    text = re.sub(r'(\b\w+\s*\()\s*returns\s*(\))', r'\1"returns"\2', text)
    # 4. , returns → , "returns"
    text = re.sub(r',\s*returns\b', r', "returns"', text, flags=re.IGNORECASE)
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
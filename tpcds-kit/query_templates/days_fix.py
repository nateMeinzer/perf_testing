import os
import re

def rewrite_date_add(content):
    # Pattern: (cast('[VAR]' as date) + N days)
    pattern = re.compile(
        r"\(cast\('(?P<var>\[[^\]]+\])'\s+as\s+date\)\s*\+\s*(?P<num>\d+)\s+days\)",
        re.IGNORECASE
    )

    def replacer(match):
        var = match.group("var")
        num = match.group("num")
        return f"DATE_ADD(cast('{var}' as date), {num})"

    return pattern.sub(replacer, content)

for filename in os.listdir("."):
    if filename.endswith(".tpl"):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                original = f.read()
            updated = rewrite_date_add(original)
            if original != updated:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(updated)
                print(f"Rewritten: {filename}")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
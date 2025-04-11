import os
import re

def rewrite_date_add(content):
    # Matches: (cast('...' as date) +/- N days)
    pattern = re.compile(
        r"""
        \(?                      # optional opening paren
        cast\s*\(\s*             # cast(
        (?P<quote>['"])          # opening quote
        (?P<var>\[?[^\]'"]+\]?)  # variable or date literal
        (?P=quote)               # closing quote
        \s+as\s+date\s*\)        # as date)
        \s*                      # optional whitespace
        (?P<op>[+-])             # + or -
        \s*                      # optional whitespace
        (?P<days>\d+)            # the number
        \s+days\s*               # " days"
        \)?                      # optional closing paren
        """,
        re.IGNORECASE | re.VERBOSE
    )

    def replacer(m):
        var = m.group("var")
        op = m.group("op")
        days = m.group("days")
        sign = "-" if op == "-" else ""
        return f'DATE_ADD(cast(\'{var}\' as date), {sign}{days})'

    return pattern.sub(replacer, content)

# Process all .tpl files in current directory
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
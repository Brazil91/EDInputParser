import os
import re
import pyperclip

# Directory containing the .mail files
directory = r"c:\Users\adm-hamersdlu\ERS\IT Dept. - General\Projekte und Vorfälle\17042 - stakeholder1 TMS Amendment\no_procedure-Production"

ojns = []

for filename in os.listdir(directory):
    if filename.endswith(".mail"):
        filepath = os.path.join(directory, filename)
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline()
            match = re.search(r"'A8(\d+)'", first_line)
            if match:
                ojns.append(match.group(1))
                # continue

result = "|".join(ojns)
pyperclip.copy(result)
print("\nCopied to clipboard:", result)
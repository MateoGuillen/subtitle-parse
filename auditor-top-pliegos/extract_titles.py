import csv
import re

titles = set()
with open('C:/Users/mguillen/Desktop/tesis/campos_tesis_isolation_forest_clasificacion.csv', 'r', encoding='latin-1') as f:
    content = f.read()

# Split by records - each starts with pct_garantia_cumplimiento,
records = content.split('\npct_garantia_cumplimiento,')

for i, rec in enumerate(records):
    if i == 0:
        continue
    # Parse first 6 fields
    fields = rec.split(',', 5)
    if len(fields) >= 5:
        title = fields[4].strip('"')
        # Filter out content fragments (long text with semicolons)
        if title and len(title) < 200 and ';' not in title and '\n' not in title:
            titles.add(title)

for t in sorted(titles):
    print(t)
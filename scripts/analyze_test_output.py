"""Analyze test output sections for Adenda."""
import pandas as pd

df = pd.read_parquet('./data/processed/test_2021/sections_2021_test.parquet')
adenda = df[df['title'].str.lower().str.strip() == 'adenda']
print(f"Total Adenda: {len(adenda)}")
print(f"line_end value_counts:")
print(adenda['line_end'].value_counts().sort_index())
print(f"")
print(f"page value_counts:")
print(adenda['page'].value_counts().sort_index())
print(f"")
print(f"Max content_length: {adenda['content_length'].max()}")
print(f"Min content_length: {adenda['content_length'].min()}")
print(f"Median content_length: {adenda['content_length'].median()}")

# Show examples with highest content_length
worst = adenda.nlargest(3, 'content_length')
nl = "\n"
for _, row in worst.iterrows():
    print(f"")
    print(f"--- {row['document_id']} ---")
    print(f"  page={row['page']} line_start={row['line_start']} line_end={row['line_end']} content_length={row['content_length']}")
    content_lines = row['content'].split(nl) if isinstance(row['content'], str) else ['N/A']
    print(f"  First 3 lines: {content_lines[:3]}")
    print(f"  Last 3 lines: {content_lines[-3:]}")

# Also check which documents have Caso 3 behavior in the test
# (line_end == -1 means None was filled)
caso3 = adenda[adenda['line_end'] == -1]
print(f"")
print(f"Adenda with line_end=-1 (Caso 3 or Caso 5): {len(caso3)}")
if len(caso3) > 0:
    print(f"  Examples:")
    for _, row in caso3.head(3).iterrows():
        print(f"    {row['document_id']}: page={row['page']} content_length={row['content_length']}")

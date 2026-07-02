from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

engine = create_engine('postgresql+psycopg2://postgres:Temporal123@172.31.233.136:5433/dncp')

# Sample 5000 documents with all sections
print("Sampling 5000 documents...")
query = """
WITH doc_sample AS (
    SELECT nro_licitacion
    FROM dncp.pliegos_secciones
    WHERE title_normalized IS NOT NULL
    GROUP BY nro_licitacion
    ORDER BY random()
    LIMIT 5000
)
SELECT p.nro_licitacion, p.title_normalized,
       p.content_length, p.estimated_tokens, p.size_bytes,
       p.content_length_clean, p.page, p.line_start, p.line_end
FROM dncp.pliegos_secciones p
INNER JOIN doc_sample d ON p.nro_licitacion = d.nro_licitacion
WHERE p.title_normalized IS NOT NULL
ORDER BY p.nro_licitacion, p.title_normalized
"""
df = pd.read_sql(query, engine)
print(f"Loaded {len(df)} sections from {df['nro_licitacion'].nunique()} documents")

# === DOCUMENT-LEVEL AGGREGATE FEATURES ===
doc_agg = df.groupby('nro_licitacion').agg(
    total_sections=('content_length', 'count'),
    unique_titles=('title_normalized', 'nunique'),
    avg_content_length=('content_length', 'mean'),
    std_content_length=('content_length', 'std'),
    max_content_length=('content_length', 'max'),
    sum_content_length=('content_length', 'sum'),
    avg_tokens=('estimated_tokens', 'mean'),
    std_tokens=('estimated_tokens', 'std'),
    max_tokens=('estimated_tokens', 'max'),
    sum_tokens=('estimated_tokens', 'sum'),
    avg_size_bytes=('size_bytes', 'mean'),
    std_size_bytes=('size_bytes', 'std'),
    max_size_bytes=('size_bytes', 'max'),
    sum_size_bytes=('size_bytes', 'sum'),
).reset_index()

# === TOP 80 TITLES FOR PIVOTING ===
top_titles = df['title_normalized'].value_counts().head(80).index.tolist()
print(f"Top 80 titles: {top_titles[:5]}...")

df_top = df[df['title_normalized'].isin(top_titles)].copy()

# Make safe column names: clean and add suffix
def make_safe_col(prefix, title, max_len=45):
    name = title.replace(' ', '_').replace(',', '').replace('.', '').replace('-', '_').lower()
    name = name[:max_len]
    return f"{prefix}{name}"

# Pivot content_length sum
pivot_len = df_top.pivot_table(
    index='nro_licitacion', columns='title_normalized',
    values='content_length', aggfunc='sum', fill_value=0
)
pivot_len.columns = [make_safe_col('len_', c) for c in pivot_len.columns]

# Pivot presence flag
pivot_has = df_top.pivot_table(
    index='nro_licitacion', columns='title_normalized',
    values='content_length', aggfunc='count', fill_value=0
)
pivot_has.columns = [make_safe_col('has_', c) for c in pivot_has.columns]
pivot_has = (pivot_has > 0).astype(int)

# Pivot estimated_tokens sum
pivot_tok = df_top.pivot_table(
    index='nro_licitacion', columns='title_normalized',
    values='estimated_tokens', aggfunc='sum', fill_value=0
)
pivot_tok.columns = [make_safe_col('tok_', c) for c in pivot_tok.columns]

# === MERGE ALL ===
features = doc_agg.merge(pivot_len, on='nro_licitacion', how='left')
features = features.merge(pivot_has, on='nro_licitacion', how='left')
features = features.merge(pivot_tok, on='nro_licitacion', how='left')
features.fillna(0, inplace=True)

# Deduplicate column names
seen = defaultdict(int)
new_cols = []
for c in features.columns:
    seen[c] += 1
    if seen[c] > 1:
        new_cols.append(f"{c}_{seen[c]-1}")
    else:
        new_cols.append(c)
features.columns = new_cols

print(f"\nFinal feature matrix: {features.shape}")

# Check for remaining duplicates
dup_check = [c for c in features.columns if list(features.columns).count(c) > 1]
if dup_check:
    print(f"WARNING: {len(dup_check)} duplicate cols remain: {dup_check[:5]}")
else:
    print("No duplicate columns - OK")

# Save
features.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\features_doc_level.csv", index=False)
print("Saved to CSV")

# Quick stats
print(f"\n=== Feature Summary ===")
binary_count = 0
constant_count = 0
for c in features.columns:
    if c == 'nro_licitacion':
        continue
    n_uniq = features[c].nunique()
    if n_uniq <= 2:
        binary_count += 1
    if n_uniq <= 1:
        constant_count += 1

print(f"Total features: {len(features.columns)-1}")
print(f"Binary features: {binary_count}")
print(f"Constant features: {constant_count}")
print(f"Feature names sample: {list(features.columns[1:6])}")

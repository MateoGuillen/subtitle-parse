from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

engine = create_engine('postgresql+psycopg2://postgres:Temporal123@172.31.233.136:5433/dncp')

# Sample: get ALL sections for a random subset of documents
print("Sampling 5000 documents with all their sections...")
query = """
WITH doc_sample AS (
    SELECT nro_licitacion
    FROM dncp.pliegos_secciones
    GROUP BY nro_licitacion
    ORDER BY random()
    LIMIT 5000
)
SELECT p.nro_licitacion, p.title_normalized, 
       p.content_length, p.estimated_tokens, p.size_bytes, 
       p.content_length_clean, p.page, p.line_start, p.line_end,
       p.depth, p.word_count, p.content_text
FROM dncp.pliegos_secciones p
INNER JOIN doc_sample d ON p.nro_licitacion = d.nro_licitacion
WHERE p.title_normalized IS NOT NULL
ORDER BY p.nro_licitacion, p.title_normalized
"""
df = pd.read_sql(query, engine)
print(f"Loaded {len(df)} sections from {df['nro_licitacion'].nunique()} documents")

# Save raw
df.to_parquet("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\doc_sample_5k.parquet", index=False)
print("Saved raw sample")

# ============ FEATURE ENGINEERING ============
print("\n=== Engineering document-level features ===")

# 1. Document-level aggregate stats
doc_agg = df.groupby('nro_licitacion').agg(
    total_sections=('title_normalized', 'count'),
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

# 2. Text features from content_text
df['content_text_len'] = df['content_text'].str.len()
text_agg = df.groupby('nro_licitacion').agg(
    total_text_chars=('content_text_len', 'sum'),
    avg_text_chars=('content_text_len', 'mean'),
    max_text_chars=('content_text_len', 'max'),
    min_text_chars=('content_text_len', 'min'),
).reset_index()

# 3. Pivot: for each title -> content_length sum + presence flag
# Only use top 80 titles (frequent enough to be informative)
top_titles = df['title_normalized'].value_counts().head(80).index.tolist()
print(f"Top 80 titles selected for pivoting: {top_titles[:5]}...")

# Per-title features: sum of content_length for each title
df_top = df[df['title_normalized'].isin(top_titles)].copy()
pivot_length = df_top.pivot_table(
    index='nro_licitacion', 
    columns='title_normalized', 
    values='content_length',
    aggfunc='sum',
    fill_value=0
)
pivot_length.columns = [f"len_{c[:40].replace(' ', '_')}" for c in pivot_length.columns]

# Per-title: presence flag
pivot_presence = df_top.pivot_table(
    index='nro_licitacion',
    columns='title_normalized',
    values='content_length',
    aggfunc='count',
    fill_value=0
)
pivot_presence.columns = [f"has_{c[:40].replace(' ', '_')}" for c in pivot_presence.columns]
pivot_presence = (pivot_presence > 0).astype(int)

# Per-title: estimated_tokens sum
pivot_tokens = df_top.pivot_table(
    index='nro_licitacion',
    columns='title_normalized',
    values='estimated_tokens',
    aggfunc='sum',
    fill_value=0
)
pivot_tokens.columns = [f"tokens_{c[:35].replace(' ', '_')}" for c in pivot_tokens.columns]

# 4. Merge all features
print("Merging feature blocks...")
features = doc_agg.merge(text_agg, on='nro_licitacion', how='left')
features = features.merge(pivot_length, on='nro_licitacion', how='left')
features = features.merge(pivot_presence, on='nro_licitacion', how='left')
features = features.merge(pivot_tokens, on='nro_licitacion', how='left')

# Fill NAs
features.fillna(0, inplace=True)

print(f"Final feature matrix: {features.shape}")
print(f"Columns: {list(features.columns[:15])}... ({len(features.columns)} total)")

# Save
features.to_parquet("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\features_doc_level.parquet", index=False)
features.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\features_doc_level.csv", index=False)
print("Saved feature matrix")

# Quick stats
print(f"\nFeature cardinalities:")
for col in features.columns[:20]:
    if col == 'nro_licitacion':
        continue
    n_unique = features[col].nunique()
    nulls = features[col].isna().sum()
    print(f"  {col:45s} unique={n_unique:5d}  nulls={nulls}")

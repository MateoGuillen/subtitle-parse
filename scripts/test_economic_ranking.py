"""Ranking pipeline runner with reduced memory footprint."""
import sys; sys.path.insert(0, r'D:\projects\subtitle-parse')
import pandas as pd
import numpy as np
from config.settings import DB_CONFIG
from sqlalchemy import create_engine

user = DB_CONFIG['user']; pw = DB_CONFIG['password']
host = DB_CONFIG['host']; port = DB_CONFIG['port']; db = DB_CONFIG['database']
conn_str = f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}'
engine = create_engine(conn_str)

# Load only necessary columns from document_features
df_doc = pd.read_sql(
    "SELECT nro_licitacion, year, category_id, total_sections, unique_titles FROM dncp.document_features ORDER BY nro_licitacion",
    engine
)
print(f'doc features: {len(df_doc)} rows, {len(df_doc.columns)} cols')

# Get title slugs and has_* columns
all_cols = pd.read_sql("SELECT * FROM dncp.document_features LIMIT 0", engine).columns
title_slugs = sorted({c[4:] for c in all_cols if c.startswith('has_') and c != 'has_otros'})
print(f'{len(title_slugs)} title slugs')

# Load has_* columns
has_cols = ['nro_licitacion'] + [f'has_{s}' for s in title_slugs] + ['has_otros']
col_subset = [c for c in has_cols if c in all_cols]
has_df = pd.read_sql(
    f'SELECT {", ".join(col_subset)} FROM dncp.document_features ORDER BY nro_licitacion',
    engine, chunksize=5000
)
has_data = pd.concat(has_df, ignore_index=True)
print(f'has data: {len(has_data)} rows, {len(has_data.columns)} cols')

# Load economic features
df_econ = pd.read_sql("SELECT * FROM dncp.document_economic_features ORDER BY nro_licitacion", engine)
print(f'econ features: {len(df_econ)} rows, {len(df_econ.columns)} cols')

# Test 6th strategy for a few slugs
from src.etl.transformers.title_ranking_transformer import ECONOMIC_RISK_COLS
risk_cols = [c for c in ECONOMIC_RISK_COLS if c in df_econ.columns]
print(f'risk cols: {risk_cols}')

eco = df_econ[['nro_licitacion'] + risk_cols].copy()
for c in risk_cols:
    eco[c] = pd.to_numeric(eco[c], errors='coerce').fillna(0)

scores = {}
for slug in title_slugs[:10]:
    has_col = f'has_{slug}'
    merged = has_data[['nro_licitacion', has_col]].merge(eco, on='nro_licitacion', how='inner')
    if len(merged) < 100:
        scores[slug] = 0.0
        continue
    has = merged[has_col].astype(float)
    corrs = [abs(has.corr(merged[c])) for c in risk_cols if np.isfinite(has.corr(merged[c]))]
    scores[slug] = float(np.mean(corrs)) if corrs else 0.0

print('Top-10 economic risk scores:')
for slug, score in sorted(scores.items(), key=lambda x: -x[1])[:10]:
    print(f'  {slug.replace("_", " ").title():40s} {score:.6f}')

print('SUCCESS')

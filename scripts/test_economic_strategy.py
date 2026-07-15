"""Test the economic risk strategy in isolation."""
import sys
sys.path.insert(0, r'D:\projects\subtitle-parse')

import pandas as pd
import numpy as np
from config.settings import DB_CONFIG
from sqlalchemy import create_engine

user = DB_CONFIG['user']
pw = DB_CONFIG['password']
host = DB_CONFIG['host']
port = DB_CONFIG['port']
db = DB_CONFIG['database']
conn_str = f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}'
engine = create_engine(conn_str)

# Load document_features with only needed columns
df_doc = pd.read_sql("""
    SELECT nro_licitacion, has_otros
    FROM dncp.document_features
    ORDER BY nro_licitacion
""", engine)
print(f'document_features: {len(df_doc)} rows, {len(df_doc.columns)} cols')

# Load economic features
df_econ = pd.read_sql("SELECT nro_licitacion, es_unico_oferente, overbudget_ratio, is_high_value_single_bidder, winner_category_frequency, is_repeat_winner, winner_total_contracts, bidder_diversity FROM dncp.document_economic_features ORDER BY nro_licitacion", engine)
print(f'document_economic_features: {len(df_econ)} rows, {len(df_econ.columns)} cols')

# Test the _strategy_economic_risk logic
from src.etl.transformers.title_ranking_transformer import ECONOMIC_RISK_COLS

risk_cols = [c for c in ECONOMIC_RISK_COLS if c in df_econ.columns]
print(f'risk_cols available: {risk_cols}')

eco = df_econ[["nro_licitacion"] + risk_cols].copy()
for c in risk_cols:
    eco[c] = pd.to_numeric(eco[c], errors="coerce").fillna(0)

# Test for has_otros
merged = df_doc[["nro_licitacion", "has_otros"]].merge(eco, on="nro_licitacion", how="inner")
print(f'merged: {len(merged)} rows')
has = merged["has_otros"].astype(float)
correlations = []
for c in risk_cols:
    r = has.corr(merged[c])
    if np.isfinite(r):
        correlations.append(abs(r))
score = float(np.mean(correlations)) if correlations else 0.0
print(f'score for has_otros: {score:.6f}, based on {len(correlations)} correlations')
print('SUCCESS')

"""Paso CERO del plan v4: congelar 50 held-out ANTES de analizar.

Split determinista seed 4242 sobre los 238 = 288(GT∩db, 5 campos) MENOS los 49 de
pct_monto_referencia_tecnica (regex tuneado sobre estos archivos → no held-out).
12-13 por campo. El script escribe el CSV y su sha256 PERO no imprime etiquetas
(individual-level ceguera). Limitación: los agregados por campo ya fueron vistos.
"""
import hashlib
import json
from pathlib import Path

import pandas as pd

FIELDS = ['dias_validez_garantia', 'anos_experiencia_minima_tecnica',
          'dias_pago_anticipo', 'dias_solicitud_anticipo']
GT_DIR = Path('data/processed/alolab/gt_por_campo')
SRC = {'dias_validez_garantia': 'dias_validez_garantia_GT.csv',
       'anos_experiencia_minima_tecnica': 'anos_experiencia_minima_tecnica_GT (1).csv',
       'dias_pago_anticipo': 'dias_pago_anticipo_GT.csv',
       'dias_solicitud_anticipo': 'dias_solicitud_anticipo_GT.csv'}
OUT = Path('data/processed/llm_benchmark/heldout50_frozen.csv')
SEED = 4242

rows = []
for f in FIELDS:
    g = pd.read_csv(GT_DIR / SRC[f], usecols=['nro_licitacion', 'campo',
                                              'text', 'db_value', 'ground_truth'])
    g = g[g['ground_truth'].notna() & g['db_value'].notna()].copy()
    g['campo'] = f
    rows.append(g)
df = pd.concat(rows, ignore_index=True)
df = df.sample(frac=1.0, random_state=SEED).reset_index(drop=True)

hold = df.groupby('campo', group_keys=False).head(12)
rest = df[~df.index.isin(hold.index)]
take2 = rest.groupby('campo', group_keys=False).nth([0])[:2]
hold = pd.concat([hold, take2])
assert len(hold) == 50, len(hold)
hold.to_csv(OUT, index=False)
h = hashlib.sha256(OUT.read_bytes()).hexdigest()
meta = Path('data/processed/llm_benchmark/heldout50_meta.json')
meta.write_text(json.dumps({
    'seed': SEED, 'n': int(len(hold)),
    'per_field': hold['campo'].value_counts().to_dict(),
    'sha256': h, 'excluded': 'pct_monto_referencia_tecnica (regex tuneado sobre este set)',
    'limitation': 'agregados por campo vistos antes del freeze; item-level labels no',
    'frozen_at': '2026-09-14'}, indent=1))
print(f'congelados {len(hold)} -> {OUT}')
print('per_field:', hold['campo'].value_counts().to_dict())
print('sha256:', h[:16], '...')

"""Paso CERO del plan v4: congelar 50 held-out ANTES de analizar.

Split determinista seed 4242 sobre los 238 = 288(GT∩db, 5 campos) MENOS los 49 de
pct_monto_referencia_tecnica (regex tuneado sobre estos archivos → no held-out).
12-13 por campo. El script escribe el CSV y su sha256 PERO no imprime etiquetas
(individual-level ceguera). Limitación: los agregados por campo ya fueron vistos.

Sesión 23 (plan F0.6):
  --verify  re-genera el split en memoria y comprueba que su sha256 coincide con
            heldout50_meta.json y con el CSV en disco; no escribe nada.
  Sin flags y con la reserva ya congelada: se niega a sobrescribir (usar --force
  solo si se quiere re-congelar a propósito, lo que invalida la reserva).
  Nota (auditoría H2.2): la reserva es held-out para los MODELOS, no para las
  reglas deterministas de extraction_postprocess.py, validadas sobre los mismos
  archivos gt_por_campo.
"""
import argparse
import hashlib
import io
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FIELDS = ['dias_validez_garantia', 'anos_experiencia_minima_tecnica',
          'dias_pago_anticipo', 'dias_solicitud_anticipo']
GT_DIR = ROOT / 'data/processed/alolab/gt_por_campo'
SRC = {'dias_validez_garantia': 'dias_validez_garantia_GT.csv',
       'anos_experiencia_minima_tecnica': 'anos_experiencia_minima_tecnica_GT (1).csv',
       'dias_pago_anticipo': 'dias_pago_anticipo_GT.csv',
       'dias_solicitud_anticipo': 'dias_solicitud_anticipo_GT.csv'}
OUT = ROOT / 'data/processed/llm_benchmark/heldout50_frozen.csv'
META = ROOT / 'data/processed/llm_benchmark/heldout50_meta.json'
SEED = 4242


def build_split() -> pd.DataFrame:
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
    return hold


def csv_bytes(hold: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    hold.to_csv(buf, index=False)
    return buf.getvalue().encode('utf-8')


def sha_lf(data: bytes) -> str:
    return hashlib.sha256(data.replace(b'\r\n', b'\n')).hexdigest()


def verify() -> int:
    meta = json.loads(META.read_text(encoding='utf-8'))
    regen = sha_lf(csv_bytes(build_split()))
    disk = sha_lf(OUT.read_bytes())
    print(f'meta   : {meta["sha256"][:16]}…')
    print(f'regen  : {regen[:16]}…  {"OK" if regen == meta["sha256"] else "DIFIERE"}')
    print(f'disco  : {disk[:16]}…  {"OK" if disk == meta["sha256"] else "DIFIERE"}')
    return 0 if regen == disk == meta['sha256'] else 1


def freeze(force: bool) -> int:
    if META.exists() and not force:
        print(f'reserva ya congelada ({META.name}); usar --verify, o --force para '
              're-congelar (invalida la reserva)')
        return 2
    hold = build_split()
    data = csv_bytes(hold)
    OUT.write_bytes(data)
    h = sha_lf(data)
    META.write_text(json.dumps({
        'seed': SEED, 'n': int(len(hold)),
        'per_field': hold['campo'].value_counts().to_dict(),
        'sha256': h, 'excluded': 'pct_monto_referencia_tecnica (regex tuneado sobre este set)',
        'limitation': 'agregados por campo vistos antes del freeze; item-level labels no',
        'frozen_at': date.today().isoformat()}, indent=1))
    print(f'congelados {len(hold)} -> {OUT}')
    print('per_field:', hold['campo'].value_counts().to_dict())
    print('sha256:', h[:16], '...')
    return 0


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--verify', action='store_true')
    ap.add_argument('--force', action='store_true')
    a = ap.parse_args()
    sys.exit(verify() if a.verify else freeze(a.force))

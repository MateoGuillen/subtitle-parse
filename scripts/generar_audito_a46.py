"""Genera planillas/planilla_audito_A46_15.csv — auditoria exploratorio-cualitativa
del mecanismo caja-A (plan v4.1 A4.6, diseno pre-registrado 15-sep):

  validez  = semilla 434875 + 7 aleatorios de sus caja-A (seed 466, excl reserva) -> 8
  exp_tec  = 7 aleatorios de sus caja-A (seed 466, excl reserva)                   -> 7
  pago_A (10) excluido declaradamente. Techo: n=15 no estima proporciones.

Cada fila lleva la NOTA de agosto: el auditor tipifica ESA razon, no re-juzga el pliego.
"""
import math
import random
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
GT_DIR = ROOT / 'data/processed/alolab/gt_por_campo'
OUT = ROOT / 'data/processed/llm_benchmark/planillas/planilla_audito_A46_15.csv'
SEED = 466
SEMILLA_VALIDEZ = '434875'
FILE = {'dias_validez_garantia': GT_DIR / 'dias_validez_garantia_GT.csv',
        'anos_experiencia_minima_tecnica':
            GT_DIR / 'anos_experiencia_minima_tecnica_GT (1).csv'}
fz = pd.read_csv(ROOT / 'data/processed/llm_benchmark/heldout50_frozen.csv',
                 usecols=['nro_licitacion', 'campo'])
frozen = {(str(int(float(a))), b) for a, b in fz.values}


def isnull(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


rows = []
rng = random.Random(SEED)
for fld, n_take in [('dias_validez_garantia', 7),
                    ('anos_experiencia_minima_tecnica', 7)]:
    g = pd.read_csv(FILE[fld]).drop_duplicates('nro_licitacion')
    pool = [r for r in g.itertuples()
            if not isnull(r.db_value) and isnull(r.ground_truth)
            and (str(int(float(r.nro_licitacion))), fld) not in frozen]
    picks = []
    if fld == 'dias_validez_garantia':
        seed_row = [r for r in pool
                    if str(int(float(r.nro_licitacion))) == SEMILLA_VALIDEZ]
        assert seed_row, 'semilla no esta en caja-A validez'
        picks.append(seed_row[0])
        pool = [r for r in pool
                if str(int(float(r.nro_licitacion))) != SEMILLA_VALIDEZ]
    picks += rng.sample(pool, n_take)
    assert len(picks) == (8 if fld == 'dias_validez_garantia' else 7)
    for r in picks:
        rows.append(dict(
            campo=fld, nro_licitacion=str(int(float(r.nro_licitacion))),
            valor_regex=str(r.db_value), nota_agosto=str(r.nota)[:900],
            texto=(str(r.text) if not isnull(r.text) else '')[:5000],
            mech_plantilla_vacia='', mech_clausula_mal_anclada='',
            mech_sobre_abstencion_revisor='', mech_otra='',
            evidencia_frag='', disenso_sugerido=''))
out = pd.DataFrame(rows)
out.to_csv(OUT, index=False)
print(f'{len(out)} filas -> {OUT}')
print(out.groupby(['campo']).size().to_dict())
print('semilla incluida:', SEMILLA_VALIDEZ in set(out['nro_licitacion']))

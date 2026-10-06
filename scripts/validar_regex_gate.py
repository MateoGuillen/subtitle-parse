"""Valida el gate A1 con los regex reescritos (sesion 21): corre los 4 metodos nuevos
sobre los textos de los 200x5 GT y calcula P(regex==humano) + Wilson por campo,
poblacion = filas donde el regex NUEVO afirma valor (excl. reserva congelada).
Regla pre-registrada (plan v4.1 A1b): LCB>=0.60 SOSTIENE; punto<0.50 DEGRADA;
resto AMBIGUO->no entra. Compara contra la linea base del regex viejo.
"""
import math
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.etl.transformers.document_features_v2_transformer import (
    DocumentFeaturesV2Transformer)

GT_DIR = Path('data/processed/alolab/gt_por_campo')
METHOD = {'dias_validez_garantia': '_re_dias_validez_garantia',
          'anos_experiencia_minima_tecnica': '_re_anos_experiencia_minima_tecnica',
          'dias_pago_anticipo': '_re_dias_pago_anticipo',
          'dias_solicitud_anticipo': '_re_dias_solicitud_anticipo'}
FILE = {'dias_validez_garantia': 'dias_validez_garantia_GT.csv',
        'anos_experiencia_minima_tecnica': 'anos_experiencia_minima_tecnica_GT (1).csv',
        'dias_pago_anticipo': 'dias_pago_anticipo_GT.csv',
        'dias_solicitud_anticipo': 'dias_solicitud_anticipo_GT.csv'}


def num(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(str(v).strip().replace(',', '.'))
    except ValueError:
        return None


def wilson(k, n, z=1.96):
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def eq(a, b):
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) < 1e-9


def main():
    t = DocumentFeaturesV2Transformer.__new__(DocumentFeaturesV2Transformer)
    fz = pd.read_csv('data/processed/llm_benchmark/heldout50_frozen.csv',
                     usecols=['nro_licitacion', 'campo'])
    frozen = {(str(int(float(a))), b) for a, b in fz.values}
    print(f"{'campo':34s} {'n':>4s} {'R_nuevo':>8s} {'gate':>22s} | viejo")
    for f, meth in METHOD.items():
        g = pd.read_csv(GT_DIR / FILE[f]).drop_duplicates('nro_licitacion')
        g = g[[(str(int(float(n))), f) not in frozen
               for n in g['nro_licitacion']]]
        fn = getattr(t, meth)
        k = n = 0
        for _, r in g.iterrows():
            v = fn(str(r['text'] or ''))
            if v is None:
                continue
            n += 1
            k += eq(num(v), num(r['ground_truth']))
        lo, hi = wilson(k, n) if n else (0, 0)
        if lo >= 0.60:
            gate = 'SOSTIENE'
        elif (k / n if n else 0) < 0.50:
            gate = 'DEGRADA'
        else:
            gate = 'AMBIGUO->no entra'
        print(f'{f:34s} {n:4d} {k/n*100 if n else 0:7.1f}% '
              f'{gate:>22s}')


if __name__ == '__main__':
    main()

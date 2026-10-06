"""A4.7 — planilla_audito_a47_12.csv: auditoria de SUSTANCIA del 239/239 (C3).

Gate pre-registrado (revision externa 15-sep, binomial verificado P(X>=7|n=8,p=.5)=3.5%):
  estrato GATE  = 8 casos-M de dias_pago_anticipo (llm==humano != regex)
                  >=7/8 sustantivos -> pago-M FIRME (7/8 con nota del caso fallido)
                  <=6/8             -> pago-M degradado a caja-A; A4.5 -> hipotesis
  estrato descriptivo = 1 exp + 2 sol + 1 validez (nunca activan degradacion)

El auditor juzga el TEXTO (no la nota): 'el valor de la etiqueta aparece en el texto
como plazo del campo correcto?' si/no/otro. Muestreo determinista seed 999.
"""
import json
import math
import random
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
R = ROOT / 'data/processed/llm_benchmark'
GT_DIR = R.parent / 'alolab' / 'gt_por_campo'
FILE = {'dias_validez_garantia': 'dias_validez_garantia_GT.csv',
        'anos_experiencia_minima_tecnica': 'anos_experiencia_minima_tecnica_GT (1).csv',
        'dias_pago_anticipo': 'dias_pago_anticipo_GT.csv',
        'dias_solicitud_anticipo': 'dias_solicitud_anticipo_GT.csv'}


def isnull(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


m_cases = {f: [] for f in FILE}
res = json.load(open(R / 'C3_Llama_31_8B-FP8.json'))['results']
for r in res:
    if r['a_m'] == 'M':
        m_cases[r['field']].append(r)
rng = random.Random(999)
picks = rng.sample(m_cases['dias_pago_anticipo'], 8)
desc = (rng.sample(m_cases['anos_experiencia_minima_tecnica'], 1)
        + rng.sample(m_cases['dias_solicitud_anticipo'], 2)
        + rng.sample(m_cases['dias_validez_garantia'], 1))
rows = []
for src, grp in [(picks, 'GATE-pago'), (desc, 'descriptivo')]:
    for r in src:
        g = pd.read_csv(GT_DIR / FILE[r['field']]).drop_duplicates('nro_licitacion')
        row = g[g['nro_licitacion'].astype(str).map(
            lambda x: str(int(float(x))) == r['nro'])].iloc[0]
        rows.append(dict(
            grupo=grp, campo=r['field'], nro_licitacion=r['nro'],
            valor_regex=r['db'], etiqueta_humana=r['gt'], llm_value=r['llm_value'],
            nota_agosto=str(row['nota'])[:700],
            texto=str(row['text'])[:6000],
            valor_en_texto_como_plazo_correcto='', evidencia_frag='',
            otro_valor_posible_en_texto=''))
out = pd.DataFrame(rows)
out.to_csv(R / 'planillas/planilla_audito_a47_12.csv', index=False)
print(f'{len(out)} filas (GATE={sum(out.grupo=="GATE-pago")}, '
      f'descriptivo={sum(out.grupo=="descriptivo")}) -> planillas/planilla_audito_a47_12.csv')
print(out[['grupo', 'campo', 'nro_licitacion', 'valor_regex', 'etiqueta_humana']].to_string())
print('\nsemillas M disponibles: pago', len(m_cases['dias_pago_anticipo']),
      'exp', len(m_cases['anos_experiencia_minima_tecnica']),
      'sol', len(m_cases['dias_solicitud_anticipo']),
      'val', len(m_cases['dias_validez_garantia']))

"""B — Genera planillas de anotacion humana (plan v4.1 §3):
   1) planilla_iaa_25.csv     : re-anotacion ciega de 25 del exploratorio (para kappa)
   2) planilla_estrat_75.csv  : 75 docs conf<0.8 reales (validacion de dominio)
Ciegas: sin etiquetas humanas del set origen. Incluye portada de limitaciones.
"""
import json
import random
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'data/processed/llm_benchmark/planillas'
GT_DIR = ROOT / 'data/processed/alolab/gt_por_campo'
FIELDS = ['dias_validez_garantia', 'anos_experiencia_minima_tecnica',
          'dias_pago_anticipo', 'dias_solicitud_anticipo']
FILE = {f: GT_DIR / f'{f}_GT.csv' for f in FIELDS}
FILE['anos_experiencia_minima_tecnica'] = GT_DIR / 'anos_experiencia_minima_tecnica_GT (1).csv'
OUT.mkdir(exist_ok=True)
rng = random.Random(20260915)

# ---------- 1) IAA 25 ----------
fz = pd.read_csv(ROOT / 'data/processed/llm_benchmark/heldout50_frozen.csv',
                 usecols=['nro_licitacion', 'campo'])
frozen = {(str(int(float(a))), b) for a, b in fz.values}
rows = []
for f in FIELDS:
    g = pd.read_csv(FILE[f]).drop_duplicates('nro_licitacion')
    pool = [(str(int(float(n))), txt) for n, txt in
            g[['nro_licitacion', 'text']].values
            if (str(int(float(n))), f) not in frozen]
    pick = rng.sample(pool, min(7, len(pool)))
    for n, txt in pick:
        rows.append(dict(campo=f, nro_licitacion=n,
                         texto=(txt or '')[:6000],
                         tu_valor='', tu_es_null='no', ambiguo='no', notas=''))
iaa = pd.DataFrame(rows)
iaa = iaa.sample(frac=1, random_state=1).reset_index(drop=True)[:25]
iaa.to_csv(OUT / 'planilla_iaa_25.csv', index=False)

# ---------- 2) Estratificado 75 (conf < 0.8 reales) ----------
import psycopg2
from config.settings import DB_CONFIG
cfg = dict(DB_CONFIG); cfg['dbname'] = cfg.pop('database')
conn = psycopg2.connect(**cfg); cur = conn.cursor()
rows = []
TITLE = {'dias_validez_garantia': 'periodo de validez de la garantia de los bienes',
         'anos_experiencia_minima_tecnica': 'capacidad tecnica',
         'dias_pago_anticipo': 'solicitud de pago de anticipo',
         'dias_solicitud_anticipo': 'solicitud de pago de anticipo'}
for f in FIELDS:
    title = TITLE[f]
    # poblacion real del fallback: el regex NO encontro nada (conf=0, valor 0.0
    # placeholder) pero el doc TIENE la seccion del titulo (hay texto para juzgar)
    cur.execute(
        f'SELECT s.nro_licitacion, s.content_text FROM dncp.pliegos_secciones s '
        f'JOIN dncp.document_features_v2 v ON v.nro_licitacion = s.nro_licitacion '
        f'WHERE s.title_normalized = %s AND v."{f}_conf" = 0 '
        f'AND length(s.content_text) > 200 '
        f'ORDER BY RANDOM() LIMIT 19', (title,))
    for n, txt in cur.fetchall():
        rows.append(dict(campo=f, nro_licitacion=str(n),
                         texto=(txt or '')[:6000], tu_valor='', tu_es_null='no',
                         ambiguo='no', notas=''))
conn.close()
est = pd.DataFrame(rows)
est.to_csv(OUT / 'planilla_estratificado_75.csv', index=False)

portada = """# Portada — instrucciones de anotacion (igual que GT-100)
Guia de criterios: valor = NUMERO tal como lo define el campo (dias del plazo
PROPIO del campo; si la seccion lista varios plazos plausibles, marca ambiguo=s i
tu lectura no es la unica defendible). tu_es_null=s si la clausula existe pero la
plantilla esta sin completar, o no hay plazo explicito. NO te dejes influir por
conf_regex (esta oculto a proposito en la IAA).
Limitaciones pre-registradas (van a la tesis): kappa con n=25 tiene IC ancho ->
techo orientativo, no definitivo; estratificado n=75 con conf<0.8 es la validacion
de dominio que faltaba pero tampoco tiene poder para desempates finos.
"""
(OUT / 'LEEME_anotacion.md').write_text(portada)
print('IAA:', len(iaa), '| Estrat:', len(est), '->', OUT)
print(est['campo'].value_counts().to_dict() if len(est) else '')

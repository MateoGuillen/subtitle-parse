"""A4 — Taxonomia de desacuerdos referencia-humano + score ponderado + baseline tonto.

Cajas pre-registradas (deterministas, sobre texto):
  A. AUSENCIA-FP    humano=NULL y regex afirma valor (plantilla vacia / clause sin plazo)
  M. AMBIGUO-MULTI  el numero del regex Y el humano estan ambos en el texto (>=2
                    numeros plausibles; eligieron distintos) -> nadie "miente"
  H. ARTEFACTO-REGEX el numero del regex NO esta en el texto (parse/normalizacion)
  E. ERROR-REAL     el numero humano esta en el texto y el regex lo paso por alto
                    (incl. valores en palabras: el regex solo ve digitos)
  O. OTRA           resto (lectura manual)
Score: campos gateados (sol SOSTIENE, pct caveat; degrados/ambiguo peso 0).
Baseline tonto: valor modal del pseudo por campo.
"""
import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
GT_DIR = ROOT / 'data/processed/alolab/gt_por_campo'
OUT = ROOT / 'data/processed/llm_benchmark'
FIELDS = ['dias_validez_garantia', 'anos_experiencia_minima_tecnica',
          'dias_pago_anticipo', 'dias_solicitud_anticipo']
ALLF = FIELDS + ['pct_monto_referencia_tecnica']
FILE = {f: GT_DIR / f'{f}_GT.csv' for f in ALLF}
FILE['anos_experiencia_minima_tecnica'] = GT_DIR / 'anos_experiencia_minima_tecnica_GT (1).csv'
NUM = re.compile(r'(?<![\d.,])\d{1,3}(?:[.,]\d{1,2})?(?![\d])')


def nums(text):
    s = set()
    for tok in NUM.findall(text or ''):
        try:
            s.add(float(tok.replace(',', '.')))
        except ValueError:
            pass
    return s


def to_num(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(str(v).strip().replace(',', '.'))
    except ValueError:
        return None


def classify(db, gt, text):
    gtn, dbn = to_num(gt), to_num(db)
    if gtn is None:
        return 'A. AUSENCIA-FP'
    if dbn is None:
        return 'O. OTRA'
    T = nums(text)
    if abs(dbn - gtn) < 1e-9:
        return None  # acuerdo
    db_in, gt_in = dbn in T or abs(dbn * 100 in T or dbn / 100 in T) if False else dbn in T, gtn in T or abs(gtn * 100) in T
    if not db_in and gt_in:
        return 'E. ERROR-REAL (el humano lo vio; el regex no esta en el texto)'
    if not db_in:
        return 'H. ARTEFACTO-REGEX'
    if db_in and gt_in:
        return 'M. AMBIGUO-MULTI'
    if gt_in is False and db_in:
        return 'E2. ERROR-REAL-humano-verbal'  # humano con numero no literal (palabra/fecha)
    return 'O. OTRA'


def main():
    fz = pd.read_csv(OUT / 'heldout50_frozen.csv', usecols=['nro_licitacion', 'campo'])
    frozen = {(str(int(float(a))), b) for a, b in fz[['nro_licitacion', 'campo']].values}
    rows = []
    counts = {f: [0, 0] for f in ALLF}  # [poblacion, desacuerdos]
    for f in ALLF:
        g = pd.read_csv(FILE[f]).drop_duplicates('nro_licitacion')
        g = g[g['db_value'].notna()]
        g = g[[(str(int(float(n))), f) not in frozen
               for n in g['nro_licitacion']]]
        for n, db, gt, txt in g[['nro_licitacion', 'db_value', 'ground_truth',
                                  'text']].values:
            counts[f][0] += 1
            caja = classify(db, gt, txt)
            if caja:
                counts[f][1] += 1
                rows.append(dict(campo=f, nro=str(int(float(n))), caja=caja))
    t = pd.DataFrame(rows)
    piv = t.pivot_table(index='caja', columns='campo', values='nro',
                        aggfunc='count', fill_value=0)
    for f in ALLF:
        if f not in piv.columns:
            piv[f] = 0
    pobl = pd.Series({f: counts[f][0] - counts[f][1] for f in ALLF})
    pob2 = pd.Series({f: counts[f][0] for f in ALLF})
    fia = pd.Series({f: round((counts[f][0] - counts[f][1]) / max(1, counts[f][0]) * 100, 1)
                     for f in ALLF})
    piv.loc['ACUERDOS', pobl.index] = pobl.values
    piv.loc['POBLACION', pobl.index] = pob2.values
    piv.loc['FIABILIDAD%', pobl.index] = fia.values
    piv['TOTAL'] = [piv.loc[i, ALLF].sum() if i != 'FIABILIDAD%' else ''
                    for i in piv.index]
    piv.to_csv(OUT / 'A4_taxonomia_regex_vs_humano.csv')
    print('== Taxonomia regex-vs-humano (exploratorio) ==')
    print(piv.assign(_s=pd.to_numeric(piv['TOTAL'], errors='coerce'))
          .sort_values('_s', ascending=False).drop(columns='_s').to_string())

    p9 = json.load(open(ROOT / 'data/processed/llm_benchmark/pseudo900.json'))['samples']
    df = pd.DataFrame(p9)
    print('\n== Baseline tonto (siempre el valor modal del campo) vs Llama ==')
    for f in ALLF:
        s = df[df['field'] == f]
        modal = s['pseudo'].mode().iloc[0]
        acc = (s['pseudo'] == modal).mean()
        print(f'  {f:34s} n={len(s):3d} modal={modal:6.2f} acc_baseline={acc:.1%}')

    A3 = pd.read_csv(OUT / 'A3_agreement_recalibrada.csv')
    acc_sol = A3[A3['seta'].str.startswith('Pseudo-recal')].set_index('modelo')['acc']
    acc_pct = A3[A3['seta'].str.contains('pct-cota')].set_index('modelo')['acc']
    schemes = {'propuesto(3x pct,1x sol)': {f: w for f, w in
                                            [('p', 3), ('s', 1)]},
               'plano(1x,1x)': {'p': 1, 's': 1},
               'extremo(5x pct,1x sol)': {'p': 5, 's': 1}}
    print('\n== Score ponderado sobre campos gateados ==')
    ranks = {}
    for name, w in schemes.items():
        tot = w['p'] + w['s']
        score = {m: (acc_pct[m] * w['p'] + acc_sol[m] * w['s']) / tot
                 for m in acc_pct.index}
        order = sorted(score, key=score.get, reverse=True)
        ranks[name] = order
        print(f'  {name:28s} ' + ' > '.join(m.split('_')[0] for m in order[:4])
              + f'  (top={score[order[0]]:.3f})')
    same = all(v == ranks[list(schemes)[0]] for v in ranks.values())
    print(f'  todos los esquemas ordenan identico: {same} '
          '(sol es identico entre modelos -> el score = affine de pct)')
    json.dump({'tabla': piv.to_dict(), 'ranks': ranks, 'esquemas_iguales': same},
              open(OUT / 'A4_score_baselines.json', 'w'), indent=1, default=str)
    print('guardados A4_taxonomia_regex_vs_humano.csv + A4_score_baselines.json')


if __name__ == '__main__':
    main()

"""A1 — Triangulación: ¿confía el acuerdo-con-regex en la accuracy humana? (plan v4.1 §2-A1)

REGISTRO PREVIO A VER RESULTADOS (escrito antes de ejecutar):
- Gate formal = φ por campo sobre los solapados 288∩867, regla: n<25 -> INCONCLUYENTE
  por poder; n>=25: φ̂>=0.3 y IC95 bootstrap (10k, semilla fija) inferior > 0 -> SOSTIENE.
- Como se preveía que 4/5 campos tendrían n<25, el plan designa estadístico DOMINANTE:
  fiabilidad de la referencia R = P(regex==humano) sobre la población producción-relevante
  (filas con db_value no nulo = regex afirma un valor; GT-NULL cuenta como regex equivocado),
  set EXPLORATORIO (excluye reserva congelada y, para el agregado, pct_monto por tuning).
  Umbral de decisión del dominante, fijado AHORA:
    SOSTIENE       si LCB95(Wilson) >= 0.60
    DEGRADA        si punto        <  0.50
    AMBIGUO        en otro caso  -> a efectos del agregado se trata como DEGRADA (in dubio,
                                   no entra; consistente con "inconcluso != sostiene" del plan).
- pct_monto: poblacion exploratoria sin pct (contaminado); se calcula aparte y SOLO con
  la etiqueta "cota optimista". No entra a ningun agregado.
- exp_tecnica con n<25 en solapados -> φ inconcluso; el dominante resuelve.
- Matriz condicional A1c y φ de solapados: descriptivos NO independientes (limitacion fijada).
"""
import json
import math
import random
import statistics
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
F5_DIR = ROOT / "data/processed/llm_benchmark/multimodel_results"
OUT = ROOT / "data/processed/llm_benchmark/A1_triangulacion.json"
SEED_BOOT = 777
FIELDS = ['dias_validez_garantia', 'anos_experiencia_minima_tecnica',
          'dias_pago_anticipo', 'dias_solicitud_anticipo',
          'pct_monto_referencia_tecnica']
FILE = {f: f for f in FIELDS}
FILE['anos_experiencia_minima_tecnica'] = 'anos_experiencia_minima_tecnica_GT (1)'
FILE = {f: (GT_DIR / f'{FILE[f] if "(" in FILE[f] else f}_GT.csv')
        if "(" not in FILE[f] else (GT_DIR / f'{FILE[f]}.csv') for f in FIELDS}
FROZEN_FILE = ROOT / 'data/processed/llm_benchmark/heldout50_frozen.csv'


def to_num(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return float(str(v).strip().replace(',', '.'))
    except ValueError:
        return None


def match(a, b, field):
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    if field.startswith('pct_'):
        return abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6
    return False


def wilson(k, n, z=1.96):
    if n == 0:
        return (float('nan'), float('nan'))
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def classify_A_M(lvn, psn, gtn):
    """Particion deterministica de los desacuerdos llm!=pseudo (pre-registrada 15-sep,
    revision externa punto 1). Reglas estructurales, sin lectura de texto:
      A: humano=NULL y llm=NULL (regex afirma; coincidencia por abstencion)
      M: humano=valor, llm=valor igual al humano y distinto del pseudo (resolucion)
      O: resto.  NO se aplica a pct_monto (su historia es otra, ver plan §2-A4.5)."""
    if gtn is None and lvn is None:
        return 'A'
    if (gtn is not None and lvn is not None and psn is not None
            and abs(lvn - gtn) < 1e-9 and abs(lvn - psn) > 1e-9):
        return 'M'
    return 'O'


def phi(bs):
    a = sum(1 for x, y in bs if x and y)
    b = sum(1 for x, y in bs if x and not y)
    c = sum(1 for x, y in bs if not x and y)
    d = sum(1 for x, y in bs if not x and not y)
    den = math.sqrt((a + b) * (c + d) * (a + c) * (b + d)) if (a+b) and (c+d) and (a+c) and (b+d) else 0
    return ((a * d - b * c) / den) if den > 0 else float('nan')


def boot_phi_ci(bs, n_iter=10000):
    pts = [v for v in (phi(random.sample(bs, len(bs))) for _ in range(n_iter))
           if not math.isnan(v)]
    if not pts:
        return (float('nan'), float('nan'))
    pts.sort()
    return (pts[int(0.025 * len(pts))], pts[int(0.975 * len(pts))])


def load_sets():
    frozen = set()
    fz = pd.read_csv(FROZEN_FILE, usecols=['nro_licitacion', 'campo'])
    frozen = {(str(int(float(r[0]))), r[1]) for r in fz.values}
    rows = {}
    for fld in FIELDS:
        g = pd.read_csv(FILE[fld])
        g = g.drop_duplicates(subset='nro_licitacion')
        rec = [(str(int(float(n))), to_num(db), to_num(gt))
               for n, db, gt in g[['nro_licitacion', 'db_value', 'ground_truth']].values]
        rows[fld] = rec
    return frozen, rows


def main():
    random.seed(SEED_BOOT)
    frozen, rows = load_sets()

    # ---- poblaciones de solapados: (nro,fld) presentes en Fase5_*.json ----
    f5 = {}
    for fp in sorted(F5_DIR.glob('Fase5_*.json')):
        model = fp.stem[6:]
        for r in json.load(open(fp))['results']:
            f5.setdefault((str(r['nro']), r['field']), {})[model] = \
                (r['llm_value'], r['pseudo'], r['agree'])
    print(f'solapados 288∩867 con GT-file: ', end='')
    overlap_n = {}
    for fld in FIELDS:
        ov = [n for n, db, gt in rows[fld] if (n, fld) in f5]
        overlap_n[fld] = len(ov)
    print(overlap_n)

    # sanity: pseudo == db_value de agosto en solapados (v2 reconstruido despues)
    mism = sum(1 for fld in FIELDS for n, db, gt in rows[fld]
               if (n, fld) in f5 and db is not None
               and next(iter(f5[(n, fld)].values()))[1] is not None
               and abs(db - next(iter(f5[(n, fld)].values()))[1]) > 1e-9)
    print(f'pseudo(sep) != db_value(ago) en solapados: {mism}')

    # ---- A1-dominante: fiabilidad de la referencia ----
    print('\n== A1-dominante: P(regex==humano | regex afirma valor), exploratorio ==')
    dom = {}
    for fld in FIELDS:
        pop = [(db, gt) for n, db, gt in rows[fld]
               if db is not None and (n, fld) not in frozen]
        k = sum(1 for db, gt in pop if match(db, gt, fld))
        lo, hi = wilson(k, len(pop))
        dom[fld] = dict(n=len(pop), k=k, p=round(k / len(pop), 3),
                        wilson=[round(lo, 3), round(hi, 3)])
        print(f'{fld:34s} n={len(pop):3d} R={k/len(pop)*100:5.1f}% '
              f'IC95[{lo*100:.0f},{hi*100:.0f}]')

    # ---- gate por campo ----
    print('\n== Gate A1b (phi en solapados si n>=25; dominante si no) ==')
    gate = {}
    for fld in FIELDS:
        ov = [(n, db, gt) for n, db, gt in rows[fld] if (n, fld) in f5]
        ov = [(n, db, gt) for n, db, gt in ov if (n, fld) not in frozen]
        per_model = {}
        for model in {m for v in f5.values() for m in v}:
            bs = []
            for n, db, gt in ov:
                lv = to_num(f5[(n, fld)][model][0])
                bs.append((match(lv, f5[(n, fld)][model][1], fld)
                           if f5[(n, fld)][model][1] is not None else False,
                           match(lv, gt, fld)))
            if len(bs) >= 25:
                p = phi(bs)
                lo, hi = boot_phi_ci(bs)
                per_model[model] = (round(p, 3), round(lo, 3), round(hi, 3), len(bs))
        any_phi = [m for m, v in per_model.items() if v[0] == v[0]]
        if fld == 'pct_monto_referencia_tecnica':
            decision = 'SOLO CON COTA OPTIMISTA (no entra a agregados)'
            note = f'phi por modelo (n={per_model})'
        else:
            # phi queda inconcluso si n_solapados<25 (nota en 'note'); decide el
            # dominante, como fija el pre-registro
            d = dom[fld]
            if d['wilson'][0] >= 0.60:
                decision = 'SOSTIENE (dominante LCB>=0.60)'
            elif d['p'] < 0.50:
                decision = 'DEGRADA (dominante punto<0.50)'
            else:
                decision = 'AMBIGUO->no entra (in dubio pro referencia)'
            note = f'phi n_solapados={len(ov)} (<25 inconcluso) ' + (
                f'phi={per_model}' if any_phi else '')
        gate[fld] = dict(decision=decision, note=note, phi_models=per_model)
        print(f'{fld:34s} -> {decision}   [{note[:90]}]')

    # ---- A1c matriz condicional (descriptiva, no independiente) ----
    print('\n== A1c matriz condicional sobre solapados (pooled modelos) ==')
    mat = {}
    for fld in FIELDS:
        ov = [(n, db, gt) for n, db, gt in rows[fld] if (n, fld) in f5
              and (n, fld) not in frozen]
        agree_ok = agree_no = dis_llm = dis_re = dis_neither = dis_n = 0
        a_m = {'A': 0, 'M': 0, 'O': 0}
        for n, db, gt in ov:
            for model, (lv, ps, ag) in f5[(n, fld)].items():
                lvn = to_num(lv)
                x = match(lvn, ps, fld) if ps is not None else False
                y = match(lvn, gt, fld)
                if x:
                    agree_ok += y
                    agree_no += not y
                else:
                    dis_n += 1
                    rh = match(db, gt, fld)
                    if y and not rh:
                        dis_llm += 1
                        if not fld.startswith('pct_'):
                            a_m[classify_A_M(lvn, to_num(ps), gt)] += 1
                    elif rh and not y:
                        dis_re += 1
                    elif not y and not rh:
                        dis_neither += 1
        mat[fld] = dict(P_correcta_si_acuerdo=round(
                            agree_ok / max(1, agree_ok + agree_no), 3),
                        n_desacuerdos=dis_n, razon_LLM=dis_llm,
                        razon_regex=dis_re, ninguno=dis_neither,
                        a_m_desacuerdos_llm_razon=a_m)
        m = mat[fld]
        print(f'{fld:34s} P(hum|acuerdo)={m["P_correcta_si_acuerdo"]*100:.0f}% '
              f'| desacuerdos={dis_n}: LLM {dis_llm} / regex {dis_re} '
              f'/ ninguno {dis_neither}')

    OUT.write_text(json.dumps(
        dict(seed=SEED_BOOT, overlap_n=overlap_n, pseudo_vs_db_mismatch=mism,
             dominante=dom, gate=gate, matriz_A1c=mat),
        ensure_ascii=False, indent=1))
    print(f'\nguardado -> {OUT}')


if __name__ == '__main__':
    main()

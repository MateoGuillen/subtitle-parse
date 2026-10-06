"""A3 — Significancia (plan v4.1 §2-A3). Dos métricas NUNCA fusionadas:

  stats_accuracy_humana()  -> GT-100: acc(match) + Wilson + bootstrap; McNemar top-3.
  stats_agreement()        -> Pseudo-867 RECALIBRADA por gate A1: solo el campo que
                              SOSTIENE (dias_solicitud_anticipo, 180 m); pct_monto se
                              calcula aparte etiquetado "cota_optimista (tuning)".

Limitaciones fijadas aquí (pre-registradas): GT-100 sirvió para ajustar hints
(Sesión 14, AloLab) -> valiosa para comparaciones pareadas (mismos ítems, todos los
modelos) pero NO como held-out limpio. Los 180 sol son la única Pseudo-867 que el
gate A1 sostiene. McNemar top-3 con Bonferroni alpha=0.05/3=0.0167.
"""
import csv
import json
import math
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIR = ROOT / 'data/processed/llm_benchmark/multimodel_results'
OUTD = ROOT / 'data/processed/llm_benchmark'
MODELS = ['Llama_31_8B-FP8', 'Llama_31_8B-AWQ', 'Qwen3-8B-FP8', 'Qwen3-14B-AWQ',
          'Ministral_3_14B', 'Mistral_7B_v03-AWQ', 'EXAONE-35-78B-AWQ',
          'Gemma_4_12B', 'Granite_42_8B', 'Mistral_Small_24B-AWQ']
TOP3 = ['Llama_31_8B-FP8', 'Llama_31_8B-AWQ', 'Qwen3-8B-FP8']
SOL = 'dias_solicitud_anticipo'
PCT = 'pct_monto_referencia_tecnica'
ALPHA_MCNEMAR = 0.05 / 3
SEED = 555


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 1.0)
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def boot_ci(vals, n_iter=10000):
    if not vals:
        return (0.0, 1.0)
    rng = random.Random(SEED)
    N = len(vals)
    means = sorted(sum(rng.choices(vals, k=N)) / N for _ in range(n_iter))
    return (means[int(0.025 * n_iter)], means[int(0.975 * n_iter)])


def mcnemar(items_a, items_b, keys):
    b = sum(1 for k in keys if items_a[k] and not items_b[k])
    c = sum(1 for k in keys if items_b[k] and not items_a[k])
    n = b + c
    p = 1.0 if n == 0 else min(1.0, 2 * sum(
        math.comb(n, i) for i in range(min(b, c) + 1)) / 2 ** n)
    return b, c, p, p < ALPHA_MCNEMAR


def per_model_items(models, keyf, valf, prefix=''):
    out = {}
    for m in models:
        fp = DIR / f'{prefix}{m}.json'
        if not fp.exists():
            continue
        recs = json.load(open(fp))['results']
        out[m] = {keyf(r): valf(r) for r in recs}
    return out


def stats_accuracy_humana():
    """GT-100: accuracy contra humano. Output propio."""
    items = per_model_items(MODELS, lambda r: (r['nro'], r['field']),
                            lambda r: bool(r['match']))
    rows, percs = [], {}
    for m, d in items.items():
        vals = [int(v) for v in d.values()]
        percs[m] = d
        k = sum(vals)
        lo, hi = wilson(k, len(vals))
        blo, bhi = boot_ci(vals)
        rows.append(dict(seta='GT-100-humana', modelo=m, n=len(vals), k=k,
                         acc=round(k / len(vals), 3),
                         wilson=f'[{lo:.2f},{hi:.2f}]',
                         bootstrap=f'[{blo:.2f},{bhi:.2f}]'))
    rows.sort(key=lambda r: -r['acc'])
    # McNemar top-3 sobre intersection de items
    keys = set.intersection(*[set(percs[m]) for m in TOP3 if m in percs])
    for i in range(len(TOP3)):
        for j in range(i + 1, len(TOP3)):
            a, b_ = percs[TOP3[i]], percs[TOP3[j]]
            bb, cc, p, sig = mcnemar(a, b_, keys)
            rows.append(dict(seta='GT-100-humana',
                             modelo=f'McNemar {TOP3[i]} vs {TOP3[j]}',
                             n=len(keys), k=f'b={bb},c={cc}', acc=round(p, 4),
                             wilson='', bootstrap=f'sig(a=0.0167)={sig}'))
    return rows


def stats_agreement():
    """Pseudo-867 recalibrada: solo campo SOSTENIDO + pct aparte (cota optimista)."""
    items = per_model_items(MODELS, lambda r: (str(r['nro']), r['field']),
                            lambda r: bool(r['agree']), prefix='Fase5_')
    rows, sol_p, pct_p = [], {}, {}
    for m, d in items.items():
        sol = [int(v) for (n, f), v in d.items() if f == SOL]
        pct = [int(v) for (n, f), v in d.items() if f == PCT]
        sol_p[m], pct_p[m] = d, d
        for label, vals in ((f'Pseudo-recal({SOL})', sol),
                            ('Pseudo-pct-cota-optimista', pct)):
            k = sum(vals)
            lo, hi = wilson(k, len(vals))
            blo, bhi = boot_ci(vals)
            rows.append(dict(seta=label, modelo=m, n=len(vals), k=k,
                             acc=round(k / len(vals), 3),
                             wilson=f'[{lo:.2f},{hi:.2f}]',
                             bootstrap=f'[{blo:.2f},{bhi:.2f}]'))
    rows.sort(key=lambda r: (r['seta'], -r['acc']))
    for i in range(len(TOP3)):
        for j in range(i + 1, len(TOP3)):
            keys = {kk for kk in sol_p[TOP3[i]] if sol_p[TOP3[i]][kk] is not None
                    and kk in sol_p[TOP3[j]] and sol_p[TOP3[i]][kk] is not None}
            keys = {kk for kk in keys if kk[1] == SOL}
            bb, cc, p, sig = mcnemar(sol_p[TOP3[i]], sol_p[TOP3[j]], keys)
            rows.append(dict(seta=f'McNemar-recal({SOL})',
                             modelo=f'{TOP3[i]} vs {TOP3[j]}', n=len(keys),
                             k=f'b={bb},c={cc}', acc=round(p, 4), wilson='',
                             bootstrap=f'sig(a=0.0167)={sig}'))
    return rows


def save(rows, name):
    with open(OUTD / name, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f'guardado -> {OUTD / name}')


def main():
    random.seed(SEED)
    acc = stats_accuracy_humana()
    agr = stats_agreement()
    print('== accuracy humana (GT-100) ==')
    for r in acc:
        print(f"{r['modelo']:34s} {str(r['acc']):>6s} {r['wilson']} {r['bootstrap']}")
    print('== agreement recalibrada (solo SOSTENIDO + pct aparte) ==')
    for r in agr:
        print(f"{r['seta'][:26]:26s} {r['modelo']:34s} "
              f"{str(r['acc']):>6s} {r['wilson']} {r['bootstrap']}")
    save(acc, 'A3_accuracy_humana_GT100.csv')
    save(agr, 'A3_agreement_recalibrada.csv')


if __name__ == '__main__':
    main()

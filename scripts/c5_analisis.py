"""Analisis C5 — clasificacion de replicacion pre-registrada:
  interseccion limpia por modelo: items sin ERROR en base(Fase5) y en P2,P3,P4.
  inversion(modelo-par, variante) := acc(variante) del modelo A cae por debajo del B
  donde base(B) <= base(A) — en la practica: P_x revierte el orden FP8>AQ>QW ->
  contar de {P2,P3,P4} cuantas INVIERTE (QWEN supera a FP8 sobre la misma interseccion).
  Estados: 3/3 estable, 2/3 real-frágil, 1/3 artefacto de redaccion, 0/3 retirar.
  P2 sobre 288 humanos: accuracy-humano por modelo x campo, vs base C3.
"""
import json
from pathlib import Path

R = Path('data/processed/llm_benchmark')
MODELS = ['Llama_31_8B-FP8', 'Llama_31_8B-AWQ', 'Qwen3-8B-FP8']

d = {}
for m in MODELS:
    c5 = json.load(open(R / f'C5_replica_{m}.json'))
    base5 = json.load(open(R / 'multimodel_results' / f'Fase5_{m}.json'))['results']
    base = {str(r['nro']) + '|' + r['field']: (r['agree'], str(r['llm_raw']))
            for r in base5}
    var = {v: {i['nro'] + '|' + i['field']: i for i in c5['variants'][v]['items']}
           for v in ['P2', 'P3', 'P4']}
    ok = [k for k, (a, raw) in base.items()
          if not raw.startswith('ERROR') and all(k in var[v] for v in var)]
    n = len(ok)
    acc = {'base': sum(bool(base[k][0]) for k in ok) / n}
    for v in var:
        acc[v] = sum(bool(x['agree_pseudo']) for x in var[v].values()
                     if x['nro'] + '|' + x['field'] in set(ok)) / n
    d[m] = {'n': n, 'acc': {k: round(v, 3) for k, v in acc.items()}}

print('== agreement-vs-pseudo sobre INTERSECCION LIMPIA (por modelo) ==')
for m, r in d.items():
    print(f"  {m:20s} n={r['n']:3d} base={r['acc']['base']:.3f} "
          f"P2={r['acc']['P2']:+.3f} P3={r['acc']['P3']:.3f} P4={r['acc']['P4']:.3f} "
          f"(deltas P2{r['acc']['P2']-r['acc']['base']:+.3f} "
          f"P3{r['acc']['P3']-r['acc']['base']:+.3f} "
          f"P4{r['acc']['P4']-r['acc']['base']:+.3f})")

inv = {v: 0 for v in ['P2', 'P3', 'P4']}
for v in ['P2', 'P3', 'P4']:
    fp = d['Llama_31_8B-FP8']['acc'][v]
    qw = d['Qwen3-8B-FP8']['acc'][v]
    inv[v] = qw > fp
print('\ninversion FP8<-QWEN por rewrite:', {k: bool(v) for k, v in inv.items()},
      '-> conteo', sum(inv.values()), '/3')

# ranking base y por variante (sobre intersecciones propias — se declara)
for tag in ['base', 'P2', 'P3', 'P4']:
    order = sorted(d, key=lambda m: -d[m]['acc'][tag])
    print(f'  ranking {tag}: ' + ' > '.join(f'{m.split("_")[0]}({d[m]["acc"][tag]:.3f})'
                                            for m in order))

print('\n== P2 sobre 288 humanos (accuracy-humano, interseccion con C3) ==')
for m in MODELS:
    c5 = json.load(open(R / f'C5_replica_{m}.json'))
    c3 = json.load(open(R / f'C3_{"Llama_31_8B-FP8" if m=="Llama_31_8B-FP8" else ("Llama_31_8B-AWQ" if m=="Llama_31_8B-AWQ" else "Qwen3-8B-FP8")}.json'))['results']
    c3d = {str(r['nro']) + '|' + r['field']: r for r in c3}
    it = {i['nro'] + '|' + i['field']: i for i in c5['P2_288']['items']}
    ok = [k for k in it if k in c3d and str(c3d[k]['llm_raw'])[:5] != 'ERROR']
    dias = [k for k in ok if not k.split('|')[1].startswith('pct_')]
    pct = [k for k in ok if k.split('|')[1].startswith('pct_')]
    a_d3 = sum(bool(c3d[k]['m_human']) for k in dias) / max(1, len(dias))
    a_d2 = sum(bool(it[k]['m_human']) for k in dias) / max(1, len(dias))
    a_p3 = sum(bool(c3d[k]['m_human']) for k in pct) / max(1, len(pct))
    a_p2 = sum(bool(it[k]['m_human']) for k in pct) / max(1, len(pct))
    print(f'  {m:20s} DIAS base={a_d3:.3f} P2={a_d2:.3f} | pct base={a_p3:.3f} '
          f'P2={a_p2:.3f} (n dias={len(dias)}, pct={len(pct)}, '
          f'excluidos={len(c5["P2_288"]["failed"])})')
json.dump(d, open(R / 'C5_analisis.json', 'w'), indent=1)
print('\nguardado C5_analisis.json')

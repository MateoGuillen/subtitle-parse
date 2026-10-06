"""A2 — Tasa de validez de schema x10 (plan v4.1 §2-A2).

Guía de decisión pre-registrada:
- vLLM+guided: raw deberia ser exactamente `numero` o `NULL`. Validez = match full
  del regex ^(-?\\d+(?:[.,]\\d+)?|NULL)$ sobre llm_raw.
- GGUF plain (Granite/Gemma/Ministral): no hubo guided -> validez N/A; se reporta
  parseo exitoso (llm_value no None o NULL explicito) como metrica paralela.
- Caveat truncado: llm_raw truncado a 80 chars en logging (solo logging, no generacion):
  Gemma 720/867 y Ministral 9/867 tocan el tope -> su tasa desde disco queda sesgada
  a INVALIDO; se marca "PENDING_RELOG" (sesion GPU fusionada, plan §4).
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
F5 = ROOT / 'data/processed/llm_benchmark/multimodel_results'
OUT = ROOT / 'data/processed/llm_benchmark/A2_schema_valid.csv'
VALID = re.compile(r'^(-?\d+(?:[.,]\d+)?|NULL)$')
GGUF = {'Granite_42_8B', 'Gemma_4_12B', 'Ministral_3_14B'}
TRUNC_LIMIT = 80

rows = []
for fp in sorted(F5.glob('Fase5_*.json')):
    model = fp.stem[6:]
    res = json.load(open(fp))['results']
    n = len(res)
    trunc = sum(1 for r in res if len(str(r['llm_raw'])) >= TRUNC_LIMIT)
    valid = sum(1 for r in res
                if VALID.match(str(r['llm_raw']).strip().split('\n')[0])
                and len(str(r['llm_raw']).strip()) <= TRUNC_LIMIT
                and '\n' not in str(r['llm_raw']).strip())
    parsed = sum(1 for r in res if r['llm_value'] is not None
                 or str(r['llm_raw']).strip().upper().startswith('NULL'))
    kind = 'GGUF-plain' if model in GGUF else 'vLLM-guided'
    rows.append({
        'modelo': model, 'tipo': kind, 'n': n,
        'valid_rate': round(valid / n, 3) if kind == 'vLLM-guided' else '',
        'parse_ok_rate': round(parsed / n, 3),
        'truncados_80': trunc,
        'estado': ('PENDING_RELOG (84% raw al tope)' if model == 'Gemma_4_12B'
                   else 'PENDING_RELOG (1% al tope)' if model == 'Ministral_3_14B'
                   else 'OK'),
    })

with open(OUT, 'w', encoding='utf-8') as f:
    import csv
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

for r in rows:
    print(f"{r['modelo']:24s} {r['tipo']:11s} valid={str(r['valid_rate']):>5s} "
          f"parse={r['parse_ok_rate']:.3f} trunc={r['truncados_80']:3d} {r['estado']}")
print('guardado ->', OUT)

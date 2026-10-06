"""Registra anotaciones del pseudo-GT Opus 5.5 en pseudo_gt_opus/anotaciones.json (no toca muestra.csv).
Uso: /root/vllm-env/bin/python scripts/anotar_pseudo_gt.py '<json {id: [valor|null, estado, nota]}>'
estado: "" (valor), "no_fija" (el pliego no fija el valor), "ambiguo".
"""
import json
import sys
from pathlib import Path

P = Path(__file__).resolve().parents[1] / "data/processed/llm_benchmark/pseudo_gt_opus/anotaciones.json"
cur = json.loads(P.read_text(encoding="utf-8")) if P.exists() else {}
new = json.loads(sys.argv[1])
cur.update(new)
P.write_text(json.dumps(cur, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
print(f"+{len(new)} -> {len(cur)} anotados")

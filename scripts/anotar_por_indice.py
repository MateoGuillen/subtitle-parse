"""Anota por índice dentro de un campo (mismo orden que ver_muestra_pseudo_gt.py).
Uso: anotar_por_indice.py <campo> '<json {"default": [valor, estado, nota], "idx": {"18": [..]}}>'
"default" se aplica a todos los índices no listados en "idx".
"""
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
d = pd.read_csv(ROOT / "data/processed/llm_benchmark/pseudo_gt_opus/muestra.csv")
d = d[d.campo == sys.argv[1]].reset_index(drop=True)
spec = json.loads(sys.argv[2])
out = {}
for i, r in d.iterrows():
    v = spec.get("idx", {}).get(str(i), spec.get("default"))
    if v is None:
        raise SystemExit(f"falta anotación para índice {i}")
    out[r.id] = v
subprocess.run([sys.executable, str(ROOT / "scripts/anotar_pseudo_gt.py"), json.dumps(out)], check=True)

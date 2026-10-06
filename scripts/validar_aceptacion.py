"""Valida una planilla de aceptación auditada: precisión por campo + IC Wilson + decisión.

Reglas pre-registradas (27-sep):
  LCB95 >= 0.85  -> PROMOTE (el campo se publica a producción)
  punto  < 0.70  -> HOLD (no se promueve; se ajusta prompt/parse y se re-corre)
  resto          -> juicio con notas (casos límite documentados uno por uno)
`veredicto` admite: si / no / otro(=medio punto: valor cercano pero inexacto).
Uso: /root/vllm-env/bin/python scripts/validar_aceptacion.py planilla_aceptacion_A_valores.csv
"""
import math
import sys
from pathlib import Path

import pandas as pd

PLANILLAS = Path(__file__).resolve().parents[1] / "data" / "processed" / "llm_benchmark" / "planillas"


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 1.0)
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def main():
    fp = PLANILLAS / sys.argv[1]
    df = pd.read_csv(fp, dtype=str).fillna("")
    pending = (df["veredicto"].str.strip() == "").sum()
    if pending:
        print(f"pendiente de auditar: {pending}/{len(df)} filas — nada que validar todavía")
        return
    print(f"{'campo':34s} {'n':>4s} {'prec':>6s} {'IC95':>13s} decisión")
    for campo, g in df.groupby("campo"):
        v = g["veredicto"].str.strip().str.lower()
        k = (v == "si").sum() + 0.5 * (v == "otro").sum()
        n = len(g)
        lo, hi = wilson(k, n)
        p = k / n
        dec = "PROMOTE" if lo >= 0.85 else ("HOLD" if p < 0.70 else "JUICIO")
        print(f"{campo:34s} {n:4d} {p:6.1%} [{lo:.2f},{hi:.2f}] {dec}")
        bad = g[~v.isin(["si", "otro"])]
        for _, r in bad.head(5).iterrows():
            print(f"    x nro={r['nro_licitacion']} llm={r['valor_llm']} "
                  f"corr={r['valor_correcto_si_no'] or '?'} :: {r['notas'][:80]}")


if __name__ == "__main__":
    main()

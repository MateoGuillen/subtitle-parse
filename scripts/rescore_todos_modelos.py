"""Re-score de TODOS los modelos del benchmark sobre salida cruda (extensión de F1, CPU).

F1 solo re-midió a los 3 finalistas (los únicos con C3). Este script usa los JSON que ya
existen para los ~15 modelos probados:
  - GT-100 humano (multimodel_results/<modelo>.json; 5 campos × 20): crudo vs humano,
    publicado (post) y baseline siempre-NULL con el mismo postproceso.
  - Fase 5 (867 pseudo-GT v1, 10 modelos): crudo vs pseudo y post, separando pct
    (pseudo-GT = regex tuneado: referencia débil, solo descriptivo).
Salida: data/processed/llm_benchmark/F1b_todos_modelos.csv + tabla en consola.
Uso: /root/vllm-env/bin/python scripts/rescore_todos_modelos.py
"""
import json
import math
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.etl.transformers.extraction_postprocess import parse_response  # noqa: E402

B = ROOT / "data/processed/llm_benchmark"
RES = B / "multimodel_results"
RAW_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")
PCT = "pct_monto_referencia_tecnica"


def num(v):
    try:
        x = float(str(v).strip().replace(",", "."))
        return None if math.isnan(x) else x
    except (ValueError, TypeError):
        return None


def raw_num(raw):
    """Número literal de la respuesta. Para modelos sin guided (GGUF) se toma la primera
    línea, que es el contrato de esos runs (A2: 'primera línea válida')."""
    s = (raw or "").strip().splitlines()[0].strip() if (raw or "").strip() else ""
    return float(s.replace(",", ".")) if RAW_RE.match(s) else None


def eq(a, b, f):
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    return f.startswith("pct_") and (abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6)


def main():
    texts = json.loads((B / "gt_texts.json").read_text(encoding="utf-8"))
    rows = []
    for p in sorted(RES.glob("*.json")):
        if p.name.startswith(("Fase5", "Conc", "C3", "C4", "C5")):
            continue
        d = json.loads(p.read_text(encoding="utf-8"))
        rs = d.get("results", [])
        if not rs or "gt" not in rs[0]:
            continue
        model = d.get("model") or p.stem
        agg = {"dias": [0, 0, 0, 0], "pct": [0, 0, 0, 0]}  # n, crudo, post, null
        for r in rs:
            f = r["field"]
            gt = num(r["gt"])
            text = texts.get(str(r["nro"]), "")
            k = "pct" if f == PCT else "dias"
            agg[k][0] += 1
            agg[k][1] += eq(raw_num(r.get("llm_raw")), gt, f)
            agg[k][2] += bool(r.get("match"))
            agg[k][3] += eq(num(parse_response("NULL", field=f, text=text)), gt, f)
        rows.append(dict(set="GT100_humano", modelo=model,
                         dias_n=agg["dias"][0], dias_crudo=agg["dias"][1], dias_post=agg["dias"][2],
                         dias_null=agg["dias"][3], pct_n=agg["pct"][0], pct_crudo=agg["pct"][1],
                         pct_post=agg["pct"][2], pct_null=agg["pct"][3]))
    for p in sorted(list(RES.glob("Fase5_*.json")) + list((RES / "fase5_v1").glob("Fase5_*.json"))):
        d = json.loads(p.read_text(encoding="utf-8"))
        if str(d.get("set", "")).startswith("pseudo900-v2"):
            continue  # se usa la versión v1 (misma referencia para los 10 modelos)
        rs = d["results"]
        agg = {"dias": [0, 0, 0], "pct": [0, 0, 0]}
        for r in rs:
            k = "pct" if r["field"] == PCT else "dias"
            agg[k][0] += 1
            agg[k][1] += eq(raw_num(r.get("llm_raw")), num(r["pseudo"]), r["field"])
            agg[k][2] += bool(r["agree"])
        rows.append(dict(set="Fase5_pseudo_v1", modelo=d["model"],
                         dias_n=agg["dias"][0], dias_crudo=agg["dias"][1], dias_post=agg["dias"][2],
                         pct_n=agg["pct"][0], pct_crudo=agg["pct"][1], pct_post=agg["pct"][2]))
    df = pd.DataFrame(rows)
    df.to_csv(B / "F1b_todos_modelos.csv", index=False)
    pd.set_option("display.width", 220)
    for s, g in df.groupby("set"):
        g = g.copy()
        g["dias_crudo_%"] = (100 * g.dias_crudo / g.dias_n).round(1)
        g["pct_crudo_%"] = (100 * g.pct_crudo / g.pct_n).round(1)
        print(f"\n== {s}")
        cols = ["modelo", "dias_n", "dias_crudo", "dias_crudo_%", "dias_post", "pct_n",
                "pct_crudo", "pct_crudo_%", "pct_post"]
        if s == "GT100_humano":
            cols.insert(5, "dias_null")
        print(g.sort_values("dias_crudo", ascending=False)[cols].to_string(index=False))


if __name__ == "__main__":
    main()

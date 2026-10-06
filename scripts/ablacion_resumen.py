"""F6.1 — Tabla de ablación final contra GT humano (12 campos hybrid × 200), CPU.

Lee F4_ablacion_gt_<modelo>.json (crudos ya generados por scripts/ablacion_gt.py) y agrega:
  modal     baseline "responder siempre el valor más frecuente del GT" (o NULL)
  regex     extractor regex de producción (DocumentFeaturesV2Transformer)
  regla     reglas deterministas de extraction_postprocess (rule_only)
  llm       modelo solo (llm_only)  <- única métrica limpia del MODELO
  pipeline  lo que hace producción: regex -> si None, regla -> si no aplica, LLM
Caveat (H2.2): regex y reglas se desarrollaron sobre estos mismos GT -> cotas optimistas
in-sample. El brazo llm es limpio. Validación fuera de muestra: F5 (estratificado-75).

Salida: docs/ablacion_gt.md + data/processed/llm_benchmark/F6_ablacion_resumen.csv
"""
import json
import math
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.etl.transformers.document_features_v2_transformer import (  # noqa: E402
    DocumentFeaturesV2Transformer)
from src.etl.transformers.extraction_postprocess import resolve  # noqa: E402

B = ROOT / "data/processed/llm_benchmark"
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
MODELS = {"qwen3-8b-fp8": "Qwen3-8B-FP8", "llama-fp8": "Llama-3.1-8B-FP8"}


def num(v):
    try:
        x = float(str(v).strip().replace(",", "."))
        return None if math.isnan(x) else x
    except (ValueError, TypeError):
        return None


def eq(a, b, f):
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    return f.startswith("pct_") and (abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6)


def wilson(k, n, z=1.96):
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def main():
    schemas = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    slug_of = {f["name"]: (k, f["type"]) for k, v in schemas.items() if k != "meta"
               for f in v["fields"]}
    tr = DocumentFeaturesV2Transformer(str(SCHEMAS))
    texts = {}
    for p in GT_DIR.glob("*.csv"):
        if p.name.startswith("errores_"):
            continue
        g = pd.read_csv(p)
        for r in g.itertuples():
            texts[(r.campo, str(r.nro_licitacion))] = str(r.text)
    rows = []
    for tag, label in MODELS.items():
        items = json.loads((B / f"F4_ablacion_gt_{tag}.json").read_text(encoding="utf-8"))["items"]
        by_f = {}
        for it in items:
            by_f.setdefault(it["field"], []).append(it)
        for f, lst in sorted(by_f.items()):
            slug, ftype = slug_of[f]
            it_type = "int" if ftype in ("int", "binary") else "float"
            gts = [it["gt"] for it in lst]
            modal = Counter(gts).most_common(1)[0][0]
            k = Counter()
            for it in lst:
                text = texts[(f, it["nro"])]
                rx = tr._extract_field_text(text, slug, f, ftype)
                rx = None if rx is None else float(rx)
                pipe = rx if rx is not None else resolve(f, it["raw"], text, mode="rule_first",
                                                         ftype=it_type).value
                pipe = None if pipe is None else float(pipe)
                k["modal"] += eq(modal, it["gt"], f)
                k["regex"] += eq(rx, it["gt"], f)
                k["regla"] += it["ok_regla"]
                k["llm"] += it["ok_llm"]
                k["pipeline"] += eq(pipe, it["gt"], f)
            rows.append(dict(modelo=label, campo=f, n=len(lst), **{a: k[a] for a in
                        ("modal", "regex", "regla", "llm", "pipeline")}))
    df = pd.DataFrame(rows)
    df.to_csv(B / "F6_ablacion_resumen.csv", index=False)
    arms = ["modal", "regex", "regla", "llm", "pipeline"]
    md = ["# Ablación de 3+2 brazos contra GT humano (F4.5/F6.1)", "",
          "> `scripts/ablacion_gt.py` (GPU, crudos) + `scripts/ablacion_resumen.py` (CPU). 12 campos "
          "hybrid × 200 textos con etiqueta humana (`gt_por_campo`).",
          "> ⚠ **regex** y **regla** fueron desarrollados sobre estos mismos GT: son cotas "
          "**in-sample** (optimistas). **llm** es la única métrica limpia del modelo. "
          "Validación fuera de muestra de reglas/regex: F5 (estratificado-75).", ""]
    for label in MODELS.values():
        d = df[df.modelo == label]
        md += [f"## {label}", "", "| campo | n | " + " | ".join(arms) + " |",
               "|---|---|" + "---|" * len(arms)]
        for r in d.itertuples():
            md.append(f"| {r.campo} | {r.n} | " + " | ".join(str(getattr(r, a)) for a in arms) + " |")
        tot = {a: int(d[a].sum()) for a in arms}
        n = int(d.n.sum())
        md.append(f"| **TOTAL** | {n} | " + " | ".join(
            f"**{tot[a]}** ({tot[a]/n:.1%}, IC [{wilson(tot[a], n)[0]:.2f},{wilson(tot[a], n)[1]:.2f}])"
            for a in arms) + " |")
        md.append("")
    (ROOT / "docs/ablacion_gt.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


if __name__ == "__main__":
    main()

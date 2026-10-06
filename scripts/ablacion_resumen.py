"""F6.1 — Tabla de ablación final contra GT humano para TODOS los modelos retesteados (CPU).

Lee data/processed/llm_benchmark/F4_ablacion_gt_<modelo>.json (crudos de scripts/ablacion_gt.py,
protocolo único: prompt de producción + decodificación restringida número|NULL) y reporta:
  Tabla A — 12 campos hybrid × 200 (2.400): modal · regex · regla · llm · pipeline
            (pipeline = regex -> regla -> LLM, como producción; y orden alternativo regla -> regex -> LLM)
  Tabla B — por modelo: LLM solo en hybrid-12, en el subconjunto C3 dias (GT∩regex con valor,
            239 en los 4 campos dias) y en pct_monto (C3 pct, 49); McNemar exacto del brazo LLM
            de cada modelo contra el de referencia (Qwen3-8B-FP8), Bonferroni por nº de comparaciones.
Caveat (H2.2): regex y reglas se desarrollaron sobre estos GT -> cotas in-sample. El brazo
LLM es limpio (ningún modelo vio estas etiquetas).

Salida: docs/ablacion_gt.md + data/processed/llm_benchmark/F6_ablacion_resumen.csv
"""
import json
import math
import sys
from collections import Counter
from math import comb
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.etl.transformers.document_features_v2_transformer import (  # noqa: E402
    DocumentFeaturesV2Transformer)
from src.etl.transformers.extraction_postprocess import (  # noqa: E402
    NO_RULE, resolve, rule_value)

B = ROOT / "data/processed/llm_benchmark"
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
PCT = "pct_monto_referencia_tecnica"
DIAS = ["dias_validez_garantia", "anos_experiencia_minima_tecnica",
        "dias_pago_anticipo", "dias_solicitud_anticipo"]
REF = "qwen3-8b-fp8"
LABEL = {"qwen3-8b-fp8": "Qwen3-8B-FP8", "llama-fp8": "Llama-3.1-8B-FP8",
         "exaone-3.5-7.8b-awq": "EXAONE-3.5-7.8B-AWQ", "qwen3-14b-awq": "Qwen3-14B-AWQ",
         "gemma-4-12b-gguf": "Gemma-4-12B (GGUF)", "ministral-3-14b-gguf": "Ministral-3-14B (GGUF)",
         "qwen3.8-27b-gguf": "Qwen3.8-27B UD-Q3_K_XL (GGUF)"}


def eq(a, b, f):
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    return f.startswith("pct_") and (abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6)


def wilson(k, n, z=1.96):
    if n == 0:
        return (float("nan"), float("nan"))
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def mcnemar(x, y):
    b = sum(1 for a, c in zip(x, y) if a and not c)
    c = sum(1 for a, c_ in zip(x, y) if c_ and not a)
    n = b + c
    p = 1.0 if n == 0 else min(1.0, 2 * sum(comb(n, i) for i in range(min(b, c) + 1)) / 2 ** n)
    return b, c, p


def fmt(k, n):
    lo, hi = wilson(k, n)
    return f"{k}/{n} ({k/n:.1%}) [{lo:.2f},{hi:.2f}]"


def main():
    schemas = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    slug_of = {f["name"]: (k, f["type"]) for k, v in schemas.items() if k != "meta"
               for f in v["fields"]}
    hybrid = sorted(f for f, (k, t) in slug_of.items()
                    if any(x["name"] == f and x.get("extraction_method") == "hybrid"
                           for x in schemas[k]["fields"]))
    tr = DocumentFeaturesV2Transformer(str(SCHEMAS))
    texts = {}
    for p in GT_DIR.glob("*.csv"):
        if not p.name.startswith("errores_"):
            for r in pd.read_csv(p).itertuples():
                texts[(r.campo, str(r.nro_licitacion))] = str(r.text)

    runs = {}
    for p in sorted(B.glob("F4_ablacion_gt_*.json")):
        tag = p.stem.replace("F4_ablacion_gt_", "")
        d = json.loads(p.read_text(encoding="utf-8"))
        if not any(it["field"] == PCT for it in d["items"]):
            continue  # corrida vieja sin protocolo unificado
        runs[tag] = d
    order = [t for t in LABEL if t in runs] + [t for t in runs if t not in LABEL]

    # deterministas (independientes del modelo) + brazos por modelo
    det = {}
    rows, keyed = [], {}
    for tag in order:
        items = runs[tag]["items"]
        kv = {}
        for it in items:
            f, key = it["field"], (it["field"], it["nro"])
            t = "int" if slug_of[f][1] in ("int", "binary") else "float"
            text = texts[key]
            if key not in det:
                rx = tr._extract_field_text(text, slug_of[f][0], f, slug_of[f][1])
                det[key] = (None if rx is None else float(rx), rule_value(f, text))
            rx, rv = det[key]
            llm = resolve(f, it["raw"], text, mode="llm_only", ftype=t).value
            sys_rf = resolve(f, it["raw"], text, mode="rule_first", ftype=t).value
            pipe = rx if rx is not None else sys_rf
            alt = (rv if rv is not NO_RULE else (rx if rx is not None else llm))
            kv[key] = dict(gt=it["gt"], db=it.get("db"), llm=eq(llm, it["gt"], f),
                           pipe=eq(pipe, it["gt"], f), alt=eq(alt, it["gt"], f),
                           regex=eq(rx, it["gt"], f),
                           regla=eq(None if rv is NO_RULE else rv, it["gt"], f),
                           err=it["raw"] == "ERROR")
        keyed[tag] = kv
        for f in hybrid + [PCT]:
            sub = [v for (ff, _), v in kv.items() if ff == f]
            gts = [v["gt"] for v in sub]
            modal = Counter(gts).most_common(1)[0][0]
            rows.append(dict(modelo=LABEL.get(tag, tag), tag=tag, campo=f, n=len(sub),
                             modal=sum(eq(modal, g, f) for g in gts),
                             regex=sum(v["regex"] for v in sub), regla=sum(v["regla"] for v in sub),
                             llm=sum(v["llm"] for v in sub), pipeline=sum(v["pipe"] for v in sub),
                             regla_primero=sum(v["alt"] for v in sub),
                             errores=sum(v["err"] for v in sub)))
    df = pd.DataFrame(rows)
    df.to_csv(B / "F6_ablacion_resumen.csv", index=False)

    def subset(tag, which):
        kv = keyed[tag]
        if which == "hybrid":
            return {k: v for k, v in kv.items() if k[0] in hybrid}
        if which == "c3dias":
            return {k: v for k, v in kv.items() if k[0] in DIAS and v["gt"] is not None
                    and v["db"] is not None}
        return {k: v for k, v in kv.items() if k[0] == PCT and v["gt"] is not None
                and v["db"] is not None}

    md = ["# Ablación contra GT humano — protocolo único para 6 modelos (F4.5/F6.1)", "",
          "> `scripts/ablacion_gt.py` (GPU) + `scripts/ablacion_resumen.py` (CPU). Mismo prompt de producción y "
          "decodificación restringida número|NULL (vLLM: xgrammar; GGUF: llama-server con GBNF equivalente).",
          "> ⚠ **regex** y **regla** se desarrollaron sobre estos GT: cotas **in-sample**. **LLM solo** es la "
          "única métrica limpia del modelo.", "",
          "## B. Modelos — LLM solo (sin reglas)", "",
          f"McNemar exacto vs {LABEL[REF]}; α Bonferroni = 0,05/{max(len(order)-1, 1)}.", "",
          "| modelo | hybrid-12 (2.400) | C3 dias (GT∩regex) | C3 pct | McNemar hybrid-12 vs ref (b/c, p) |",
          "|---|---|---|---|---|"]
    alpha = 0.05 / max(len(order) - 1, 1)
    summ = []
    for tag in order:
        h, c3, pc = subset(tag, "hybrid"), subset(tag, "c3dias"), subset(tag, "pct")
        kh = sum(v["llm"] for v in h.values())
        mc = "—"
        if tag != REF and REF in keyed:
            ref = subset(REF, "hybrid")
            ks = sorted(set(h) & set(ref))
            b, c, p = mcnemar([h[k]["llm"] for k in ks], [ref[k]["llm"] for k in ks])
            mc = f"{b}/{c}, p={p:.2g}{' *' if p < alpha else ''}"
        md.append(f"| {LABEL.get(tag, tag)} | {fmt(kh, len(h))} | "
                  f"{fmt(sum(v['llm'] for v in c3.values()), len(c3))} | "
                  f"{fmt(sum(v['llm'] for v in pc.values()), len(pc))} | {mc} |")
        summ.append((tag, kh, len(h)))
    md += ["", "`*` = significativo tras Bonferroni.", ""]
    md += ["## A. Brazos por modelo (12 campos hybrid, 2.400 ítems)", "",
           "| modelo | modal | regex (in-sample) | regla (in-sample) | **LLM solo** | pipeline regex→regla→LLM | pipeline regla→regex→LLM | errores |",
           "|---|---|---|---|---|---|---|---|"]
    for tag in order:
        d = df[(df.tag == tag) & (df.campo.isin(hybrid))]
        n = int(d.n.sum())
        md.append(f"| {LABEL.get(tag, tag)} | {int(d.modal.sum())} | {int(d.regex.sum())} | "
                  f"{int(d.regla.sum())} | **{int(d.llm.sum())}** ({d.llm.sum()/n:.1%}) | "
                  f"{int(d.pipeline.sum())} ({d.pipeline.sum()/n:.1%}) | "
                  f"{int(d.regla_primero.sum())} ({d.regla_primero.sum()/n:.1%}) | {int(d.errores.sum())} |")
    md += ["", "## C. LLM solo por campo", "",
           "| campo | modal | " + " | ".join(LABEL.get(t, t) for t in order) + " |",
           "|---|---|" + "---|" * len(order)]
    for f in hybrid + [PCT]:
        cells = [str(int(df[(df.tag == t) & (df.campo == f)].llm.iloc[0])) for t in order]
        modal = int(df[(df.tag == order[0]) & (df.campo == f)].modal.iloc[0])
        md.append(f"| {f} | {modal} | " + " | ".join(cells) + " |")
    (ROOT / "docs/ablacion_gt.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


if __name__ == "__main__":
    main()

"""Varianza del benchmark: GT humano (gt_por_campo, in-sample para reglas) vs pseudo-GT Opus 5.5
(344 ítems fuera de muestra, anotados a ciegas; ambiguos excluidos). CPU.

Reporta:
  A. Por modelo: LLM solo en GT humano (12 hybrid) y en pseudo-GT (13 campos y 12 hybrid),
     ranking en cada referencia y correlación de rankings (Spearman ρ, Kendall τ).
  B. Capas deterministas FUERA DE MUESTRA (pseudo-GT): regex de producción, reglas, y pipeline
     (regex → regla → LLM con el mejor modelo) por campo -> responde H2.2.
  C. LLM solo por campo en el pseudo-GT.
Caveat: el pseudo-GT es de un LLM (Opus 5.5), no humano; es un segundo anotador independiente.
Salida: docs/pseudo_gt_varianza.md + data/processed/llm_benchmark/F7_pseudo_resumen.csv
"""
import json
import math
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from ablacion_resumen import LABEL, eq, wilson  # noqa: E402
from src.etl.transformers.document_features_v2_transformer import (  # noqa: E402
    DocumentFeaturesV2Transformer)
from src.etl.transformers.extraction_postprocess import NO_RULE, resolve, rule_value  # noqa: E402

B = ROOT / "data/processed/llm_benchmark"
SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
PSEUDO = B / "pseudo_gt_opus"


def rank(values):
    """Rangos promedio (1 = mejor) para correlaciones con empates."""
    order = sorted(range(len(values)), key=lambda i: -values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        for k in range(i, j + 1):
            ranks[order[k]] = (i + j) / 2 + 1
        i = j + 1
    return ranks


def spearman(a, b):
    ra, rb = rank(a), rank(b)
    n = len(a)
    ma, mb = sum(ra) / n, sum(rb) / n
    num = sum((x - ma) * (y - mb) for x, y in zip(ra, rb))
    den = math.sqrt(sum((x - ma) ** 2 for x in ra) * sum((y - mb) ** 2 for y in rb))
    return num / den


def kendall(a, b):
    n, c, d = len(a), 0, 0
    for i in range(n):
        for j in range(i + 1, n):
            s = (a[i] - a[j]) * (b[i] - b[j])
            c += s > 0
            d += s < 0
    return (c - d) / (n * (n - 1) / 2)


def main():
    schemas = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    slug_of = {f["name"]: (k, f["type"]) for k, v in schemas.items() if k != "meta"
               for f in v["fields"]}
    hybrid = {f["name"] for k, v in schemas.items() if k != "meta" for f in v["fields"]
              if f.get("extraction_method") == "hybrid"}
    muestra = pd.read_csv(PSEUDO / "muestra.csv").set_index("id")
    gt_res = pd.read_csv(B / "F6_ablacion_resumen.csv")
    tr = DocumentFeaturesV2Transformer(str(SCHEMAS))

    rows, per_field, det_done = [], [], {}
    for p in sorted(B.glob("F7_pseudo_opus_*.json")):
        tag = p.stem.replace("F7_pseudo_opus_", "")
        items = json.loads(p.read_text(encoding="utf-8"))["items"]
        ok = {"all": [], "hyb": []}
        for it in items:
            f = it["field"]
            t = "int" if slug_of[f][1] in ("int", "binary") else "float"
            text = str(muestra.loc[it["id"], "texto"])
            llm = resolve(f, it["raw"], text, mode="llm_only", ftype=t).value
            good = eq(None if llm is None else float(llm), it["gt"], f)
            ok["all"].append(good)
            if f in hybrid:
                ok["hyb"].append(good)
            per_field.append(dict(tag=tag, campo=f, ok=good))
            if it["id"] not in det_done:
                rx = tr._extract_field_text(text, slug_of[f][0], f, slug_of[f][1])
                rv = rule_value(f, text)
                det_done[it["id"]] = dict(campo=f, gt=it["gt"],
                                          regex=None if rx is None else float(rx), regla=rv)
        g = gt_res[(gt_res.tag == tag) & (gt_res.campo.isin(hybrid))]
        rows.append(dict(tag=tag, modelo=LABEL.get(tag, tag),
                         gt_llm=int(g.llm.sum()), gt_n=int(g.n.sum()),
                         ps_llm=sum(ok["all"]), ps_n=len(ok["all"]),
                         ps_hyb=sum(ok["hyb"]), ps_hyb_n=len(ok["hyb"])))
    df = pd.DataFrame(rows)
    df["gt_p"] = df.gt_llm / df.gt_n
    df["ps_p"] = df.ps_llm / df.ps_n
    df["ps_hyb_p"] = df.ps_hyb / df.ps_hyb_n
    df["rank_gt"] = rank(list(df.gt_p))
    df["rank_ps"] = rank(list(df.ps_p))
    df.to_csv(B / "F7_pseudo_resumen.csv", index=False)
    rho = spearman(list(df.gt_p), list(df.ps_p))
    tau = kendall(list(df.gt_p), list(df.ps_p))
    rho_h = spearman(list(df.gt_p), list(df.ps_hyb_p))

    # B. deterministas fuera de muestra + pipeline con el mejor modelo del pseudo-GT
    best = df.sort_values("ps_p", ascending=False).iloc[0].tag
    best_items = {it["id"]: it for it in json.loads(
        (B / f"F7_pseudo_opus_{best}.json").read_text(encoding="utf-8"))["items"]}
    det_rows = []
    for iid, d in det_done.items():
        f = d["campo"]
        t = "int" if slug_of[f][1] in ("int", "binary") else "float"
        text = str(muestra.loc[iid, "texto"])
        rv = d["regla"]
        llm = resolve(f, best_items[iid]["raw"], text, mode="llm_only", ftype=t).value
        llm = None if llm is None else float(llm)
        pipe = d["regex"] if d["regex"] is not None else (
            (None if rv is None else float(rv)) if rv is not NO_RULE else llm)
        alt = ((None if rv is None else float(rv)) if rv is not NO_RULE else
               (d["regex"] if d["regex"] is not None else llm))
        det_rows.append(dict(campo=f, regex=eq(d["regex"], d["gt"], f),
                             regla=(eq(None if rv is None else float(rv), d["gt"], f)
                                    if rv is not NO_RULE else None),
                             regla_aplica=rv is not NO_RULE,
                             pipeline=eq(pipe, d["gt"], f), regla_primero=eq(alt, d["gt"], f)))
    det = pd.DataFrame(det_rows)

    md = ["# Varianza del benchmark: GT humano vs pseudo-GT Opus 5.5 (fuera de muestra)", "",
          "> `scripts/generar_muestra_pseudo_gt.py` (muestra pre-registrada, sha256 en el repo) + anotación ciega "
          "de Claude Opus 5.5 (`pseudo_gt_opus/anotaciones.json`, commiteada antes de evaluar) + "
          "`scripts/ablacion_gt.py --set pseudo` + `scripts/pseudo_resumen.py`.",
          "> 344 ítems de documentos que NO están en `gt_por_campo`; 4 ambiguos excluidos → 340 evaluados. "
          "⚠ El pseudo-GT es de un LLM (segundo anotador independiente), no humano.", "",
          f"## A. Ranking de modelos (LLM solo) — Spearman ρ = **{rho:.2f}**, Kendall τ = **{tau:.2f}** "
          f"(ρ solo hybrid = {rho_h:.2f})", "",
          "| modelo | GT humano hybrid-12 | rank | pseudo-GT 13 campos | rank | pseudo-GT hybrid |",
          "|---|---|---|---|---|---|"]
    for r in df.sort_values("rank_gt").itertuples():
        md.append(f"| {r.modelo} | {r.gt_llm}/{r.gt_n} ({r.gt_p:.1%}) | {r.rank_gt:g} | "
                  f"{r.ps_llm}/{r.ps_n} ({r.ps_p:.1%}) [{wilson(r.ps_llm, r.ps_n)[0]:.2f},"
                  f"{wilson(r.ps_llm, r.ps_n)[1]:.2f}] | {r.rank_ps:g} | {r.ps_hyb}/{r.ps_hyb_n} ({r.ps_hyb_p:.1%}) |")
    md += ["", f"## B. Capas deterministas FUERA DE MUESTRA (respuesta a H2.2) — pipeline con {LABEL.get(best, best)}", "",
           "| campo | n | regex prod | regla (si aplica) | pipeline regex→regla→LLM | pipeline regla→regex→LLM |",
           "|---|---|---|---|---|---|"]
    for f, g in det.groupby("campo"):
        ga = g[g.regla_aplica]
        regla = f"{int(ga.regla.sum())}/{len(ga)}" if len(ga) else "—"
        md.append(f"| {f} | {len(g)} | {int(g.regex.sum())}/{len(g)} | {regla} | "
                  f"{int(g.pipeline.sum())}/{len(g)} | {int(g.regla_primero.sum())}/{len(g)} |")
    n = len(det)
    md.append(f"| **TOTAL** | {n} | {int(det.regex.sum())} ({det.regex.mean():.1%}) | "
              f"{int(det[det.regla_aplica].regla.sum())}/{int(det.regla_aplica.sum())} | "
              f"**{int(det.pipeline.sum())}** ({det.pipeline.mean():.1%}) | "
              f"**{int(det.regla_primero.sum())}** ({det.regla_primero.mean():.1%}) |")
    pf = pd.DataFrame(per_field)
    tags = list(df.sort_values("rank_ps").tag)
    md += ["", "## C. LLM solo por campo (pseudo-GT)", "",
           "| campo | n | " + " | ".join(LABEL.get(t, t) for t in tags) + " |",
           "|---|---|" + "---|" * len(tags)]
    for f, g in pf.groupby("campo"):
        cells = [str(int(g[g.tag == t].ok.sum())) for t in tags]
        md.append(f"| {f} | {len(g[g.tag == tags[0]])} | " + " | ".join(cells) + " |")
    (ROOT / "docs/pseudo_gt_varianza.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


if __name__ == "__main__":
    main()

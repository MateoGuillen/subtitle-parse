"""Corpus reproducible (solo archivos versionados) para los tests del postproceso.

Cada ítem: (cid, field, raw, text, ftype). `cid` es estable entre corridas y sirve de
clave del snapshot `tests/fixtures/legacy_snapshot.json` (plan F2.0).

Fuentes:
  - C3 / C5-P2 sobre los 288 humanos (texto desde gt_por_campo)
  - Fase 5 v1 (multimodel_results/fase5_v1) y v2 (texto desde pseudo900 v1/v2)
  - 150 fijos de C4 (P1, P2) y C5 (P2, P3, P4)
  - 26 archivos GT × 200 filas × respuestas sintéticas: NULL, llm_raw de agosto,
    el valor GT, "1.5", "15", "0", "7" (cubre rutas de reglas, escala y truncado)
"""
import json
import math
from functools import lru_cache
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
B = ROOT / "data/processed/llm_benchmark"
RES = B / "multimodel_results"
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
MODELS = ["Llama_31_8B-FP8", "Llama_31_8B-AWQ", "Qwen3-8B-FP8"]
SYNTH_RAWS = ["NULL", "1.5", "15", "0", "7"]
C3_FIELDS = ["dias_validez_garantia", "anos_experiencia_minima_tecnica",
             "dias_pago_anticipo", "dias_solicitud_anticipo",
             "pct_monto_referencia_tecnica"]


def _num_str(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "NULL"
    f = float(v)
    return str(int(f)) if f == int(f) else str(f)


@lru_cache(maxsize=1)
def field_types() -> dict:
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    return {f["name"]: f["type"] for k, v in s.items() if k != "meta" for f in v["fields"]}


def ftype_of(field: str) -> str:
    return field_types().get(field, "int" if field.startswith(("dias_", "anos_", "num_"))
                             else "float")


@lru_cache(maxsize=1)
def gt_frames() -> dict:
    out = {}
    for p in sorted(GT_DIR.glob("*.csv")):
        if p.name.startswith("errores_"):
            continue
        g = pd.read_csv(p)
        out[p.name] = g
    return out


@lru_cache(maxsize=1)
def gt_text_index() -> dict:
    idx = {}
    for g in gt_frames().values():
        for r in g.drop_duplicates("nro_licitacion").itertuples():
            idx.setdefault((str(int(float(r.nro_licitacion))), r.campo), str(r.text))
    return idx


@lru_cache(maxsize=1)
def pseudo_index() -> dict:
    out = {}
    for ver, name in (("v1", "pseudo900_v1_867.json"), ("v2", "pseudo900.json")):
        s = json.loads((B / name).read_text(encoding="utf-8"))["samples"]
        out[ver] = {(x["nro"], x["field"]): x for x in s}
    return out


def _load(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))


def iter_corpus():
    gti = gt_text_index()
    ps = pseudo_index()
    for m in MODELS:
        for r in _load(B / f"C3_{m}.json")["results"]:
            yield (f"C3|{m}|{r['nro']}|{r['field']}", r["field"], r["llm_raw"],
                   gti[(r["nro"], r["field"])], ftype_of(r["field"]))
        c5 = _load(B / f"C5_replica_{m}.json")
        for r in c5["P2_288"]["items"]:
            yield (f"C5P2_288|{m}|{r['nro']}|{r['field']}", r["field"], r["raw"],
                   gti[(r["nro"], r["field"])], ftype_of(r["field"]))
        for ver, path in (("v1", RES / "fase5_v1" / f"Fase5_{m}.json"),
                          ("v2", RES / f"Fase5_{m}.json")):
            for r in _load(path)["results"]:
                yield (f"F5{ver}|{m}|{r['nro']}|{r['field']}", r["field"], r["llm_raw"],
                       ps[ver][(r["nro"], r["field"])]["text"], ftype_of(r["field"]))
        c4 = _load(B / f"C4_parapfrase_{m}.json")
        variants = {"C4P1": c4["P1_sin_hint"], "C4P2": c4["P2_reescrito"],
                    **{f"C5{k}": v["items"] for k, v in c5["variants"].items()}}
        for vname, items in variants.items():
            for r in items:
                yield (f"{vname}|{m}|{r['nro']}|{r['field']}", r["field"], r["raw"],
                       ps["v1"][(r["nro"], r["field"])]["text"], ftype_of(r["field"]))
    for fname, g in gt_frames().items():
        for i, r in enumerate(g.itertuples()):
            raws = ["NULL" if pd.isna(r.llm_raw) else str(r.llm_raw).strip(),
                    _num_str(r.ground_truth)] + SYNTH_RAWS
            for j, raw in enumerate(dict.fromkeys(raws)):
                yield (f"GT|{fname}|{i}|{j}", r.campo, raw, str(r.text), ftype_of(r.campo))

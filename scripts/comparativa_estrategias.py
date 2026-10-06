"""Comparative evaluation C: extraction strategies vs the frozen human GT.

For each of the 26 hybrid-origin fields with a per-field GT CSV, evaluates 5
strategies against the same human ground truth:

  R     regex-only               v2 transformer _extract_field_text (bare)
  Ri    regex + postproceso      R + deterministic layer (x100, contexto N%)
  L_raw LLM bruto                first number in llm_raw, NO postprocess
  L_pp  LLM + postproceso        parse_response(llm_raw) strict (the production layer)
  H     híbrido arquitetural     deterministic -> regex -> LLM fallback

Output:
  data/processed/alolab/comparativa_estrategias.csv  (per-field acc, TP/FP/FN/TN)
  console summary with "best strategy per field" and LLM-added-value column.

GPU-free: llm_raw is recomputed locally with parse_response (no re-run).
"""
import re
import math
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.etl.transformers.extraction_postprocess import (
    NO_RULE, deterministic_pct, postprocess, parse_response,
)
from src.etl.transformers.document_features_v2_transformer import (
    DocumentFeaturesV2Transformer,
)

FOLDER = Path("data/processed/alolab/gt_por_campo")
SCHEMAS = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
OUT_CSV = Path("data/processed/alolab/comparativa_estrategias.csv")


def field_from_filename(name):
    b = re.sub(r"_GT \(1\)\.csv$|_gt \(1\)\.csv$", "", name)
    b = re.sub(r"_GT\.csv$|_gt\.csv$", "", b)
    return b


def to_num(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    s = str(v).strip()
    if s == "" or s == "nan":
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def match(gt, llm, flex=False):
    if gt is None and llm is None:
        return True
    if gt is None or llm is None:
        return False
    if gt == llm:
        return True
    if flex and gt != 0 and llm != 0 and (abs(gt / llm) == 100.0 or abs(llm / gt) == 100.0):
        return True
    return False


def build_field_types():
    with open(SCHEMAS, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for slug, schema in data.items():
        if slug == "meta":
            continue
        for f in schema["fields"]:
            out[f["name"]] = f["type"]
    return out


def main():
    files = sorted(f for f in FOLDER.glob("*.csv") if not f.name.startswith("errores_"))
    if not files:
        print(f"No CSVs in {FOLDER}")
        sys.exit(1)

    transformer = DocumentFeaturesV2Transformer(str(SCHEMAS))
    field_types = build_field_types()

    rows = []
    for f in files:
        campo = field_from_filename(f.name)
        df = pd.read_csv(f, encoding="utf-8")
        need = {"text", "llm_raw", "ground_truth", "title_slug", "llm_value"}
        if not need.issubset(set(df.columns)):
            print(f"  [SKIP] {f.name}: missing cols {need - set(df.columns)}")
            continue

        slug = df["title_slug"].iloc[0]
        ftype = field_types.get(campo, "float")

        gts = [to_num(v) for v in df["ground_truth"].tolist()]
        texts = df["text"].astype(str)

        n = len(df)
        hits = {"R": 0, "Ri": 0, "L_raw": 0, "L_pp": 0, "H": 0}
        fpc = {"R": 0, "Ri": 0, "L_raw": 0, "L_pp": 0, "H": 0}
        conf = {"R": [], "Ri": [], "L_raw": [], "L_pp": [], "H": []}

        for i in range(n):
            gt = gts[i]
            text = str(texts.iloc[i])
            if text in ("nan", "None", "<NA>"):
                text = ""
            raw = str(df["llm_raw"].iloc[i]) if pd.notna(df["llm_raw"].iloc[i]) else ""

            text_is_blank = not text.strip()
            if text_is_blank:
                v_re = None
            else:
                v_re = transformer._extract_field_text(text, slug, campo, ftype)
            v_ri = postprocess(campo, v_re, text, strict=True) if v_re is not None else None

            m_raw = re.search(r"-?\d+[.,]?\d*", re.sub(r" thinking.*? response", "", raw, flags=re.DOTALL).strip())
            v_lraw = to_num(m_raw.group(0)) if m_raw else None
            v_lpp = parse_response(raw, field=campo, text=text, strict=True)

            det = deterministic_pct(campo, text)
            if det is not NO_RULE:
                v_h = det
            elif v_re is not None and v_re != 0:
                v_h = v_ri
            else:
                v_h = v_lpp

            for key, val in [("R", v_re), ("Ri", v_ri), ("L_raw", v_lraw),
                             ("L_pp", v_lpp), ("H", v_h)]:
                if match(gt, val, flex=True):
                    hits[key] += 1
                if gt is None and val is not None:
                    fpc[key] += 1
                conf[key].append(1 if match(gt, val, flex=False) else 0)

        res = {
            "campo": campo,
            "archivo": f.name,
            "n": n,
            "gt_valor": int(sum(1 for g in gts if g is not None)),
            "gt_null": int(sum(1 for g in gts if g is None)),
        }
        for key in ("R", "Ri", "L_raw", "L_pp", "H"):
            res[f"acc_flex_{key.lower()}"] = round(hits[key] / n * 100, 1)
            res[f"fp_{key.lower()}"] = fpc[key]
        res["best"] = max(("H", res["acc_flex_h"]), ("L_pp", res["acc_flex_l_pp"]),
                          ("Ri", res["acc_flex_ri"]), ("L_raw", res["acc_flex_l_raw"]),
                          ("R", res["acc_flex_r"]), key=lambda x: x[1])[0]
        rows.append(res)
        print(f"  {campo:32s} n={n:4d} val={res['gt_valor']:4d} null={res['gt_null']:4d} | "
              f"R={res['acc_flex_r']:5.1f} Ri={res['acc_flex_ri']:5.1f} "
              f"L_raw={res['acc_flex_l_raw']:5.1f} L_pp={res['acc_flex_l_pp']:5.1f} "
              f"H={res['acc_flex_h']:5.1f} | best={res['best']}")

    res_df = pd.DataFrame(rows).sort_values("acc_flex_h", ascending=False)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    res_df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    print(f"\n=== RESUMEN ({len(res_df)} campos, {res_df['n'].sum()} juicios) ===")
    agg = res_df[["acc_flex_r", "acc_flex_ri", "acc_flex_l_raw", "acc_flex_l_pp", "acc_flex_h"]].mean().round(1)
    print("Media acc flex por estrategia:")
    for k, v in agg.items():
        print(f"  {k:14s}: {v}%")
    print(f"\nMejor estrategia por campo: {res_df['best'].value_counts().to_dict()}")
    print(f"\nGuardado: {OUT_CSV}")


if __name__ == "__main__":
    main()
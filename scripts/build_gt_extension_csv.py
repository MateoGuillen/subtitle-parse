"""
AloLab GT extension: build a labeled-GT CSV for a large validation sample.

Strategy: stratified-by-value sampling (target ~100 samples/field across the
5 AloLab-improved fields). For each field we:
  1. pull the value distribution from document_features_v2 (non-zero distinct
     values + the zero/NULL bucket),
  2. sample all rare values, a cap for common values, and the remaining
     budget from the zero/NULL bucket,
  3. run the champion model (Llama 3.1 8B-AWQ) with the CURRENT hints,
  4. export a CSV with the exact prompt, db_value, llm_raw, llm_value,
     and EMPTY ground_truth/nota columns to be filled by a human.

Usage::

    VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/build_gt_extension_csv.py \
        --per-field 100 --out data/processed/alolab/gt_extension.csv

Output: CSV with columns
    nro_licitacion, campo, title_slug, db_value, db_confidence,
    text, prompt, llm_raw, llm_value, ground_truth, nota
"""
import sys, re, json, time, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import psycopg2
from transformers import AutoTokenizer
from vllm import LLM, SamplingParams
from vllm.sampling_params import StructuredOutputsParams

from scripts.benchmark_multi_vs_gt import (
    MODELS, SCHEMAS, TITLE_MAP, build_prompt, parse,
)
from config.settings import DB_CONFIG

FIELDS = ["dias_validez_garantia", "pct_monto_referencia_tecnica",
          "anos_experiencia_minima_tecnica", "dias_pago_anticipo", "dias_solicitud_anticipo"]

RARE_CAP = 10      # values with fewer rows than this are sampled entirely
COMMON_CAP = 25    # max samples per frequent value
NULL_BUDGET_MIN = 25  # at least this many from the zero/NULL bucket per field


def field_schemas():
    with open(SCHEMAS, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for slug, schema in data.items():
        if slug == "meta":
            continue
        for f in schema["fields"]:
            out[f["name"]] = (slug, f)
    return out


def load_value_distribution(conn, field):
    """Return {value: count} for non-zero distinct values in document_features_v2."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT {field}, COUNT(*) AS n
        FROM dncp.document_features_v2
        WHERE {field} IS NOT NULL AND {field} != 0
        GROUP BY {field}
        ORDER BY n DESC
    """)
    dist = {}
    for val, n in cur.fetchall():
        try:
            v = float(val)
        except (TypeError, ValueError):
            v = val
        dist[v] = n
    cur.close()
    return dist


def sample_rows(conn, field, per_field):
    """Stratified sampling across ALL distinct values + guaranteed NULL bucket.

    Budget per field: NULL/zero bucket gets a fixed share, and the remainder is
    spread evenly across every distinct non-zero value (rare values sampled
    entirely, common values with an even cap). Surplus budget rolls into the
    NULL bucket. Guarantees full cardinality coverage.
    """
    dist = load_value_distribution(conn, field)
    selected = []

    n_null = min(max(per_field // 4, NULL_BUDGET_MIN), per_field // 2)
    value_budget = per_field - n_null
    n_distinct = len(dist)
    per_value = max(1, value_budget // n_distinct) if n_distinct else 0

    # 1. distinct non-zero values: even spread, rare values complete
    for val, n in dist.items():
        cap = min(RARE_CAP, n) if n < RARE_CAP else min(per_value, COMMON_CAP)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT nro_licitacion, {field} AS v
            FROM dncp.document_features_v2
            WHERE {field} IS NOT NULL AND {field} != 0 AND {field} = %s
            ORDER BY RANDOM() LIMIT %s
        """, (val, cap))
        got = cur.fetchall()
        for nro, v in got:
            selected.append((str(nro), float(v)))
        cur.close()

    # 2. roll any unspent value budget into the NULL/zero bucket
    if len(selected) < value_budget:
        n_null += value_budget - len(selected)

    if n_null > 0:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT nro_licitacion FROM dncp.document_features_v2
            WHERE {field} IS NULL OR {field} = 0
            ORDER BY RANDOM() LIMIT %s
        """, (n_null,))
        for (nro,) in cur.fetchall():
            selected.append((str(nro), None))
        cur.close()

    return selected


def load_texts(conn, field, nros):
    """Fetch content_text from pliegos_secciones for the given nros."""
    cur = conn.cursor()
    ph = ",".join("%s" for _ in nros)
    cur.execute(f"""
        SELECT nro_licitacion, content_text
        FROM dncp.pliegos_secciones
        WHERE nro_licitacion IN ({ph}) AND title_normalized = %s
    """, nros + [TITLE_MAP[field]])
    texts = {str(r[0]): (r[1] or "") for r in cur.fetchall()}
    cur.close()
    return texts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-field", type=int, default=100)
    parser.add_argument("--out", default="data/processed/alolab/gt_extension.csv")
    parser.add_argument("--model", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    cfg = MODELS[args.model]
    fs_map = field_schemas()
    conn = psycopg2.connect(**DB_CONFIG)

    # Build sample list (nro, field, db_value)
    sample = []
    for field in FIELDS:
        rows = sample_rows(conn, field, args.per_field)
        for nro, val in rows:
            sample.append({"nro": nro, "campo": field, "db_value": val})
    print(f"Sample built: {len(sample)} rows across {len(FIELDS)} fields")

    # Fetch texts
    for field in FIELDS:
        nros = list({s["nro"] for s in sample if s["campo"] == field})
        texts = load_texts(conn, field, nros)
        for s in sample:
            if s["campo"] == field:
                s["text"] = texts.get(s["nro"], "")
    conn.close()
    missing = sum(1 for s in sample if not s.get("text"))
    print(f"  texts missing: {missing}")

    # Load LLM
    hf_tok = AutoTokenizer.from_pretrained(cfg.get("tokenizer_path", cfg["path"]), trust_remote_code=True)
    llm = LLM(model=cfg["path"], quantization=cfg["quant"], max_model_len=cfg["max_len"],
              max_num_seqs=4, gpu_memory_utilization=0.60,
              enforce_eager=True, trust_remote_code=True)
    sp = SamplingParams(max_tokens=64, temperature=0.0,
                        structured_outputs=StructuredOutputsParams(regex=r'^(-?\d+(?:[.,]\d+)?|NULL)$'))

    # Run model and build rows
    rows = []
    t0 = time.time()
    for i, s in enumerate(sample):
        field = s["campo"]
        text = s["text"]
        if len(text) > 6000:
            text = text[:6000]
            s["text"] = text
        slug, fs = fs_map[field]
        msgs = build_prompt(field, text, cfg["merge_system"])
        rendered = hf_tok.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True)
        out = llm.generate([rendered], sp)[0].outputs[0].text.strip()
        llm_value = parse(out, field=field, text=text)
        prompt_txt = msgs[-1]["content"]
        if cfg["merge_system"]:
            prompt_txt = msgs[0]["content"]
        rows.append({
            "nro_licitacion": s["nro"],
            "campo": field,
            "title_slug": slug,
            "db_value": s["db_value"],
            "text": text,
            "prompt": prompt_txt,
            "llm_raw": out,
            "llm_value": llm_value,
            "ground_truth": "",
            "nota": "",
        })
        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(sample)} ({time.time()-t0:.0f}s)")

    elapsed = time.time() - t0
    df = pd.DataFrame(rows)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"\nDone: {len(rows)} rows in {elapsed:.0f}s")
    print(f"Saved: {out_path}")
    print(f"Per-field counts:")
    print(df.groupby("campo").size().to_string())


if __name__ == "__main__":
    main()

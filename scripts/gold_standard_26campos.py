"""
Gold standard generator: labeled-GT CSV for ALL 26 hybrid/LLM fields (no regex).

Builds the definitive manual ground-truth CSV to measure the precision of the
LLM fallback pipeline. Regex fields (extraction_method != "hybrid") are excluded
because they are deterministic.

For each of the 26 hybrid fields we:
  1. pull the value distribution from document_features_v2 (non-zero distinct
     values + the zero/NULL bucket),
  2. sample with a fixed NULL/value split (default 40% NULL / 60% values):
       - values: full cardinality coverage (rare values complete, common capped),
       - remainder across the zero/NULL bucket,
  3. run the champion model (Llama 3.1 8B-AWQ) with the CURRENT hints,
  4. export a CSV with title, campo, db_value, text, prompt, llm_raw,
     llm_value, and EMPTY ground_truth/nota columns to be filled by a human.

Usage (run inside WSL, /root/vllm-env, VLLM_USE_V2_MODEL_RUNNER=0)::

    VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python \
        scripts/gold_standard_26campos.py --per-field 200 \
        --out data/processed/alolab/gold_standard_26campos.csv

Output CSV columns:
    nro_licitacion, campo, title_slug, db_value, text, prompt,
    llm_raw, llm_value, ground_truth, nota
"""
import sys, json, time, argparse
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

RARE_CAP = 10      # values with fewer rows than this are sampled entirely
COMMON_CAP = 100   # max samples per frequent value (>= per-field value budget)
DEFAULT_NULL_SHARE = 0.40  # fraction of each field budget given to zero/NULL


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


def hybrid_fields():
    """Names of the fields whose extraction_method == 'hybrid'."""
    with open(SCHEMAS, encoding="utf-8") as f:
        data = json.load(f)
    fields = []
    for slug, schema in data.items():
        if slug == "meta":
            continue
        for f in schema["fields"]:
            if f.get("extraction_method") == "hybrid":
                fields.append(f["name"])
    return fields


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


def sample_rows(conn, field, per_field, null_share=DEFAULT_NULL_SHARE):
    """Stratified sampling: fixed NULL/zero share + full value cardinality.

    Budget per field:
      - NULL/zero bucket gets null_share (default 40%).
      - the remainder is spread evenly across EVERY distinct non-zero value
        (rare values sampled entirely, common values with a cap). Surplus rolls
        into the NULL bucket. Guarantees full cardinality coverage.
    """
    dist = load_value_distribution(conn, field)
    selected = []

    n_null = min(int(round(per_field * null_share)), per_field)
    value_budget = per_field - n_null
    n_distinct = len(dist)
    per_value = max(1, value_budget // n_distinct) if n_distinct else 0
    per_value = min(per_value, COMMON_CAP)

    # 1. distinct non-zero values: even spread, rare values complete
    for val, n in dist.items():
        cap = min(RARE_CAP, n) if n < RARE_CAP else per_value
        cur = conn.cursor()
        cur.execute(f"""
            SELECT nro_licitacion, {field} AS v
            FROM dncp.document_features_v2
            WHERE {field} IS NOT NULL AND {field} != 0 AND {field} = %s
            ORDER BY RANDOM() LIMIT %s
        """, (val, cap))
        for nro, v in cur.fetchall():
            selected.append((str(nro), float(v)))
        cur.close()

    # 2. roll any unspent value budget into the NULL/zero bucket
    if len(selected) < value_budget:
        n_null += value_budget - len(selected)

    # 3. NULL/zero bucket
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
    parser = argparse.ArgumentParser(description="Gold standard for 26 hybrid fields")
    parser.add_argument("--per-field", type=int, default=200)
    parser.add_argument("--null-share", type=float, default=DEFAULT_NULL_SHARE,
                        help="Fraction of each field budget given to zero/NULL")
    parser.add_argument("--out", default="data/processed/alolab/gold_standard_26campos.csv")
    parser.add_argument("--model", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-text-chars", type=int, default=6000)
    args = parser.parse_args()

    fields = hybrid_fields()
    if not fields:
        print("ERROR: no hybrid fields found in schema")
        sys.exit(1)
    print(f"Hybrid fields ({len(fields)}): {', '.join(fields)}")

    for f in fields:
        if f not in TITLE_MAP:
            print(f"ERROR: TITLE_MAP missing title for field '{f}'")
            sys.exit(1)

    cfg = MODELS[args.model]
    fs_map = field_schemas()
    conn = psycopg2.connect(**DB_CONFIG)

    # Build sample list (nro, field, db_value)
    sample = []
    for field in fields:
        rows = sample_rows(conn, field, args.per_field, args.null_share)
        for nro, val in rows:
            sample.append({"nro": nro, "campo": field, "db_value": val})
    print(f"Sample built: {len(sample)} rows across {len(fields)} fields "
          f"({len(sample)}/ {len(fields)} = {len(sample) / len(fields):.0f} per field)")

    # Fetch texts
    for field in fields:
        nros = list({s["nro"] for s in sample if s["campo"] == field})
        texts = load_texts(conn, field, nros)
        for s in sample:
            if s["campo"] == field:
                s["text"] = texts.get(s["nro"], "")
    conn.close()
    missing = sum(1 for s in sample if not s.get("text"))
    print(f"  texts missing: {missing}")

    # Load LLM
    hf_tok = AutoTokenizer.from_pretrained(cfg.get("tokenizer_path", cfg["path"]),
                                           trust_remote_code=True)
    llm_kwargs = dict(model=cfg["path"], max_model_len=cfg["max_len"],
                      max_num_seqs=4, gpu_memory_utilization=0.60,
                      enforce_eager=True, trust_remote_code=True)
    if cfg["quant"]:
        llm_kwargs["quantization"] = cfg["quant"]
        llm_kwargs["dtype"] = "float16"
    else:
        llm_kwargs["dtype"] = "bfloat16"
    llm = LLM(**llm_kwargs)
    sp = SamplingParams(max_tokens=64, temperature=0.0,
                        structured_outputs=StructuredOutputsParams(
                            regex=r'^(-?\d+(?:[.,]\d+)?|NULL)$'))

    # Run model and build rows
    rows = []
    t0 = time.time()
    for i, s in enumerate(sample):
        field = s["campo"]
        text = s["text"]
        if len(text) > args.max_text_chars:
            text = text[:args.max_text_chars]
            s["text"] = text
        slug, fs = fs_map[field]
        msgs = build_prompt(field, text, cfg["merge_system"])
        tmpl_kwargs = {"enable_thinking": False} if cfg["thinking_fix"] else {}
        rendered = hf_tok.apply_chat_template(msgs, tokenize=False,
                                              add_generation_prompt=True,
                                              **tmpl_kwargs)
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
    print("Per-field counts:")
    print(df.groupby("campo").size().to_string())
    print("\nNULL share per field (db_value):")
    print(df.groupby("campo")["db_value"].apply(
        lambda s: f"{s.isna().mean() * 100:.0f}%").to_string())


if __name__ == "__main__":
    main()
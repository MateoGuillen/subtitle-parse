"""
Evaluate LLM extraction quality vs regex fallback.

Takes N random samples per hybrid field (where regex conf < threshold),
runs LLM extraction, saves CSV for manual review.

Usage::
    python scripts/eval_llm_quality.py [--samples 50] [--fields "campo1,campo2"]
"""
import os
from dotenv import load_dotenv

load_dotenv()


import sys, argparse, random, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.etl.extractors.document_features_v2_llm_extractor import DocumentFeaturesV2LLMExtractor
from src.etl.transformers.document_features_v2_llm_transformer import DocumentFeaturesV2LLMTransformer
from src.etl.transformers.llm_provider import LocalLLMProvider
from config.settings import DB_CONFIG


SCHEMAS_PATH = str(
    Path(__file__).resolve().parents[1]
    / "data" / "processed" / "section_clustering_v2" / "master_schemas_v2.json"
)
VLLM_URL = os.getenv("VLLM_URL", "http://localhost:8000/v1")
CONF_THRESHOLD = 0.8


def main():
    parser = argparse.ArgumentParser(description="Evaluate LLM extraction quality")
    parser.add_argument("--samples", type=int, default=50, help="Samples per field")
    parser.add_argument("--fields", type=str, default=None, help="Comma-separated field list")
    args = parser.parse_args()

    llm = LocalLLMProvider(base_url=VLLM_URL, model="")
    extractor = DocumentFeaturesV2LLMExtractor(DB_CONFIG, SCHEMAS_PATH, CONF_THRESHOLD)
    transformer = DocumentFeaturesV2LLMTransformer(SCHEMAS_PATH, llm)

    print(f"LLM: {llm.name()}")
    print(f"Samples per field: {args.samples}")
    print()

    failed = extractor.get_failed_extractions_grouped()
    if not failed:
        print("No failed extractions found.")
        return

    field_filter = set(args.fields.split(",")) if args.fields else None

    all_rows = []
    for field_name, df_all in sorted(failed.items()):
        if field_filter and field_name not in field_filter:
            continue
        n_avail = len(df_all)
        n_sample = min(args.samples, n_avail)
        df_sample = df_all.sample(n=n_sample, random_state=42)

        df_result, metrics = transformer.compute_llm_fallback(
            field_name, df_sample, model_name=llm.model_name,
        )

        for _, row in df_result.iterrows():
            doc_id = row["nro_licitacion"]
            orig = df_all[df_all["nro_licitacion"] == doc_id]
            text = (orig.iloc[0].get("content_text") or "")[:300] if not orig.empty else ""
            all_rows.append({
                "field": field_name,
                "nro_licitacion": doc_id,
                "text_preview": text,
                "llm_value": row["llm_value"],
                "llm_confidence": row["llm_confidence"],
            })

    if not all_rows:
        print("No results produced.")
        return

    df_out = pd.DataFrame(all_rows)
    out_path = Path(__file__).resolve().parents[1] / "data" / "processed" / "llm_eval_results.csv"
    df_out.to_csv(out_path, index=False, encoding="utf-8")

    summary = df_out.groupby("field").agg(
        samples=("llm_value", "count"),
        found=("llm_confidence", lambda x: int((x > 0).sum())),
    )
    summary["pct"] = (summary["found"] / summary["samples"] * 100).round(1)

    print()
    print("=" * 60)
    print(f"EVAL RESULTS — {llm.model_name}")
    print("=" * 60)
    print(summary.to_string())
    print()
    print(f"Saved to: {out_path}")
    print(f"Total samples: {len(df_out)}")
    print(f"Total found: {int((df_out['llm_confidence'] > 0).sum())} ({df_out['llm_confidence'].sum() / len(df_out) * 100:.1f}%)")


if __name__ == "__main__":
    main()

"""
Analyze ranking stability via bootstrap (S2) and sensitivity to K (S1).

Usage:
    python scripts/analyze_ranking_stability.py
    python scripts/analyze_ranking_stability.py --n-iter 50 --K 12
    python scripts/analyze_ranking_stability.py --skip-bootstrap
"""

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from src.etl.extractors.title_ranking_extractor import TitleRankingExtractor
from src.etl.transformers.title_ranking_transformer import TitleRankingTransformer
from config.settings import DB_CONFIG, BASE_OUTPUT_PROCESSED_DIR


def main():
    parser = argparse.ArgumentParser(
        description="Analyze ranking stability (S1 + S2)"
    )
    parser.add_argument("--n-iter", type=int, default=30, help="Bootstrap iterations")
    parser.add_argument("--K", type=int, default=10, help="Top-K to evaluate")
    parser.add_argument("--skip-bootstrap", action="store_true", help="Skip S2, run only S1")
    parser.add_argument("--top-n", type=int, default=80, help="Number of candidates")
    parser.add_argument("--section-sample", type=int, default=200_000)
    args = parser.parse_args()

    output_dir = os.path.join(BASE_OUTPUT_PROCESSED_DIR, "title_ranking")
    os.makedirs(output_dir, exist_ok=True)

    extractor = TitleRankingExtractor(DB_CONFIG)
    transformer = TitleRankingTransformer(random_state=42)

    print("Loading data...")
    df_doc = extractor.get_document_features()
    if df_doc is None or df_doc.empty:
        print("ERROR: No document_features data.")
        return

    title_slugs = extractor.get_title_slugs_from_columns(df_doc)
    if not title_slugs:
        print("ERROR: No title slugs found.")
        return
    print(f"Loaded {len(title_slugs)} title slugs, {len(df_doc)} docs.")

    top_titles_raw = extractor.get_top_titles(args.top_n)
    df_sec = extractor.sample_sections(args.section_sample, top_titles_raw)

    print(f"\nComputing reference ranking...")
    ranking = transformer.compute_ranking(df_doc, df_sec, title_slugs)
    print(f"Top-1: {ranking.iloc[0]['display_name']} (score={ranking.iloc[0]['score_total']:.4f})")

    # S1: Sensitivity to K
    print(f"\n=== S1: Sensitivity to K ===")
    k_sensitivity = transformer.sensitivity_to_k(ranking)
    for pair, rho in k_sensitivity.items():
        status = "OK" if rho > 0.85 else "WARN"
        print(f"  Spearman rho (K={pair}): {rho:.4f} [{status}]")

    sens_path = os.path.join(output_dir, "sensitivity_to_k.json")
    with open(sens_path, "w") as f:
        json.dump(k_sensitivity, f, indent=2)
    print(f"Saved: {sens_path}")

    # S2: Bootstrap
    if not args.skip_bootstrap:
        print(f"\n=== S2: Bootstrap ({args.n_iter} iterations, K={args.K}) ===")
        print("  This will take approximately:", end="")
        print(f" {args.n_iter * 2} minutes of compute.")
        bootstrap_freq = transformer.bootstrap_stability(
            df_doc, df_sec, title_slugs,
            K=args.K, n_iter=args.n_iter,
        )

        freq_df = pd.DataFrame(
            sorted(bootstrap_freq.items(), key=lambda x: -x[1]),
            columns=["title_slug", "frequency"],
        )

        stable_titles = [s for s, f in bootstrap_freq.items() if f >= 0.8]
        print(f"\n  Stable titles (freq >= 0.8): {len(stable_titles)}/{len(bootstrap_freq)}")
        print(f"  Top-5 stable:")
        for _, row in freq_df.head(5).iterrows():
            flag = " STABLE" if row["frequency"] >= 0.8 else ""
            print(f"    {row['title_slug']}: {row['frequency']:.2f}{flag}")

        csv_path = os.path.join(output_dir, "bootstrap_frequencies.csv")
        freq_df.to_csv(csv_path, index=False, float_format="%.4f")
        print(f"Saved: {csv_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()

"""
Entry point for the title anomaly-ranking pipeline (v2.1).

Usage::

    # Basic ranking (80 titles, 6 strategies)
    python scripts/run_title_ranking_pipeline.py

    # With optimal K* evaluation (coverage + diversity + synthetic AUC)
    python scripts/run_title_ranking_pipeline.py --auto-k

    # Override K value manually
    python scripts/run_title_ranking_pipeline.py --auto-k --top-k 12

    # With bootstrap stability (30 iterations)
    python scripts/run_title_ranking_pipeline.py --auto-k --stability
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipelines.title_ranking_pipeline import TitleRankingPipeline
from config.settings import DB_CONFIG, BASE_OUTPUT_PROCESSED_DIR


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Title anomaly-ranking pipeline (v2.1)"
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=80,
        help="Number of candidate titles to evaluate (default: 80)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=None,
        help="Override: force selection of top-K titles (default: auto if --auto-k, else 10)",
    )
    parser.add_argument(
        "--auto-k",
        action="store_true",
        help="Evaluate optimal K (5-20) using coverage + NMI diversity + synthetic AUC",
    )
    parser.add_argument(
        "--stability",
        action="store_true",
        help="Run bootstrap stability analysis (30 iterations, ~90 min compute)",
    )
    parser.add_argument(
        "--section-sample",
        type=int,
        default=200_000,
        help="Number of sections to sample for strategy 4 (default: 200000)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: data/processed/title_ranking)",
    )
    args = parser.parse_args()

    output_dir = args.output_dir or os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "title_ranking"
    )

    config = {
        "db_params": DB_CONFIG,
        "top_n": args.top_n,
        "top_k": args.top_k or (10 if not args.auto_k else None),
        "section_sample": args.section_sample,
        "output_dir": output_dir,
        "base_output_dir": BASE_OUTPUT_PROCESSED_DIR,
        "auto_k": args.auto_k,
        "run_stability": args.stability,
    }
    pipeline = TitleRankingPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()

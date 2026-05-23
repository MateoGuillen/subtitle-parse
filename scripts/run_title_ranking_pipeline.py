"""
Entry point for the title anomaly-ranking pipeline.

Usage::

    python scripts/run_title_ranking_pipeline.py

Ranks the top-80 titles from ``dncp.document_features`` by their relevance
for anomaly detection using five complementary strategies.
"""

import os
from src.pipelines.title_ranking_pipeline import TitleRankingPipeline
from config.settings import DB_CONFIG, BASE_OUTPUT_PROCESSED_DIR


def main() -> None:
    output_dir = os.path.join(BASE_OUTPUT_PROCESSED_DIR, "title_ranking")

    config = {
        "db_params": DB_CONFIG,
        "top_n": 80,
        "top_k": 20,
        "section_sample": 200_000,
        "output_dir": output_dir,
        "base_output_dir": BASE_OUTPUT_PROCESSED_DIR,
    }
    pipeline = TitleRankingPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()

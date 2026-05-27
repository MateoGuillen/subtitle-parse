"""
Entry point for the section clustering & schema pipeline.

Usage:
    python scripts/run_section_clustering_pipeline.py                        # modo normal
    python scripts/run_section_clustering_pipeline.py --compare             # con comparación de enfoques
"""

import argparse
import os
import sys
from pathlib import Path

# Ensure project root is on sys.path so src/ and config/ resolve
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipelines.section_clustering_pipeline import SectionClusteringPipeline
from config.settings import DB_CONFIG, BASE_OUTPUT_PROCESSED_DIR, OPENROUTER_API_KEY


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Section clustering & schema generation pipeline"
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Run in comparison mode: test multiple embedding models, UMAP configs, and clustering methods",
    )
    parser.add_argument(
        "--skip-titles",
        nargs="*",
        default=[],
        help="Titles to skip (space-separated)",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=5000,
        help="Max sections per title for clustering",
    )
    parser.add_argument(
        "--no-hdbscan",
        action="store_true",
        help="Disable HDBSCAN clustering",
    )
    parser.add_argument(
        "--k-min",
        type=int,
        default=2,
        help="Minimum k for K-Means search (default: 2)",
    )
    parser.add_argument(
        "--k-max",
        type=int,
        default=15,
        help="Maximum k for K-Means search (default: 15)",
    )
    args = parser.parse_args()

    if not OPENROUTER_API_KEY or OPENROUTER_API_KEY == "tu_clave_aqui":
        print("ERROR: Configurá OPENROUTER_API_KEY en el archivo .env")
        return

    output_dir = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "section_clustering"
    )

    config = {
        "db_params": DB_CONFIG,
        "llm_api_key": OPENROUTER_API_KEY,
        "llm_model": "openai/gpt-oss-20b:free",
        "embedding_model": "paraphrase-multilingual-MiniLM-L12-v2",
        "output_dir": output_dir,
        "base_output_dir": BASE_OUTPUT_PROCESSED_DIR,
        "skip_titles": args.skip_titles,
        "max_samples_per_title": args.max_samples,
        "k_range": (args.k_min, args.k_max),
        "use_hdbscan": not args.no_hdbscan,
        "validate_schema": True,
        "schema_validation_retries": 2,
        "compare_embeddings": args.compare,
    }

    if args.compare:
        config["embedding_models_to_test"] = [
            "paraphrase-multilingual-MiniLM-L12-v2",
            "paraphrase-multilingual-mpnet-base-v2",
            "distiluse-base-multilingual-cased-v2",
        ]
        config["umap_params_grid"] = [
            {"n_components": 10, "n_neighbors": 15},
            {"n_components": 10, "n_neighbors": 30},
            {"n_components": 20, "n_neighbors": 15},
            {"n_components": 20, "n_neighbors": 30},
        ]

    pipeline = SectionClusteringPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()

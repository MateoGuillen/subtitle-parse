"""
Entry point for the section clustering & schema pipeline.

Usage:
    python scripts/run_section_clustering_pipeline.py                                          # modo normal
    python scripts/run_section_clustering_pipeline.py --compare                               # con comparación de enfoques
    python scripts/run_section_clustering_pipeline.py --llm-provider openrouter               # con OpenRouter
    python scripts/run_section_clustering_pipeline.py --export-chat-prompts                   # + prompts para chat
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
        "--k-min",
        type=int,
        default=2,
        help="Minimum k for K-Means search (default: 2)",
    )
    parser.add_argument(
        "--k-max",
        type=int,
        default=20,
        help="Maximum k for K-Means search (default: 20)",
    )
    parser.add_argument(
        "--llm-provider",
        choices=["local", "openrouter"],
        default="local",
        help="LLM provider to use: 'local' (LM Studio / llama-server) or 'openrouter' (default: local)",
    )
    parser.add_argument(
        "--llm-base-url",
        type=str,
        default="http://localhost:1234/v1",
        help="Base URL for local LLM endpoint (default: http://localhost:1234/v1)",
    )
    parser.add_argument(
        "--llm-model",
        type=str,
        default="openai/gpt-oss-20b:free",
        help="Model name for OpenRouter provider (default: openai/gpt-oss-20b:free)",
    )
    parser.add_argument(
        "--export-chat-prompts",
        action="store_true",
        help="Export prompts listos para copiar-pegar en DeepSeek/ChatGPT/Claude",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Skip LLM schema generation (uses default schema). Useful for fast export regeneration.",
    )
    args = parser.parse_args()

    if args.llm_provider == "openrouter":
        if not OPENROUTER_API_KEY or OPENROUTER_API_KEY == "tu_clave_aqui":
            print("ERROR: Configurá OPENROUTER_API_KEY en el archivo .env")
            return

    output_dir = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "section_clustering"
    )

    config = {
        "db_params": DB_CONFIG,
        "llm_provider_type": args.llm_provider,
        "llm_base_url": args.llm_base_url,
        "llm_api_key": OPENROUTER_API_KEY,
        "llm_model": args.llm_model,
        "embedding_model": "paraphrase-multilingual-MiniLM-L12-v2",
        "output_dir": output_dir,
        "base_output_dir": BASE_OUTPUT_PROCESSED_DIR,
        "skip_titles": args.skip_titles,
        "max_samples_per_title": args.max_samples,
        "k_range": (args.k_min, args.k_max),
        "validate_schema": True,
        "schema_validation_retries": 2,
        "compare_embeddings": args.compare,
        "export_chat_prompts": args.export_chat_prompts,
        "skip_llm": args.skip_llm,
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

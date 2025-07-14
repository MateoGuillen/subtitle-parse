"""
Script to run the LLM extraction pipeline for processing pliego documents.
"""

import os
from src.pipelines.llm_extraction_pipeline import LLMExtractionPipeline
from config.settings import (
    DB_CONFIG,
    LLM_ENDPOINT,
    LLM_USERNAME,
    LLM_PASSWORD,
    BASE_OUTPUT_PROCESSED_DIR,
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_RETRIES,
)


def main():
    """
    Main function to configure and run the LLM extraction pipeline.

    This function sets up the configuration for processing pliego documents
    using LLM models to extract structured information. It processes multiple
    title_slugs and can work with different LLM models for comparison.

    The pipeline:
    1. Extracts active configurations from llm_config table
    2. Processes pliegos content for each title_slug
    3. Calls LLM endpoints to get structured responses
    4. Saves results to llm_resultados table
    5. Optionally processes features and saves to llm_features table
    """

    # Create output directories
    output_processed_dir = f"{BASE_OUTPUT_PROCESSED_DIR}/llm_extraction/"
    output_logs_dir = f"{BASE_OUTPUT_PROCESSED_DIR}/logs/"

    os.makedirs(output_processed_dir, exist_ok=True)
    os.makedirs(output_logs_dir, exist_ok=True)

    # Pipeline configuration
    config = {
        "db_params": DB_CONFIG,
        "llm_endpoint": LLM_ENDPOINT,
        "llm_username": LLM_USERNAME,
        "llm_password": LLM_PASSWORD,
        "timeout_llm_response": 60,
        "output_processed_dir": output_processed_dir,
        "output_logs_dir": output_logs_dir,
        "batch_size": DEFAULT_BATCH_SIZE,
        "max_retries": DEFAULT_MAX_RETRIES,
        "title_slugs": [
            "capacidad_financiera_v3",
            # "experiencia_tecnica",1
        ],
        "models_to_test": [
            "qwen2.5-7b-instruct",
            # "google/gemma-3-1b",
            "deepseek-r1-distill-llama-8b",
        ],
        "enable_feature_extraction": False,
        "enable_model_comparison": False,
        "save_intermediate_results": False,
    }

    # Initialize and run the pipeline
    pipeline = LLMExtractionPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()

"""Script to run the OCDS data processing pipeline."""

import os
from src.pipelines.ocds_pipeline import OCDSPipeline
from config.settings import (
    DNCP_BASE_URL,
    BASE_OUTPUT_RAW_DIR,
    BASE_OUTPUT_PROCESSED_DIR,
    BASE_INPUT_EXTERNAL_DATA_DIR,
)


def main():
    """
    Main function to configure and run the OCDS data processing pipeline.

    This function sets up directory paths and filenames for raw and processed
    output data. It constructs the URL for the OCDS dataset and ensures that
    the necessary directories exist. The function then initializes the
    OCDSPipeline with the required parameters and executes the pipeline
    for the specified years.

    The pipeline processes OCDS data for each year, combines the results,
    and filters for unique tenders, saving the results in specified output
    files.

    """

    output_raw_dir_name = f"{BASE_OUTPUT_RAW_DIR}/csv/datasets/"
    output_processed_dir_name = f"{BASE_OUTPUT_PROCESSED_DIR}/csv/"
    url_ocds_dataset = f"{DNCP_BASE_URL}/images/opendata-v3/final/ocds"

    input_external_dir = f"{BASE_INPUT_EXTERNAL_DATA_DIR}/unique_categories_sorted.csv"
    print("input_external_dir", input_external_dir)

    output_path = os.path.abspath(output_raw_dir_name)
    os.makedirs(output_path, exist_ok=True)

    output_processed_path = os.path.abspath(output_processed_dir_name)
    os.makedirs(output_processed_path, exist_ok=True)

    # Config Pipeline
    config = {
        "base_url": url_ocds_dataset,
        "output_dir": output_path,
        "output_processed_dir": output_processed_path,
        "input_external_dir": input_external_dir,
        "years": [2021, 2022, 2023, 2024, 2025, 2026],
        # "years": [2025, 2026],
        "prefix_name": "merged_tender_data",
        "output_pdf_name": "ten_documents_pliego_pdf_every_year.csv",
        "output_json_name": "ten_documents_pliego_json_every_year.csv",
        "output_filtered_name": "ten_documents_pliego_pdf_every_year_filtered.csv",
    }
    # Initialize and run the pipeline
    pipeline = OCDSPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()

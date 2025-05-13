"""Script to run the PDF to Parquet conversion pipeline."""

import os
from src.pipelines.pdf_parquet_pipeline import PDFParquetPipeline
from config.settings import BASE_OUTPUT_RAW_DIR, BASE_OUTPUT_PROCESSED_DIR


def main():
    """
    Main function to configure and run the PDF to Parquet processing pipeline.

    This function sets up directory paths for raw PDF input and Parquet output
    data. It ensures that the necessary directories exist, then initializes
    the PDFParquetPipeline with the required parameters and executes it.

    The pipeline processes PDF files, converts them to Parquet format with
    structured data about document content, and combines the results into
    a single Parquet file.
    """
    # Set up directory paths
    years = [2021, 2022, 2023, 2024]
    output_processed_dir = f"{BASE_OUTPUT_PROCESSED_DIR}/parquet/pdf-to-parquet/"

    # Create output directory if it doesn't exist
    os.makedirs(output_processed_dir, exist_ok=True)

    # Configure pipeline
    config = {
        "years": years,
        "input_base_dir": f"{BASE_OUTPUT_RAW_DIR}/pdf",
        "output_dir": output_processed_dir,
        "batch_size": 1000000,
        "file_batch_size": 1000,
        "cpu_count": None,  # Use default (all available cores)
        "timeout": 60,
    }

    # Initialize and run the pipeline
    pipeline = PDFParquetPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()

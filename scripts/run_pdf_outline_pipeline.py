"""Script to run the PDF outline processing pipeline."""

import os
import asyncio
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

from src.pipelines.pdf_outline_pipeline import PDFOutlinePipeline
from config.settings import BASE_OUTPUT_RAW_DIR, BASE_OUTPUT_PROCESSED_DIR


async def main():
    """
    Main function to configure and run the PDF outline processing pipeline.

    This function sets up directory paths for raw input and processed output data.
    It ensures that the necessary directories exist, initializes the PDFOutlinePipeline
    with the required parameters, and executes the pipeline for the specified years.

    The pipeline processes PDF outline data for each year, extracting outlines from
    PDF files and saving the results to CSV files.

    Additionally, the pipeline merges yearly outlines into a single CSV and Parquet file.
    """
    # Set up input and output directories
    input_dir = BASE_OUTPUT_RAW_DIR
    output_dir = BASE_OUTPUT_PROCESSED_DIR

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Configure pipeline
    config = {
        "input_dir": input_dir,
        "output_dir": output_dir,
        "years": [2021, 2022, 2023, 2024, 2025],
        "max_workers": multiprocessing.cpu_count() * 2,
        "batch_size": 10000,
        "timeout": 60,
        "merge_output": {
            "csv_filename": "merged_outlines.csv",
            "parquet_filename": "merged_outlines.parquet",
            "compression": "snappy",
            "row_group_size": 10000,
        },
    }

    # Initialize and run the pipeline
    pipeline = PDFOutlinePipeline(config)
    await pipeline.run()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.set_default_executor(
        ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 4)
    )
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

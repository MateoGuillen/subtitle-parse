"""Script to run the Asynchronous Download Pipeline for multiple years."""

import os
import asyncio
from src.pipelines.async_download_pipeline import AsyncDownloadPipeline
from src.utils.logging_utils import setup_logger
from config.settings import BASE_OUTPUT_PROCESSED_DIR, BASE_OUTPUT_RAW_DIR


async def process_year(year, file_type):
    """
    Process a single year with the AsyncDownloadPipeline.

    Args:
        year (str): The year to process
        file_type (str): The file type to download (pdf, json)

    Returns:
        bool: True if processing was successful, False otherwise
    """
    logger = setup_logger(f"{__name__}_{year}")

    # Configure paths
    dataset_path = f"{BASE_OUTPUT_PROCESSED_DIR}/csv/ten_documents_pliego_{file_type}_every_year.csv"
    output_dir = f"{BASE_OUTPUT_RAW_DIR}/{file_type}/{year}/"

    # Create directories if they don't exist
    os.makedirs(output_dir, exist_ok=True)

    # Configure pipeline parameters
    config = {
        "input_file_path": dataset_path,
        "output_dir": output_dir,
        "file_type": file_type,
        "batch_size": 1000,  # Number of files to download in each batch
        "pause_time": 10,  # Seconds to pause between batches
        "filter_column": "year",  # Column to filter on
        "filter_value": year,  # Value to filter for
    }

    # Initialize and run the pipeline
    pipeline = AsyncDownloadPipeline(config)
    success = await pipeline.run()

    if success:
        logger.info(
            "Download pipeline completed successfully for %s %s files.", year, file_type
        )
        # Optionally organize downloaded files by category
        pipeline.file_loader.organize_files_by_category()
        # Create a summary of downloaded files
        summary_path = pipeline.file_loader.create_download_summary(
            summary_name=f"download_summary_{file_type}_{year}.csv"
        )
        if summary_path:
            logger.info("Download summary created %s", summary_path)
        # Clean up any temporary files
        pipeline.file_loader.clean_temporary_files()
    else:
        logger.error("Download pipeline failed for year %s.", year)

    return success


async def main():
    """
    Main function to configure and run the Asynchronous Download Pipeline for multiple years.

    This function processes a range of years, executing the pipeline for each year.
    It can process either sequentially or in parallel depending on the configuration.
    """
    logger = setup_logger(__name__)

    # Set parameters for the pipeline
    # years = ["2021", "2022", "2023", "2024"]
    years = ["2025", "2026"]  # Array of years to process
    file_type = "pdf"  # Options: pdf, json

    # Process sequentially - one year at a time
    for year in years:
        logger.info("Starting pipeline for year %s", year)
        success = await process_year(year, file_type)
        if success:
            logger.info("Completed pipeline for year %s", year)
        else:
            logger.warning("Failed pipeline for year %s", year)

    logger.info("All years processed")

    # Alternatively, process all years concurrently
    # tasks = [process_year(year, file_type) for year in years]
    # results = await asyncio.gather(*tasks)
    #
    # for year, success in zip(years, results):
    #     if success:
    #         logger.info("Completed pipeline for year %s", year)
    #     else:
    #         logger.warning("Failed pipeline for year %s", year)
    #
    # logger.info("All years processed concurrently")


if __name__ == "__main__":
    asyncio.run(main())

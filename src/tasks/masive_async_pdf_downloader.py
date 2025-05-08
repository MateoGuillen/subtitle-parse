""" 
    This script downloads all PDF files from a CSV file."""
import asyncio
from src.utils.file_async_downloader import FileHandler
from src.utils.file_utility import FileUtility
from src.utils.csv_utility import CSVUtility
from config.settings import BASE_OUTPUT_PROCESSED_DIR, BASE_OUTPUT_RAW_DIR
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


async def main():
    """ 
        This function downloads all PDF files from a CSV file."""
    try:
        # Initial configuration
        logger = setup_logger(__name__)
        year = "2021"
        file_type = "pdf"
        output_root_dir = ".//"
        outline_pdf_root_dir = f'{BASE_OUTPUT_RAW_DIR}/{file_type}/{year}/'
        dataset_path = f'{BASE_OUTPUT_PROCESSED_DIR}/csv/ten_documents_pliego_{file_type}_every_year.csv'
        dataset_path_filtered = f'{BASE_OUTPUT_PROCESSED_DIR}/csv/ten_documents_pliego_{file_type}_every_year_filtered.csv'
        dataset_path_filtered_limit = f'{BASE_OUTPUT_PROCESSED_DIR}/csv/ten_documents_pliego_{file_type}_every_year_filtered_limit_{year}.csv'

        # Create necessary directories
        FileUtility.ensure_directory_exists(output_root_dir)
        FileUtility.ensure_directory_exists(outline_pdf_root_dir)

        # Filter CSV
        CSVUtility.filter_csv_by_column(
            dataset_path,
            dataset_path_filtered,
            "nro_licitacion",
            filter_method="unique"
        )
        CSVUtility.filter_by_column_and_limit(
            dataset_path_filtered,
            dataset_path_filtered_limit,
            "year",
            year
        )

        # Initialize downloader
        downloader = FileHandler(
            file_path=dataset_path_filtered_limit,
            output_dir=outline_pdf_root_dir,
            low_memory=False,
            batch_size=1000,  # New parameter for batch size
            pause_time=30    # New parameter for pause time between batches (in seconds)
        )

        # Download files in batches
        total_downloaded = 0
        batch_number = 1

        while True:
            downloaded_files = await downloader.download_files_from_urls_batch()
            if not downloaded_files:
                break

            batch_count = len(downloaded_files)
            total_downloaded += batch_count
            logger.info("Batch %s: Successfully downloaded %s files", batch_number, batch_count)
            logger.info("Total files downloaded so far: %s", total_downloaded)

            if batch_count == downloader.batch_size:
                print(f"Pausing for {downloader.pause_time} seconds before next batch...")
                await asyncio.sleep(downloader.pause_time)

            batch_number += 1

        logger.info("Download complete. Total files downloaded: %s", total_downloaded)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
    
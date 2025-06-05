"""A pipeline for managing asynchronous massive downloads."""

import os
import asyncio
from src.etl.extractors.file_downloader import FileDownloader
from src.etl.extractors.async_file_extractor import AsyncFileExtractor
from src.etl.transformers.csv_processor import CSVProcessor
from src.etl.loaders.file_loader import FileLoader
from src.utils.csv_utility import CSVUtility
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
from src.utils.file_checker import FileChecker
from src.utils.file_utility import FileUtility


class AsyncDownloadPipeline:
    """
    A pipeline for managing asynchronous massive downloads of different file types.

    Attributes:
        config (dict): Configuration dictionary.
        input_file_path (str): Path to the input CSV file with URLs to download.
        output_dir (str): Directory for output files.
        file_type (str): Type of files to download (pdf, json, etc.).
        batch_size (int): Number of files to download in each batch.
        pause_time (int): Time to pause between batches (in seconds).
        logger (logging.Logger): Logger object for logging messages.
    """

    def __init__(self, config):
        self.config = config
        self.input_file_path = config.get("input_file_path")
        self.output_dir = config.get("output_dir")
        self.file_type = config.get("file_type", "pdf")
        self.batch_size = config.get("batch_size", 1000)
        self.pause_time = config.get("pause_time", 300)
        self.filter_column = config.get("filter_column")
        self.filter_value = config.get("filter_value")
        self.logger = setup_logger(__name__)
        self._initialize_components()

    def _initialize_components(self):
        """Initialize the components of the pipeline."""
        # Base components
        self.file_downloader = FileDownloader()
        self.async_extractor = AsyncFileExtractor()
        self.csv_processor = CSVProcessor()
        self.csv_utility = CSVUtility()
        self.file_checker = FileChecker()
        self.file_utility = FileUtility()

        # ETL components
        self.file_loader = FileLoader(self.output_dir)

    @error_handling(default_return=None)
    def prepare_data(self):
        """Prepare data by filtering and limiting the dataset."""
        self.logger.info("Preparando datos para la descarga...")

        # Ensure directories exist
        self.file_utility.ensure_directory_exists(self.output_dir)

        # Apply filters if specified in config
        filtered_path = None
        if self.filter_column and self.filter_value:
            filtered_file_name = f"{os.path.splitext(os.path.basename(self.input_file_path))[0]}_filtered_{self.filter_value}.csv"
            filtered_path = os.path.join(
                os.path.dirname(self.input_file_path), filtered_file_name
            )

            # Filter by column value
            self.csv_utility.filter_by_column_and_limit(
                self.input_file_path,
                filtered_path,
                self.filter_column,
                self.filter_value,
            )
            self.logger.info("Datos filtrados guardados en: %s", filtered_path)
            return filtered_path

        return self.input_file_path

    async def download_files(self, file_path):
        """Download files asynchronously in batches."""
        self.logger.info("Iniciando descarga asincrónica de archivos...")

        # Initialize downloader with appropriate configuration
        downloader_config = {
            "file_path": file_path,
            "output_dir": self.output_dir,
            "low_memory": False,
            "batch_size": self.batch_size,
            "pause_time": self.pause_time,
        }

        # Initialize the downloader with the extracted file
        downloader = self.async_extractor.create_file_handler(downloader_config)

        # Download files in batches
        total_downloaded = 0
        batch_number = 1

        download_method = getattr(
            downloader,
            f"download_{self.file_type}_files_batch",
            downloader.download_files_from_urls_batch,
        )

        while True:
            downloaded_files = await download_method()
            if not downloaded_files:
                break

            batch_count = len(downloaded_files)
            total_downloaded += batch_count
            self.logger.info(
                "Batch %s: Successfully downloaded %s files", batch_number, batch_count
            )
            self.logger.info("Total files downloaded so far: %s", total_downloaded)

            if batch_count == downloader.batch_size:
                self.logger.info(
                    "Pausing for %s seconds before next batch...", downloader.pause_time
                )
                await asyncio.sleep(downloader.pause_time)

            batch_number += 1

        self.logger.info(
            "Download complete. Total files downloaded: %s", total_downloaded
        )
        return total_downloaded

    async def run(self):
        """Run the asynchronous download pipeline using the config dictionary."""
        self.logger.info("Iniciando pipeline de descargas masivas asincrónicas...")

        try:
            # Prepare data by filtering and limiting
            prepared_file_path = self.prepare_data()
            if not prepared_file_path:
                self.logger.error("Error en la preparación de datos.")
                return False

            # Download files
            total_downloaded = await self.download_files(prepared_file_path)

            # Log results
            self.logger.info("Pipeline de descargas completado con éxito.")
            self.logger.info("Total de archivos descargados: %s", total_downloaded)

            return True

        except Exception as e:
            self.logger.error("Error en el pipeline de descargas: %s", str(e))
            return False

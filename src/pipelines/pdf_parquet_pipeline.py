"""A pipeline for converting PDF files to Parquet format."""

import os
import asyncio
from src.etl.extractors.pdf_extractor import PDFExtractor
from src.etl.transformers.pdf_transformer import PDFTransformer
from src.etl.loaders.parquet_loader import ParquetLoader
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
from src.utils.file_checker import FileChecker


class PDFParquetPipeline:
    """
    A pipeline for processing PDF files and converting them to Parquet format.

    Attributes:
        config (dict): Configuration dictionary.
        input_base_dir (str): Base directory for PDF input files.
        output_dir (str): Directory for Parquet output files.
        years (list): List of years to process.
        batch_size (int): Number of records per batch for Parquet files.
        file_batch_size (int): Number of PDF files to process in each batch.
        logger (logging.Logger): Logger object for logging messages.
    """

    def __init__(self, config):
        self.config = config
        self.input_base_dir = config.get("input_base_dir")
        self.output_dir = config.get("output_dir")
        self.years = config.get("years", [])
        self.batch_size = config.get("batch_size", 10000)
        self.file_batch_size = config.get("file_batch_size", 50)
        self.cpu_count = config.get("cpu_count")
        self.timeout = config.get("timeout", 60)
        self.logger = setup_logger(__name__)
        self._initialize_components()

    def _initialize_components(self):
        """Initialize the components of the pipeline."""
        # Base components
        self.file_checker = FileChecker()

        # ETL components
        self.pdf_extractor = PDFExtractor(self.input_base_dir)
        self.pdf_transformer = PDFTransformer(
            cpu_count=self.cpu_count,
            batch_size=self.batch_size,
            file_batch_size=self.file_batch_size,
        )
        self.parquet_loader = ParquetLoader(self.output_dir, batch_size=self.batch_size)

    @error_handling(default_return=False)
    def process_year(self, year):
        """Process PDF files for a specific year."""
        self.logger.info("Processing PDFs for year %s...", year)

        output_file = os.path.join(self.output_dir, f"pdf_text_{year}.parquet")

        # Check if output already exists
        if self.file_checker.file_exists(output_file):
            self.logger.info("Parquet file for %s already exists. Skipping...", year)
            return True

        # Run async operations using asyncio.run() - handles loop creation/cleanup properly
        try:
            result = asyncio.run(self._process_year_async(year))
            if result:
                self.logger.info("Successfully processed PDFs for year %s", year)
                return True
            else:
                self.logger.error("Failed to process PDFs for year %s", year)
                return False
        except Exception as e:
            self.logger.error("Exception processing year %s: %s", year, e)
            return False

    async def _process_year_async(self, year):
        """Async helper to process a year."""
        # Extract
        pdf_files = await self.pdf_extractor.extract_pdf_files(year)
        if not pdf_files:
            self.logger.warning("No PDF files found for year %s", year)
            return False

        # Transform and Load
        result = await self.pdf_transformer.process_and_load(
            pdf_files, self.parquet_loader, year_dir=str(year)
        )

        return result

    @error_handling(default_return=False)
    def merge_all_years(self):
        """Merge Parquet files from all years into a single file."""
        self.logger.info("Merging Parquet files from all years...")

        final_output = os.path.join(
            self.output_dir, "combined_documents_all_years.parquet"
        )

        # Check if combined file already exists
        if self.file_checker.file_exists(final_output):
            self.logger.info("Combined Parquet file already exists. Skipping merge...")
            return True

        # Create list of files to merge
        files_to_merge = []
        for year in self.years:
            year_file = os.path.join(self.output_dir, f"pdf_text_{year}.parquet")
            if self.file_checker.file_exists(year_file):
                files_to_merge.append(year_file)

        if not files_to_merge:
            self.logger.warning("No Parquet files found to merge")
            return False

        # Create async event loop and run the merge
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        result = loop.run_until_complete(
            self.parquet_loader.merge_parquet_files(files_to_merge, final_output)
        )

        loop.close()

        if result:
            self.logger.info("Successfully merged Parquet files")
            return True
        else:
            self.logger.error("Failed to merge Parquet files")
            return False

    def run(self):
        """Run the PDF to Parquet conversion pipeline using the config dictionary."""
        self.logger.info("Starting PDF to Parquet conversion pipeline...")

        # Process PDFs for each year
        for year in self.years:
            self.process_year(year)

        # Merge results from all years
        self.merge_all_years()

        self.logger.info("PDF to Parquet conversion pipeline completed.")

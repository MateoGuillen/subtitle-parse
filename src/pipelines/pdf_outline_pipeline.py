"""A pipeline for processing PDF outline data."""

import os
import multiprocessing
from pathlib import Path
import pandas as pd
from src.etl.extractors.pdf_outline_extractor import PDFOutlineExtractor
from src.etl.transformers.outline_processor import OutlineProcessor
from src.etl.transformers.outline_merger import OutlineMerger
from src.etl.loaders.csv_outline_loader import CSVOutlineLoader
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
from src.utils.file_checker import FileChecker


class PDFOutlinePipeline:
    """
    A pipeline for processing PDF outline data.

    Attributes:
        config (dict): Configuration dictionary.
        input_dir (str): Base directory for input PDF files.
        output_dir (str): Directory for output files.
        years (list): Years to process.
        logger (logging.Logger): Logger object for logging messages.
    """

    def __init__(self, config):
        self.config = config
        self.input_dir = config.get("input_dir")
        self.output_dir = config.get("output_dir")
        self.years = config.get("years")
        self.max_workers = config.get("max_workers", multiprocessing.cpu_count() * 2)
        self.batch_size = config.get("batch_size", 10)
        self.timeout = config.get("timeout", 60)
        self.logger = setup_logger(__name__)
        self._initialize_components()

    def _initialize_components(self):
        """Initialize the components of the pipeline."""
        # Base components
        self.file_checker = FileChecker()

        # ETL components
        self.pdf_outline_extractor = PDFOutlineExtractor(self.timeout, self.max_workers)
        self.outline_processor = OutlineProcessor()
        self.csv_outline_loader = CSVOutlineLoader(self.output_dir)

        # Create output directories for merged files
        output_csv_dir = os.path.join(self.output_dir, "csv")
        output_parquet_dir = os.path.join(self.output_dir, "parquet")
        os.makedirs(output_csv_dir, exist_ok=True)
        os.makedirs(output_parquet_dir, exist_ok=True)

        # Initialize merger
        self.outline_merger = OutlineMerger(output_csv_dir, output_parquet_dir)

    @error_handling(default_return=False)
    async def process_year(self, year):
        """Process PDF outlines for a specific year."""
        self.logger.info("Processing PDF outlines for year %s...", year)

        year_str = str(year)
        input_year_dir = os.path.join(self.input_dir, "pdf", year_str)
        output_year_dir = os.path.join(self.output_dir, "outlines", year_str)

        # Ensure output directory exists
        os.makedirs(output_year_dir, exist_ok=True)

        # Check if output file already exists
        output_file = os.path.join(output_year_dir, f"outlines_{year_str}.csv")
        if self.file_checker.file_exists(output_file):
            self.logger.info(
                "Output file for year %s already exists. Skipping...", year
            )
            return True

        # Extract all PDF files for the year
        pdf_files = await self.pdf_outline_extractor.get_pdf_files(input_year_dir)
        if not pdf_files:
            self.logger.info("No PDF files found for year %s", year)
            return False

        self.logger.info("Found %s PDF files for year %s", len(pdf_files), year)

        # Process files in batches
        all_outlines = []
        for i in range(0, len(pdf_files), self.batch_size):
            batch = pdf_files[i : i + self.batch_size]
            batch_outlines = await self.pdf_outline_extractor.extract_outlines_batch(
                batch
            )

            # Transform outlines
            processed_outlines = self.outline_processor.process_outlines(batch_outlines)
            all_outlines.extend(processed_outlines)

        # Load results
        if all_outlines:
            df = pd.DataFrame(all_outlines)
            self.csv_outline_loader.save_outlines(df, year_str)
            self.logger.info(
                "Successfully processed %s outlines for year %s",
                len(all_outlines),
                year,
            )
            return True

        self.logger.info("No outlines found for year %s", year)
        return False

    @error_handling(default_return=(None, None))
    def merge_outlines(self):
        """
        Merge yearly outline files into single CSV and Parquet files.

        Returns:
            Tuple[Optional[Path], Optional[Path]]: Paths to the saved CSV and Parquet files,
                                                  or (None, None) if an error occurred.
        """
        self.logger.info("Merging outline files from all years...")

        # # Check if merged files already exist
        # csv_path = os.path.join(self.output_dir, "csv", "merged_outlines.csv")
        # parquet_path = os.path.join(
        #     self.output_dir, "parquet", "merged_outlines.parquet"
        # )

        # if self.file_checker.all_files_exist([csv_path, parquet_path]):
        #     self.logger.info("Merged files already exist. Skipping merge operation.")
        #     return Path(csv_path), Path(parquet_path)

        # Merge yearly outlines
        base_path = os.path.join(self.output_dir, "outlines")
        csv_path, parquet_path = self.outline_merger.merge_and_save(
            self.years, base_path
        )

        if csv_path and parquet_path:
            self.logger.info("Successfully merged outlines from all years.")
            return csv_path, parquet_path

    async def run(self):
        """Run the PDF outline processing pipeline using the config dictionary."""
        self.logger.info("Starting PDF outline processing pipeline...")

        # Process data for each year
        for year in self.years:
            await self.process_year(year)

        # Merge yearly outlines into single CSV and Parquet files
        csv_path, parquet_path = self.merge_outlines()

        if csv_path and parquet_path:
            self.logger.info("PDF outline processing pipeline completed successfully.")
        else:
            self.logger.warning(
                "PDF outline processing pipeline completed with warnings."
            )

        self.logger.info("PDF outline processing pipeline completed.")

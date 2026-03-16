"""Module for extracting PDF files for processing."""

import asyncio
import os
from pathlib import Path
from typing import List, Optional
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
import pyarrow.parquet as pq


class PDFExtractor:
    """
    Extractor for PDF files from specified directories.

    Attributes:
        base_dir (str): Base directory containing PDF files organized by year.
        logger: Logger for this class.
    """

    def __init__(self, base_dir: str):
        """
        Initialize the PDF extractor.

        Args:
            base_dir: Base directory containing PDF files organized by year.
        """
        self.base_dir = Path(base_dir)
        self.logger = setup_logger(__name__)

    @error_handling(default_return=[])
    async def extract_pdf_files(self, year: str) -> List[Path]:
        """
        Extract all PDF files for a specific year, recursively searching through all subdirectories.

        Args:
            year: The year to extract PDFs for.

        Returns:
            List of Path objects for PDF files.
        """
        year_dir = self.base_dir / str(year)
        self.logger.info("Looking for PDF files in %s and all subdirectories", year_dir)

        if not year_dir.exists():
            self.logger.warning("Directory %s does not exist", year_dir)
            return []

        # Function to recursively collect all PDF files
        async def collect_pdf_files(directory):
            pdf_files = []
            try:
                # Get all items in directory
                items = await asyncio.to_thread(os.listdir, directory)

                for item in items:
                    item_path = Path(directory) / item

                    # If it's a directory, recurse into it
                    if await asyncio.to_thread(os.path.isdir, item_path):
                        sub_pdfs = await collect_pdf_files(item_path)
                        pdf_files.extend(sub_pdfs)
                    # If it's a PDF file, add it to our list
                    elif item.lower().endswith(".pdf"):
                        pdf_files.append(item_path)
            except Exception as e:
                self.logger.error("Error accessing directory %s: %s", directory, e)

            return pdf_files

        # Collect all PDF files recursively
        pdf_files = await collect_pdf_files(year_dir)

        self.logger.info("Found %d PDF files for year %s", len(pdf_files), year)
        return pdf_files

    @error_handling(default_return=None)
    async def read_pdf_bytes(self, pdf_path: Path) -> Optional[bytes]:
        """
        Read a PDF file into memory as bytes.

        Args:
            pdf_path: Path to the PDF file.

        Returns:
            The PDF file content as bytes or None if error.
        """
        pdf_bytes = await asyncio.to_thread(pdf_path.read_bytes)
        return pdf_bytes

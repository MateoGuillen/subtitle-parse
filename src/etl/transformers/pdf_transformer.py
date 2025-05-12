"""Module for transforming PDF files into structured data using Apache Tika."""

import re
import asyncio
import multiprocessing
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from tqdm import tqdm
from tika import parser as tika_parser
from tika import initVM
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


initVM()  # Initialize the Java VM for Tika


@dataclass
class PDFSection:
    """Data structure for a text line in a PDF"""

    document_id: str
    page_number: int
    line_number: int
    line_text: str
    processed_date: Optional[str] = None


# Define a standalone function for multiprocessing
def process_pdf_worker(data_tuple: Tuple[bytes, str]) -> List[PDFSection]:
    """
    Standalone function for processing a PDF in a worker process.

    Args:
        data_tuple: Tuple containing (pdf_bytes, filename)

    Returns:
        List of PDFSection objects
    """
    pdf_bytes, filename = data_tuple
    sections = []

    try:
        # Parse PDF with Tika
        parsed_pdf = tika_parser.from_buffer(pdf_bytes)
        content = parsed_pdf.get("content", "")

        if not content:
            return []

        # Split content into pages using form feed character or treat as single page
        if "\f" in content:
            raw_pages = content.split("\f")
            pages = [p.strip() for p in raw_pages if p.strip()]
        else:
            pages = [content]

        for page_number, page_text in enumerate(pages, start=1):
            if not page_text:
                continue

            # Split page into lines
            lines = page_text.split("\n")
            page_line_number = 0

            for line in lines:
                clean_line = line.strip()
                page_line_number += 1

                # Skip page number indicators like "1/40"
                if re.fullmatch(r"\d{1,3}/\d{1,3}", clean_line):
                    continue

                if clean_line:
                    sections.append(
                        PDFSection(
                            document_id=filename,
                            page_number=page_number,
                            line_number=page_line_number,
                            line_text=clean_line,
                            processed_date=datetime.now().isoformat(),
                        )
                    )

        return sections
    except Exception as e:
        # Simple error handling for worker process
        print(f"Error processing PDF {filename}: {str(e)}")
        return []


class PDFTransformer:
    """
    Transformer for converting PDF files to structured data using Apache Tika.

    Attributes:
        cpu_count: Number of CPU cores to use for processing.
        batch_size: Number of records per batch for processing.
        file_batch_size: Number of PDF files to process in each batch.
        logger: Logger for this class.
    """

    def __init__(
        self,
        cpu_count: Optional[int] = None,
        batch_size: int = 10000,
        file_batch_size: int = 50,
    ):
        """
        Initialize the PDF transformer.

        Args:
            cpu_count: Number of CPU cores to use, defaults to all available cores.
            batch_size: Number of records to process in each batch.
            file_batch_size: Number of PDF files to process in each batch.
        """
        self.cpu_count = cpu_count if cpu_count else multiprocessing.cpu_count()
        self.batch_size = batch_size
        self.file_batch_size = file_batch_size
        self.logger = setup_logger(__name__)

        # Thread pool for I/O operations
        self._thread_pool = ThreadPoolExecutor(max_workers=self.cpu_count * 2)

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Placeholder for future text cleaning implementation.
        Currently returns the text as-is.
        """
        return text

    @error_handling(default_return=[])
    def process_pdf_in_memory(
        self, pdf_bytes: bytes, filename: str
    ) -> List[PDFSection]:
        """
        Process a single PDF file in memory.

        Args:
            pdf_bytes: PDF file content as bytes.
            filename: Name of the PDF file.

        Returns:
            List of PDFSection objects containing structured data.
        """
        return process_pdf_worker((pdf_bytes, filename))

    @error_handling(default_return=[])
    async def process_pdf_batch(self, files: List[Path]) -> List[PDFSection]:
        """
        Process a batch of PDF files using multiprocessing.

        Args:
            files: List of PDF file paths to process.

        Returns:
            List of PDFSection objects containing structured data from all PDFs.
        """
        sections = []

        # Read all PDF files first
        pdf_data = []
        for file in files:
            try:
                pdf_bytes = await asyncio.to_thread(file.read_bytes)
                pdf_data.append((pdf_bytes, file.stem))
            except Exception as e:
                self.logger.error("Error reading %s: %s", file, e)
                continue

        # Process in parallel using the standalone function
        with ProcessPoolExecutor(max_workers=self.cpu_count) as executor:
            results = list(executor.map(process_pdf_worker, pdf_data))

        # Combine results
        for result in results:
            sections.extend(result)

        return sections

    @error_handling(default_return=False)
    async def process_and_load(
        self, pdf_files: List[Path], parquet_loader: Any, year_dir: str
    ) -> bool:
        """
        Process PDF files and load them to Parquet format.

        Args:
            pdf_files: List of PDF file paths to process.
            parquet_loader: Loader for saving Parquet files.
            year_dir: Directory name for the year being processed.

        Returns:
            True if successful, False otherwise.
        """
        if not pdf_files:
            self.logger.info("No PDF files to process")
            return False

        self.logger.info("Processing %s PDF files", len(pdf_files))

        all_sections = []
        output_path = Path(parquet_loader.output_dir) / f"pdf_text_{year_dir}.parquet"

        # Process in batches with progress bar
        with tqdm(total=len(pdf_files), desc=f"Processing PDFs for {year_dir}") as pbar:
            for i in range(0, len(pdf_files), self.file_batch_size):
                batch = pdf_files[i : i + self.file_batch_size]
                batch_sections = await self.process_pdf_batch(batch)
                all_sections.extend(batch_sections)
                pbar.update(len(batch))

                # Save intermediate batches to manage memory
                if len(all_sections) >= self.batch_size:
                    batch_path = (
                        Path(parquet_loader.output_dir)
                        / f"batch_{year_dir}_{i}.parquet"
                    )
                    await parquet_loader.save_to_parquet(all_sections, batch_path)
                    all_sections = []

        # Save remaining sections
        if all_sections:
            final_path = (
                Path(parquet_loader.output_dir) / f"final_batch_{year_dir}.parquet"
            )
            await parquet_loader.save_to_parquet(all_sections, final_path)

        # Merge all batch files
        batch_files = list(
            Path(parquet_loader.output_dir).glob(f"*batch_{year_dir}*.parquet")
        )
        if batch_files:
            await parquet_loader.merge_parquet_files(batch_files, output_path)
            # Clean up
            for batch_file in batch_files:
                await asyncio.to_thread(batch_file.unlink)

        self.logger.info("Finished processing PDFs for %s", year_dir)
        return True

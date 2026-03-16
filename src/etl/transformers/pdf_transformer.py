"""Module for transforming PDF files into structured data using Apache Tika."""

import re
import asyncio
import multiprocessing
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from threading import Semaphore
from tqdm import tqdm
import requests
from lxml import etree

from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
from src.utils.tika_init import init_tika_for_windows, get_next_tika_port


# Initialize Tika ONCE at module import
try:
    init_tika_for_windows()
except Exception as e:
    print(f"Warning: Tika initialization had issues: {e}")


# Semaphore to limit concurrent HTTP requests to Tika servers
tika_semaphore = Semaphore(64)

# Regex to skip page number indicators like "1/45", "2/45", etc.
PAGE_NUM_PATTERN = re.compile(r"^\d{1,3}/\d{1,3}$")

# Namespaces used in Tika XHTML output
XHTML_NS = "http://www.w3.org/1999/xhtml"


@dataclass
class PDFSection:
    """Data structure for a text line in a PDF"""

    document_id: str
    page_number: int
    line_number: int
    line_text: str
    processed_date: Optional[str] = None


def extract_text_from_page_div(page_element) -> List[str]:
    """
    Extract all text lines from a single <div class="page"> element.

    Tika XHTML wraps each page in <div class="page">, with text inside <p> tags.
    We extract all text recursively, split by newlines, and filter empty/noise lines.

    Args:
        page_element: lxml element for the <div class="page">

    Returns:
        List of clean, non-empty text lines for this page.
    """
    lines = []

    # itertext() recursively yields all text nodes inside the element
    # We join them, then split by newline to get individual lines
    full_text = "".join(page_element.itertext())

    for raw_line in full_text.split("\n"):
        clean = raw_line.strip()
        # Skip empty lines and page number indicators (e.g. "3/45")
        if not clean or PAGE_NUM_PATTERN.fullmatch(clean):
            continue
        lines.append(clean)

    return lines


def process_pdf_worker(data_tuple: Tuple[bytes, str]) -> List[PDFSection]:
    """
    Process a single PDF using Tika's XHTML output for EXACT page extraction.

    Strategy:
    - 1 request to PUT /tika with Accept: text/xml
    - Tika returns XHTML with one <div class="page"> per PDF page
    - Parse XHTML with lxml, iterate divs → exact page number from array index
    - Extract text lines per page from <p> tags / text nodes

    This gives:
    - Exact page numbers (from div order, not char counting)
    - Clean text (HTML entities decoded by lxml)
    - Single HTTP request per PDF (fast)

    Args:
        data_tuple: (pdf_bytes, filename)

    Returns:
        List[PDFSection] with exact page numbers and line numbers.
    """
    pdf_bytes, filename = data_tuple
    sections = []

    try:
        port = get_next_tika_port()

        with tika_semaphore:
            try:
                response = requests.put(
                    f"http://localhost:{port}/tika",
                    data=pdf_bytes,
                    headers={
                        "Content-Type": "application/pdf",
                        "Accept": "text/xml",
                    },
                    timeout=60,
                )
                response.raise_for_status()
                xhtml_bytes = response.content
            except requests.exceptions.RequestException as e:
                print(f"Error extracting XHTML from {filename} on port {port}: {e}")
                return []

        if not xhtml_bytes:
            print(f"No content extracted from {filename}")
            return []

        # Parse XHTML — lxml handles XML 1.1 and entity decoding automatically
        try:
            root = etree.fromstring(xhtml_bytes)
        except etree.XMLSyntaxError as e:
            # Fallback: try with recovery mode for malformed XHTML
            parser = etree.XMLParser(recover=True)
            root = etree.fromstring(xhtml_bytes, parser=parser)

        # Find all <div class="page"> elements — one per PDF page
        # Tika XHTML uses the http://www.w3.org/1999/xhtml namespace
        page_divs = root.findall(f".//{{{XHTML_NS}}}div[@class='page']")

        if not page_divs:
            # Fallback: try without namespace (some Tika versions omit it)
            page_divs = root.findall(".//div[@class='page']")

        if not page_divs:
            print(
                f"Warning: No <div class='page'> found in {filename}. "
                f"Falling back to full-text extraction."
            )
            # Last resort: dump all text as page 1
            all_text = "".join(root.itertext())
            for line_num, raw_line in enumerate(all_text.split("\n"), start=1):
                clean = raw_line.strip()
                if clean and not PAGE_NUM_PATTERN.fullmatch(clean):
                    sections.append(
                        PDFSection(
                            document_id=filename,
                            page_number=1,
                            line_number=line_num,
                            line_text=clean,
                            processed_date=datetime.now().isoformat(),
                        )
                    )
            return sections

        # Process each page div — index+1 gives exact page number
        processed_date = datetime.now().isoformat()

        for page_idx, page_div in enumerate(page_divs):
            page_num = page_idx + 1
            lines = extract_text_from_page_div(page_div)

            for line_num, line_text in enumerate(lines, start=1):
                sections.append(
                    PDFSection(
                        document_id=filename,
                        page_number=page_num,
                        line_number=line_num,
                        line_text=line_text,
                        processed_date=processed_date,
                    )
                )

        return sections

    except Exception as e:
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
        batch_size: int = 100,
        file_batch_size: int = 50,
    ):
        self.cpu_count = cpu_count if cpu_count else multiprocessing.cpu_count()
        self.batch_size = batch_size
        self.file_batch_size = file_batch_size
        self.logger = setup_logger(__name__)
        self._thread_pool = ThreadPoolExecutor(max_workers=self.cpu_count * 2)

    @staticmethod
    def clean_text(text: str) -> str:
        """Placeholder for future text cleaning. Currently returns text as-is."""
        return text

    @error_handling(default_return=[])
    def process_pdf_in_memory(
        self, pdf_bytes: bytes, filename: str
    ) -> List[PDFSection]:
        """Process a single PDF file in memory."""
        return process_pdf_worker((pdf_bytes, filename))

    @error_handling(default_return=[])
    async def process_pdf_batch(self, files: List[Path]) -> List[PDFSection]:
        """Process a batch of PDF files using multithreading."""
        sections = []

        pdf_data = []
        for file in files:
            try:
                pdf_bytes = await asyncio.to_thread(file.read_bytes)
                pdf_data.append((pdf_bytes, file.stem))
            except Exception as e:
                self.logger.error("Error reading %s: %s", file, e)

        def process_batch():
            with ThreadPoolExecutor(max_workers=self.cpu_count) as executor:
                return list(executor.map(process_pdf_worker, pdf_data))

        results = await asyncio.to_thread(process_batch)
        for result in results:
            sections.extend(result)

        return sections

    @error_handling(default_return=False)
    async def process_and_load(
        self, pdf_files: List[Path], parquet_loader: Any, year_dir: str
    ) -> bool:
        """
        Process PDF files in batches and save to Parquet.

        Uses 1 Tika request per PDF (text/xml XHTML), parsed with lxml
        for exact page numbers via <div class="page"> elements.
        """
        if not pdf_files:
            self.logger.info("No PDF files to process")
            return False

        self.logger.info(
            "Processing %s PDF files in batches of %s",
            len(pdf_files),
            self.file_batch_size,
        )

        all_sections = []
        output_path = Path(parquet_loader.output_dir) / f"pdf_text_{year_dir}.parquet"

        with tqdm(
            total=len(pdf_files), desc=f"Processing PDFs for {year_dir}", unit="file"
        ) as pbar:
            for batch_start in range(0, len(pdf_files), self.file_batch_size):
                batch_end = min(batch_start + self.file_batch_size, len(pdf_files))
                batch_files = pdf_files[batch_start:batch_end]

                pdf_data = []
                for file in batch_files:
                    try:
                        pdf_bytes = await asyncio.to_thread(file.read_bytes)
                        pdf_data.append((pdf_bytes, file.stem))
                    except Exception as e:
                        self.logger.error("Error reading %s: %s", file, e)
                        pbar.update(1)
                        continue

                num_workers = min(self.cpu_count * 8, len(pdf_data))
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    futures = [
                        executor.submit(process_pdf_worker, data) for data in pdf_data
                    ]
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            all_sections.extend(result)
                        except Exception as e:
                            self.logger.error("Error processing PDF: %s", e)
                        finally:
                            pbar.update(1)

                # Save intermediate batch if memory threshold reached
                if len(all_sections) >= self.batch_size:
                    batch_path = (
                        Path(parquet_loader.output_dir)
                        / f"batch_{year_dir}_{batch_start}.parquet"
                    )
                    await parquet_loader.save_to_parquet(all_sections, batch_path)
                    all_sections = []

        # Save remaining sections
        if all_sections:
            final_path = (
                Path(parquet_loader.output_dir) / f"final_batch_{year_dir}.parquet"
            )
            await parquet_loader.save_to_parquet(all_sections, final_path)

        # Merge all batch files into final output
        batch_files_list = list(
            Path(parquet_loader.output_dir).glob(f"*batch_{year_dir}*.parquet")
        )
        if batch_files_list:
            await parquet_loader.merge_parquet_files(batch_files_list, output_path)
            for batch_file in batch_files_list:
                await asyncio.to_thread(batch_file.unlink)

        self.logger.info("Finished processing PDFs for %s", year_dir)
        return True

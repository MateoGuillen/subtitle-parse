""" PDF Outline Extractor module """
import asyncio
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Dict, Optional

from PyPDF2 import PdfReader
import pdfplumber
from tqdm import tqdm

from src.utils.logging_utils import setup_logger

class PDFOutlineExtractor:
    """
    Extracts outlines from PDF files.
    
    Attributes:
        timeout (int): Maximum time in seconds for PDF processing operations.
        max_workers (int): Maximum number of worker threads.
        logger (logging.Logger): Logger object for logging messages.
    """
    def __init__(self, timeout=60, max_workers=None):
        self.timeout = timeout
        self._thread_executor = ThreadPoolExecutor(max_workers=max_workers)
        self.logger = setup_logger(__name__)

    async def get_pdf_files(self, input_dir: str) -> List[Path]:
        """Get all PDF files from the input directory."""
        input_path = Path(input_dir)
        return list(input_path.glob("**/*.pdf"))

    def parse_document_id(self, filename: str) -> Dict[str, str]:
        """
        Parse document ID to extract year, category_id, and nro_licitacion
        Handles cases with -1 for category_id
        """
        match = re.match(r'(\d{4})_(-?\d+)_(\d+)', filename)
        if match:
            return {
                'year': match.group(1),
                'category_id': match.group(2),
                'nro_licitacion': match.group(3)
            }
        return {
            'year': '',
            'category_id': '',
            'nro_licitacion': ''
        }

    def clean_text(self, text: str) -> str:
        """ Clean text with improved handling """
        if not text:
            return ""
        return ' '.join(text.split())

    async def safe_pdf_operation(self, operation, *args, **kwargs):
        """ Execute PDF operations with timeout and error handling """
        try:
            return await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    self._thread_executor,
                    operation,
                    *args,
                    **kwargs
                ),
                timeout=self.timeout
            )
        except asyncio.TimeoutError:
            self.logger.error("Operation timed out for %s", args[0])
            return None
        except Exception as e:
            self.logger.error("Error in PDF operation: %s", e)
            return None

    async def extract_pdf_outline(self, pdf_path: Path) -> List[Dict[str, Optional[str]]]:
        """ Extract PDF outline """
        def extract_outline_content(pdf_path):
            """ Extract outline content """
            outlines = []
            try:
                # Parse document ID details
                doc_details = self.parse_document_id(pdf_path.stem)

                with pdfplumber.open(str(pdf_path)) as pdf:
                    reader = PdfReader(str(pdf_path))
                    outline = reader.outline

                    if not outline:
                        self.logger.info("No outline found in %s", pdf_path)
                        return outlines

                    def process_outline_item(item, depth=0):
                        if isinstance(item, list):
                            for subitem in item:
                                process_outline_item(subitem, depth + 1)
                        elif hasattr(item, 'title') and hasattr(item, 'page'):
                            try:
                                page_number = (item.page if isinstance(item.page, int)
                                               else reader.get_destination_page_number(item) + 1)
                                title = self.clean_text(item.title)

                                if depth == 2:
                                    outlines.append({
                                        "document_id": pdf_path.stem,
                                        "year": doc_details['year'],
                                        "category_id": doc_details['category_id'],
                                        "nro_licitacion": doc_details['nro_licitacion'],
                                        "title": title,
                                        "page": page_number + 1,
                                        "depth": depth
                                    })
                            except Exception as e:
                                self.logger.error("Error processing outline item: %s", e)

                    process_outline_item(outline)

            except Exception as e:
                self.logger.error("Error extracting outline from %s: %s", pdf_path, e)

            return outlines

        return await self.safe_pdf_operation(extract_outline_content, pdf_path)

    async def extract_outlines_batch(self, pdf_files: List[Path]) -> List[Dict[str, Optional[str]]]:
        """Extract outlines from a batch of PDF files."""
        all_outlines = []
        with tqdm(total=len(pdf_files), desc="Extracting PDF Outlines") as pbar:
            for file in pdf_files:
                outlines = await self.extract_pdf_outline(file)
                if outlines:
                    all_outlines.extend(outlines)
                pbar.update(1)
        return all_outlines

import asyncio
import os
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import pdfplumber
from PyPDF2 import PdfReader
import logging
import re
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from typing import List, Dict, Optional

class AsyncPDFOutlineExtractor:
    def __init__(self, input_dir: str, output_dir: str, year: str): 
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._thread_executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 2)
        self.timeout = 60
        self.year = year

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
        if not text:
            return ""
        return ' '.join(text.split())

    async def safe_pdf_operation(self, operation, *args, **kwargs):
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
            logging.error(f"Operation timed out for {args[0]}")
            return None
        except Exception as e:
            logging.error(f"Error in PDF operation: {e}")
            return None

    async def extract_pdf_outline(self, pdf_path: Path) -> List[Dict[str, Optional[str]]]:
        def extract_outline_content(pdf_path):
            outlines = []
            try:
                # Parse document ID details
                doc_details = self.parse_document_id(pdf_path.stem)

                with pdfplumber.open(str(pdf_path)) as pdf:
                    reader = PdfReader(str(pdf_path))
                    outline = reader.outline

                    if not outline:
                        logging.info(f"No outline found in {pdf_path}")
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
                                logging.error(f"Error processing outline item: {e}")

                    process_outline_item(outline)

            except Exception as e:
                logging.error(f"Error extracting outline from {pdf_path}: {e}")
            
            return outlines

        return await self.safe_pdf_operation(extract_outline_content, pdf_path)

    async def convert_all_outlines(self):
        all_files = list(self.input_dir.glob("**/*.pdf"))
        if not all_files:
            logging.info("No PDF files found")
            return

        logging.info(f"Found {len(all_files)} PDF files")
        batch_size = 10
        all_outlines = []

        with tqdm(total=len(all_files), desc="Extracting PDF Outlines") as pbar:
            for i in range(0, len(all_files), batch_size):
                batch = all_files[i:i + batch_size]
                batch_outlines = await self.process_batch(batch, pbar)
                all_outlines.extend(batch_outlines)

        if all_outlines:
            output_path = self.output_dir / f'outlines_{self.year}.csv'
            df = pd.DataFrame(all_outlines)
            df.to_csv(output_path, index=False, encoding='utf-8')
            logging.info(f"Outlines saved to {output_path}")

    async def process_batch(self, files: List[Path], pbar: tqdm) -> List[Dict[str, Optional[str]]]:
        all_outlines = []
        for file in files:
            outlines = await self.extract_pdf_outline(file)
            all_outlines.extend(outlines)
            pbar.update(1)
        return all_outlines

async def main():
    
    years = [2021, 2022, 2023, 2024, 2025]
    for year in years:
        
        year = str(year)
        input_dir = f'./downloads/pdf/{year}/'
        output_dir = f'./outputs/processed_pdf/outlines/{year}/'
        extractor = AsyncPDFOutlineExtractor(input_dir, output_dir, year)
        await extractor.convert_all_outlines()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 4))
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
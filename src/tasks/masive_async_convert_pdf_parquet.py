"""Module for converting PDFs to Parquet files using asyncio and multiprocessing."""

import re
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
from typing import Optional, List
from dataclasses import dataclass
from datetime import datetime
import io
from functools import partial
import asyncio
from pathlib import Path
from tqdm import tqdm
import pdfplumber
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger
from config.settings import BASE_OUTPUT_PROCESSED_DIR, BASE_OUTPUT_RAW_DIR


logger = setup_logger(__name__)


@dataclass
class PDFSection:
    """Data structure for a text line in a PDF"""

    document_id: str
    page_number: int
    line_number: int
    line_text: str
    processed_date: Optional[str] = None


def process_pdf_in_memory(
    pdf_bytes: bytes, filename: str, clean_text_func
) -> List[PDFSection]:
    """Process a single PDF file in memory"""
    sections = []
    known_combinations = [
        ("REQUISITOS DE PARTICIPACIÓN Y CRITERIOS DE", "EVALUACIÓN"),
        ("SUMINISTROS REQUERIDOS - ESPECIFICACIONES", "TÉCNICAS"),
    ]

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_number, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text()
                if not page_text:
                    continue

                lines = page_text.split("\n")
                page_line_number = 0
                i = 0

                while i < len(lines):
                    clean_line = clean_text_func(lines[i].strip())
                    page_line_number += 1

                    if i + 1 < len(lines):
                        next_line = clean_text_func(lines[i + 1].strip())

                        for part1, part2 in known_combinations:
                            if clean_line == part1 and next_line == part2:
                                clean_line += " " + next_line
                                i += 1
                                break

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
                    i += 1

        return sections
    except Exception as e:
        logger.error("Error processing %s: %s", filename, e)
        return []


class AsyncPDFConverter:
    """Class to convert PDFs to Parquet asynchronously"""

    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.cpu_count = multiprocessing.cpu_count()
        self._process_pool = ProcessPoolExecutor(max_workers=self.cpu_count)
        self._thread_pool = ThreadPoolExecutor(max_workers=self.cpu_count * 2)

        self.timeout = 60
        self.batch_size = 10000
        self.file_batch_size = 50

        self.schema = pa.schema(
            [
                ("document_id", pa.string()),
                ("page_number", pa.int32()),
                ("line_number", pa.int32()),
                ("line_text", pa.string()),
                ("processed_date", pa.string()),
            ]
        )

    @staticmethod
    def clean_text(text: str) -> str:
        """Optimized text cleaning function"""
        text = re.sub(r"(\w)\1+", r"\1", text)
        text = text.translate(str.maketrans({"-": "-", ":": ":", ".": ".", ",": ","}))
        return " ".join(text.split())

    async def process_pdf_batch(self, files: List[Path]) -> List[PDFSection]:
        """Process a batch of PDFs using multiprocessing"""
        sections = []

        pdf_data = []
        for file in files:
            try:
                pdf_bytes = await asyncio.to_thread(file.read_bytes)
                pdf_data.append((pdf_bytes, file.stem))
            except Exception as e:
                logger.error("Error reading %s: %s", file, e)
                continue

        process_func = partial(process_pdf_in_memory, clean_text_func=self.clean_text)
        tasks = [
            asyncio.get_event_loop().run_in_executor(
                self._process_pool, process_func, pdf_bytes, filename
            )
            for pdf_bytes, filename in pdf_data
        ]

        results = await asyncio.gather(*tasks)
        for result in results:
            sections.extend(result)

        return sections

    async def write_to_parquet(self, sections: List[PDFSection], output_path: Path):
        """Optimized Parquet writing"""
        if not sections:
            return

        try:
            data = {
                "document_id": [s.document_id for s in sections],
                "page_number": [s.page_number for s in sections],
                "line_number": [s.line_number for s in sections],
                "line_text": [s.line_text for s in sections],
                "processed_date": [s.processed_date for s in sections],
            }

            table = pa.Table.from_pydict(data, schema=self.schema)

            await asyncio.to_thread(
                pq.write_table,
                table,
                output_path,
                compression="snappy",
                row_group_size=self.batch_size,
            )
        except Exception as e:
            logger.error("Error writing to parquet %s: %s", output_path, e)

    async def convert_all(self):
        """Improved conversion process"""
        all_files = list(self.input_dir.glob("**/*.pdf"))
        if not all_files:
            logger.info("No PDF files found in %s", self.input_dir)
            return

        logger.info("Found %d PDF files", len(all_files))

        all_sections = []
        with tqdm(total=len(all_files), desc="Processing PDFs") as pbar:
            for i in range(0, len(all_files), self.file_batch_size):
                batch = all_files[i : i + self.file_batch_size]
                batch_sections = await self.process_pdf_batch(batch)
                all_sections.extend(batch_sections)
                pbar.update(len(batch))

                if len(all_sections) >= self.batch_size:
                    output_path = self.output_dir / f"batch_{i}.parquet"
                    await self.write_to_parquet(all_sections, output_path)
                    all_sections = []

        if all_sections:
            output_path = self.output_dir / "final_batch.parquet"
            await self.write_to_parquet(all_sections, output_path)

        await self.merge_parquet_files()

    async def merge_parquet_files(self):
        """Optimized Parquet file merging"""
        try:
            parquet_files = list(self.output_dir.glob("*.parquet"))
            if len(parquet_files) <= 1:
                return

            async def read_table(file):
                return await asyncio.to_thread(pq.read_table, str(file))

            tasks = [read_table(file) for file in parquet_files]
            tables = await asyncio.gather(*tasks)

            combined_table = pa.concat_tables(tables)
            final_path = self.output_dir / "combined_documents.parquet"

            await asyncio.to_thread(
                pq.write_table,
                combined_table,
                final_path,
                compression="snappy",
                row_group_size=self.batch_size,
            )

            await asyncio.gather(
                *[
                    asyncio.to_thread(file.unlink)
                    for file in parquet_files
                    if file != final_path
                ]
            )

            logger.info("All files merged into: %s", final_path)
        except Exception as e:
            logger.error("Error merging parquet files: %s", e)


async def main():
    """Main execution function"""
    year = "2021"
    input_dir = f"{BASE_OUTPUT_RAW_DIR}/pdf/{year}/"
    output_dir = f"{BASE_OUTPUT_PROCESSED_DIR}/parquet/pdf-to-parquet/"

    converter = AsyncPDFConverter(input_dir, output_dir)
    await converter.convert_all()


if __name__ == "__main__":
    asyncio.run(main())

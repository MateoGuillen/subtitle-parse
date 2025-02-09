import asyncio
import os
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import pdfplumber
import pyarrow as pa
import pyarrow.parquet as pq
import re
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import platform
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pdf_to_parquet_processing.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class PDFSection:
    """Data structure for a text line in a PDF"""
    document_id: str
    page_number: int
    line_number: int
    line_text: str
    processed_date: Optional[str] = None


class AsyncPDFConverter:
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._thread_executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 2)
        self.timeout = 60
        self.batch_size = 1000  # Number of records per Parquet group
        
    @staticmethod
    def configure_system_limits():
        """Configure system limits for better performance"""
        if platform.system() != 'Windows':
            import resource
            resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))

    def clean_text(self, texto: str) -> str:
        """
        Function to clean duplicate text and replace hyphens
        """
        texto = re.sub(r'(\w)\1+', r'\1', texto)  # Remove duplicate letters
        texto = texto.replace("--", "-")  # Replace double hyphens with single hyphen
        texto = texto.replace("::", ":")  # Replace multiple consecutive colons
        texto = texto.replace("..", ".")  # Replace consecutive periods
        texto = re.sub(r',+', ',', texto)  # Replace multiple commas with a single comma
        return re.sub(r'\s+', ' ', texto).strip()  # Clean extra spaces

    async def safe_pdf_operation(self, operation, *args, **kwargs) -> Any:
        """Execute PDF operations with timeout and error handling"""
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
            logging.error("Operation timed out")
            return None
        except Exception as e:
            logging.error(f"Error in PDF operation: {e}")
            return None

    async def extract_pdf_content(self, pdf_path: Path) -> List[PDFSection]:
        """Extract PDF content line by line asynchronously"""
        sections = []
        try:
            def extract_line_content(pdf_path):
                pdf_lines = []  # List to store (text, page, line_number) tuples
                known_combinations = [
                    ('REQUISITOS DE PARTICIPACIÓN Y CRITERIOS DE', 'EVALUACIÓN'),
                    ('SUMINISTROS REQUERIDOS - ESPECIFICACIONES', 'TÉCNICAS')
                ]

                with pdfplumber.open(pdf_path) as pdf:
                    for page_number, page in enumerate(pdf.pages, start=1):  # Enumerate pages
                        page_text = page.extract_text()
                        if page_text:  # Check if there's text on the page
                            lines = page_text.split('\n')
                            page_line_number = 0  # Line number within the page
                            i = 0  # Index to iterate through lines
                            while i < len(lines):
                                clean_line = self.clean_text(lines[i].strip())
                                page_line_number += 1

                                if i + 1 < len(lines):
                                    next_line = self.clean_text(lines[i + 1].strip())

                                    # Check if current line combination is in known combinations
                                    for part1, part2 in known_combinations:
                                        if clean_line == part1 and next_line == part2:
                                            clean_line += " " + next_line  # Combine lines
                                            i += 1  # Increment index to avoid processing next line separately
                                            break  # No need to continue searching combinations

                                if clean_line:  # Add combined line
                                    pdf_lines.append((clean_line, page_number, page_line_number))
                                i += 1
                return pdf_lines

            # Execute line extraction in thread pool
            pdf_lines = await self.safe_pdf_operation(extract_line_content, pdf_path)

            if pdf_lines:
                for line_text, page_number, line_number in pdf_lines:
                    sections.append(PDFSection(
                        document_id=pdf_path.stem,
                        page_number=page_number,
                        line_number=line_number,
                        line_text=line_text,
                        processed_date=datetime.now().isoformat()
                    ))

            return sections

        except Exception as e:
            logging.error(f"Error extracting content from {pdf_path}: {e}")
            return []

    async def write_to_parquet(self, sections: List[PDFSection], output_path: Path):
        """Write text lines to a Parquet file"""
        try:
            # Create a dictionary with specific columns
            data = {
                'document_id': [],
                'page_number': [],
                'line_number': [],
                'line_text': [],
                'processed_date': []
            }

            # Populate the dictionary with section data
            for section in sections:
                data['document_id'].append(section.document_id)
                data['page_number'].append(section.page_number)
                data['line_number'].append(section.line_number)
                data['line_text'].append(section.line_text)
                data['processed_date'].append(section.processed_date)

            # Debug logging
            logging.info(f"Writing {len(sections)} sections to {output_path}")
            logging.info(f"Sample document_ids: {data['document_id'][:3]}")
            logging.info(f"Sample page_numbers: {data['page_number'][:3]}")
            logging.info(f"Sample line_numbers: {data['line_number'][:3]}")

            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Define the schema
            schema = pa.schema([
                ('document_id', pa.string()),
                ('page_number', pa.int32()),
                ('line_number', pa.int32()),
                ('line_text', pa.string()),
                ('processed_date', pa.string())
            ])

            # Convert to Arrow table
            table = pa.Table.from_pandas(df, schema=schema)

            # Write to Parquet with compression
            await asyncio.to_thread(
                pq.write_table,
                table,
                output_path,
                compression='snappy',
                row_group_size=self.batch_size
            )

        except Exception as e:
            logging.error(f"Error writing to parquet: {e}")
    async def convert_all(self):
        """Convert all PDFs to Parquet"""
        all_files = list(self.input_dir.glob("**/*.pdf"))
        if not all_files:
            logging.info("No PDF files found")
            return

        logging.info(f"Found {len(all_files)} PDF files")
        batch_size = 10  # Process 10 PDFs at a time

        all_sections = []
        with tqdm(total=len(all_files), desc="Processing PDFs") as pbar:
            for i in range(0, len(all_files), batch_size):
                batch = all_files[i:i + batch_size]
                batch_sections = await self.process_batch(batch, pbar)
                all_sections.extend(batch_sections)

                # Write to Parquet every certain number of sections
                if len(all_sections) >= self.batch_size:
                    output_path = self.output_dir / f"batch_{i//batch_size}.parquet"
                    await self.write_to_parquet(all_sections, output_path)
                    all_sections = []  # Clear memory

        # Write remaining sections
        if all_sections:
            output_path = self.output_dir / f"final_batch.parquet"
            await self.write_to_parquet(all_sections, output_path)

        await self.merge_parquet_files()

        
    async def merge_parquet_files(self):
        """Combine all Parquet files into one"""
        try:
            parquet_files = list(self.output_dir.glob("*.parquet"))
            if len(parquet_files) <= 1:
                return

            # Read and combine all files
            tables = []
            for file in parquet_files:
                table = pq.read_table(str(file))
                
                # Validate schema
                expected_columns = ['document_id', 'page_number', 'line_number', 'line_text', 'processed_date']
                table_columns = table.column_names
                
                if set(table_columns) != set(expected_columns):
                    logging.error(f"Unexpected schema in {file}: {table_columns}")
                    raise ValueError(f"Unexpected schema in {file}")
                
                tables.append(table)

            # Concatenate tables
            combined_table = pa.concat_tables(tables)

            # Write final file
            final_path = self.output_dir / "combined_documents.parquet"
            pq.write_table(
                combined_table,
                final_path,
                compression='snappy',
                row_group_size=self.batch_size
            )

            # Remove intermediate files
            for file in parquet_files:
                if file != final_path:
                    file.unlink()

            logging.info(f"All files merged into: {final_path}")

        except Exception as e:
            logging.error(f"Error merging parquet files: {e}")
    async def process_batch(self, files: List[Path], pbar: tqdm) -> List[PDFSection]:
        """Process a batch of PDF files"""
        all_sections = []
        for file in files:
            sections = await self.extract_pdf_content(file)
            all_sections.extend(sections)
            pbar.update(1)
        return all_sections

async def main():
    # Configure directories
    year = "2021"
    input_dir = f'./downloads/pdf/{year}/'
    output_dir = "./outputs/processed_pdf/parquet"

    # Create converter
    converter = AsyncPDFConverter(input_dir, output_dir)
    converter.configure_system_limits()

    # Execute conversion
    await converter.convert_all()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 4))
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
"""Module for transforming PDF files into structured data."""

import re
import asyncio
import multiprocessing
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import io
from tqdm import tqdm
import pandas as pd

# Reemplazando pdfplumber por tika
import tika

# Configurar Tika antes de la importación
tika.TikaClientOnly = (
    True  # No iniciar el servidor localmente si existe un servidor remoto
)
os.environ["TIKA_SERVER_JAR"] = (
    "auto"  # Descargar el JAR automáticamente si es necesario
)

# Inicializa Tika con reintentos
max_retries = 3
for i in range(max_retries):
    try:
        tika.initVM()
        from tika import parser as tika_parser

        break
    except Exception as e:
        if i == max_retries - 1:
            raise Exception(
                f"No se pudo inicializar Tika después de {max_retries} intentos: {e}"
            )
        time.sleep(2)

from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


@dataclass
class PDFSection:
    """Data structure for a text line in a PDF"""

    document_id: str
    page_number: int
    line_number: int
    line_text: str
    processed_date: Optional[str] = None


class PDFTransformer:
    """
    Transformer for converting PDF files to structured data.

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

        # Known text patterns to combine across lines
        self.known_combinations = [
            ("REQUISITOS DE PARTICIPACIÓN Y CRITERIOS DE", "EVALUACIÓN"),
            ("SUMINISTROS REQUERIDOS - ESPECIFICACIONES", "TÉCNICAS"),
        ]

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Limpia y normaliza texto extraído del PDF, preservando fechas y horas.

        - Corrige duplicados de símbolos (--, .., ::, etc.)
        - Elimina repeticiones innecesarias de caracteres
        - Protege patrones como fechas y horas
        - Normaliza espacios
        """
        if pd.isna(text):
            return ""

        # Paso 1: Reemplazar duplicados de símbolos comunes
        replacements = {
            "--": "-",
            ",,": ",",
            "..": ".",
            ";;": ";",
            "::": ":",
            "°°": "°",
            "//": "/",
            "((": "(",
            "))": ")",
        }
        for old, new in replacements.items():
            text = text.replace(old, new)

        # Paso 2: Proteger fechas (dd/mm/yyyy) y horas (hh:mm)
        protected = {}

        def protect_pattern(pattern, label):
            nonlocal text
            matches = re.findall(pattern, text)
            for i, match in enumerate(matches):
                placeholder = f"<<{label}_{i}>>"
                protected[placeholder] = match
                text = text.replace(match, placeholder)

        protect_pattern(r"\b\d{2}/\d{2}/\d{4}\b", "DATE")
        protect_pattern(r"\b\d{1,2}:\d{2}\b", "TIME")

        # Paso 3: Eliminar repeticiones de caracteres (ej: leeeeeey -> ley)
        text = re.sub(r"(\w)\1+", r"\1", text)

        # Paso 4: Restaurar fechas y horas
        for placeholder, original in protected.items():
            text = text.replace(placeholder, original)

        # Paso 5: Normalizar espacios
        return " ".join(text.strip().split())

    @error_handling(default_return=[])
    def process_pdf_in_memory(
        self, pdf_bytes: bytes, filename: str
    ) -> List[PDFSection]:
        """
        Process a single PDF file in memory. Uses the static method.

        Args:
            pdf_bytes: PDF file content as bytes.
            filename: Name of the PDF file.

        Returns:
            List of PDFSection objects containing structured data.
        """
        try:
            return self.process_pdf_static(pdf_bytes, filename)
        except Exception as e:
            self.logger.error(f"Error processing {filename}: {e}")
            return []

    # Define a static method for multiprocessing to avoid pickle issues
    @staticmethod
    def _process_pdf_static(data):
        """Static method for multiprocessing to process a PDF"""
        pdf_bytes, filename = data
        return PDFTransformer.process_pdf_static(pdf_bytes, filename)

    @staticmethod
    def process_pdf_static(pdf_bytes, filename):
        """Static version of process_pdf_in_memory for multiprocessing"""
        sections = []
        known_combinations = [
            ("REQUISITOS DE PARTICIPACIÓN Y CRITERIOS DE", "EVALUACIÓN"),
            ("SUMINISTROS REQUERIDOS - ESPECIFICACIONES", "TÉCNICAS"),
        ]

        try:
            # Usar tika en lugar de pdfplumber con reintentos
            max_attempts = 3
            content = None

            for attempt in range(max_attempts):
                try:
                    # Versiones más recientes de tika-python pueden no tener el parámetro options
                    parsed_pdf = tika_parser.from_buffer(pdf_bytes)
                    content = parsed_pdf.get("content", "")
                    if content:
                        break
                except Exception as e:
                    if attempt == max_attempts - 1:
                        print(
                            f"Error procesando PDF {filename} después de {max_attempts} intentos: {e}"
                        )
                        return []
                    time.sleep(2)  # Esperar antes de reintentar

            if not content:
                return []

            # Dividir el texto en páginas (aproximación, ya que Tika no siempre preserva el formato de página)
            # Buscamos patrones que indiquen cambios de página
            pages = []

            # Dividir por saltos de página o usar todo el contenido como una página si no hay divisiones claras
            if "\f" in content:
                raw_pages = content.split("\f")
                for page_content in raw_pages:
                    if page_content.strip():
                        pages.append(page_content.strip())
            else:
                # Intenta dividir utilizando patrones comunes de numeración de página como alternativa
                page_pattern = re.compile(r"\n\s*\d+\s*\n")
                potential_pages = page_pattern.split(content)

                if len(potential_pages) > 1:
                    pages = [page.strip() for page in potential_pages if page.strip()]
                else:
                    pages = [content]

            for page_number, page_text in enumerate(pages, start=1):
                if not page_text:
                    continue

                # Dividir la página en líneas
                lines = page_text.split("\n")
                page_line_number = 0
                i = 0

                while i < len(lines):
                    # Usar el método clean_text estático
                    clean_line = PDFTransformer.clean_text(lines[i].strip())
                    page_line_number += 1

                    if i + 1 < len(lines):
                        next_line = PDFTransformer.clean_text(lines[i + 1].strip())

                        for part1, part2 in known_combinations:
                            if clean_line == part1 and next_line == part2:
                                clean_line += " " + next_line
                                i += 1
                                break

                    # No añadir líneas con notas al pie en formato 1/40 .. 9/40, 10/40 .. 19/40
                    if re.fullmatch(r"\d{1,3}/\d{1,3}", clean_line):
                        print(f"Skipping line with footnote: {clean_line}")
                        # self.logger.debug("Skipping line with footnote: %s", clean_line)
                        i += 1
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
                    i += 1

        except Exception as e:
            # No se puede usar self.logger aquí, así que simplemente devolvemos vacío
            return []

        return sections

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

        pdf_data = []
        for file in files:
            try:
                pdf_bytes = await asyncio.to_thread(file.read_bytes)
                pdf_data.append((pdf_bytes, file.stem))
            except Exception as e:
                self.logger.error(f"Error reading {file}: {e}")
                continue

        # Use a process pool with static method to avoid pickle issues
        with ProcessPoolExecutor(max_workers=self.cpu_count) as executor:
            results = list(executor.map(self._process_pdf_static, pdf_data))

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

        self.logger.info(f"Processing {len(pdf_files)} PDF files")

        all_sections = []
        output_path = Path(parquet_loader.output_dir) / f"pdf_text_{year_dir}.parquet"

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

        # Save any remaining sections
        if all_sections:
            final_batch_path = (
                Path(parquet_loader.output_dir) / f"final_batch_{year_dir}.parquet"
            )
            await parquet_loader.save_to_parquet(all_sections, final_batch_path)

        # Merge all batch files for this year
        batch_files = list(
            Path(parquet_loader.output_dir).glob(f"*batch_{year_dir}*.parquet")
        )
        if batch_files:
            await parquet_loader.merge_parquet_files(batch_files, output_path)
            # Clean up batch files
            for batch_file in batch_files:
                await asyncio.to_thread(batch_file.unlink)

        return True

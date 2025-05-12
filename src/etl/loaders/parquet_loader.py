"""Module for loading processed data into Parquet format."""

import asyncio
from pathlib import Path
from typing import List, Any
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class ParquetLoader:
    """
    Loader for saving processed data to Parquet format.

    Attributes:
        output_dir: Directory for output Parquet files.
        batch_size: Number of records per batch for Parquet row groups.
        schema: PyArrow schema for the Parquet files.
        logger: Logger for this class.
    """

    def __init__(self, output_dir: str, batch_size: int = 10000):
        """
        Initialize the Parquet loader.

        Args:
            output_dir: Directory for output Parquet files.
            batch_size: Number of records per batch for Parquet row groups.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.logger = setup_logger(__name__)

        # Define schema for PDF Sections
        self.schema = pa.schema(
            [
                ("document_id", pa.string()),
                ("page_number", pa.int32()),
                ("line_number", pa.int32()),
                ("line_text", pa.string()),
                ("processed_date", pa.string()),
            ]
        )

    @error_handling(default_return=False)
    async def save_to_parquet(self, sections: List[Any], output_path: Path) -> bool:
        """
        Save sections data to Parquet file.

        Args:
            sections: List of PDFSection objects to save.
            output_path: Path where to save the Parquet file.

        Returns:
            True if successful, False otherwise.
        """
        if not sections:
            self.logger.warning("No sections to save")
            return False

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

            self.logger.info("Saved %d records to %s", len(sections), output_path)
            return True
        except Exception as e:
            self.logger.error("Error saving to Parquet %s: %s", output_path, e)
            return False

    @error_handling(default_return=False)
    async def merge_parquet_files(
        self, parquet_files: List[Path], output_path: Path
    ) -> bool:
        """
        Merge multiple Parquet files into one.

        Args:
            parquet_files: List of Parquet file paths to merge.
            output_path: Path where to save the merged Parquet file.

        Returns:
            True if successful, False otherwise.
        """
        try:
            if len(parquet_files) <= 0:
                self.logger.warning("No Parquet files to merge")
                return False

            if len(parquet_files) == 1 and parquet_files[0] == output_path:
                self.logger.info(
                    "Only one file to merge, which is the output file. Skipping merge."
                )
                return True

            self.logger.info("Merging %d Parquet files", len(parquet_files))

            async def read_table(file):
                try:
                    return await asyncio.to_thread(pq.read_table, str(file))
                except Exception as e:
                    self.logger.error("Error reading Parquet file %s: %s", file, e)
                    return None

            tasks = [read_table(file) for file in parquet_files]
            tables = await asyncio.gather(*tasks)

            tables = [table for table in tables if table is not None]

            if not tables:
                self.logger.error("No tables could be read, merge failed")
                return False

            combined_table = pa.concat_tables(tables)

            await asyncio.to_thread(
                pq.write_table,
                combined_table,
                output_path,
                compression="snappy",
                row_group_size=self.batch_size,
            )

            self.logger.info("Successfully merged files into %s", output_path)
            return True
        except Exception as e:
            self.logger.error("Error merging Parquet files: %s", e)
            return False

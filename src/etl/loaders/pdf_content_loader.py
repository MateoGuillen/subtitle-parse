"""Loader for PDF content data."""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.error_handler import error_handling
from src.core.entities.pdf_models import PDFOutline, ContentSection
from src.utils.logging_utils import setup_logger


class PdfContentLoader:
    """
    Loader for PDF content data.

    This class is responsible for saving processed data to parquet files.

    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    def __init__(self):
        """Initialize the PdfContentLoader."""
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def save_outlines_with_lines(
        self, merged_df: pd.DataFrame, output_path: str
    ) -> pd.DataFrame:
        """
        Save outlines with line numbers to a parquet file.

        Args:
            merged_df (pd.DataFrame): DataFrame with outlines and line numbers.
            output_path (str): Path to save the parquet file.

        Returns:
            pd.DataFrame: The saved DataFrame.
        """
        self.logger.info("Saving outlines with lines to %s...", output_path)

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Get schema from model
        schema = PDFOutline.get_outline_with_lines_schema()

        # Save to parquet
        table = pa.Table.from_pandas(merged_df, schema=schema)
        pq.write_table(table, output_path, compression="snappy", row_group_size=10000)

        self.logger.info(
            "Saved %d outlines with lines to %s", len(merged_df), output_path
        )
        return merged_df

    @error_handling(default_return=None)
    def save_content_sections(
        self, sections_df: pd.DataFrame, output_path: str, include_metrics: bool = True
    ) -> pd.DataFrame:
        self.logger.info("Saving content sections to %s...", output_path)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        schema = (
            ContentSection.get_enhanced_schema()
            if include_metrics
            else ContentSection.get_schema()
        )
        schema_fields = schema.names

        # Asegurar columnas
        for field in schema_fields:
            if field not in sections_df.columns:
                field_type = schema.field(field).type
                if pa.types.is_string(field_type):
                    sections_df[field] = ""
                elif pa.types.is_integer(field_type):
                    sections_df[field] = 0
                else:
                    sections_df[field] = None

        sections_df = sections_df[
            [col for col in schema_fields if col in sections_df.columns]
        ]

        # ✅ Escribir en chunks para evitar OOM
        CHUNK_SIZE = 100_000
        writer = None

        try:
            for start in range(0, len(sections_df), CHUNK_SIZE):
                chunk = sections_df.iloc[start : start + CHUNK_SIZE]
                table = pa.Table.from_pandas(chunk, schema=schema)

                if writer is None:
                    writer = pq.ParquetWriter(
                        output_path, schema=schema, compression="snappy"
                    )

                writer.write_table(table)
                self.logger.info(
                    "Written %d/%d rows...",
                    min(start + CHUNK_SIZE, len(sections_df)),
                    len(sections_df),
                )
        finally:
            if writer:
                writer.close()

        self.logger.info(
            "Saved %d content sections to %s", len(sections_df), output_path
        )
        self.logger.info("Total documents: %d", sections_df["document_id"].nunique())
        self.logger.info(
            "Total licitaciones: %d", sections_df["nro_licitacion"].nunique()
        )
        self.logger.info(
            "Average content length: %.2f lines", sections_df["content_length"].mean()
        )

        return sections_df

    @error_handling(default_return=None)
    def save_content_sections_partitioned(
        self, sections_df: pd.DataFrame, output_dir: str
    ) -> None:
        """
        Save content sections partitioned by year using pyarrow.parquet.write_to_dataset.

        Each batch is appended to the existing partition, so calling this
        multiple times (once per pipeline batch) safely accumulates all data.

        Args:
            sections_df (pd.DataFrame): DataFrame with content sections for this batch.
            output_dir (str): Root directory for partitioned output.
                            Generates: output_dir/year=YYYY/part-N.parquet
        """
        schema = ContentSection.get_enhanced_schema()
        schema_fields = schema.names

        # Asegurar columnas requeridas
        for field in schema_fields:
            if field not in sections_df.columns:
                field_type = schema.field(field).type
                if pa.types.is_string(field_type):
                    sections_df[field] = ""
                elif pa.types.is_integer(field_type):
                    sections_df[field] = 0
                else:
                    sections_df[field] = None

        # Incluir 'year' para la partición (no está en schema_fields si es clave de partición)
        cols = [col for col in schema_fields if col in sections_df.columns]
        if "year" not in cols:
            cols.append("year")
        sections_df = sections_df[cols]

        # ✅ write_to_dataset append: si ya existe year=2021/, agrega un nuevo part-N
        pq.write_to_dataset(
            pa.Table.from_pandas(sections_df, preserve_index=False),
            root_path=output_dir,
            partition_cols=["year"],
            compression="snappy",
            existing_data_behavior="overwrite_or_ignore",
        )

        years = sections_df["year"].unique().tolist()
        self.logger.info(
            "Written %d rows to %s (years: %s)", len(sections_df), output_dir, years
        )

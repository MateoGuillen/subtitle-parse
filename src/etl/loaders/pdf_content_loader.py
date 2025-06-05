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
        """
        Save content sections to a parquet file.

        Args:
            sections_df (pd.DataFrame): DataFrame with content sections.
            output_path (str): Path to save the parquet file.
            include_metrics (bool): Whether to include additional metrics in the schema.
                                Default changed to True to include all metrics.

        Returns:
            pd.DataFrame: The saved DataFrame.
        """
        self.logger.info("Saving content sections to %s...", output_path)

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Get schema from model based on whether we want metrics
        if include_metrics:
            schema = ContentSection.get_enhanced_schema()
        else:
            schema = ContentSection.get_schema()

        # Ensure all columns in schema are in DataFrame
        # En PyArrow, usar schema.names para obtener los nombres de campos
        schema_fields = schema.names

        for field in schema_fields:
            if field not in sections_df.columns:
                field_type = schema.field(field).type
                if pa.types.is_string(field_type):
                    sections_df[field] = ""
                elif pa.types.is_integer(field_type):
                    sections_df[field] = 0
                else:
                    sections_df[field] = None

        # Keep only columns that are in the schema
        sections_df = sections_df[
            [col for col in schema_fields if col in sections_df.columns]
        ]

        # Save to parquet
        table = pa.Table.from_pandas(sections_df, schema=schema)
        pq.write_table(table, output_path, compression="snappy", row_group_size=10000)

        self.logger.info(
            "Saved %d content sections to %s", len(sections_df), output_path
        )

        # Print summary
        self.logger.info(
            "Total documents processed: %d", sections_df["document_id"].nunique()
        )
        self.logger.info(
            "Total licitaciones processed: %d", sections_df["nro_licitacion"].nunique()
        )
        self.logger.info(
            "Average content length: %.2f lines", sections_df["content_length"].mean()
        )

        return sections_df

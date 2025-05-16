"""Loader for PDF content data."""
import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.error_handler import error_handling

class PdfContentLoader:
    """
    Loader for PDF content data.
    
    This class is responsible for saving processed data to parquet files.
    
    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    def __init__(self):
        """Initialize the PdfContentLoader."""
        self.logger = logging.getLogger(__name__)

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

        # Define schema
        schema = pa.schema(
            [
                ("document_id", pa.string()),
                ("year", pa.string()),
                ("category_id", pa.string()),
                ("nro_licitacion", pa.string()),
                ("title", pa.string()),
                ("page", pa.int32()),
                ("depth", pa.int32()),
                ("line_number", pa.int32()),
            ]
        )

        # Save to parquet
        table = pa.Table.from_pandas(merged_df, schema=schema)
        pq.write_table(table, output_path, compression="snappy", row_group_size=10000)

        self.logger.info("Saved %d outlines with lines to %s", len(merged_df), output_path)
        return merged_df

    @error_handling(default_return=None)
    def save_content_sections(
        self, sections_df: pd.DataFrame, output_path: str
    ) -> pd.DataFrame:
        """
        Save content sections to a parquet file.
        
        Args:
            sections_df (pd.DataFrame): DataFrame with content sections.
            output_path (str): Path to save the parquet file.
            
        Returns:
            pd.DataFrame: The saved DataFrame.
        """
        self.logger.info("Saving content sections to %s...", output_path)

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Define schema
        schema = pa.schema(
            [
                ("document_id", pa.string()),
                ("nro_licitacion", pa.string()),
                ("category_id", pa.string()),
                ("year", pa.string()),
                ("title", pa.string()),
                ("content", pa.string()),
                ("page", pa.int32()),
                ("line_start", pa.int32()),
                ("line_end", pa.int32()),
                ("depth", pa.int32()),
                ("content_length", pa.int32()),
            ]
        )

        # Save to parquet
        table = pa.Table.from_pandas(sections_df, schema=schema)
        pq.write_table(table, output_path, compression="snappy", row_group_size=10000)

        self.logger.info("Saved %d content sections to %s", len(sections_df), output_path)

        # Print summary
        self.logger.info("Total documents processed: %d", sections_df["document_id"].nunique())
        self.logger.info(
            "Total licitaciones processed: %d", sections_df["nro_licitacion"].nunique()
        )
        self.logger.info("Average content length: %.2f lines", sections_df["content_length"].mean())

        return sections_df

"""Extractor for PDF outlines and lines."""
import logging
from typing import Tuple, Optional
import pandas as pd
from src.utils.error_handler import error_handling

class PdfContentExtractor:
    """
    Extractor for PDF content data.
    
    This class is responsible for loading PDF lines and outlines data from parquet files.
    
    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    def __init__(self):
        """Initialize the PdfContentExtractor."""
        self.logger = logging.getLogger(__name__)

    @error_handling(default_return=(None, None))
    def load_dataframes(
        self, pdf_lines_path: str, outlines_path: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load and prepare the PDF lines and outlines DataFrames.
        
        Args:
            pdf_lines_path (str): Path to the PDF lines parquet file.
            outlines_path (str): Path to the outlines parquet file.
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Loaded PDF lines and outlines DataFrames.
        """
        print(f"Loading PDF lines from {pdf_lines_path}...")
        pdf_lines_df = pd.read_parquet(pdf_lines_path)

        print(f"Loading outlines from {outlines_path}...")
        outlines_df = pd.read_parquet(outlines_path)

        print(f"Loaded {len(outlines_df)} outlines and {len(pdf_lines_df)} PDF lines")

        return pdf_lines_df, outlines_df

    @error_handling(default_return=None)
    def load_single_dataframe(self, path: str) -> Optional[pd.DataFrame]:
        """
        Load a single DataFrame from a parquet file.
        
        Args:
            path (str): Path to the parquet file.
            
        Returns:
            Optional[pd.DataFrame]: Loaded DataFrame or None if there was an error.
        """
        print(f"Loading data from {path}...")
        df = pd.read_parquet(path)
        print(f"Loaded {len(df)} rows from {path}")
        return df

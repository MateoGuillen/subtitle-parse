"""Outline Merger module for combining yearly outline files into single CSV and Parquet files."""

from pathlib import Path
from typing import Tuple, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class OutlineMerger:
    """
    Merges outline CSV files from multiple years into single CSV and Parquet files.

    Attributes:
        output_csv_dir (str): Directory to save merged CSV file.
        output_parquet_dir (str): Directory to save merged Parquet file.
        logger (logging.Logger): Logger object for logging messages.
    """

    def __init__(self, output_csv_dir: str, output_parquet_dir: str):
        self.output_csv_dir = Path(output_csv_dir)
        self.output_parquet_dir = Path(output_parquet_dir)
        self.logger = setup_logger(__name__)

        # Ensure directories exist
        self.output_csv_dir.mkdir(parents=True, exist_ok=True)
        self.output_parquet_dir.mkdir(parents=True, exist_ok=True)

    @error_handling(default_return=None)
    def read_yearly_outline(self, year_path: Path) -> Optional[pd.DataFrame]:
        """
        Read outline CSV file for a specific year.

        Args:
            year_path (Path): Path to the yearly outline CSV file.

        Returns:
            Optional[pd.DataFrame]: DataFrame containing outline data, or None if an error occurred.
        """

        # Read CSV with proper data types
        df = pd.read_csv(
            year_path,
            dtype={
                "document_id": str,
                "year": str,
                "category_id": str,
                "nro_licitacion": str,
                "title": str,
                "page": int,
                "depth": int,
            },
        )

        self.logger.info("Successfully read %s with %d rows", year_path, len(df))
        return df

    @error_handling(default_return=(None, None))
    def merge_and_save(
        self, years: list, base_path: str
    ) -> Tuple[Optional[Path], Optional[Path]]:
        """
        Merge outline CSV files from multiple years into single CSV and Parquet files.

        Args:
            years (list): List of years to process.
            base_path (str): Base directory containing year folders.

        Returns:
            Tuple[Optional[Path], Optional[Path]]: Paths to the saved CSV and Parquet files,
                                                  or (None, None) if an error occurred.
        """
        all_dataframes = []

        # Process each year
        for year in tqdm(years, desc="Processing years"):
            year_path = Path(base_path) / str(year) / f"outlines_{year}.csv"

            if year_path.exists():
                df = self.read_yearly_outline(year_path)
                if df is not None:
                    all_dataframes.append(df)
            else:
                self.logger.warning("File not found: %s", year_path)

        if not all_dataframes:
            self.logger.error("No data frames to merge")
            return None, None

        # Merge all dataframes
        merged_df = pd.concat(all_dataframes, ignore_index=True)
        self.logger.info("Total rows in merged dataset: %d", len(merged_df))

        # Save as CSV
        csv_path = self.output_csv_dir / "merged_outlines.csv"
        merged_df.to_csv(csv_path, index=False, encoding="utf-8")
        self.logger.info("Saved merged CSV to %s", csv_path)

        # Save as Parquet
        parquet_path = self.output_parquet_dir / "merged_outlines.parquet"

        # Define schema for Parquet
        schema = pa.schema(
            [
                ("document_id", pa.string()),
                ("year", pa.string()),
                ("category_id", pa.string()),
                ("nro_licitacion", pa.string()),
                ("title", pa.string()),
                ("page", pa.int32()),
                ("depth", pa.int32()),
            ]
        )

        # Convert to Parquet with compression
        table = pa.Table.from_pandas(merged_df, schema=schema)
        pq.write_table(table, parquet_path, compression="snappy", row_group_size=10000)
        self.logger.info("Saved merged Parquet to %s", parquet_path)

        # Log summary statistics
        self.logger.info("Merge Summary:")
        self.logger.info("Total rows: %d", len(merged_df))

        rows_per_year = merged_df["year"].value_counts().sort_index()
        self.logger.info("Rows per year:\n%s", rows_per_year.to_string())

        rows_per_category = merged_df["category_id"].value_counts().sort_index()
        self.logger.info("Rows per category:\n%s", rows_per_category.to_string())

        return csv_path, parquet_path

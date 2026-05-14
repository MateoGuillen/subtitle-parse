"""Extractor for sections-to-database pipeline."""

from typing import Iterator, List
import pandas as pd
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger
import os


class SectionsDbExtractor:
    """
    Extractor that reads cleaned sections from the partitioned parquet dataset
    produced by ContentCleaningPipeline and yields one year at a time.

    Reading year by year keeps peak RAM bounded to a single year's data
    (~300-500 MB) regardless of total dataset size.

    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    def __init__(self):
        """Initialize the SectionsDbExtractor."""
        self.logger = setup_logger(__name__)

    @error_handling(default_return=[])
    def get_available_years(self, sections_dir: str) -> List[int]:
        years = sorted(
            int(entry.name.split("year=")[-1])
            for entry in os.scandir(sections_dir)
            if entry.is_dir() and entry.name.startswith("year=")
        )
        self.logger.info("Available years: %s", years)
        return years

    @error_handling(default_return=None)
    def load_year(self, sections_dir: str, year: int) -> pd.DataFrame:
        year_dir = os.path.join(sections_dir, f"year={year}")
        self.logger.info("Loading year %d...", year)
        df = pd.read_parquet(year_dir)
        self.logger.info("Loaded %d rows for year %d", len(df), year)
        return df

    @error_handling(default_return=iter([]))
    def iter_years(self, sections_dir: str) -> Iterator[tuple]:
        """
        Iterate over each year's DataFrame one at a time.

        Yields (year, DataFrame) tuples so the pipeline can process and
        discard each year before loading the next.

        Args:
            sections_dir (str): Root directory of the cleaned parquet dataset.

        Yields:
            tuple: (year: int, df: pd.DataFrame)
        """
        years = self.get_available_years(sections_dir)
        for year in years:
            df = self.load_year(sections_dir, year)
            if df is not None and not df.empty:
                yield year, df

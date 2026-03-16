"""Extractor for content cleaning pipeline."""

import os
from typing import Optional, Iterator
import pandas as pd
import pyarrow.parquet as pq
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


class ContentCleaningExtractor:
    """
    Extractor for the content cleaning pipeline.

    Reads partitioned parquet files (year=YYYY/) produced by the PDF content
    pipeline and exposes them for cleaning. Supports reading year-by-year to
    keep memory usage bounded.

    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    def __init__(self):
        """Initialize the ContentCleaningExtractor."""
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def load_year(self, sections_dir: str, year: str) -> Optional[pd.DataFrame]:
        self.logger.info("Loading sections for year %s from %s...", year, sections_dir)

        year_filter = int(year)
        df = pd.read_parquet(sections_dir, filters=[("year", "=", year_filter)])

        self.logger.info("Loaded %d sections for year %s", len(df), year)
        return df

    @error_handling(default_return=[])
    def get_available_years(self, sections_dir: str) -> list:
        dataset = pq.ParquetDataset(sections_dir)
        years = sorted(
            {
                str(frag.path).split("year=")[-1].split("/")[0]
                for frag in dataset.fragments
            }
        )
        self.logger.info("Available years in dataset: %s", years)
        return years

    @error_handling(default_return=[])
    def get_available_years(self, sections_dir: str) -> list:
        """
        Discover which years are available in the partitioned dataset.

        Scans the root directory for folders named ``year=YYYY`` — the
        standard Hive partitioning layout written by ``write_to_dataset``.
        This approach is version-agnostic and does not depend on PyArrow's
        internal fragment API.

        Args:
            sections_dir (str): Root directory of partitioned parquet dataset.

        Returns:
            list: Sorted list of year strings, e.g. ["2019", "2020", "2021"].
        """
        years = sorted(
            entry.name.split("year=")[-1]
            for entry in os.scandir(sections_dir)
            if entry.is_dir() and entry.name.startswith("year=")
        )
        self.logger.info("Available years in dataset: %s", years)
        return years

    @error_handling(default_return=iter([]))
    def iter_years(self, sections_dir: str) -> Iterator[tuple]:
        """
        Iterate over each year's DataFrame one at a time.

        Yields one (year, DataFrame) tuple at a time so the pipeline can
        process and discard each year before loading the next, keeping
        peak RAM to a single year's worth of data.

        Args:
            sections_dir (str): Root directory of partitioned parquet dataset.

        Yields:
            tuple: (year: str, df: pd.DataFrame)
        """
        years = self.get_available_years(sections_dir)
        for year in years:
            df = self.load_year(sections_dir, year)
            if df is not None:
                yield year, df

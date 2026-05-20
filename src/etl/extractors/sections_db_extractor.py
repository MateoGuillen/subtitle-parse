"""Extractor for sections-to-database pipeline."""

from typing import List
import pandas as pd
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger
import os


class SectionsDbExtractor:
    """
    Extractor that reads cleaned sections from the partitioned parquet dataset
    produced by ContentCleaningPipeline and yields one part file at a time.

    Reading part by part keeps peak RAM bounded to a single part's data
    (~200 MB per 200K rows) regardless of total dataset size.
    """

    def __init__(self):
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

    @error_handling(default_return=[])
    def get_part_files(self, sections_dir: str, year: int) -> List[str]:
        year_dir = os.path.join(sections_dir, f"year={year}")
        if not os.path.isdir(year_dir):
            self.logger.warning("Directory not found: %s", year_dir)
            return []
        parts = sorted(
            entry.path
            for entry in os.scandir(year_dir)
            if entry.is_file() and entry.name.endswith(".parquet")
        )
        self.logger.info("Year %d: %d part files", year, len(parts))
        return parts

    @error_handling(default_return=None)
    def load_part(self, part_path: str) -> pd.DataFrame:
        self.logger.info("Loading %s...", part_path)
        df = pd.read_parquet(part_path)
        self.logger.info("Loaded %d rows from %s", len(df), part_path)
        return df

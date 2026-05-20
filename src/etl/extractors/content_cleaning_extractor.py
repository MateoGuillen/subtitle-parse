"""Extractor for content cleaning pipeline."""

import os
from typing import Optional, Iterator
import pandas as pd
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


READ_COLUMNS = [
    "document_id", "nro_licitacion", "category_id", "year",
    "title", "page", "line_start", "line_end", "depth",
    "content_length", "estimated_tokens", "word_count", "size_bytes",
    "content",
]


class ContentCleaningExtractor:

    def __init__(self):
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def load_year(self, sections_dir: str, year: str) -> Optional[pd.DataFrame]:
        self.logger.info("Loading sections for year %s from %s...", year, sections_dir)
        year_filter = int(year)
        df = pd.read_parquet(
            sections_dir,
            filters=[("year", "=", year_filter)],
            columns=READ_COLUMNS,
        )
        self.logger.info("Loaded %d sections for year %s", len(df), year)
        return df

    @error_handling(default_return=[])
    def get_available_years(self, sections_dir: str) -> list:
        years = sorted(
            entry.name.split("year=")[-1]
            for entry in os.scandir(sections_dir)
            if entry.is_dir() and entry.name.startswith("year=")
        )
        self.logger.info("Available years in dataset: %s", years)
        return years

    @error_handling(default_return=iter([]))
    def iter_years(self, sections_dir: str) -> Iterator[tuple]:
        years = self.get_available_years(sections_dir)
        for year in years:
            df = self.load_year(sections_dir, year)
            if df is not None:
                yield year, df

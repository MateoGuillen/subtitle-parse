"""Pipeline for loading cleaned sections into PostgreSQL."""

import os
from src.etl.extractors.sections_db_extractor import SectionsDbExtractor
from src.etl.transformers.sections_db_transformer import SectionsDbTransformer
from src.etl.loaders.sections_db_loader import SectionsDbLoader
from src.utils.logging_utils import setup_logger


class SectionsToDbPipeline:
    """
    Pipeline that reads cleaned sections from partitioned parquet and
    inserts them into PostgreSQL using COPY for maximum throughput.

    Processing per year (one part file at a time to limit RAM):
        1. List all part-*.parquet files in year directory.
        2. For each part: load → prepare tuples → bulk-insert.
        3. ANALYZE partition to refresh query planner statistics.
        4. Mark year checkpoint as done (idempotent on re-run).
    """

    CHECKPOINT_PREFIX = ".sections_db_done_"

    def __init__(self, config: dict):
        self.config = config
        self.extractor = SectionsDbExtractor()
        self.transformer = SectionsDbTransformer()
        self.loader = SectionsDbLoader()
        self.logger = setup_logger(__name__)

    def _checkpoint_path(self, year: int) -> str:
        return os.path.join(
            self.config["sections_dir"],
            f"{self.CHECKPOINT_PREFIX}{year}",
        )

    def _is_year_done(self, year: int) -> bool:
        return os.path.exists(self._checkpoint_path(year))

    def _mark_year_done(self, year: int) -> None:
        with open(self._checkpoint_path(year), "w") as f:
            f.write(f"done year {year}\n")

    def run(self):
        self.logger.info("Starting sections-to-DB pipeline...")

        sections_dir = self.config["sections_dir"]
        db_dsn = self.config["db_dsn"]
        batch_size = self.config.get("batch_size", 10_000)
        target_years = self.config.get("years", None)
        start_year = self.config.get("start_year", None)

        self.loader.connect(db_dsn)

        try:
            total_rows = 0
            years = self.extractor.get_available_years(sections_dir)

            # Filter by start_year
            if start_year is not None:
                years = [y for y in years if y >= start_year]

            # Filter by explicit target list
            if target_years is not None:
                years = [y for y in years if y in target_years]

            for year in years:
                if self._is_year_done(year):
                    self.logger.info("Year %d already completed (checkpoint found), skipping.", year)
                    continue

                part_files = self.extractor.get_part_files(sections_dir, year)
                if not part_files:
                    self.logger.warning("No part files for year %d, skipping.", year)
                    continue

                year_rows = 0
                for part_path in part_files:
                    df = self.extractor.load_part(part_path)
                    if df is None or df.empty:
                        continue

                    self.logger.info("Processing year %d (%d rows from %s)...", year, len(df), part_path)
                    rows = self.transformer.prepare_sections_rows(df)
                    del df

                    inserted = self.loader.copy_sections(rows, batch_size=batch_size)
                    del rows

                    year_rows += inserted
                    self.logger.info("Part done: %d rows inserted for year %d (cumulative %d).", inserted, year, year_rows)

                if year_rows > 0:
                    self.loader.run_analyze(year)

                self._mark_year_done(year)
                total_rows += year_rows
                self.logger.info("Year %d done: %d rows inserted.", year, year_rows)

        finally:
            self.loader.disconnect()

        self.logger.info("Pipeline complete. Total rows inserted: %d", total_rows)

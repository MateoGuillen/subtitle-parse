"""Pipeline for loading cleaned sections into PostgreSQL."""

from src.etl.extractors.sections_db_extractor import SectionsDbExtractor
from src.etl.transformers.sections_db_transformer import SectionsDbTransformer
from src.etl.loaders.sections_db_loader import SectionsDbLoader
from src.utils.logging_utils import setup_logger


class SectionsToDbPipeline:
    """
    Pipeline that reads cleaned sections from partitioned parquet and
    inserts them into PostgreSQL using COPY for maximum throughput.

    Processing order per year:
        1. Load year's DataFrame from parquet.
        2. Upsert categorias (FK root — must exist first).
        3. Upsert licitaciones (FK parent of pliegos_secciones).
        4. Prepare row tuples for COPY.
        5. Bulk-insert sections in batches of BATCH_SIZE.
        6. ANALYZE partition to refresh query planner statistics.

    Attributes:
        config (dict): Pipeline configuration.
        extractor  (SectionsDbExtractor):  reads parquet.
        transformer(SectionsDbTransformer): prepares rows.
        loader     (SectionsDbLoader):      writes to PostgreSQL.
        logger     (logging.Logger): logger for this class.
    """

    def __init__(self, config: dict):
        """
        Initialize the pipeline.

        Args:
            config (dict): Must contain:
                - sections_dir (str): root of cleaned parquet dataset.
                - db_dsn       (str): PostgreSQL DSN string.
                - batch_size   (int): rows per commit batch (default 10_000).
                - years        (list[int] | None): specific years to load,
                               or None to load all available years.
        """
        self.config = config
        self.extractor = SectionsDbExtractor()
        self.transformer = SectionsDbTransformer()
        self.loader = SectionsDbLoader()
        self.logger = setup_logger(__name__)

    def run(self):
        """
        Execute the full load pipeline.

        Connects to PostgreSQL, iterates over each year's parquet data,
        and performs upserts + bulk inserts year by year.
        """
        self.logger.info("Starting sections-to-DB pipeline...")

        sections_dir = self.config["sections_dir"]
        db_dsn = self.config["db_dsn"]
        batch_size = self.config.get("batch_size", 10_000)
        target_years = self.config.get("years", None)  # None = all years

        self.loader.connect(db_dsn)

        try:
            total_rows = 0

            for year, df in self.extractor.iter_years(sections_dir):

                # Skip years not in the target list (if specified)
                if target_years and year not in target_years:
                    self.logger.info("Skipping year %d (not in target list).", year)
                    continue

                self.logger.info("Processing year %d (%d sections)...", year, len(df))

                # Step 1 — upsert FK parents
                cat_df = self.transformer.prepare_categorias(df)
                licit_df = self.transformer.prepare_licitaciones(df)

                self.loader.upsert_categorias(cat_df)
                self.loader.upsert_licitaciones(licit_df)

                # Step 2 — bulk insert sections
                rows = self.transformer.prepare_sections_rows(df)
                # print(rows[0])  # debug: show sample row format
                del df  # free memory before inserting

                inserted = self.loader.copy_sections(rows, batch_size=batch_size)
                self.logger.info("Inserted %d sections for year %d.", inserted, year)
                del rows

                # Step 3 — refresh planner statistics
                self.loader.run_analyze(year)

                total_rows += inserted
                self.logger.info("Year %d done: %d rows inserted.", year, inserted)

        finally:
            self.loader.disconnect()

        self.logger.info("Pipeline complete. Total rows inserted: %d", total_rows)

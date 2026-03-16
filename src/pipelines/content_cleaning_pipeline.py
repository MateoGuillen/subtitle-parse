"""Pipeline for cleaning the content field of extracted PDF sections."""

from src.etl.extractors.content_cleaning_extractor import ContentCleaningExtractor
from src.etl.transformers.content_cleaning_transformer import ContentCleaningTransformer
from src.etl.loaders.content_cleaning_loader import ContentCleaningLoader
from src.utils.logging_utils import setup_logger


class ContentCleaningPipeline:
    """
    Pipeline for cleaning the ``content`` field of extracted PDF sections.

    Reads the partitioned parquet dataset produced by ``PdfContentPipeline``
    (layout: ``sections_dir/year=YYYY/part-N.parquet``), applies a sequential
    text-cleaning pipeline to each section's raw line list, and writes the
    result to a new partitioned dataset under ``cleaned_sections_dir``.

    Processing is done one year at a time so that peak RAM usage is bounded
    by the size of a single year's data (~300-500 MB) rather than the full
    2 GB dataset.

    Attributes:
        config (dict): Configuration dictionary with the following keys:
            - ``sections_dir``         : input partitioned parquet directory.
            - ``cleaned_sections_dir`` : output partitioned parquet directory.
        extractor  (ContentCleaningExtractor):  reads input partitions.
        transformer(ContentCleaningTransformer): applies cleaning steps.
        loader     (ContentCleaningLoader):      writes output partitions.
        logger     (logging.Logger): logger for this class.
    """

    def __init__(self, config: dict):
        """
        Initialize the ContentCleaningPipeline.

        Args:
            config (dict): Configuration dictionary (see class docstring).
        """
        self.config = config
        self.extractor = ContentCleaningExtractor()
        self.transformer = ContentCleaningTransformer()
        self.loader = ContentCleaningLoader()
        self.logger = setup_logger(__name__)

    def run(self):
        """
        Execute the full content-cleaning pipeline.

        For each year found in the input dataset:
            1. Load the year's parquet partition into memory.
            2. Apply all cleaning steps via the transformer.
            3. Save the cleaned DataFrame to the output partition.
            4. Immediately free the DataFrames to keep RAM usage low.
        """
        self.logger.info("Starting content cleaning pipeline...")

        sections_dir = self.config["sections_dir"]
        cleaned_sections_dir = self.config["cleaned_sections_dir"]

        for year, df in self.extractor.iter_years(sections_dir):
            sample = df["content"].iloc[0]
            self.logger.info(
                "content type: %s | value: %s", type(sample), repr(sample)[:200]
            )
            break

        total_rows_in = 0
        total_rows_out = 0

        for year, df in self.extractor.iter_years(sections_dir):
            self.logger.info("── Processing year %s (%d sections) ──", year, len(df))
            rows_in = len(df)
            total_rows_in += rows_in

            # Apply cleaning pipeline
            df_clean = self.transformer.clean_content_field(df)
            del df  # free raw data immediately

            if df_clean is None:
                self.logger.error("Cleaning failed for year %s, skipping.", year)
                continue

            # Optional: drop rows where cleaned content is completely empty.
            # These are sections that contained only noise (page numbers, separators).
            empty_mask = df_clean["content_length_clean"] == 0
            n_empty = empty_mask.sum()
            if n_empty > 0:
                self.logger.info(
                    "Dropping %d fully-empty sections for year %s", n_empty, year
                )
            df_clean = df_clean[~empty_mask]

            rows_out = len(df_clean)
            total_rows_out += rows_out

            # Save partitioned output
            self.loader.save_cleaned_sections_partitioned(
                df_clean, cleaned_sections_dir
            )
            del df_clean  # free cleaned data immediately

            self.logger.info(
                "Year %s done: %d → %d rows (dropped %d empty)",
                year,
                rows_in,
                rows_out,
                rows_in - rows_out,
            )

        self.logger.info("Content cleaning pipeline completed.")
        self.logger.info(
            "Total: %d input rows → %d output rows (%.1f%% kept)",
            total_rows_in,
            total_rows_out,
            (total_rows_out / total_rows_in * 100) if total_rows_in > 0 else 0,
        )

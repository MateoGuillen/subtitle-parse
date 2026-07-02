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
            2. Process and clean in batches of BATCH_SIZE rows.
            3. Save each batch to the output partition immediately.
            4. Free each batch from RAM before loading the next.
        """
        self.logger.info("Starting content cleaning pipeline...")

        sections_dir = self.config["sections_dir"]
        cleaned_sections_dir = self.config["cleaned_sections_dir"]
        BATCH_SIZE = self.config.get("batch_size", 200_000)
        years_filter = self.config.get("years", None)

        total_rows_in = 0
        total_rows_out = 0

        for year, year_df in self.extractor.iter_years(sections_dir):
            if years_filter is not None and str(year) not in [str(y) for y in years_filter]:
                self.logger.info("Skipping year %s (not in filter %s)", year, years_filter)
                continue

            total_rows = len(year_df)
            total_rows_in += total_rows
            rows_out = 0

            self.logger.info(
                "Processing year %s (%d sections, batch_size=%d)...",
                year,
                total_rows,
                BATCH_SIZE,
            )

            # ✅ Procesar por batches — nunca más de BATCH_SIZE filas
            # limpias en RAM simultáneamente
            for start in range(0, total_rows, BATCH_SIZE):
                end = min(start + BATCH_SIZE, total_rows)
                batch_df = year_df.iloc[start:end].copy()

                self.logger.info(
                    "Year %s: cleaning rows %d-%d / %d...", year, start, end, total_rows
                )

                df_clean = self.transformer.clean_content_field(batch_df)
                del batch_df  # ✅ liberar batch raw inmediatamente

                if df_clean is None:
                    self.logger.error(
                        "Cleaning failed for year %s batch %d-%d, skipping.",
                        year,
                        start,
                        end,
                    )
                    continue

                empty_mask = df_clean["content_length_clean"] == 0
                n_empty = empty_mask.sum()
                if n_empty > 0:
                    self.logger.info(
                        "Dropping %d empty sections in batch %d-%d", n_empty, start, end
                    )
                df_clean = df_clean[~empty_mask]

                rows_out += len(df_clean)

                # ✅ Escribir a disco y liberar antes del siguiente batch
                self.loader.save_cleaned_sections_partitioned(
                    df_clean, cleaned_sections_dir
                )
                del df_clean

            del year_df  # ✅ liberar el año completo

            total_rows_out += rows_out
            self.logger.info(
                "Year %s done: %d -> %d rows (dropped %d empty)",
                year,
                total_rows,
                rows_out,
                total_rows - rows_out,
            )

        self.logger.info("Content cleaning pipeline completed.")
        self.logger.info(
            "Total: %d input rows -> %d output rows (%.1f%% kept)",
            total_rows_in,
            total_rows_out,
            (total_rows_out / total_rows_in * 100) if total_rows_in > 0 else 0,
        )

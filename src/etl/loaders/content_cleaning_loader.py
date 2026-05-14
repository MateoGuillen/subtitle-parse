"""Loader for content cleaning pipeline."""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


# Output schema for the cleaned sections dataset.
# We keep all original fields and append the three cleaning outputs.
CLEANED_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string()),
        pa.field("nro_licitacion", pa.string()),
        pa.field("category_id", pa.string()),
        pa.field("year", pa.string()),
        pa.field("title", pa.string()),
        pa.field("page", pa.int32()),
        pa.field("line_start", pa.int32()),
        pa.field("line_end", pa.int32()),
        pa.field("depth", pa.int32()),
        pa.field("content_length", pa.int32()),
        pa.field("estimated_tokens", pa.int32()),
        pa.field("word_count", pa.int32()),
        pa.field("size_bytes", pa.int32()),
        # ── Cleaning outputs ──────────────────────────────────────────────
        pa.field("content_clean", pa.list_(pa.string())),  # cleaned line list
        pa.field("content_text", pa.string()),  # joined string for LLM
        pa.field("content_length_clean", pa.int32()),  # lines after cleaning
        pa.field(
            "title_normalized", pa.string()
        ),  # lowercase accent-free title for queries
    ]
)


class ContentCleaningLoader:
    """
    Loader for the content cleaning pipeline.

    Saves cleaned DataFrames partitioned by year so the next pipeline
    (LLM structured output) can load one year at a time with the same
    memory-efficient pattern used in the extraction pipeline.

    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    def __init__(self):
        """Initialize the ContentCleaningLoader."""
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def save_cleaned_sections_partitioned(
        self, df: pd.DataFrame, output_dir: str
    ) -> None:
        """
        Save a cleaned DataFrame to a partitioned parquet dataset.

        Uses ``pyarrow.parquet.write_to_dataset`` with
        ``existing_data_behavior="overwrite_or_ignore"`` so that calling
        this method once per year appends a new part file to the correct
        partition folder without touching other years.

        Output layout::

            output_dir/
              year=2019/part-0.parquet
              year=2020/part-0.parquet
              year=2021/part-0.parquet
              ...

        Args:
            df (pd.DataFrame): Cleaned DataFrame for one year (or one batch).
            output_dir (str): Root directory for the partitioned output.
            chunk_size (int): Rows per write chunk. Default 100_000.
                              Reduce to 50_000 if OOM persists.
        """
        os.makedirs(output_dir, exist_ok=True)

        # Ensure all schema columns are present with correct defaults
        for field in CLEANED_SCHEMA:
            if field.name not in df.columns:
                if pa.types.is_string(field.type):
                    df[field.name] = ""
                elif pa.types.is_integer(field.type):
                    df[field.name] = 0
                elif pa.types.is_list(field.type):
                    df[field.name] = [[] for _ in range(len(df))]
                else:
                    df[field.name] = None

        schema_names = CLEANED_SCHEMA.names
        cols = [c for c in schema_names if c in df.columns]
        if "year" not in cols:
            cols.append("year")
        df = df[cols].copy()
        df["year"] = df["year"].astype(str)

        # ✅ Escribir por año y por chunks de 100k filas.
        # pa.Table.from_pandas() sobre 1-2M filas consume 2GB+ de RAM de golpe.
        # Con chunks de 100k nunca se piden más de ~100MB por conversión.
        CHUNK_SIZE = 100_000
        writers = {}
        try:
            for year, year_df in df.groupby("year", observed=True):
                year_dir = os.path.join(output_dir, f"year={year}")
                os.makedirs(year_dir, exist_ok=True)
                existing_parts = [
                    f
                    for f in os.listdir(year_dir)
                    if f.startswith("part-") and f.endswith(".parquet")
                ]
                next_part = len(existing_parts)
                part_path = os.path.join(year_dir, f"part-{next_part}.parquet")

                writer = pq.ParquetWriter(
                    part_path, CLEANED_SCHEMA, compression="snappy"
                )
                writers[year] = writer

                total_year = len(year_df)
                written = 0

                for start in range(0, total_year, CHUNK_SIZE):
                    chunk = year_df.iloc[start : start + CHUNK_SIZE]
                    table = pa.Table.from_pandas(
                        chunk, schema=CLEANED_SCHEMA, preserve_index=False
                    )
                    writer.write_table(table)
                    del table, chunk
                    written += min(CHUNK_SIZE, total_year - start)
                    self.logger.info(
                        "Year %s: written %d / %d rows...",
                        year,
                        written,
                        total_year,
                    )
        finally:
            for year, writer in writers.items():
                try:
                    writer.close()
                except Exception:
                    pass

        years = df["year"].unique().tolist()
        self.logger.info(
            "Saved %d cleaned rows to %s (years: %s)",
            len(df),
            output_dir,
            years,
        )

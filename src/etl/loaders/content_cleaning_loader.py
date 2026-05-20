"""Loader for content cleaning pipeline."""

import os
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


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
        pa.field("content_clean", pa.list_(pa.string())),
        pa.field("content_text", pa.string()),
        pa.field("content_length_clean", pa.int32()),
        pa.field("title_normalized", pa.string()),
    ]
)


class ContentCleaningLoader:

    def __init__(self):
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def save_cleaned_sections_partitioned(
        self, df: pd.DataFrame, output_dir: str
    ) -> None:
        os.makedirs(output_dir, exist_ok=True)

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

        cols = [c for c in CLEANED_SCHEMA.names if c in df.columns]
        df = df[cols].copy()
        df["year"] = df["year"].astype(str)

        suffix = uuid.uuid4().hex[:8]
        for year, year_df in df.groupby("year", observed=True):
            year_dir = os.path.join(output_dir, f"year={year}")
            os.makedirs(year_dir, exist_ok=True)
            part_path = os.path.join(year_dir, f"part-{suffix}.parquet")

            table = pa.Table.from_pandas(
                year_df, schema=CLEANED_SCHEMA, preserve_index=False
            )
            with pq.ParquetWriter(part_path, CLEANED_SCHEMA, compression="snappy") as writer:
                writer.write_table(table)

        years = df["year"].unique().tolist()
        self.logger.info(
            "Saved %d cleaned rows to %s (years: %s)",
            len(df),
            output_dir,
            years,
        )

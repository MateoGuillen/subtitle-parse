"""Transformer for sections-to-database pipeline."""

import logging
from typing import List
import pandas as pd
from src.utils.error_handler import error_handling


class SectionsDbTransformer:
    """
    Transformer that prepares cleaned section DataFrames for bulk insertion
    into PostgreSQL.

    Responsibilities:
    - Cast parquet types to PostgreSQL-compatible Python types.
    - Convert TEXT[] arrays (content_clean) to the format psycopg2 expects.
    - Build row tuples in the exact column order expected by the COPY command.

    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    # Columns to insert into pliegos_secciones, in exact DDL order.
    # Must match the COPY target column list in the loader.
    SECTION_COLUMNS = [
        "document_id",
        "nro_licitacion",
        "category_id",
        "year",
        "title",
        "title_normalized",
        "page",
        "line_start",
        "line_end",
        "depth",
        "content_length",
        "estimated_tokens",
        "word_count",
        "size_bytes",
        "content_text",
        "content_clean",
        "content_length_clean",
    ]

    def __init__(self):
        """Initialize the SectionsDbTransformer."""
        self.logger = logging.getLogger(__name__)

    @error_handling(default_return=[])
    def prepare_sections_rows(self, df: pd.DataFrame) -> List[tuple]:
        """
        Convert the sections DataFrame into a list of tuples ready for
        PostgreSQL COPY.

        Type conversions applied:
        - year, page, depth, line_start, line_end → Python int (or 0)
        - content_length, estimated_tokens, word_count, size_bytes,
          content_length_clean → Python int (or 0)
        - content_clean (list/array) → Python list of str
        - All string fields → str or None (no numpy strings).

        Args:
            df (pd.DataFrame): Cleaned sections DataFrame.

        Returns:
            List[tuple]: Row tuples in SECTION_COLUMNS order.
        """
        self.logger.info("Converting %d rows to insert tuples...", len(df))

        rows = []
        for row in df.itertuples(index=False):
            rows.append((
                str(row.document_id) if row.document_id is not None else None,
                str(row.nro_licitacion) if row.nro_licitacion is not None else None,
                str(row.category_id) if row.category_id is not None else None,
                int(row.year) if row.year is not None else None,
                str(row.title) if row.title is not None else None,
                str(row.title_normalized) if row.title_normalized is not None else None,
                int(row.page) if row.page is not None else None,
                int(row.line_start) if row.line_start is not None else None,
                int(row.line_end) if row.line_end is not None else None,
                int(row.depth) if row.depth is not None else None,
                int(row.content_length) if row.content_length is not None else None,
                int(row.estimated_tokens) if row.estimated_tokens is not None else None,
                int(row.word_count) if row.word_count is not None else None,
                int(row.size_bytes) if row.size_bytes is not None else None,
                str(row.content_text) if row.content_text is not None else None,
                self._to_pg_array(row.content_clean),
                int(row.content_length_clean) if row.content_length_clean is not None else None,
            ))

        self.logger.info("Prepared %d row tuples", len(rows))
        return rows

    @staticmethod
    def _to_pg_array(val):
        if val is None:
            return []
        if isinstance(val, list):
            return [str(v) for v in val]
        if hasattr(val, "tolist"):
            return [str(v) for v in val.tolist()]
        return []

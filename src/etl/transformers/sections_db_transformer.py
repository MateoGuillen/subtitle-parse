"""Transformer for sections-to-database pipeline."""

import logging
from typing import List, Tuple, Set
import pandas as pd
from src.utils.error_handler import error_handling


class SectionsDbTransformer:
    """
    Transformer that prepares cleaned section DataFrames for bulk insertion
    into PostgreSQL.

    Responsibilities:
    - Cast parquet types to PostgreSQL-compatible Python types.
    - Convert TEXT[] arrays (content_clean) to the format psycopg2 expects.
    - Extract unique licitaciones and categorias for upsert before sections.
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

    @error_handling(default_return=None)
    def prepare_licitaciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract unique licitaciones from the sections DataFrame.

        WHY: The licitaciones table must be populated before sections because
        pliegos_secciones has a FK to licitaciones.nro_licitacion.  Using
        INSERT ... ON CONFLICT DO NOTHING (upsert) makes the operation
        idempotent — safe to re-run without duplicating rows.

        Args:
            df (pd.DataFrame): Cleaned sections DataFrame.

        Returns:
            pd.DataFrame: Unique (nro_licitacion, category_id, year) rows.
        """
        licit_df = (
            df[["nro_licitacion", "category_id", "year"]]
            .drop_duplicates(subset=["nro_licitacion"])
            .copy()
        )
        licit_df["year"] = licit_df["year"].astype("Int16")
        self.logger.info(
            "Prepared %d unique licitaciones", len(licit_df)
        )
        return licit_df

    @error_handling(default_return=None)
    def prepare_categorias(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract unique categorias from the sections DataFrame.

        WHY: Same idempotency rationale as licitaciones.  The categorias
        table has no descripcion in the parquet data, so it is inserted
        with NULL descripcion and can be enriched later.

        Args:
            df (pd.DataFrame): Cleaned sections DataFrame.

        Returns:
            pd.DataFrame: Unique (category_id,) rows.
        """
        cat_df = (
            df[["category_id"]]
            .drop_duplicates()
            .copy()
        )
        cat_df = cat_df[cat_df["category_id"].notna()]
        self.logger.info("Prepared %d unique categorias", len(cat_df))
        return cat_df

    @error_handling(default_return=[])
    def prepare_sections_rows(self, df: pd.DataFrame) -> List[tuple]:
        """
        Convert the sections DataFrame into a list of tuples ready for
        PostgreSQL COPY.

        Type conversions applied:
        - year, page, depth, line_start, line_end → Python int (or None)
          WHY: parquet int32/int16 are numpy types; psycopg2 needs native
          Python int to map to SMALLINT/INTEGER correctly.
        - content_length, estimated_tokens, word_count, size_bytes,
          content_length_clean → Python int (or None)
        - content_clean (list/array) → Python list of str
          WHY: psycopg2 serializes Python list[str] as PostgreSQL TEXT[].
        - All string fields → str or None (no numpy strings).

        Args:
            df (pd.DataFrame): Cleaned sections DataFrame.

        Returns:
            List[tuple]: Row tuples in SECTION_COLUMNS order.
        """
        self.logger.info("Converting %d rows to insert tuples...", len(df))

        def safe_int(val):
            """Convert numpy int or NaN to Python int or None."""
            if val is None:
                return None
            try:
                import math
                if math.isnan(float(val)):
                    return None
                return int(val)
            except (TypeError, ValueError):
                return None

        def safe_str(val):
            """Convert to str or None."""
            if val is None or (isinstance(val, float) and val != val):
                return None
            return str(val)

        def safe_list(val):
            """Convert content_clean to Python list[str] or empty list."""
            if val is None:
                return []
            if isinstance(val, list):
                return [str(v) for v in val]
            try:
                import numpy as np
                if isinstance(val, np.ndarray):
                    return [str(v) for v in val.tolist()]
            except ImportError:
                pass
            if hasattr(val, "as_py"):
                py = val.as_py()
                return [str(v) for v in py] if isinstance(py, list) else []
            return []

        rows = []
        for _, row in df.iterrows():
            rows.append((
                safe_str(row.get("document_id")),
                safe_str(row.get("nro_licitacion")),
                safe_str(row.get("category_id")),
                safe_int(row.get("year")),
                safe_str(row.get("title")),
                safe_str(row.get("title_normalized")),
                safe_int(row.get("page")),
                safe_int(row.get("line_start")),
                safe_int(row.get("line_end")),
                safe_int(row.get("depth")),
                safe_int(row.get("content_length")),
                safe_int(row.get("estimated_tokens")),
                safe_int(row.get("word_count")),
                safe_int(row.get("size_bytes")),
                safe_str(row.get("content_text")),
                safe_list(row.get("content_clean")),
                safe_int(row.get("content_length_clean")),
            ))

        self.logger.info("Prepared %d row tuples", len(rows))
        return rows

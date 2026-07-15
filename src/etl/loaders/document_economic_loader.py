"""Loader for document-level economic features."""

from typing import List
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class DocumentEconomicLoader:
    """
    Populates the ``dncp.document_economic_features`` table.

    Uses ``INSERT … ON CONFLICT DO NOTHING`` to avoid duplicates.
    """

    TARGET_TABLE = "dncp.document_economic_features"

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)
        self._conn = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(**self.db_params)
        self._conn.autocommit = False
        self.logger.info("Connected to PostgreSQL.")

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self.logger.info("Disconnected.")

    @error_handling(default_return=False)
    def truncate_and_insert(self, df: pd.DataFrame) -> bool:
        """
        Truncate the table and bulk-insert using ``execute_values``.

        Args:
            df: Feature DataFrame whose columns match the table's column set.

        Returns:
            True on success.
        """
        self.logger.info("Truncating %s…", self.TARGET_TABLE)
        with self._conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {self.TARGET_TABLE}")
        self._conn.commit()

        self.logger.info("Inserting %d rows into %s…", len(df), self.TARGET_TABLE)
        columns = [c for c in df.columns if c != "category_id"]
        # Convert datetime NaT to None (psycopg2 handles None as NULL)
        df_clean = df[columns].copy()
        for col in df_clean.select_dtypes(include=["datetime"]).columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: None if pd.isna(x) else x
            )
        cols_str = ", ".join(columns)
        rows = self._df_to_tuples(df_clean)

        sql = (
            f"INSERT INTO {self.TARGET_TABLE} ({cols_str}) VALUES %s"
            f" ON CONFLICT (nro_licitacion) DO NOTHING"
        )

        with self._conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        self._conn.commit()
        self.logger.info("Inserted %d rows successfully.", len(rows))
        return True

    def create_indexes(self) -> None:
        self.logger.info("Creating index on convocante_region…")
        with self._conn.cursor() as cur:
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_def_region "
                f"ON {self.TARGET_TABLE} (convocante_region)"
            )
        self._conn.commit()

    def validate_statistics(self, df: pd.DataFrame) -> None:
        self.logger.info("=== Economic Feature Statistics ===")
        self.logger.info("Documents (rows):  %d", len(df))
        self.logger.info("Features (cols):   %d", len(df.columns))
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            s = df[col]
            missing = s.isna().sum()
            self.logger.info(
                "  %-40s  min=%12.2f  max=%12.2f  mean=%12.2f  std=%12.2f  nulls=%d",
                col,
                s.min() if not s.empty else 0,
                s.max() if not s.empty else 0,
                s.mean() if not s.empty else 0,
                s.std() if not s.empty else 0,
                missing,
            )

    @staticmethod
    def _df_to_tuples(df: pd.DataFrame) -> List[tuple]:
        rows = []
        for row in df.itertuples(index=False):
            cleaned = tuple(
                None if (v is None or pd.isna(v))
                else (int(v) if isinstance(v, (np.integer,)) else
                      float(v) if isinstance(v, (np.floating,)) else
                      bool(v) if isinstance(v, (np.bool_,)) else v)
                for v in row
            )
            rows.append(cleaned)
        return rows

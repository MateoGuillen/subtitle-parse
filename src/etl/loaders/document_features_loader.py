"""Loader for document features pipeline."""

from typing import List
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class DocumentFeaturesLoader:
    """
    Creates and populates the ``dncp.document_features`` table.

    **DDL strategy (Enfoque 1 — DDL dinámico):**

    1. ``DROP TABLE IF EXISTS`` + ``CREATE TABLE`` with the fixed columns
       (``nro_licitacion`` PK, aggregates, year, category_id).
    2. ``ALTER TABLE … ADD COLUMN`` for each title feature column
       (``has_*`` as SMALLINT, ``len_*`` / ``tok_*`` as INTEGER).
    3. ``INSERT`` via ``psycopg2.extras.execute_values`` for bulk throughput.
    4. ``CREATE INDEX`` on ``year`` and ``category_id``.
    """

    TARGET_TABLE = "dncp.document_features"

    FIXED_COLUMNS_DDL = """
        nro_licitacion         TEXT PRIMARY KEY,
        year                   SMALLINT NOT NULL,
        category_id            TEXT,
        total_sections         SMALLINT NOT NULL,
        unique_titles          SMALLINT NOT NULL,
        avg_content_length     NUMERIC(10,2),
        std_content_length     NUMERIC(10,2),
        max_content_length     INTEGER,
        sum_content_length     INTEGER,
        avg_tokens             NUMERIC(10,2),
        std_tokens             NUMERIC(10,2),
        max_tokens             INTEGER,
        sum_tokens             INTEGER,
        avg_size_bytes         NUMERIC(10,2),
        std_size_bytes         NUMERIC(10,2),
        max_size_bytes         INTEGER,
        sum_size_bytes         INTEGER
    """

    def __init__(self, db_params: dict):
        """
        Args:
            db_params: psycopg2-compatible connection dictionary with keys
                       ``host``, ``port``, ``dbname``, ``user``, ``password``.
        """
        self.db_params = db_params
        self.logger = setup_logger(__name__)
        self._conn = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open a transactional psycopg2 connection."""
        self.logger.info("Connecting to PostgreSQL…")
        self._conn = psycopg2.connect(**self.db_params)
        self._conn.autocommit = False
        self.logger.info("Connected.")

    def disconnect(self) -> None:
        """Close the connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self.logger.info("Disconnected.")

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    @error_handling(default_return=False)
    def _execute(self, sql: str) -> bool:
        """Execute a single SQL statement and commit."""
        with self._conn.cursor() as cur:
            cur.execute(sql)
        self._conn.commit()
        return True

    def create_table(self, title_safe_names: List[str]) -> None:
        """
        Drop existing table, create with fixed columns, then ``ALTER TABLE``
        for each title feature column.

        Args:
            title_safe_names: List of safe column names for title features
                              (e.g. ``["has_fraude", "len_fraude", "tok_fraude",
                              …, "has_otros", "len_otros", "tok_otros"]``).
        """
        self.logger.info("Creating table %s…", self.TARGET_TABLE)

        # Drop
        self._execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")

        # Create with fixed columns
        create_sql = f"CREATE TABLE {self.TARGET_TABLE} (\n{self.FIXED_COLUMNS_DDL}\n)"
        self._execute(create_sql)
        self.logger.info("Base table created with 17 fixed columns.")

        # ALTER TABLE for each title feature column
        added = 0
        for name in title_safe_names:
            if name.startswith("has_"):
                col_type = "SMALLINT DEFAULT 0"
            elif name.startswith("len_") or name.startswith("tok_"):
                col_type = "INTEGER DEFAULT 0"
            else:
                continue
            sql = f"ALTER TABLE {self.TARGET_TABLE} ADD COLUMN {name} {col_type}"
            self._execute(sql)
            added += 1

        self.logger.info("Added %d title feature columns via ALTER TABLE.", added)

    # ------------------------------------------------------------------
    # Data insertion
    # ------------------------------------------------------------------

    @error_handling(default_return=False)
    def insert_features(self, df: pd.DataFrame) -> bool:
        """
        Bulk-insert the feature matrix using ``execute_values``.

        Args:
            df: Feature DataFrame whose columns exactly match the table's
                column set.  Index is ignored.

        Returns:
            True on success.
        """
        self.logger.info("Inserting %d rows into %s…", len(df), self.TARGET_TABLE)

        columns = list(df.columns)
        cols_str = ", ".join(columns)

        # Convert DataFrame rows to tuples, replacing NaN with None
        rows = self._df_to_tuples(df)

        sql = (
            f"INSERT INTO {self.TARGET_TABLE} ({cols_str}) VALUES %s"
            f" ON CONFLICT (nro_licitacion) DO NOTHING"
        )

        with self._conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        self._conn.commit()

        self.logger.info("Inserted %d rows successfully.", len(rows))
        return True

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    def create_indexes(self) -> None:
        """Create indexes on ``year`` and ``category_id``."""
        self.logger.info("Creating indexes…")
        self._execute(
            f"CREATE INDEX IF NOT EXISTS idx_df_year "
            f"ON {self.TARGET_TABLE} (year)"
        )
        self._execute(
            f"CREATE INDEX IF NOT EXISTS idx_df_category_id "
            f"ON {self.TARGET_TABLE} (category_id)"
        )
        self.logger.info("Indexes created on year, category_id.")

    # ------------------------------------------------------------------
    # Validation / statistics
    # ------------------------------------------------------------------

    def validate_statistics(self, df: pd.DataFrame) -> None:
        """Log basic descriptive statistics for the new feature table."""
        self.logger.info("=== Feature Statistics ===")
        self.logger.info("Documents (rows):  %d", len(df))
        self.logger.info("Features (cols):   %d", len(df.columns))

        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            s = df[col]
            missing = s.isna().sum()
            self.logger.info(
                "  %-45s  min=%12.2f  max=%12.2f  mean=%12.2f  std=%12.2f  nulls=%d",
                col,
                s.min() if not s.empty else 0,
                s.max() if not s.empty else 0,
                s.mean() if not s.empty else 0,
                s.std() if not s.empty else 0,
                missing,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _df_to_tuples(df: pd.DataFrame) -> List[tuple]:
        """
        Convert a DataFrame into a list of Python-native tuples suitable for
        ``psycopg2.extras.execute_values``.

        * ``numpy.integer`` → ``int``
        * ``numpy.floating`` → ``float``
        * ``numpy.bool_``    → ``bool``
        * ``numpy.nan``     → ``None``
        """
        rows = []
        for row in df.itertuples(index=False):
            cleaned = tuple(
                None
                if (v is None or (isinstance(v, float) and np.isnan(v)))
                else (int(v) if isinstance(v, (np.integer,)) else
                      float(v) if isinstance(v, (np.floating,)) else
                      bool(v) if isinstance(v, (np.bool_,)) else v)
                for v in row
            )
            rows.append(cleaned)
        return rows

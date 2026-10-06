from typing import List
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class DocumentFeaturesV2Loader:
    TARGET_TABLE = "dncp.document_features_v2"

    FIXED_COLUMNS_DDL = """
        nro_licitacion     TEXT PRIMARY KEY,
        year               SMALLINT NOT NULL,
        category_id        TEXT
    """

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)
        self._conn = None

    def connect(self) -> None:
        self.logger.info("Connecting to PostgreSQL…")
        self._conn = psycopg2.connect(**self.db_params)
        self._conn.autocommit = False
        self.logger.info("Connected.")

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self.logger.info("Disconnected.")

    @error_handling(default_return=False)
    def _execute(self, sql: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(sql)
        self._conn.commit()
        return True

    def create_table(self, schema_fields: List[dict]) -> None:
        self.logger.info("Creating table %s…", self.TARGET_TABLE)
        self._execute(f"DROP TABLE IF EXISTS {self.TARGET_TABLE}")

        create_sql = f"CREATE TABLE {self.TARGET_TABLE} (\n{self.FIXED_COLUMNS_DDL}\n)"
        self._execute(create_sql)
        self.logger.info("Base table created with fixed columns.")

        added = 0
        for f in schema_fields:
            col_type = self._pg_type(f["type"])
            sql = f"ALTER TABLE {self.TARGET_TABLE} ADD COLUMN {f['name']} {col_type}"
            self._execute(sql)
            added += 1

            if f.get("extraction_method") == "hybrid":
                conf_col = f"{f['name']}_conf"
                self._execute(
                    f"ALTER TABLE {self.TARGET_TABLE} ADD COLUMN {conf_col} REAL DEFAULT 1.0"
                )
                added += 1
                src_col = f"{f['name']}_source"
                self._execute(
                    f"ALTER TABLE {self.TARGET_TABLE} ADD COLUMN {src_col} TEXT DEFAULT 'regex'"
                )
                added += 1
        self.logger.info("Added %d feature columns via ALTER TABLE.", added)

    @staticmethod
    def _pg_type(ftype: str) -> str:
        if ftype == "int":
            return "INTEGER DEFAULT 0"
        elif ftype == "float":
            return "NUMERIC(12,4) DEFAULT 0"
        elif ftype == "binary":
            return "SMALLINT DEFAULT 0"
        elif ftype == "categorical":
            return "TEXT DEFAULT ''"
        return "INTEGER DEFAULT 0"

    @error_handling(default_return=False)
    def insert_features(self, df: pd.DataFrame) -> bool:
        self.logger.info("Inserting %d rows into %s…", len(df), self.TARGET_TABLE)
        columns = list(df.columns)
        cols_str = ", ".join(columns)
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

    def create_indexes(self) -> None:
        self.logger.info("Creating indexes…")
        self._execute(
            f"CREATE INDEX IF NOT EXISTS idx_dfv2_year "
            f"ON {self.TARGET_TABLE} (year)"
        )
        self._execute(
            f"CREATE INDEX IF NOT EXISTS idx_dfv2_category_id "
            f"ON {self.TARGET_TABLE} (category_id)"
        )
        self.logger.info("Indexes created on year, category_id.")

    def validate_statistics(self, df: pd.DataFrame) -> None:
        self.logger.info("=== Feature Statistics v2 ===")
        self.logger.info("Documents (rows):  %d", len(df))
        self.logger.info("Features (cols):   %d", len(df.columns))
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            s = df[col]
            missing = s.isna().sum()
            self.logger.info(
                "  %-45s  min=%12.4f  max=%12.4f  mean=%12.4f  std=%12.4f  nulls=%d",
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
                None
                if (v is None or (isinstance(v, float) and np.isnan(v)))
                else (int(v) if isinstance(v, (np.integer,)) else
                      float(v) if isinstance(v, (np.floating,)) else
                      bool(v) if isinstance(v, (np.bool_,)) else v)
                for v in row
            )
            rows.append(cleaned)
        return rows

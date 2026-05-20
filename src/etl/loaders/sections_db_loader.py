"""Loader for sections-to-database pipeline."""

import logging
from typing import List
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from src.utils.error_handler import error_handling


class SectionsDbLoader:
    """
    Loader that inserts cleaned sections into PostgreSQL using COPY for
    maximum bulk-insert performance.

    Why COPY instead of INSERT:
        PostgreSQL COPY bypasses the query planner and writes directly to
        heap pages, achieving 10-50x higher throughput than individual
        INSERT statements.  For 2.8M rows this difference is the gap
        between minutes and hours.

    Connection management:
        A single connection is reused across all batches to avoid the
        overhead of repeated TCP handshakes and authentication.

    Attributes:
        logger (logging.Logger): Logger for this class.
        _conn: Active psycopg2 connection (None until connect() is called).
    """

    # Target table and column order — must match SectionsDbTransformer.SECTION_COLUMNS
    TARGET_TABLE = "dncp.pliegos_secciones"
    TARGET_COLUMNS = (
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
    )

    def __init__(self):
        """Initialize the SectionsDbLoader."""
        self.logger = logging.getLogger(__name__)
        self._conn = None

    def connect(self, dsn: str) -> None:
        """
        Open a connection to PostgreSQL.

        Args:
            dsn (str): PostgreSQL DSN string.
                       Example: "host=localhost port=5432 dbname=dncp
                                 user=postgres password=secret"
        """
        self.logger.info("Connecting to PostgreSQL...")
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        self.logger.info("Connected.")

    def disconnect(self) -> None:
        """Close the PostgreSQL connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self.logger.info("Disconnected from PostgreSQL.")

    @contextmanager
    def _cursor(self):
        """Context manager that yields a cursor and commits/rolls back."""
        cur = self._conn.cursor()
        try:
            yield cur
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    @error_handling(default_return=0)
    def copy_sections(self, rows: List[tuple], batch_size: int = 10_000) -> int:
        """
        Bulk-insert section rows using PostgreSQL COPY.

        Uses psycopg2.extras.execute_values in batches of `batch_size`.
        Each batch is committed independently so a failure mid-year does
        not roll back already-inserted data.

        WHY batch commits: for 2.8M rows, a single transaction holds all
        dirty pages in memory until commit — risking OOM in WAL buffers.
        Committing every 10k rows keeps memory usage flat.

        Args:
            rows (List[tuple]): Row tuples in TARGET_COLUMNS order.
            batch_size (int): Rows per commit batch. Default 10_000.

        Returns:
            int: Total rows inserted.
        """
        if not rows:
            self.logger.warning("No rows to insert.")
            return 0

        cols = ", ".join(self.TARGET_COLUMNS)

        conflict_cols = "nro_licitacion, title, line_start, year"
        sql = (
            f"INSERT INTO {self.TARGET_TABLE} ({cols}) VALUES %s"
            f" ON CONFLICT ({conflict_cols}) DO NOTHING"
        )
        self.logger.info(
            "Inserting %d rows into %s in batches of %d...",
            len(rows),
            self.TARGET_TABLE,
            batch_size,
        )
        self.logger.debug("Sample row: %s", rows[0])
        self.logger.debug("SQL: %s", sql)

        total = 0
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            with self._cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    sql,
                    batch,
                    template=None,
                    page_size=batch_size,
                )
            total += len(batch)
            self.logger.info("Inserted %d / %d rows...", total, len(rows))

        return total

    @error_handling(default_return=None)
    def run_analyze(self, year: int) -> None:
        """
        Run ANALYZE on the year partition after loading.

        WHY: PostgreSQL's query planner relies on table statistics to choose
        efficient execution plans.  After a large bulk load the statistics
        are stale.  ANALYZE updates them so subsequent queries use optimal
        indexes and join strategies.

        Args:
            year (int): The year partition just loaded.
        """
        partition = f"dncp.pliegos_secciones_{year}"
        self.logger.info("Running ANALYZE on %s...", partition)
        with self._cursor() as cur:
            cur.execute(f"ANALYZE {partition}")
        self.logger.info("ANALYZE complete for %s.", partition)

"""Extractor for document features pipeline."""

from typing import List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class DocumentFeaturesExtractor:
    """
    Extracts section data from ``dncp.pliegos_secciones`` for the document-level
    feature engineering pipeline.

    Two extraction modes:
        1. :meth:`get_top_titles` — lightweight aggregate query to find the N
           most frequent ``title_normalized`` values by document count.
        2. :meth:`load_all_sections` — full scan of all sections with the
           columns needed for pivot and aggregate features.
    """

    SECTION_QUERY = """
        SELECT nro_licitacion,
               title_normalized,
               content_length,
               estimated_tokens,
               size_bytes,
               year,
               category_id
        FROM dncp.pliegos_secciones
        WHERE title_normalized IS NOT NULL
        ORDER BY nro_licitacion
    """

    TOP_TITLES_QUERY = """
        SELECT title_normalized
        FROM dncp.pliegos_secciones
        WHERE title_normalized IS NOT NULL
        GROUP BY title_normalized
        ORDER BY COUNT(DISTINCT nro_licitacion) DESC
        LIMIT :top_n
    """

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)
        conn_str = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        self._engine = create_engine(conn_str)

    @error_handling(default_return=[])
    def get_top_titles(self, top_n: int = 80) -> List[str]:
        """
        Return the *top_n* most frequent ``title_normalized`` values,
        ordered by number of distinct documents they appear in
        (descending).

        Args:
            top_n: Number of top titles to return (default 80).

        Returns:
            List of title strings, most frequent first.
        """
        with self._engine.connect() as conn:
            result = pd.read_sql(
                text(self.TOP_TITLES_QUERY), conn, params={"top_n": top_n}
            )
        titles = result["title_normalized"].tolist()
        self.logger.info("Top %d titles: %s ...", top_n, titles[:5])
        return titles

    @error_handling(default_return=None)
    def load_all_sections(self) -> Optional[pd.DataFrame]:
        """
        Load every section row from ``dncp.pliegos_secciones`` that has a
        non-null ``title_normalized``.

        Returns:
            DataFrame with columns ``nro_licitacion``, ``title_normalized``,
            ``content_length``, ``estimated_tokens``, ``size_bytes``,
            ``year``, ``category_id``, or *None* on failure.
        """
        self.logger.info("Loading all sections from database…")
        df = pd.read_sql(self.SECTION_QUERY, self._engine)
        if df.empty:
            self.logger.warning("No sections found.")
            return None
        self.logger.info(
            "Loaded %d sections from %d documents.",
            len(df),
            df["nro_licitacion"].nunique(),
        )
        return df

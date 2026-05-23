"""Extractor for title ranking pipeline."""

from typing import List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class TitleRankingExtractor:
    """
    Extracts data from ``dncp.document_features`` and
    ``dncp.pliegos_secciones`` for the title anomaly-ranking pipeline.
    """

    TOP_TITLES_QUERY = """
        SELECT title_normalized
        FROM dncp.pliegos_secciones
        WHERE title_normalized IS NOT NULL
        GROUP BY title_normalized
        ORDER BY COUNT(DISTINCT nro_licitacion) DESC
        LIMIT :top_n
    """

    DOC_FEATURES_QUERY = """
        SELECT *
        FROM dncp.document_features
        ORDER BY nro_licitacion
    """

    SECTIONS_SAMPLE_QUERY = """
        SELECT nro_licitacion,
               title_normalized,
               content_length,
               estimated_tokens,
               page,
               line_start,
               line_end,
               year,
               category_id
        FROM dncp.pliegos_secciones
        WHERE title_normalized IS NOT NULL
        ORDER BY random()
        LIMIT :n
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
        """Return original ``title_normalized`` values of the top *top_n* titles."""
        with self._engine.connect() as conn:
            result = pd.read_sql(
                text(self.TOP_TITLES_QUERY), conn, params={"top_n": top_n}
            )
        titles = result["title_normalized"].tolist()
        self.logger.info("Top %d titles retrieved.", len(titles))
        return titles

    @error_handling(default_return=None)
    def get_document_features(self) -> Optional[pd.DataFrame]:
        """Load the full ``dncp.document_features`` table."""
        self.logger.info("Loading document features…")
        df = pd.read_sql(self.DOC_FEATURES_QUERY, self._engine)
        self.logger.info("Loaded %d rows x %d cols.", len(df), len(df.columns))
        return df

    @error_handling(default_return=None)
    def sample_sections(
        self, n: int = 200_000, top_titles: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """Sample *n* sections from ``dncp.pliegos_secciones``, optionally
        filtered to *top_titles*."""
        self.logger.info("Sampling %d sections…", n)
        with self._engine.connect() as conn:
            df = pd.read_sql(
                text(self.SECTIONS_SAMPLE_QUERY), conn, params={"n": n}
            )
        if top_titles:
            title_set = set(top_titles)
            before = len(df)
            df = df[df["title_normalized"].isin(title_set)]
            self.logger.info(
                "Filtered to top titles: %d -> %d rows.", before, len(df)
            )
        self.logger.info("Sampled %d sections.", len(df))
        return df

    @staticmethod
    def get_title_slugs_from_columns(df: pd.DataFrame) -> List[str]:
        """Extract title slugs from ``has_*`` column names in the document-features
        DataFrame.  Returns a sorted list of unique slugs."""
        slugs = sorted({
            c[4:]
            for c in df.columns
            if c.startswith("has_") and c != "has_otros"
        })
        return slugs

    @staticmethod
    def slug_to_display(slug: str) -> str:
        """Convert a safe column slug to a human-readable display name."""
        return slug.replace("_", " ").title()

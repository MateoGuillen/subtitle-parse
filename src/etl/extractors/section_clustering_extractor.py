"""Extractor for the section clustering & schema pipeline."""

from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


# Mapeo de nombres solicitados -> nombres reales en la BD
TITLE_MAPPING = {
    "fraude y corrupcion": "fraude y corrupcion",
    "formato y firma de la oferta": "formato y firma de la oferta",
    "copias de la oferta cps": "copias de la oferta - cps",
    "limitacion de responsabilidad": "limitacion de responsabilidad",
    "planos y disenos": "planos y disenos",
    "porcentaje de garantia de fiel cumplimiento de con": (
        "porcentaje de garantia de fiel cumplimiento de contrato"
    ),
    "idioma de la oferta": "idioma de la oferta",
    "aclaracion de las ofertas": "aclaracion de las ofertas",
    "retiro sustitucion y modificacion de las ofertas": (
        "retiro, sustitucion y modificacion de las ofertas"
    ),
    "audiencia informativa": "audiencia informativa",
}

TOP_10_TITLES = list(TITLE_MAPPING.keys())
DB_TITLE_MAP = {v: k for k, v in TITLE_MAPPING.items()}


class SectionClusteringExtractor:
    """
    Extracts sections from ``dncp.pliegos_secciones`` for a given list of
    title_normalized values.
    """

    SECTIONS_BY_TITLE_QUERY = """
        SELECT nro_licitacion, content_text, year, category_id
        FROM dncp.pliegos_secciones
        WHERE LOWER(title_normalized) = :title
          AND content_text IS NOT NULL
    """

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)
        conn_str = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        self._engine = create_engine(conn_str)

    @error_handling(default_return={})
    def extract_all_titles(self) -> Dict[str, pd.DataFrame]:
        """For each top-10 title, query sections and return a dict of
        ``{title: DataFrame}``."""
        result: Dict[str, pd.DataFrame] = {}
        with self._engine.connect() as conn:
            for display_title in TOP_10_TITLES:
                db_title = TITLE_MAPPING[display_title]
                self.logger.info(
                    "Extracting sections for '%s' (DB: '%s')",
                    display_title,
                    db_title,
                )
                df = pd.read_sql(
                    text(self.SECTIONS_BY_TITLE_QUERY),
                    conn,
                    params={"title": db_title},
                )
                df = df.dropna(subset=["content_text"])
                df = df[df["content_text"].str.strip().astype(bool)]
                self.logger.info(
                    "  -> %d sections retrieved for '%s'.",
                    len(df),
                    display_title,
                )
                result[display_title] = df
        return result

    @staticmethod
    def get_top_10_titles() -> List[str]:
        return list(TOP_10_TITLES)

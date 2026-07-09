"""Extractor for the section clustering & schema pipeline (v2.1).

Reads the list of titles from the ranking CSV instead of hardcoding.
"""

from typing import Dict, List, Optional
import os
import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


# Default fallback mapping (used only if no ranking CSV is provided)
DEFAULT_TITLE_MAPPING = {
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

# Slug -> DB title mapping (converts rank slugs back to raw DB titles)
SLUG_TO_DB = {
    "fraude_y_corrupcion": "fraude y corrupcion",
    "formato_y_firma_de_la_oferta": "formato y firma de la oferta",
    "copias_de_la_oferta__cps": "copias de la oferta - cps",
    "limitacion_de_responsabilidad": "limitacion de responsabilidad",
    "planos_y_disenos": "planos y disenos",
    "porcentaje_de_garantia_de_fiel_cumplimiento_de_con": (
        "porcentaje de garantia de fiel cumplimiento de contrato"
    ),
    "idioma_de_la_oferta": "idioma de la oferta",
    "aclaracion_de_las_ofertas": "aclaracion de las ofertas",
    "retiro_sustitucion_y_modificacion_de_las_ofertas": (
        "retiro, sustitucion y modificacion de las ofertas"
    ),
    "audiencia_informativa": "audiencia informativa",
}


class SectionClusteringExtractor:
    """
    Extracts sections from ``dncp.pliegos_secciones`` for titles read from
    the ranking CSV or a fallback hardcoded list.
    """

    SECTIONS_BY_TITLE_QUERY = """
        SELECT nro_licitacion, content_text, year, category_id
        FROM dncp.pliegos_secciones
        WHERE LOWER(title_normalized) = :title
          AND content_text IS NOT NULL
    """

    def __init__(self, db_params: dict, ranking_csv: Optional[str] = None):
        self.db_params = db_params
        self.ranking_csv = ranking_csv
        self.logger = setup_logger(__name__)
        conn_str = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        self._engine = create_engine(conn_str)
        self._titles = None

    def _load_titles_from_ranking(self, top_k: int = 10) -> Dict[str, str]:
        """
        Read top-K titles from the ranking CSV and map slugs to DB titles.
        Falls back to DEFAULT_TITLE_MAPPING if CSV is not available.
        """
        if self.ranking_csv and os.path.exists(self.ranking_csv):
            try:
                df = pd.read_csv(self.ranking_csv)
                if "title_slug" not in df.columns:
                    self.logger.warning(
                        "Ranking CSV missing 'title_slug' column. Using defaults."
                    )
                    return dict(DEFAULT_TITLE_MAPPING)

                df = df.sort_values("rank" if "rank" in df.columns else "score_total",
                                     ascending="rank" in df.columns)
                top_slugs = df.head(top_k)["title_slug"].tolist()
                mapping = {}
                for slug in top_slugs:
                    db_title = SLUG_TO_DB.get(slug)
                    if not db_title:
                        db_title = slug.replace("_", " ")
                    mapping[slug.replace("_", " ").title()] = db_title
                self.logger.info(
                    "Loaded %d titles from ranking CSV: %s",
                    len(mapping), list(mapping.keys()),
                )
                return mapping
            except Exception as e:
                self.logger.warning(
                    "Error reading ranking CSV '%s': %s. Using defaults.",
                    self.ranking_csv, e,
                )

        self.logger.info("No ranking CSV provided. Using default title mapping.")
        return dict(DEFAULT_TITLE_MAPPING)

    def get_title_mapping(self, top_k: int = 10) -> Dict[str, str]:
        if self._titles is None:
            self._titles = self._load_titles_from_ranking(top_k)
        return self._titles

    @error_handling(default_return={})
    def extract_all_titles(self, top_k: int = 10) -> Dict[str, pd.DataFrame]:
        """For each title (from ranking CSV or defaults), query sections and
        return ``{display_name: DataFrame}``."""
        title_mapping = self.get_title_mapping(top_k)
        result: Dict[str, pd.DataFrame] = {}
        with self._engine.connect() as conn:
            for display_title, db_title in title_mapping.items():
                self.logger.info(
                    "Extracting sections for '%s' (DB: '%s')",
                    display_title, db_title,
                )
                df = pd.read_sql(
                    text(self.SECTIONS_BY_TITLE_QUERY),
                    conn,
                    params={"title": db_title},
                )
                df = df.dropna(subset=["content_text"])
                df = df[df["content_text"].str.strip().astype(bool)]
                self.logger.info("  -> %d sections.", len(df))
                result[display_title] = df
        return result

    def get_top_titles(self, top_k: int = 10) -> List[str]:
        return list(self.get_title_mapping(top_k).keys())

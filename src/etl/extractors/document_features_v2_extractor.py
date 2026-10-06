from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class DocumentFeaturesV2Extractor:
    SECTION_QUERY = """
        SELECT nro_licitacion,
               title_normalized,
               content_text,
               content_length,
               year,
               category_id,
               word_count,
               page
        FROM dncp.pliegos_secciones
        WHERE title_normalized = ANY(:titles)
        ORDER BY nro_licitacion
    """

    def __init__(self, db_params: dict, final_titles: list):
        self.db_params = db_params
        self.final_titles = final_titles
        self.logger = setup_logger(__name__)
        conn_str = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        self._engine = create_engine(conn_str)

    @error_handling(default_return=None)
    def load_all_sections(self) -> Optional[pd.DataFrame]:
        self.logger.info(
            "Loading sections for %d finalist titles (chunked)…",
            len(self.final_titles),
        )
        chunks = []
        for i, chunk in enumerate(
            pd.read_sql(
                text(self.SECTION_QUERY),
                self._engine,
                params={"titles": list(self.final_titles)},
                chunksize=200_000,
            )
        ):
            for col in chunk.select_dtypes(include=["int64"]).columns:
                chunk[col] = chunk[col].astype("int32")
            chunks.append(chunk)
            self.logger.info("  Loaded chunk %d (%d rows)", i + 1, len(chunk))
        if not chunks:
            self.logger.warning("No sections found for finalist titles.")
            return None
        df = pd.concat(chunks, ignore_index=True)
        self.logger.info(
            "Loaded %d sections from %d documents (%d unique titles).",
            len(df),
            df["nro_licitacion"].nunique(),
            df["title_normalized"].nunique(),
        )
        return df

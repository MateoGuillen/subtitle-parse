"""
Extractor for pliego documents from database.
"""

import psycopg2
from typing import List, Tuple, Optional
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class PliegoExtractor:
    """
    Extractor for pliego documents from the database.

    This class handles the extraction of pliego content from the dncp.pliegos table
    based on different criteria like title, nro_licitacion, or date ranges.
    """

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)

    @error_handling(default_return=[])
    def get_pliegos_by_title(self, title: str) -> List[Tuple[str, str]]:
        """
        Get pliegos content by title.

        Args:
            title: The title to filter pliegos

        Returns:
            List of tuples (nro_licitacion, content)
        """
        query = """
            SELECT nro_licitacion, content
            FROM dncp.pliegos
            WHERE title = %s 
            AND content IS NOT NULL 
            AND LENGTH(content) > 100
            ORDER BY nro_licitacion
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (title,))
                results = cur.fetchall()

        self.logger.info(f"Extraídos {len(results)} pliegos para title: {title}")
        return results

    @error_handling(default_return=[])
    def get_pliegos_by_licitacion(
        self, nro_licitacion: str
    ) -> List[Tuple[str, str, str]]:
        """
        Get all pliegos for a specific licitacion.

        Args:
            nro_licitacion: The licitacion number

        Returns:
            List of tuples (title, content, title_normalizado)
        """
        query = """
            SELECT title, content, title_normalizado
            FROM dncp.pliegos
            WHERE nro_licitacion = %s 
            AND content IS NOT NULL 
            AND LENGTH(content) > 100
            ORDER BY title
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (nro_licitacion,))
                results = cur.fetchall()

        self.logger.debug(
            f"Extraídos {len(results)} pliegos para licitación: {nro_licitacion}"
        )
        return results

    @error_handling(default_return=[])
    def get_pliegos_batch(
        self, title: str, offset: int = 0, limit: int = 100
    ) -> List[Tuple[str, str]]:
        """
        Get pliegos in batches for memory-efficient processing.

        Args:
            title: The title to filter pliegos
            offset: Starting position
            limit: Number of records to return

        Returns:
            List of tuples (nro_licitacion, content)
        """
        query = """
            SELECT nro_licitacion, content
            FROM dncp.pliegos
            WHERE title = %s 
            AND content IS NOT NULL 
            AND LENGTH(content) > 100
            ORDER BY nro_licitacion
            OFFSET %s LIMIT %s
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (title, offset, limit))
                results = cur.fetchall()

        self.logger.debug(
            f"Extraídos {len(results)} pliegos (batch: {offset}-{offset+limit})"
        )
        return results

    @error_handling(default_return=0)
    def count_pliegos_by_title(self, title: str) -> int:
        """
        Count total pliegos for a title.

        Args:
            title: The title to count

        Returns:
            Total count of pliegos
        """
        query = """
            SELECT COUNT(*)
            FROM dncp.pliegos
            WHERE title = %s 
            AND content IS NOT NULL 
            AND LENGTH(content) > 100
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (title,))
                count = cur.fetchone()[0]

        return count

    @error_handling(default_return=[])
    def get_available_titles(self) -> List[str]:
        """
        Get list of available titles in pliegos table.

        Returns:
            List of unique titles
        """
        query = """
            SELECT DISTINCT title
            FROM dncp.pliegos
            WHERE content IS NOT NULL 
            AND LENGTH(content) > 100
            ORDER BY title
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = [row[0] for row in cur.fetchall()]

        self.logger.info(f"Encontrados {len(results)} títulos únicos")
        return results

    @error_handling(default_return=[])
    def get_pliegos_by_year(self, title: str, year: str) -> List[Tuple[str, str]]:
        """
        Get pliegos filtered by title and year.

        Args:
            title: The title to filter pliegos
            year: The year to filter

        Returns:
            List of tuples (nro_licitacion, content)
        """
        query = """
            SELECT nro_licitacion, content
            FROM dncp.pliegos
            WHERE title = %s 
            AND year = %s
            AND content IS NOT NULL 
            AND LENGTH(content) > 100
            ORDER BY nro_licitacion
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (title, year))
                results = cur.fetchall()

        self.logger.info(f"Extraídos {len(results)} pliegos para {title} en {year}")
        return results

    @error_handling(default_return=None)
    def get_pliego_content(self, nro_licitacion: str, title: str) -> Optional[str]:
        """
        Get specific pliego content.

        Args:
            nro_licitacion: The licitacion number
            title: The title of the pliego

        Returns:
            Content string or None if not found
        """
        query = """
            SELECT content
            FROM dncp.pliegos
            WHERE nro_licitacion = %s 
            AND title = %s
            AND content IS NOT NULL
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (nro_licitacion, title))
                result = cur.fetchone()

        return result[0] if result else None

    @error_handling(default_return={})
    def get_pliego_stats(self) -> dict:
        """
        Get statistics about pliegos data.

        Returns:
            Dictionary with statistics
        """
        query = """
            SELECT 
                COUNT(*) as total_pliegos,
                COUNT(DISTINCT nro_licitacion) as total_licitaciones,
                COUNT(DISTINCT title) as total_titles,
                COUNT(DISTINCT year) as total_years,
                AVG(LENGTH(content)) as avg_content_length
            FROM dncp.pliegos
            WHERE content IS NOT NULL
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()

        if result:
            return {
                "total_pliegos": result[0],
                "total_licitaciones": result[1],
                "total_titles": result[2],
                "total_years": result[3],
                "avg_content_length": float(result[4]) if result[4] else 0,
            }

        return {}

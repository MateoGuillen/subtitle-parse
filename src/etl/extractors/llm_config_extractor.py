"""
Extractor for LLM configuration from database.
"""

from typing import Dict, List, Optional
import json
import psycopg2
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class LLMConfigExtractor:
    """
    Extractor for LLM configurations from the database.

    This class handles the extraction of LLM configuration data from the
    dncp.llm_config table, including prompts, schemas, and model parameters.
    """

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def get_config_by_slug(self, title_slug: str) -> Optional[Dict]:
        """
        Get LLM configuration by title_slug.

        Args:
            title_slug: The slug identifier for the configuration

        Returns:
            Dictionary with configuration data or None if not found
        """
        query = """
            SELECT 
                title_slug,
                title,
                descripcion,
                prompt,
                json_schema,
                temperatura,
                max_tokens,
                modelo_recomendado,
                requires_postprocessing,
                feature_parser,
                activo
            FROM dncp.llm_config
            WHERE title_slug = %s AND activo = true
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (title_slug,))
                result = cur.fetchone()

        if not result:
            self.logger.warning(
                "No se encontró configuración activa para: %s", title_slug
            )
            return None

        config = {
            "title_slug": result[0],
            "title": result[1],
            "descripcion": result[2],
            "prompt": result[3],
            "json_schema": result[4],
            "temperatura": float(result[5]) if result[5] else 0.1,
            "max_tokens": int(result[6]) if result[6] else 800,
            "modelo_recomendado": result[7],
            "requires_postprocessing": result[8],
            "feature_parser": result[9],
            "activo": result[10],
        }

        # self.logger.debug(f"Configuración extraída para: {title_slug}")
        self.logger.debug("Configuración extraída para: %s", title_slug)
        return config

    @error_handling(default_return=[])
    def get_all_active_configs(self) -> List[Dict]:
        """
        Get all active LLM configurations.

        Returns:
            List of configuration dictionaries
        """
        query = """
            SELECT 
                title_slug,
                title,
                descripcion,
                prompt,
                json_schema,
                temperatura,
                max_tokens,
                modelo_recomendado,
                requires_postprocessing,
                feature_parser,
                activo
            FROM dncp.llm_config
            WHERE activo = true
            ORDER BY title_slug
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()

        configs = []
        for result in results:
            config = {
                "title_slug": result[0],
                "title": result[1],
                "descripcion": result[2],
                "prompt": result[3],
                "json_schema": result[4],
                "temperatura": float(result[5]) if result[5] else 0.1,
                "max_tokens": int(result[6]) if result[6] else 800,
                "modelo_recomendado": result[7],
                "requires_postprocessing": result[8],
                "feature_parser": result[9],
                "activo": result[10],
            }
            configs.append(config)

        # self.logger.info(f"Extraídas {len(configs)} configuraciones activas")
        self.logger.info("Extraídas %s configuraciones activas", len(configs))
        return configs

    @error_handling(default_return=[])
    def get_title_slugs(self) -> List[str]:
        """
        Get list of active title_slugs.

        Returns:
            List of title_slug strings
        """
        query = """
            SELECT title_slug
            FROM dncp.llm_config
            WHERE activo = true
            ORDER BY title_slug
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = [row[0] for row in cur.fetchall()]

        return results

    @error_handling(default_return=False)
    def validate_config(self, title_slug: str) -> bool:
        """
        Validate that a configuration has all required fields.

        Args:
            title_slug: The slug to validate

        Returns:
            True if configuration is valid, False otherwise
        """
        config = self.get_config_by_slug(title_slug)

        if not config:
            return False

        required_fields = ["title", "prompt", "json_schema"]
        for field in required_fields:
            if not config.get(field):
                # self.logger.error(f"Campo requerido faltante '{field}' en {title_slug}")
                self.logger.error(
                    "Campo requerido faltante '%s' en %s", field, title_slug
                )
                return False

        # Validate JSON schema format
        try:
            if isinstance(config["json_schema"], str):
                json.loads(config["json_schema"])
            elif not isinstance(config["json_schema"], dict):
                raise ValueError("json_schema debe ser dict o JSON string válido")
        except ValueError as e:
            # self.logger.error(f"JSON schema inválido en {title_slug}: {e}")
            self.logger.error("JSON schema inválido en %s: %s", title_slug, e)
            return False

        return True

    @error_handling(default_return=None)
    def get_model_for_slug(self, title_slug: str) -> Optional[str]:
        """
        Get recommended model for a title_slug.

        Args:
            title_slug: The slug identifier

        Returns:
            Model name or None
        """
        config = self.get_config_by_slug(title_slug)
        return config.get("modelo_recomendado") if config else None

    @error_handling(default_return={})
    def get_config_stats(self) -> Dict:
        """
        Get statistics about configurations.

        Returns:
            Dictionary with statistics
        """
        query = """
            SELECT 
                COUNT(*) as total_configs,
                COUNT(CASE WHEN activo = true THEN 1 END) as active_configs,
                COUNT(CASE WHEN requires_postprocessing = true THEN 1 END) as requires_postprocessing,
                COUNT(DISTINCT modelo_recomendado) as unique_models
            FROM dncp.llm_config
        """

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()

        if result:
            return {
                "total_configs": result[0],
                "active_configs": result[1],
                "requires_postprocessing": result[2],
                "unique_models": result[3],
            }

        return {}

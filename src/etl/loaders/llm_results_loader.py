"""
Loader for saving LLM results and features to database.
"""

import json
import psycopg2
from typing import Dict, List, Any, Optional
from datetime import datetime
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class LLMResultsLoader:
    """
    Loader for saving LLM extraction results to the database.

    This class handles the loading/saving of:
    - LLM responses to llm_resultados table
    - Processed features to llm_features table
    - Comparison metrics to llm_comparacion table
    - Error logs to llm_logs table
    """

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)

    @error_handling(default_return=False)
    def save_llm_result(
        self,
        nro_licitacion: str,
        title: str,
        title_slug: str,
        model: str,
        json_result: Dict[str, Any],
    ) -> bool:
        """
        Save LLM result to llm_resultados table.

        Args:
            nro_licitacion: Licitacion number
            title: Title of the pliego
            title_slug: Slug identifier
            model: Model name used
            json_result: LLM response as dictionary

        Returns:
            True if successful, False otherwise
        """
        query = """
            INSERT INTO dncp.llm_resultados (
                nro_licitacion, title, title_slug, modelo, json_result, generado_en
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nro_licitacion, title_slug, modelo) 
            DO UPDATE SET 
                json_result = EXCLUDED.json_result,
                generado_en = EXCLUDED.generado_en
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query,
                        (
                            nro_licitacion,
                            title,
                            title_slug,
                            model,
                            json.dumps(json_result, ensure_ascii=False),
                            datetime.now(),
                        ),
                    )
                    conn.commit()

            self.logger.debug(
                f"Resultado guardado: {nro_licitacion} - {title_slug} - {model}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error guardando resultado: {e}")
            return False

    @error_handling(default_return=False)
    def save_features(
        self,
        nro_licitacion: str,
        title_slug: str,
        model: str,
        features: Dict[str, Any],
    ) -> bool:
        """
        Save processed features to llm_features table.

        Args:
            nro_licitacion: Licitacion number
            title_slug: Slug identifier
            model: Model name used
            features: Dictionary of feature_name -> feature_value

        Returns:
            True if successful, False otherwise
        """
        if not features:
            return True

        query = """
            INSERT INTO dncp.llm_features (
                nro_licitacion, title_slug, modelo, feature_name, feature_value, generado_en
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nro_licitacion, title_slug, modelo, feature_name)
            DO UPDATE SET 
                feature_value = EXCLUDED.feature_value,
                generado_en = EXCLUDED.generado_en
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    for feature_name, feature_value in features.items():
                        cur.execute(
                            query,
                            (
                                nro_licitacion,
                                title_slug,
                                model,
                                feature_name,
                                str(feature_value),
                                datetime.now(),
                            ),
                        )
                    conn.commit()

            self.logger.debug(
                f"Features guardadas: {len(features)} para {nro_licitacion}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error guardando features: {e}")
            return False

    @error_handling(default_return=False)
    def save_comparison_metrics(
        self,
        nro_licitacion: str,
        title_slug: str,
        model: str,
        metrics: Dict[str, float],
    ) -> bool:
        """
        Save comparison metrics to llm_comparacion table.

        Args:
            nro_licitacion: Licitacion number
            title_slug: Slug identifier
            model: Model name used
            metrics: Dictionary of metric_name -> metric_value

        Returns:
            True if successful, False otherwise
        """
        if not metrics:
            return True

        query = """
            INSERT INTO dncp.llm_comparacion (
                nro_licitacion, title_slug, modelo, metrica, valor, generado_en
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nro_licitacion, title_slug, modelo, metrica)
            DO UPDATE SET 
                valor = EXCLUDED.valor,
                generado_en = EXCLUDED.generado_en
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    for metric_name, metric_value in metrics.items():
                        cur.execute(
                            query,
                            (
                                nro_licitacion,
                                title_slug,
                                model,
                                metric_name,
                                float(metric_value),
                                datetime.now(),
                            ),
                        )
                    conn.commit()

            self.logger.debug(
                f"Métricas guardadas: {len(metrics)} para {nro_licitacion}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error guardando métricas: {e}")
            return False

    @error_handling(default_return=False)
    def save_error_log(
        self,
        nro_licitacion: str,
        title_slug: str,
        model: str,
        error: str,
        trace: Optional[str] = None,
    ) -> bool:
        """
        Save error log to llm_logs table.

        Args:
            nro_licitacion: Licitacion number
            title_slug: Slug identifier
            model: Model name used
            error: Error message
            trace: Optional stack trace

        Returns:
            True if successful, False otherwise
        """
        query = """
            INSERT INTO dncp.llm_logs (
                nro_licitacion, title_slug, modelo, error, trace, registrado_en
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query,
                        (
                            nro_licitacion,
                            title_slug,
                            model,
                            error,
                            trace,
                            datetime.now(),
                        ),
                    )
                    conn.commit()

            return True

        except Exception as e:
            self.logger.error(f"Error guardando log: {e}")
            return False

    @error_handling(default_return=False)
    def result_exists(self, nro_licitacion: str, title_slug: str, model: str) -> bool:
        """
        Check if a result already exists in the database.

        Args:
            nro_licitacion: Licitacion number
            title_slug: Slug identifier
            model: Model name

        Returns:
            True if result exists, False otherwise
        """
        query = """
            SELECT 1 FROM dncp.llm_resultados
            WHERE nro_licitacion = %s 
            AND title_slug = %s 
            AND modelo = %s
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (nro_licitacion, title_slug, model))
                    return cur.fetchone() is not None

        except Exception as e:
            self.logger.error(f"Error verificando existencia: {e}")
            return False

    @error_handling(default_return=[])
    def get_results_by_title_slug(
        self, title_slug: str, model: str = None
    ) -> List[Dict]:
        """
        Get results by title_slug and optionally model.

        Args:
            title_slug: Slug identifier
            model: Optional model filter

        Returns:
            List of result dictionaries
        """
        base_query = """
            SELECT nro_licitacion, title, title_slug, modelo, json_result, generado_en
            FROM dncp.llm_resultados
            WHERE title_slug = %s
        """

        params = [title_slug]

        if model:
            base_query += " AND modelo = %s"
            params.append(model)

        base_query += " ORDER BY nro_licitacion"

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(base_query, params)
                    results = cur.fetchall()

            return [
                {
                    "nro_licitacion": row[0],
                    "title": row[1],
                    "title_slug": row[2],
                    "modelo": row[3],
                    "json_result": row[4],
                    "generado_en": row[5],
                }
                for row in results
            ]

        except Exception as e:
            self.logger.error(f"Error obteniendo resultados: {e}")
            return []

    @error_handling(default_return={})
    def get_model_comparison_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for model comparison.

        Returns:
            Dictionary with comparison statistics
        """
        query = """
            SELECT 
                title_slug,
                modelo,
                COUNT(*) as total_procesados,
                AVG(CASE WHEN metrica = 'tiempo_procesamiento' THEN valor END) as avg_tiempo,
                AVG(CASE WHEN metrica = 'tokens_generados' THEN valor END) as avg_tokens,
                AVG(CASE WHEN metrica = 'campos_completados' THEN valor END) as avg_completitud
            FROM dncp.llm_comparacion
            GROUP BY title_slug, modelo
            ORDER BY title_slug, modelo
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    results = cur.fetchall()

            summary = {}
            for row in results:
                slug = row[0]
                if slug not in summary:
                    summary[slug] = {}

                summary[slug][row[1]] = {
                    "total_procesados": row[2],
                    "avg_tiempo": float(row[3]) if row[3] else 0,
                    "avg_tokens": float(row[4]) if row[4] else 0,
                    "avg_completitud": float(row[5]) if row[5] else 0,
                }

            return summary

        except Exception as e:
            self.logger.error(f"Error obteniendo resumen comparativo: {e}")
            return {}

    @error_handling(default_return=0)
    def get_total_processed_count(
        self, title_slug: str = None, model: str = None
    ) -> int:
        """
        Get total count of processed results.

        Args:
            title_slug: Optional slug filter
            model: Optional model filter

        Returns:
            Total count
        """
        query = "SELECT COUNT(*) FROM dncp.llm_resultados WHERE 1=1"
        params = []

        if title_slug:
            query += " AND title_slug = %s"
            params.append(title_slug)

        if model:
            query += " AND modelo = %s"
            params.append(model)

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    return cur.fetchone()[0]

        except Exception as e:
            self.logger.error(f"Error obteniendo conteo: {e}")
            return 0

    @error_handling(default_return=False)
    def cleanup_old_results(self, days_old: int = 30) -> bool:
        """
        Clean up old results older than specified days.

        Args:
            days_old: Number of days to keep

        Returns:
            True if successful, False otherwise
        """
        query = """
            DELETE FROM dncp.llm_resultados
            WHERE generado_en < NOW() - INTERVAL '%s days'
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (days_old,))
                    deleted_count = cur.rowcount
                    conn.commit()

            self.logger.info(f"Eliminados {deleted_count} resultados antiguos")
            return True

        except Exception as e:
            self.logger.error(f"Error limpiando resultados antiguos: {e}")
            return False

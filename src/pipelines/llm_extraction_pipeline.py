"""
A pipeline for extracting structured data from pliegos using LLM models.
"""

import json
import time
from typing import Dict
from src.etl.extractors.pliego_extractor import PliegoExtractor
from src.etl.extractors.llm_config_extractor import LLMConfigExtractor
from src.etl.transformers.llm_request_transformer import LLMRequestTransformer
from src.etl.transformers.feature_parser import FeatureParser
from src.etl.loaders.llm_results_loader import LLMResultsLoader
from src.utils.llm_client import LLMClient
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
from src.utils.text_cleaner import TextCleaner


class LLMExtractionPipeline:
    """
    A pipeline for extracting structured data from pliegos using LLM models.

    This pipeline processes pliego documents by:
    1. Loading configurations from database
    2. Extracting pliego content
    3. Transforming content into LLM requests
    4. Calling LLM endpoints for structured extraction
    5. Saving results and features to database

    Attributes:
        config (dict): Configuration dictionary
        db_params (dict): Database connection parameters
        llm_endpoint (str): LLM API endpoint URL
        logger: Logger instance
    """

    def __init__(self, config: Dict):
        self.config = config
        self.db_params = config.get("db_params")
        self.llm_endpoint = config.get("llm_endpoint")
        self.output_processed_dir = config.get("output_processed_dir")
        self.output_logs_dir = config.get("output_logs_dir")
        self.logger = setup_logger(__name__)
        self._initialize_components()

    def _initialize_components(self):
        """Initialize the components of the pipeline."""
        # Base components
        self.text_cleaner = TextCleaner()

        # ETL components
        self.pliego_extractor = PliegoExtractor(self.db_params)
        self.config_extractor = LLMConfigExtractor(self.db_params)
        self.request_transformer = LLMRequestTransformer()
        self.feature_parser = FeatureParser()
        self.results_loader = LLMResultsLoader(self.db_params)

        # External services
        self.llm_client = LLMClient(self.llm_endpoint)

        # Set dependencies
        self.request_transformer.set_dependencies(self.text_cleaner)

    @error_handling(default_return=False)
    def process_title_slug(self, title_slug: str, model: str) -> bool:
        """Process all pliegos for a specific title_slug and model."""
        self.logger.info(f"Procesando title_slug: {title_slug} con modelo: {model}")

        # Extract configuration
        config_data = self.config_extractor.get_config_by_slug(title_slug)
        if not config_data:
            self.logger.error(f"No se encontró configuración para {title_slug}")
            return False

        # Extract pliegos content
        pliegos_data = self.pliego_extractor.get_pliegos_by_title(config_data["title"])
        if not pliegos_data:
            self.logger.warning(
                f"No se encontraron pliegos para {config_data['title']}"
            )
            return True

        self.logger.info(f"Procesando {len(pliegos_data)} pliegos para {title_slug}")

        # Process each pliego
        processed_count = 0
        error_count = 0

        for nro_licitacion, content in pliegos_data:
            try:
                if self._process_single_pliego(
                    nro_licitacion, content, config_data, model
                ):
                    processed_count += 1
                else:
                    error_count += 1

            except Exception as e:
                self.logger.error(f"Error procesando {nro_licitacion}: {str(e)}")
                self._log_error(nro_licitacion, title_slug, model, str(e))
                error_count += 1

        self.logger.info(
            f"Completado {title_slug}: {processed_count} exitosos, {error_count} errores"
        )
        return True

    @error_handling(default_return=False)
    def _process_single_pliego(
        self, nro_licitacion: str, content: str, config_data: Dict, model: str
    ) -> bool:
        """Process a single pliego document."""
        title_slug = config_data["title_slug"]

        # Check if already processed
        if self.results_loader.result_exists(nro_licitacion, title_slug, model):
            self.logger.debug(
                f"Ya procesado: {nro_licitacion} - {title_slug} - {model}"
            )
            return True

        # Transform content into LLM request
        request_payload = self.request_transformer.create_request_payload(
            content=content, config_data=config_data, model=model
        )

        # Call LLM
        start_time = time.time()
        llm_response = self.llm_client.call_llm(request_payload)
        end_time = time.time()

        if not llm_response:
            self.logger.error(f"Error en respuesta LLM para {nro_licitacion}")
            return False

        # Save results
        success = self.results_loader.save_llm_result(
            nro_licitacion=nro_licitacion,
            title=config_data["title"],
            title_slug=title_slug,
            model=model,
            json_result=llm_response,
        )

        if success:
            # Process features if enabled
            if self.config.get("enable_feature_extraction", False):
                self._process_features(
                    nro_licitacion, title_slug, model, llm_response, config_data
                )

            # Save comparison metrics if enabled
            if self.config.get("enable_model_comparison", False):
                self._save_comparison_metrics(
                    nro_licitacion,
                    title_slug,
                    model,
                    llm_response,
                    end_time - start_time,
                )

        return success

    def _process_features(
        self,
        nro_licitacion: str,
        title_slug: str,
        model: str,
        llm_response: Dict,
        config_data: Dict,
    ):
        """Process and save features from LLM response."""
        try:
            features = self.feature_parser.parse_features(
                llm_response, title_slug, config_data
            )

            if features:
                self.results_loader.save_features(
                    nro_licitacion, title_slug, model, features
                )
                self.logger.debug(f"Features guardadas para {nro_licitacion}")

        except Exception as e:
            self.logger.error(f"Error procesando features: {str(e)}")

    def _save_comparison_metrics(
        self,
        nro_licitacion: str,
        title_slug: str,
        model: str,
        llm_response: Dict,
        processing_time: float,
    ):
        """Save comparison metrics for model evaluation."""
        try:
            metrics = {
                "tiempo_procesamiento": processing_time,
                "tokens_generados": len(str(llm_response).split()),
                "campos_completados": self._count_completed_fields(llm_response),
            }

            self.results_loader.save_comparison_metrics(
                nro_licitacion, title_slug, model, metrics
            )

        except Exception as e:
            self.logger.error(f"Error guardando métricas: {str(e)}")

    def _count_completed_fields(self, response: Dict) -> int:
        """Count non-empty fields in LLM response."""
        count = 0
        for key, value in response.items():
            if value is not None and str(value).strip():
                count += 1
        return count

    def _log_error(self, nro_licitacion: str, title_slug: str, model: str, error: str):
        """Log error to database."""
        try:
            self.results_loader.save_error_log(nro_licitacion, title_slug, model, error)
        except Exception as e:
            self.logger.error(f"Error guardando log: {str(e)}")

    @error_handling(default_return=False)
    def run_model_comparison(self):
        """Run comparison analysis between different models."""
        self.logger.info("Ejecutando análisis comparativo de modelos...")

        try:
            comparison_results = self.results_loader.get_model_comparison_summary()

            if comparison_results:
                # Save comparison summary to file
                output_file = (
                    f"{self.output_processed_dir}/model_comparison_summary.json"
                )
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(comparison_results, f, indent=2, ensure_ascii=False)

                self.logger.info(f"Resumen comparativo guardado en: {output_file}")

        except Exception as e:
            self.logger.error(f"Error en análisis comparativo: {str(e)}")
            return False

        return True

    def run(self):
        """Run the complete LLM extraction pipeline."""
        self.logger.info("Iniciando pipeline de extracción LLM...")

        title_slugs = self.config.get("title_slugs", [])
        models = self.config.get("models_to_test", ["qwen2.5-7b-instruct"])

        total_combinations = len(title_slugs) * len(models)
        current_combination = 0

        # Process each title_slug with each model
        for title_slug in title_slugs:
            for model in models:
                current_combination += 1
                self.logger.info(
                    f"Procesando combinación {current_combination}/{total_combinations}: "
                    f"{title_slug} - {model}"
                )

                success = self.process_title_slug(title_slug, model)
                if not success:
                    self.logger.error(f"Falló procesamiento: {title_slug} - {model}")

        # Run model comparison if enabled
        if self.config.get("enable_model_comparison", False):
            self.run_model_comparison()

        self.logger.info("Pipeline de extracción LLM completado.")

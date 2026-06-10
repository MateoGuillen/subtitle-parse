"""Pipeline for clustering sections and generating JSON schemas per title."""

import json
import os
import time
from typing import Dict, Any

from src.etl.extractors.section_clustering_extractor import (
    SectionClusteringExtractor,
)
from src.etl.transformers.llm_provider import (
    LLMProvider,
    LocalLLMProvider,
    OpenRouterProvider,
)
from src.etl.transformers.section_clustering_transformer import (
    SectionClusteringTransformer,
)
from src.etl.loaders.section_clustering_loader import (
    SectionClusteringLoader,
)
from src.utils.logging_utils import setup_logger


class SectionClusteringPipeline:
    """
    For each of the top-10 most relevant titles:

    1. Extract all sections from ``dncp.pliegos_secciones``.
    2. Cluster texts into groups via embeddings + UMAP + K-Means.
    3. Sample representative + extreme examples per cluster.
    4. Use an LLM to design a JSON schema.
    5. Build a master extraction prompt for each title.
    6. Save ``master_schemas.json`` and per-title Markdown reports.
    """

    def __init__(self, config: dict):
        self.config = config
        self.db_params = config["db_params"]

        llm_provider_type = config.get("llm_provider_type", "local")
        if llm_provider_type == "local":
            self.llm_provider: LLMProvider = LocalLLMProvider(
                base_url=config.get("llm_base_url", "http://localhost:1234/v1")
            )
        else:
            self.llm_provider = OpenRouterProvider(
                api_key=config["llm_api_key"],
                model=config.get("llm_model", "openai/gpt-oss-20b:free"),
            )

        self.embedding_model = config.get(
            "embedding_model",
            "paraphrase-multilingual-MiniLM-L12-v2",
        )
        self.output_dir = config.get(
            "output_dir",
            os.path.join(config.get("base_output_dir", "."), "section_clustering"),
        )
        self.skip_titles = config.get("skip_titles", [])
        self.max_samples_per_title = config.get("max_samples_per_title", 5000)
        self.k_range = config.get("k_range", (2, 20))
        self.validate_schema = config.get("validate_schema", True)
        self.schema_validation_retries = config.get("schema_validation_retries", 2)
        self.export_chat_prompts = config.get("export_chat_prompts", False)
        self.compare_embeddings = config.get("compare_embeddings", False)
        self.embedding_models_to_test = config.get(
            "embedding_models_to_test",
            [
                "paraphrase-multilingual-MiniLM-L12-v2",
                "paraphrase-multilingual-mpnet-base-v2",
                "distiluse-base-multilingual-cased-v2",
            ],
        )
        self.umap_params_grid = config.get(
            "umap_params_grid",
            [
                {"n_components": 10, "n_neighbors": 15},
                {"n_components": 10, "n_neighbors": 30},
                {"n_components": 20, "n_neighbors": 15},
                {"n_components": 20, "n_neighbors": 30},
            ],
        )
        self.skip_llm = config.get("skip_llm", False)

        self.extractor = SectionClusteringExtractor(self.db_params)
        self.transformer = SectionClusteringTransformer(
            llm_provider=self.llm_provider,
            embedding_model=self.embedding_model,
            k_range=self.k_range,
            validate_schema=self.validate_schema,
            schema_validation_retries=self.schema_validation_retries,
            compare_embeddings=self.compare_embeddings,
            embedding_models_to_test=self.embedding_models_to_test,
            umap_params_grid=self.umap_params_grid,
            skip_llm=self.skip_llm,
        )
        self.loader = SectionClusteringLoader(self.output_dir)
        self.logger = setup_logger(__name__)

    def run(self) -> None:
        t_start = time.time()
        self.logger.info("=" * 60)
        self.logger.info("SectionClusteringPipeline iniciado")
        self.logger.info("=" * 60)

        # -- 1. Extract -----------------------------------------------
        self.logger.info("Paso 1/5 — Extrayendo secciones de los 10 títulos…")
        all_data = self.extractor.extract_all_titles()
        if not all_data:
            self.logger.error("No se pudieron extraer datos. Abortando.")
            return

        titles_processed = 0
        results: Dict[str, Dict[str, Any]] = {}

        # -- 2. Process each title ------------------------------------
        self.logger.info("Paso 2/5 — Clustering + esquemas + prompts…")
        for title, df in all_data.items():
            if title in self.skip_titles:
                self.logger.info("  Skipping '%s' (skip list).", title)
                continue

            self.logger.info(
                "  Procesando '%s' (%d secciones)…",
                title,
                len(df),
            )
            try:
                result = self.transformer.process_title(
                    title, df, max_samples=self.max_samples_per_title
                )
                results[title] = result
                titles_processed += 1
                self.logger.info(
                    "  -> %d clusters, esquema: %d campos",
                    result.get("n_clusters", 0),
                    len(result.get("json_schema", {})),
                )
            except Exception as e:
                self.logger.error(
                    "  Error procesando '%s': %s", title, str(e)
                )
                import traceback
                self.logger.error(traceback.format_exc())

        # -- 3. Save results ------------------------------------------
        self.logger.info("Paso 3/5 — Guardando resultados…")

        # Master JSON
        master_path = self.loader.save_master_schema(results)

        # Per-title reports
        report_paths = []
        for title, data in results.items():
            try:
                rp = self.loader.save_title_report(title, data)
                report_paths.append(rp)
            except Exception as e:
                self.logger.error(
                    "Error guardando reporte para '%s': %s",
                    title,
                    str(e),
                )

        # -- 4. Save cluster samples (raw + merged) ----------------------
        self.logger.info("Guardando muestras de clusters (raw + merged)…")
        try:
            self.loader.save_cluster_samples(results, "all_cluster_samples_raw.json")

            merged_results: Dict[str, Any] = {}
            for title, data in results.items():
                merged_samples, merged_counts = (
                    SectionClusteringTransformer._merge_similar_clusters(
                        data.get("samples_per_cluster", {}),
                        data.get("cluster_counts", {}),
                    )
                )
                merged_results[title] = {
                    **data,
                    "samples_per_cluster": merged_samples,
                    "cluster_counts": merged_counts,
                    "total_sections": data.get("total_sections", 0),
                }
            self.loader.save_cluster_samples(
                merged_results, "all_cluster_samples_merged.json"
            )
        except Exception as e:
            self.logger.error(
                "Error guardando muestras de clusters: %s", str(e)
            )

        # -- 5. Save comparison report ---------------------------------
        self.logger.info("Guardando reporte de comparación de clustering…")
        try:
            self.loader.save_clustering_comparison(results)
        except Exception as e:
            self.logger.error(
                "Error guardando reporte de comparación: %s", str(e)
            )

        # -- 6. Export chat prompts (optional) -------------------------
        if self.export_chat_prompts and results:
            self.logger.info("Exportando prompts para chat LLM…")
            chat_prompts = []
            for title, data in results.items():
                samples = data.get("samples_per_cluster", {})
                if samples:
                    chat_prompts.append(
                        self.transformer._build_chat_prompt(
                            title,
                            samples,
                            data.get("total_sections", 0),
                            data.get("cluster_counts", {}),
                        )
                    )
            self.loader.save_chat_prompts(chat_prompts)

        # -- 7. Summary -----------------------------------------------
        elapsed = time.time() - t_start
        self.logger.info("Paso 5/5 — Pipeline completado.")
        self.logger.info(
            "Resumen: %d títulos procesados de %d.  "
            "Tiempo total = %.2f s (%.2f min).",
            titles_processed,
            len(all_data),
            elapsed,
            elapsed / 60,
        )
        self.logger.info("Master JSON: %s", master_path)
        self.logger.info("Reports:    %d", len(report_paths))

        # Print summary table
        self.logger.info("===== SUMMARY =====")
        for title, data in results.items():
            selected = data.get("selected_method", "kmeans")
            model_short = data.get("selected_embedding_model", "").split("/")[-1][:25]
            self.logger.info(
                "  %-55s  clusters=%d  method=%-7s  model=%-25s",
                title,
                data.get("n_clusters", 0),
                selected,
                model_short,
            )

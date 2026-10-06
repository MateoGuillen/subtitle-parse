import time
import json
from pathlib import Path
import pandas as pd

from src.etl.extractors.document_features_v2_llm_extractor import DocumentFeaturesV2LLMExtractor
from src.etl.transformers.document_features_v2_llm_transformer import DocumentFeaturesV2LLMTransformer
from src.etl.loaders.document_features_v2_llm_loader import DocumentFeaturesV2LLMLoader
from src.utils.logging_utils import setup_logger


class DocumentFeaturesV2LLMPipeline:
    def __init__(self, config: dict):
        self.config = config
        self.db_params = config["db_params"]
        self.llm_provider = config["llm_provider"]
        self.conf_threshold = config.get("conf_threshold", 0.8)
        self.chunk_size = config.get("chunk_size", 500)
        self.exclude_fields = set(config.get("exclude_fields", []))
        self.checkpoint_path = config.get(
            "checkpoint_path",
            str(Path(__file__).resolve().parents[2] / "data" / "processed" / "llm_pipeline_checkpoint.json"),
        )
        self.schemas_path = config.get(
            "schemas_path",
            str(Path(__file__).resolve().parents[2] / "data" / "processed" / "section_clustering_v2" / "master_schemas_v2.json"),
        )

        self.extractor = DocumentFeaturesV2LLMExtractor(
            self.db_params, self.schemas_path, self.conf_threshold,
            exclude_fields=self.exclude_fields,
        )
        few_shot_db_path = config.get(
            "few_shot_db_path",
            str(Path(__file__).resolve().parents[2] / "data" / "processed" / "section_clustering_v2" / "few_shot_db.json"),
        )
        self.transformer = DocumentFeaturesV2LLMTransformer(
            self.schemas_path, self.llm_provider,
            few_shot_db_path=few_shot_db_path,
        )
        self.loader = DocumentFeaturesV2LLMLoader(self.db_params)
        self.logger = setup_logger(__name__)

    def _load_checkpoint(self) -> set:
        cp = Path(self.checkpoint_path)
        if cp.exists():
            data = json.loads(cp.read_text(encoding="utf-8"))
            done = set(data.get("completed_fields", []))
            self.logger.info("Checkpoint loaded: %d fields already done.", len(done))
            return done
        return set()

    def _save_checkpoint(self, completed_fields: set, metrics_list: list) -> None:
        cp = Path(self.checkpoint_path)
        cp.parent.mkdir(parents=True, exist_ok=True)
        cp.write_text(
            json.dumps(
                {"completed_fields": sorted(completed_fields), "metrics": metrics_list},
                indent=2, ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        self.logger.debug("Checkpoint saved (%d fields).", len(completed_fields))

    def run(self) -> None:
        t_start = time.time()
        self.logger.info("=== DocumentFeaturesV2 LLM Fallback Pipeline ===")

        completed = self._load_checkpoint()
        model_name = self.llm_provider.model_name
        self.logger.info("Model: %s | Checkpoint: %s", model_name, self.checkpoint_path)

        self.logger.info("Paso 1/3 — Buscando extracciones fallidas…")
        failed = self.extractor.get_failed_extractions_grouped()
        if not failed:
            self.logger.info("No se encontraron campos con confianza baja. Nada que hacer.")
            return

        in_queue = sorted(failed.keys())
        pending = [f for f in in_queue if f not in completed]
        self.logger.info(
            "Paso 2/3 — %d campos totales, %d pendientes (ya completados: %d).",
            len(in_queue), len(pending), len(completed),
        )

        all_metrics = []
        all_results = []

        for field_name in pending:
            df_failed = failed[field_name]
            df_result, metrics = self.transformer.compute_llm_fallback(
                field_name, df_failed,
                model_name=model_name,
            )
            all_metrics.append(metrics)
            if not df_result.empty:
                all_results.append(df_result)

            completed.add(field_name)
            self._save_checkpoint(completed, all_metrics)

        if not all_results:
            self.logger.warning("No se produjeron resultados LLM.")
            return

        df_all = pd.concat(all_results, ignore_index=True)

        self.loader.connect()
        try:
            self.loader.ensure_llm_model_column()

            self.logger.info(
                "Paso 3/3 — Actualizando %s con %d valores LLM…",
                self.loader.TARGET_TABLE, len(df_all),
            )
            self.loader.update_field_batch(df_all)
        finally:
            self.loader.disconnect()

        elapsed = time.time() - t_start
        self.logger.info("")

        metrics_df = pd.DataFrame(all_metrics)
        metrics_df = metrics_df.sort_values("docs", ascending=False)
        total_docs = metrics_df["docs"].sum()
        total_found = metrics_df["found"].sum()
        total_tok_in = metrics_df["tok_in"].sum()
        total_tok_out = metrics_df["tok_out"].sum()
        total_time = metrics_df["time_s"].sum()

        self.logger.info("=" * 72)
        self.logger.info("RESUMEN LLM FALLBACK — %s", model_name)
        self.logger.info("=" * 72)
        self.logger.info(
            "  %d campos | %d/%d docs extraídos (%.1f%%) | %.1f s | %.1f docs/s",
            len(metrics_df), total_found, total_docs,
            total_found / total_docs * 100 if total_docs else 0,
            total_time,
            total_docs / total_time if total_time else 0,
        )
        self.logger.info(
            "  Tokens: %d in, %d out (%.1f:1 ratio)",
            total_tok_in, total_tok_out,
            total_tok_in / total_tok_out if total_tok_out else 0,
        )
        self.logger.info("")
        self.logger.info("  %-30s %6s %6s %5s %8s %7s", "Campo", "Docs", "OK", "%", "T(s)", "doc/s")
        self.logger.info("  " + "-" * 72)
        for _, r in metrics_df.iterrows():
            self.logger.info(
                "  %-30s %6d %6d %4.0f%% %7.1f %7.1f",
                r["field"][:30], r["docs"], r["found"],
                r["pct"], r["time_s"], r["docs_per_s"],
            )
        self.logger.info("=" * 72)
        self.logger.info("Pipeline LLM Fallback completado en %.1f s.", elapsed)

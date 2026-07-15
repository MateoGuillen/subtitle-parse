"""Pipeline for ranking titles by anomaly-detection relevance (v2.1)."""

import json
import time
import os
from typing import Optional
import pandas as pd

from src.etl.extractors.title_ranking_extractor import TitleRankingExtractor
from src.etl.transformers.title_ranking_transformer import TitleRankingTransformer
from src.etl.loaders.title_ranking_loader import TitleRankingLoader
from src.utils.logging_utils import setup_logger


class TitleRankingPipeline:
    """
    Identifies the 10–20 most relevant titles for anomaly detection.

    **Flujo (v2.1):**

    1. Load ``dncp.document_features`` and extract title slugs from column names.
    2. Sample sections from ``dncp.pliegos_secciones``.
    3. Run five scoring strategies → combined ranking.
    4. (optional) Evaluate optimal K* via coverage + NMI diversity + synthetic AUC.
    5. (optional) Bootstrap stability analysis (30 iterations).
    6. Save CSV + Markdown report + evaluation results.
    """

    def __init__(self, config: dict):
        self.config = config
        self.db_params = config["db_params"]
        self.top_n = config.get("top_n", 80)
        self.section_sample = config.get("section_sample", 200_000)
        self.top_k = config.get("top_k")
        self.auto_k = config.get("auto_k", False)
        self.run_stability = config.get("run_stability", False)
        self.output_dir = config.get(
            "output_dir",
            os.path.join(config.get("base_output_dir", "."), "title_ranking"),
        )

        self.extractor = TitleRankingExtractor(self.db_params)
        self.transformer = TitleRankingTransformer(random_state=42)
        self.loader = TitleRankingLoader(self.output_dir)
        self.logger = setup_logger(__name__)

    def run(self) -> Optional[dict]:
        t_start = time.time()
        self.logger.info("=== TitleRankingPipeline v2.1 iniciado ===")

        # -- 1. Loading -------------------------------------------------------
        self.logger.info("Paso 1/5 — Cargando document_features…")
        df_doc = self.extractor.get_document_features()
        if df_doc is None or df_doc.empty:
            self.logger.error("No data in document_features. Abortando.")
            return None

        title_slugs = self.extractor.get_title_slugs_from_columns(df_doc)
        if not title_slugs:
            self.logger.error("No title columns found. Abortando.")
            return None
        self.logger.info("Extracted %d title slugs from columns.", len(title_slugs))

        self.logger.info("Loading top titles from sections table…")
        top_titles_raw = self.extractor.get_top_titles(self.top_n)
        self.logger.info("Got %d raw top titles.", len(top_titles_raw))

        self.logger.info(
            "Sampling %d sections from pliegos_secciones…", self.section_sample
        )
        df_sec = self.extractor.sample_sections(self.section_sample, top_titles_raw)

        # Load economic features for 6th strategy
        self.logger.info("Loading economic features from document_economic_features…")
        df_econ = self.extractor.get_economic_features()
        if df_econ is not None:
            self.logger.info("Economic features loaded: %d rows x %d cols.",
                             len(df_econ), len(df_econ.columns))
        else:
            self.logger.warning("No economic features — 6th strategy will return zeros.")

        # -- 2. Transform (6 strategies) --------------------------------------
        self.logger.info("Paso 2/5 — Computando ranking (6 estrategias)…")
        ranking = self.transformer.compute_ranking(df_doc, df_sec, title_slugs, df_econ)

        # -- 3. Optimal K evaluation (optional) --------------------------------
        eval_result = None
        resolved_k = self.top_k
        if self.auto_k:
            self.logger.info("Paso 3/5 — Evaluando K* óptimo…")
            eval_df = self.transformer.evaluate_optimal_k(ranking, df_doc)
            if not eval_df.empty:
                eval_path = os.path.join(self.output_dir, "k_evaluation.csv")
                eval_df.to_csv(eval_path, index=False, float_format="%.4f")
                self.logger.info("K evaluation saved: %s", eval_path)

                best_row = eval_df.loc[eval_df["score"].idxmax()]
                resolved_k = int(best_row["K"])

                # Check if default K=10 is within 5% of optimal
                k10_row = eval_df[eval_df["K"] == 10]
                if not k10_row.empty:
                    score_10 = k10_row.iloc[0]["score"]
                    score_best = best_row["score"]
                    delta_pct = abs(score_10 - score_best) / max(abs(score_best), 0.01) * 100
                    if delta_pct <= 5.0:
                        self.logger.info(
                            "K=10 is within %.1f%% of K*=%d (score=%.4f). Using K=10.",
                            delta_pct, resolved_k, score_best,
                        )
                        resolved_k = 10

                eval_result = eval_df.to_dict(orient="records")
            else:
                resolved_k = resolved_k or 10
        else:
            resolved_k = resolved_k or 10

        self.logger.info("Resolved K = %d", resolved_k)

        # -- 4. Stability analysis (optional) ----------------------------------
        stability_result = None
        if self.run_stability:
            self.logger.info("Paso 4/5 — Bootstrap stability (30 iter)…")
            freq = self.transformer.bootstrap_stability(
                df_doc, df_sec, title_slugs, K=resolved_k, n_iter=30
            )
            stable_path = os.path.join(self.output_dir, "bootstrap_frequencies.csv")
            freq_df = pd.DataFrame(
                sorted(freq.items(), key=lambda x: -x[1]),
                columns=["title_slug", "frequency"],
            )
            freq_df.to_csv(stable_path, index=False, float_format="%.4f")
            self.logger.info("Bootstrap frequencies saved: %s", stable_path)

            # Sensitivity to K
            sens = TitleRankingTransformer.sensitivity_to_k(ranking)
            sens_path = os.path.join(self.output_dir, "sensitivity_to_k.json")
            with open(sens_path, "w") as f:
                json.dump(sens, f, indent=2)
            self.logger.info("Sensitivity to K saved: %s", sens_path)

            stability_result = {
                "bootstrap_frequencies": freq,
                "sensitivity_to_k": sens,
            }

        # -- 5. Save results ---------------------------------------------------
        self.logger.info("Paso 5/5 — Guardando resultados…")
        csv_path = self.loader.save_csv(ranking)
        report_path = self.loader.save_report(
            ranking, top_n=max(resolved_k, 20),
        )

        # Summary metrics
        elapsed = time.time() - t_start
        summary = {
            "n_titles": len(ranking),
            "top_1": ranking.iloc[0]["display_name"],
            "top_1_score": float(ranking.iloc[0]["score_total"]),
            "resolved_k": resolved_k,
            "elapsed_seconds": round(elapsed, 1),
            "has_evaluation": eval_result is not None,
            "has_stability": stability_result is not None,
        }

        # Save summary JSON
        summary_path = os.path.join(self.output_dir, "pipeline_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        self.logger.info("=== Pipeline completado en %.1f s ===", elapsed)
        self.logger.info("Top-1: %s (score=%.4f)", summary["top_1"], summary["top_1_score"])
        self.logger.info("CSV:   %s", csv_path)
        self.logger.info("Report: %s", report_path)
        self.logger.info("Summary: %s", summary_path)

        self.logger.info("===== TOP-%d TÍTULOS =====", resolved_k)
        for _, row in ranking.head(resolved_k).iterrows():
            self.logger.info(
                "  #%d  %-50s  total=%.4f",
                row["rank"], row["display_name"], row["score_total"],
            )

        return summary

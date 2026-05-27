"""Pipeline for ranking titles by anomaly-detection relevance."""

import time
import os

from src.etl.extractors.title_ranking_extractor import TitleRankingExtractor
from src.etl.transformers.title_ranking_transformer import TitleRankingTransformer
from src.etl.loaders.title_ranking_loader import TitleRankingLoader
from src.utils.logging_utils import setup_logger


class TitleRankingPipeline:
    """
    Identifies the 10–20 most relevant titles for anomaly detection.

    **Flujo:**

    1. Load ``dncp.document_features`` and extract title slugs from column names.
    2. Sample sections from ``dncp.pliegos_secciones``.
    3. Run five scoring strategies.
    4. Combine scores with fixed weights → final ranking.
    5. Save CSV + Markdown report.
    """

    def __init__(self, config: dict):
        self.config = config
        self.db_params = config["db_params"]
        self.top_n = config.get("top_n", 80)
        self.section_sample = config.get("section_sample", 200_000)
        self.top_k = config.get("top_k", 20)
        self.output_dir = config.get(
            "output_dir",
            os.path.join(config.get("base_output_dir", "."), "title_ranking"),
        )

        self.extractor = TitleRankingExtractor(self.db_params)
        self.transformer = TitleRankingTransformer(random_state=42)
        self.loader = TitleRankingLoader(self.output_dir)
        self.logger = setup_logger(__name__)

    def run(self) -> None:
        t_start = time.time()
        self.logger.info("=== TitleRankingPipeline iniciado ===")

        # -- 1. Loading -------------------------------------------------------
        self.logger.info("Paso 1/4 — Cargando document_features…")
        df_doc = self.extractor.get_document_features()
        if df_doc is None or df_doc.empty:
            self.logger.error("No data in document_features. Abortando.")
            return

        title_slugs = self.extractor.get_title_slugs_from_columns(df_doc)
        if not title_slugs:
            self.logger.error("No title columns found. Abortando.")
            return
        self.logger.info("Extracted %d title slugs from columns.", len(title_slugs))

        # Get raw top titles from DB (needed for section filtering)
        self.logger.info("Loading top titles from sections table…")
        top_titles_raw = self.extractor.get_top_titles(self.top_n)
        self.logger.info("Got %d raw top titles.", len(top_titles_raw))

        # Sample sections (filtered to top titles)
        self.logger.info(
            "Sampling %d sections from pliegos_secciones…", self.section_sample
        )
        df_sec = self.extractor.sample_sections(self.section_sample, top_titles_raw)

        # -- 2. Transform (5 strategies) --------------------------------------
        self.logger.info("Paso 2/4 — Computando ranking (5 estrategias)…")
        ranking = self.transformer.compute_ranking(df_doc, df_sec, title_slugs)

        # -- 3. Save results --------------------------------------------------
        self.logger.info("Paso 3/4 — Guardando resultados…")
        csv_path = self.loader.save_csv(ranking)
        report_path = self.loader.save_report(ranking, top_n=self.top_k)

        # -- 4. Summary -------------------------------------------------------
        elapsed = time.time() - t_start
        self.logger.info("Paso 4/4 — Pipeline completado.")
        self.logger.info(
            "Resumen: %d títulos rankeados, top-1: %s (score=%.4f).  "
            "Tiempo total = %.2f s.",
            len(ranking),
            ranking.iloc[0]["display_name"],
            ranking.iloc[0]["score_total"],
            elapsed,
        )
        self.logger.info("CSV:   %s", csv_path)
        self.logger.info("Report: %s", report_path)

        # Print top-10 to console
        self.logger.info("===== TOP-10 TÍTULOS =====")
        for _, row in ranking.head(10).iterrows():
            self.logger.info(
                "  #%d  %-50s  total=%.4f",
                row["rank"],
                row["display_name"],
                row["score_total"],
            )

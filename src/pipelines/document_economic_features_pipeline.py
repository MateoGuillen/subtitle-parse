"""Pipeline for building document-level economic features."""

import time
from src.etl.extractors.document_economic_extractor import DocumentEconomicExtractor
from src.etl.transformers.document_economic_transformer import DocumentEconomicTransformer
from src.etl.loaders.document_economic_loader import DocumentEconomicLoader
from src.utils.logging_utils import setup_logger


class DocumentEconomicFeaturesPipeline:
    """
    Populates ``dncp.document_economic_features`` by joining OCDS tables
    and computing derived financial indicators.

    **Flujo:**

    1. **Extraer** — JOIN licitaciones, contratos, enmiendas, adjudicaciones,
       oferentes, pagos, protestas, convocantes, proveedores.
    2. **Transformar** — compute ratios, flags, and derived features.
    3. **Cargar** — TRUNCATE + INSERT into the target table.
    """

    def __init__(self, config: dict):
        self.config = config
        self.db_params = config["db_params"]
        self.extractor = DocumentEconomicExtractor(self.db_params)
        self.transformer = DocumentEconomicTransformer()
        self.loader = DocumentEconomicLoader(self.db_params)
        self.logger = setup_logger(__name__)

    def run(self) -> None:
        t_start = time.time()
        self.logger.info("=== DocumentEconomicFeaturesPipeline iniciado ===")

        # -- 1. Extract -------------------------------------------------------
        self.logger.info("Paso 1/3 — Extrayendo datos económicos…")
        df_raw = self.extractor.load_economic_data()
        if df_raw is None or df_raw.empty:
            self.logger.error("No data extracted. Abortando.")
            return
        self.logger.info("Extraídos %d documentos.", len(df_raw))

        # -- 2. Transform ----------------------------------------------------
        self.logger.info("Paso 2/3 — Computando features derivadas…")
        df_features = self.transformer.compute_features(df_raw)
        del df_raw

        # -- 3. Load ---------------------------------------------------------
        self.logger.info("Paso 3/3 — Cargando a PostgreSQL…")
        self.loader.connect()
        try:
            self.loader.truncate_and_insert(df_features)
            self.loader.create_indexes()
            self.loader.validate_statistics(df_features)
        finally:
            self.loader.disconnect()

        elapsed = time.time() - t_start
        self.logger.info(
            "Pipeline completado: %d documentos, %d columnas, %.2f s.",
            len(df_features), len(df_features.columns), elapsed,
        )

"""Pipeline for engineering document-level features from sections."""

import time

from src.etl.extractors.document_features_extractor import DocumentFeaturesExtractor
from src.etl.transformers.document_features_transformer import DocumentFeaturesTransformer
from src.etl.loaders.document_features_loader import DocumentFeaturesLoader
from src.utils.logging_utils import setup_logger


class DocumentFeaturesPipeline:
    """
    Construye la tabla ``dncp.document_features`` a partir de las secciones
    almacenadas en ``dncp.pliegos_secciones``.

    **Flujo:**

    1. **Extraer** — lee todas las secciones desde PostgreSQL.
    2. **Top‑N títulos** — consulta los 80 ``title_normalized`` más frecuentes
       por cantidad de documentos.
    3. **Pivotar** — para cada título del top‑N genera tres columnas
       (``has_*``, ``len_*``, ``tok_*``).  Los títulos fuera del top‑N se
       agrupan en ``has_otros`` / ``len_otros`` / ``tok_otros``.
    4. **Agregar** — estadísticas por documento (total secciones, promedios,
       sumas, etc.).
    5. **Merge + limpieza** — combina pivots con agregados, completa NaN con
       cero, elimina columnas de varianza cero.
    6. **Cargar** — crea la tabla con DDL dinámico (columnas fijas +
       ``ALTER TABLE`` por cada título) e inserta los datos vía COPY.
    7. **Validar** — registra estadísticas y tiempos de ejecución.
    """

    def __init__(self, config: dict):
        """
        Args:
            config: Diccionario con al menos:

                * ``db_params`` — credenciales PostgreSQL (dict con claves
                  ``host``, ``port``, ``dbname``, ``user``, ``password``).
                * ``top_n`` — cantidad de títulos a pivotear (default 80).
        """
        self.config = config
        self.db_params = config["db_params"]
        self.top_n = config.get("top_n", 80)

        self.extractor = DocumentFeaturesExtractor(self.db_params)
        self.transformer = None
        self.loader = DocumentFeaturesLoader(self.db_params)
        self.logger = setup_logger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Ejecuta el pipeline completo."""
        t_start = time.time()
        self.logger.info("=== DocumentFeaturesPipeline iniciado ===")

        # -- 1. Top‑N títulos -------------------------------------------------
        self.logger.info("Paso 1/5 — Obteniendo top %d títulos…", self.top_n)
        top_titles = self.extractor.get_top_titles(self.top_n)
        if not top_titles:
            self.logger.error("No se encontraron títulos.  Abortando.")
            return
        self.logger.info(
            "Top %d títulos: %s …", len(top_titles), top_titles[:5]
        )

        # -- 2. Cargar todas las secciones ------------------------------------
        self.logger.info("Paso 2/5 — Cargando secciones desde PostgreSQL…")
        df_sections = self.extractor.load_all_sections()
        if df_sections is None or df_sections.empty:
            self.logger.error("No se cargaron secciones.  Abortando.")
            return

        # -- 3. Transformar (pivot + agregados) -------------------------------
        self.logger.info("Paso 3/5 — Transformando (pivot + agregados)…")
        self.transformer = DocumentFeaturesTransformer(top_titles)
        df_features = self.transformer.compute_document_features(df_sections)

        # Liberar memoria de las secciones crudas
        del df_sections

        # -- 4. Construir lista de columnas para el DDL -----------------------
        title_cols = []
        for title in top_titles:
            title_cols.append(self.transformer.make_safe_col("has_", title))
            title_cols.append(self.transformer.make_safe_col("len_", title))
            title_cols.append(self.transformer.make_safe_col("tok_", title))
        title_cols += ["has_otros", "len_otros", "tok_otros"]

        # -- 5. Cargar a PostgreSQL -------------------------------------------
        self.logger.info("Paso 4/5 — Creando tabla e insertando datos…")
        self.loader.connect()
        try:
            self.loader.create_table(title_cols)
            self.loader.insert_features(df_features)
            self.loader.create_indexes()
            self.loader.validate_statistics(df_features)
        finally:
            self.loader.disconnect()

        # -- 6. Resumen final -------------------------------------------------
        elapsed = time.time() - t_start
        self.logger.info("Paso 5/5 — Pipeline completado.")
        self.logger.info(
            "Resumen: %d documentos, %d columnas de features, "
            "tiempo total = %.2f s.",
            len(df_features),
            len(df_features.columns),
            elapsed,
        )

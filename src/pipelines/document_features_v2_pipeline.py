import time
import json
from pathlib import Path

from src.etl.extractors.document_features_v2_extractor import DocumentFeaturesV2Extractor
from src.etl.transformers.document_features_v2_transformer import DocumentFeaturesV2Transformer
from src.etl.loaders.document_features_v2_loader import DocumentFeaturesV2Loader
from src.utils.logging_utils import setup_logger


FINAL_TITLES = [
    "analisis de precios ofertados",
    "porcentaje de multas",
    "fuerza mayor",
    "porcentaje de garantia de fiel cumplimiento de contrato",
    "aclaracion de las ofertas",
    "experiencia requerida",
    "plazo de reposicion de bienes",
    "documentacion requerida para la firma del contrato",
    "causales de terminacion del contrato",
    "confidencialidad de la informacion",
    "impuestos y derechos",
    "suministros y especificaciones tecnicas",
    "derechos intelectuales",
    "periodo de validez de la garantia de los bienes",
    "capacidad tecnica",
    "formas y condiciones de pago",
    "plan de entrega de los bienes",
    "capacidad financiera",
    "reajuste",
    "criterios de adjudicacion",
    "forma de instrumentacion de garantia de anticipo",
    "requisitos de calificacion",
    "solicitud de pago de anticipo",
    "tasa de interes por mora",
    "criterios de desempate de ofertas",
]


class DocumentFeaturesV2Pipeline:
    def __init__(self, config: dict):
        self.config = config
        self.db_params = config["db_params"]
        self.schemas_path = config.get(
            "schemas_path",
            str(Path(__file__).resolve().parents[2] / "data" / "processed" / "section_clustering_v2" / "master_schemas_v2.json"),
        )

        self.extractor = DocumentFeaturesV2Extractor(self.db_params, FINAL_TITLES)
        self.transformer = DocumentFeaturesV2Transformer(self.schemas_path)
        self.loader = DocumentFeaturesV2Loader(self.db_params)
        self.logger = setup_logger(__name__)

    def run(self) -> None:
        t_start = time.time()
        self.logger.info("=== DocumentFeaturesV2Pipeline iniciado ===")

        self.logger.info("Paso 1/4 — Cargando secciones de 25 títulos finalistas…")
        df_sections = self.extractor.load_all_sections()
        if df_sections is None or df_sections.empty:
            self.logger.error("No se cargaron secciones. Abortando.")
            return

        self.logger.info("Paso 2/4 — Extrayendo features vía regex…")
        df_features = self.transformer.compute_document_features(df_sections)
        del df_sections

        all_fields = []
        for slug, schema in self.transformer.title_schemas.items():
            for f in schema["fields"]:
                f = dict(f)
                f.setdefault("extraction_method", "hybrid")
                all_fields.append(f)

        self.logger.info("Paso 3/4 — Cargando a PostgreSQL…")
        self.loader.connect()
        try:
            self.loader.create_table(all_fields)
            self.loader.insert_features(df_features)
            self.loader.create_indexes()
            self.loader.validate_statistics(df_features)
        finally:
            self.loader.disconnect()

        elapsed = time.time() - t_start
        self.logger.info("Paso 4/4 — Pipeline completado.")
        self.logger.info(
            "Resumen: %d documentos, %d columnas, tiempo total = %.2f s.",
            len(df_features),
            len(df_features.columns),
            elapsed,
        )

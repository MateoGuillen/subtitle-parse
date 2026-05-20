"""Transformer para records.csv.

records.csv es el archivo más importante del dataset OCDS.
Contiene una fila por licitación con todos los campos del tender.

Puebla:
  - dncp.convocantes   (procuringEntity)
  - dncp.licitaciones  (enriquece con todos los campos del tender)
"""

import pandas as pd
from typing import Optional, Tuple
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


# Mapeo columnas records.csv → campos de nuestra DB
RECORDS_COLS = {
    "compiledRelease/id":                                   "compiled_release_id",
    "compiledRelease/tender/id":                            "tender_id",
    "compiledRelease/ocid":                                 "ocid",
    "compiledRelease/tender/title":                         "titulo",
    "compiledRelease/tender/status":                        "estado",
    "compiledRelease/tender/statusDetails":                 "estado_detalle",
    "compiledRelease/tender/procurementMethod":             "metodo_contratacion",
    "compiledRelease/tender/procurementMethodDetails":      "metodo_detalle",
    "compiledRelease/tender/mainProcurementCategory":       "categoria_principal",
    "compiledRelease/tender/mainProcurementCategoryDetails":"categoria_detalle",
    "compiledRelease/tender/awardCriteria":                 "criterio_adjudicacion",
    "compiledRelease/tender/awardCriteriaDetails":          "criterio_detalle",
    "compiledRelease/tender/submissionMethod":              "metodo_entrega",
    "compiledRelease/tender/value/amount":                  "monto_estimado",
    "compiledRelease/tender/value/currency":                "moneda",
    "compiledRelease/tender/datePublished":                 "fecha_publicacion",
    "compiledRelease/tender/bidOpening/date":               "fecha_apertura",
    "compiledRelease/tender/enquiryPeriod/endDate":         "fecha_fin_consultas",
    "compiledRelease/tender/enquiryPeriod/durationInDays":  "duracion_consultas_dias",
    "compiledRelease/tender/tenderPeriod/durationInDays":   "duracion_oferta_dias",
    "compiledRelease/tender/numberOfTenderers":             "cantidad_oferentes",
    "compiledRelease/tender/hasEnquiries":                  "tiene_consultas",
    "compiledRelease/tender/contractPeriod/durationInDays": "duracion_contrato_dias",
    "compiledRelease/tender/eligibilityCriteria":           "criterio_elegibilidad",
    "compiledRelease/tender/techniques/hasElectronicAuction":"tiene_subasta",
    "compiledRelease/tender/techniques/hasFrameworkAgreement":"tiene_acuerdo_marco",
    "compiledRelease/tender/procuringEntity/id":            "convocante_id",
    "compiledRelease/tender/procuringEntity/name":          "convocante_nombre",
    "compiledRelease/planning/budget/amount/amount":        "presupuesto_monto",
}

# Columnas de fecha para parsear
DATE_COLS = [
    "fecha_publicacion",
    "fecha_apertura",
    "fecha_fin_consultas",
]

# Columnas numéricas
INT_COLS = [
    "duracion_consultas_dias",
    "duracion_oferta_dias",
    "duracion_contrato_dias",
    "cantidad_oferentes",
]
FLOAT_COLS = [
    "monto_estimado",
    "presupuesto_monto",
]
BOOL_COLS = [
    "tiene_consultas",
    "tiene_subasta",
    "tiene_acuerdo_marco",
]


class OcdsRecordsTransformer:
    """
    Transforma chunks de records.csv en DataFrames listos para
    upsert en dncp.licitaciones y dncp.convocantes.
    """

    def __init__(self, cat_map: dict = None):
        """
        Args:
            cat_map: Dict {nombre_categoria: category_id} para mapear
                     desde categoria_detalle. Ej: {"Pasajes y Transportes": "1"}
        """
        self.logger = setup_logger(__name__)
        self.cat_map = cat_map or {}

    @staticmethod
    def _nro_from_ocid(ocid: str) -> Optional[str]:
        """ocds-03ad3f-384916-1 → 384916"""
        if pd.isna(ocid):
            return None
        parts = str(ocid).split("-")
        if len(parts) >= 3 and parts[2].isdigit():
            return parts[2]
        return None

    @staticmethod
    def _cat_from_detalle(detalle: str, cat_map: dict) -> Optional[str]:
        """Bienes - Equipos, accesorios... → Equipos, accesorios... → lookup → 24"""
        if pd.isna(detalle):
            return None
        parts = str(detalle).split(" - ", 1)
        name = parts[1] if len(parts) > 1 else parts[0]
        return cat_map.get(name)

    @error_handling(default_return=(None, None))
    def transform(
        self, chunk: pd.DataFrame
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Transforma un chunk de records.csv.

        Args:
            chunk: DataFrame con columnas crudas de records.csv + year.

        Returns:
            Tuple (licitaciones_df, convocantes_df) listos para upsert.
            Retorna (None, None) si el chunk es inválido.
        """
        if chunk is None or chunk.empty:
            return None, None

        # ── Renombrar columnas disponibles ───────────────────────
        available = {k: v for k, v in RECORDS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        # ── Asegurar columna mínima ─────────────────────────────
        if "compiled_release_id" not in df.columns:
            self.logger.warning("Columna requerida ausente: compiled_release_id")
            return None, None

        # ── Extraer nro_licitacion numérico desde ocid ───────────
        if "ocid" not in df.columns:
            self.logger.warning("Columna requerida ausente: ocid")
            return None, None
        df["nro_licitacion"] = df["ocid"].apply(self._nro_from_ocid)
        df = df.dropna(subset=["nro_licitacion"])

        # ── Mapear category_id desde categoria_detalle ────────────
        if "categoria_detalle" in df.columns and self.cat_map:
            df["category_id"] = df["categoria_detalle"].apply(
                lambda x: self._cat_from_detalle(x, self.cat_map)
            )

        # ── Casteos de tipo ──────────────────────────────────────
        df = self._cast_types(df)

        # ── Extraer convocantes ──────────────────────────────────
        convocantes_df = self._extract_convocantes(df)

        # ── Preparar licitaciones ────────────────────────────────
        licit_cols = [
            "nro_licitacion", "compiled_release_id", "ocid",
            "tender_id", "titulo", "estado", "estado_detalle",
            "metodo_contratacion", "metodo_detalle",
            "categoria_principal", "categoria_detalle",
            "criterio_adjudicacion", "criterio_detalle",
            "metodo_entrega", "monto_estimado", "moneda",
            "fecha_publicacion", "fecha_apertura", "fecha_fin_consultas",
            "duracion_consultas_dias", "duracion_oferta_dias",
            "duracion_contrato_dias", "cantidad_oferentes",
            "tiene_consultas", "tiene_subasta", "tiene_acuerdo_marco",
            "criterio_elegibilidad", "convocante_id",
            "category_id", "year",
        ]
        existing_cols = [c for c in licit_cols if c in df.columns]
        licit_df = df[existing_cols].drop_duplicates(
            subset=["nro_licitacion"]
        ).copy()

        self.logger.info(
            "Records transform: %d licitaciones, %d convocantes",
            len(licit_df),
            len(convocantes_df) if convocantes_df is not None else 0,
        )
        return licit_df, convocantes_df

    # ─────────────────────────────────────────────────────────────
    # Helpers privados
    # ─────────────────────────────────────────────────────────────

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica casteos de tipo a las columnas conocidas."""

        # Fechas
        for col in DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        # Enteros
        for col in INT_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")

        # Flotantes
        for col in FLOAT_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Booleanos (vienen como "True"/"False" strings)
        for col in BOOL_COLS:
            if col in df.columns:
                df[col] = df[col].map(
                    {"True": True, "False": False, True: True, False: False}
                )

        # Strings — limpiar espacios
        str_cols = ["nro_licitacion", "tender_id", "compiled_release_id", "ocid",
                    "titulo", "convocante_id", "convocante_nombre",
                    "estado", "estado_detalle", "metodo_contratacion",
                    "metodo_detalle", "categoria_principal", "category_id"]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace("nan", None)

        return df

    def _extract_convocantes(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Extrae convocantes únicas del DataFrame de licitaciones."""
        needed = ["convocante_id", "convocante_nombre"]
        if not all(c in df.columns for c in needed):
            return None

        conv_df = (
            df[needed]
            .dropna(subset=["convocante_id"])
            .drop_duplicates(subset=["convocante_id"])
            .rename(columns={"convocante_nombre": "nombre"})
            .copy()
        )

        # Filtrar IDs vacíos
        conv_df = conv_df[conv_df["convocante_id"].str.strip() != ""]
        return conv_df if not conv_df.empty else None
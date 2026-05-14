"""Transformer para los CSV del tender (llamado).

Archivos que procesa:
  - ten_tenderers.csv         → dncp.oferentes
  - ten_notifiedSuppliers.csv → dncp.proveedores_notificados
  - ten_items.csv + ten_ite_additionalClassific.csv → dncp.items_licitacion
  - ten_criteria.csv + ten_cri_requirementGroups + ten_cri_req_requirements
                              → dncp.criterios_llamado
  - ten_enquiries.csv         → dncp.consultas_llamado
"""

import pandas as pd
from typing import Optional, Tuple
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


# ── Columnas por archivo ──────────────────────────────────────────

TENDERERS_COLS = {
    "compiledRelease/id":                           "compiled_release_id",
    "compiledRelease/tender/tenderers/0/id":        "proveedor_id",
    "compiledRelease/tender/tenderers/0/name":      "proveedor_nombre",
}

NOTIFIED_COLS = {
    "compiledRelease/id":                                   "compiled_release_id",
    "compiledRelease/tender/notifiedSuppliers/0/id":        "proveedor_id",
    "compiledRelease/tender/notifiedSuppliers/0/name":      "proveedor_nombre",
}

ITEMS_COLS = {
    "compiledRelease/id":                               "compiled_release_id",
    "compiledRelease/tender/items/0/id":                "item_id",
    "compiledRelease/tender/items/0/description":       "descripcion",
    "compiledRelease/tender/items/0/quantity":          "cantidad",
    "compiledRelease/tender/items/0/unit/id":           "unidad_id",
    "compiledRelease/tender/items/0/unit/name":         "unidad_nombre",
    "compiledRelease/tender/items/0/unit/value/amount": "monto",
    "compiledRelease/tender/items/0/unit/value/currency": "moneda",
    "compiledRelease/tender/items/0/classification/id": "clasificacion_id",
    "compiledRelease/tender/items/0/classification/description": "clasificacion_desc",
}

ITEMS_UNSPSC_COLS = {
    "compiledRelease/id":                                               "compiled_release_id",
    "compiledRelease/tender/items/0/id":                                "item_id",
    "compiledRelease/tender/items/0/additionalClassifications/0/id":    "unspsc_id",
    "compiledRelease/tender/items/0/additionalClassifications/0/description": "unspsc_desc",
    "compiledRelease/tender/items/0/additionalClassifications/0/scheme":"unspsc_scheme",
}

CRITERIA_COLS = {
    "compiledRelease/id":                               "compiled_release_id",
    "compiledRelease/tender/criteria/0/id":             "criterio_id",
    "compiledRelease/tender/criteria/0/title":          "criterio_titulo",
    "compiledRelease/tender/criteria/0/description":    "criterio_descripcion",
    "compiledRelease/tender/criteria/0/source":         "criterio_fuente",
}

REQ_GROUPS_COLS = {
    "compiledRelease/id":                                               "compiled_release_id",
    "compiledRelease/tender/criteria/0/id":                             "criterio_id",
    "compiledRelease/tender/criteria/0/requirementGroups/0/id":         "grupo_id",
    "compiledRelease/tender/criteria/0/requirementGroups/0/description":"grupo_descripcion",
}

REQUIREMENTS_COLS = {
    "compiledRelease/id":                                                               "compiled_release_id",
    "compiledRelease/tender/criteria/0/id":                                             "criterio_id",
    "compiledRelease/tender/criteria/0/requirementGroups/0/id":                         "grupo_id",
    "compiledRelease/tender/criteria/0/requirementGroups/0/requirements/0/id":          "requisito_id",
    "compiledRelease/tender/criteria/0/requirementGroups/0/requirements/0/title":       "requisito_titulo",
    "compiledRelease/tender/criteria/0/requirementGroups/0/requirements/0/expectedValue":"requisito_valor",
}

ENQUIRIES_COLS = {
    "compiledRelease/id":                               "compiled_release_id",
    "compiledRelease/tender/enquiries/0/id":            "consulta_id",
    "compiledRelease/tender/enquiries/0/date":          "fecha",
    "compiledRelease/tender/enquiries/0/title":         "titulo",
    "compiledRelease/tender/enquiries/0/description":   "descripcion",
    "compiledRelease/tender/enquiries/0/answer":        "respuesta",
    "compiledRelease/tender/enquiries/0/dateAnswered":  "fecha_respuesta",
    "compiledRelease/tender/enquiries/0/author/id":     "autor_id",
    "compiledRelease/tender/enquiries/0/author/name":   "autor_nombre",
}


class OcdsTenderTransformer:
    """
    Transforma los CSV del tender en DataFrames para upsert.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)

    # ── Oferentes ─────────────────────────────────────────────────

    @error_handling(default_return=None)
    def transform_tenderers(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """ten_tenderers.csv → oferentes."""
        return self._simple_transform(
            chunk, TENDERERS_COLS,
            required=["proveedor_id"],
            str_cols=["proveedor_id", "proveedor_nombre", "compiled_release_id"],
        )

    @error_handling(default_return=None)
    def transform_notified(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """ten_notifiedSuppliers.csv → proveedores_notificados."""
        return self._simple_transform(
            chunk, NOTIFIED_COLS,
            required=["proveedor_id"],
            str_cols=["proveedor_id", "proveedor_nombre", "compiled_release_id"],
        )

    # ── Items ─────────────────────────────────────────────────────

    @error_handling(default_return=None)
    def transform_items(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """ten_items.csv → base de items_licitacion."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in ITEMS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "item_id" not in df.columns:
            return None

        df = df.dropna(subset=["item_id"])

        if "cantidad" in df.columns:
            df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce")
        if "monto" in df.columns:
            df["monto"] = pd.to_numeric(df["monto"], errors="coerce")

        for col in ["item_id", "descripcion", "clasificacion_id"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        # item_id compuesto: release_id + item_id para unicidad global
        if "compiled_release_id" in df.columns:
            df["item_id"] = df["compiled_release_id"] + "_" + df["item_id"]

        cols = [c for c in [
            "item_id", "compiled_release_id", "nro_licitacion",
            "descripcion", "cantidad", "unidad_id", "unidad_nombre",
            "monto", "moneda", "clasificacion_id", "clasificacion_desc", "year",
        ] if c in df.columns]

        return df[cols].drop_duplicates(subset=["item_id"])

    @error_handling(default_return=None)
    def transform_items_unspsc(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """ten_ite_additionalClassific.csv → UNSPSC para items."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in ITEMS_UNSPSC_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "item_id" not in df.columns:
            return None

        # Filtrar solo clasificaciones UNSPSC
        if "unspsc_scheme" in df.columns:
            df = df[df["unspsc_scheme"].astype(str).str.upper() == "UNSPSC"]

        if "compiled_release_id" in df.columns:
            df["item_id"] = df["compiled_release_id"] + "_" + df["item_id"].astype(str)

        return df[["item_id", "unspsc_id", "unspsc_desc"]].drop_duplicates(
            subset=["item_id"]
        )

    def merge_items(
        self,
        items_df: Optional[pd.DataFrame],
        unspsc_df: Optional[pd.DataFrame],
    ) -> Optional[pd.DataFrame]:
        """Une items con clasificaciones UNSPSC."""
        if items_df is None or items_df.empty:
            return None
        if unspsc_df is None or unspsc_df.empty:
            return items_df
        result = items_df.merge(unspsc_df, on="item_id", how="left")
        self.logger.info("Items merge: %d items", len(result))
        return result

    # ── Criterios ─────────────────────────────────────────────────

    @error_handling(default_return=None)
    def transform_criteria(
        self,
        criteria_chunk: pd.DataFrame,
        groups_chunk: Optional[pd.DataFrame],
        reqs_chunk: Optional[pd.DataFrame],
    ) -> Optional[pd.DataFrame]:
        """
        Combina criteria + requirementGroups + requirements
        en una tabla plana de criterios_llamado.
        """
        if criteria_chunk is None or criteria_chunk.empty:
            return None

        # Criterios base
        av = {k: v for k, v in CRITERIA_COLS.items() if k in criteria_chunk.columns}
        crit = criteria_chunk.rename(columns=av).copy()

        result = crit[["compiled_release_id", "criterio_id",
                        "criterio_titulo", "criterio_descripcion",
                        "criterio_fuente"]].copy() if all(
            c in crit.columns for c in ["compiled_release_id", "criterio_id"]
        ) else None

        if result is None:
            return None

        # Grupos
        if groups_chunk is not None and not groups_chunk.empty:
            av_g = {k: v for k, v in REQ_GROUPS_COLS.items() if k in groups_chunk.columns}
            grp = groups_chunk.rename(columns=av_g)
            merge_cols = ["compiled_release_id", "criterio_id"]
            if all(c in grp.columns for c in merge_cols):
                result = result.merge(
                    grp[merge_cols + ["grupo_id", "grupo_descripcion"]],
                    on=merge_cols, how="left"
                )

        # Requisitos
        if reqs_chunk is not None and not reqs_chunk.empty:
            av_r = {k: v for k, v in REQUIREMENTS_COLS.items() if k in reqs_chunk.columns}
            req = reqs_chunk.rename(columns=av_r)
            merge_cols = ["compiled_release_id", "criterio_id", "grupo_id"]
            avail_cols = [c for c in merge_cols if c in req.columns and c in result.columns]
            if avail_cols:
                extra = [c for c in ["requisito_id", "requisito_titulo", "requisito_valor"]
                         if c in req.columns]
                result = result.merge(
                    req[avail_cols + extra],
                    on=avail_cols, how="left"
                )

        # Agregar nro_licitacion y year si vienen del chunk
        if "nro_licitacion" in criteria_chunk.columns:
            id_map = criteria_chunk[["compiledRelease/id" if "compiledRelease/id" in criteria_chunk.columns else "compiled_release_id", "nro_licitacion"]].drop_duplicates()
            id_col = "compiledRelease/id" if "compiledRelease/id" in id_map.columns else "compiled_release_id"
            id_map = id_map.rename(columns={id_col: "compiled_release_id"})
            result = result.merge(id_map, on="compiled_release_id", how="left")

        if "year" in criteria_chunk.columns:
            year_map = criteria_chunk[["compiled_release_id" if "compiled_release_id" in criteria_chunk.columns else "compiledRelease/id", "year"]].drop_duplicates()
            result["year"] = result["compiled_release_id"].map(
                dict(zip(year_map.iloc[:, 0], year_map["year"]))
            )

        self.logger.info("Criterios transform: %d filas", len(result))
        return result

    # ── Consultas ─────────────────────────────────────────────────

    @error_handling(default_return=None)
    def transform_enquiries(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """ten_enquiries.csv → consultas_llamado."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in ENQUIRIES_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "consulta_id" not in df.columns:
            return None

        df = df.dropna(subset=["consulta_id"])

        for col in ["fecha", "fecha_respuesta"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        for col in ["consulta_id", "titulo", "descripcion", "autor_id"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        cols = [c for c in [
            "consulta_id", "compiled_release_id", "nro_licitacion",
            "fecha", "titulo", "descripcion", "respuesta",
            "fecha_respuesta", "autor_id", "autor_nombre", "year",
        ] if c in df.columns]

        return df[cols].drop_duplicates(subset=["compiled_release_id", "consulta_id"])

    # ── Helper interno ────────────────────────────────────────────

    def _simple_transform(
        self,
        chunk: pd.DataFrame,
        col_map: dict,
        required: list,
        str_cols: list,
    ) -> Optional[pd.DataFrame]:
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in col_map.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        for col in required:
            if col not in df.columns:
                return None

        df = df.dropna(subset=required)

        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        return df
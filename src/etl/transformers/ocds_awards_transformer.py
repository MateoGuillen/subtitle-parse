"""Transformer para awards.csv + awa_suppliers.csv.

awards.csv contiene las adjudicaciones oficiales.
awa_suppliers.csv contiene los proveedores ganadores de cada adjudicación.

Puebla:
  - dncp.adjudicaciones
"""

import pandas as pd
from typing import Optional
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


AWARDS_COLS = {
    "compiledRelease/id":                   "compiled_release_id",
    "compiledRelease/awards/0/id":          "award_id",
    "compiledRelease/awards/0/status":      "estado",
    "compiledRelease/awards/0/statusDetails":"estado_detalle",
    "compiledRelease/awards/0/date":        "fecha_adjudicacion",
    "compiledRelease/awards/0/value/amount":"monto_adjudicado",
    "compiledRelease/awards/0/value/currency":"moneda",
    "compiledRelease/awards/0/invitationID":"invitation_id",
}

SUPPLIERS_COLS = {
    "compiledRelease/id":                           "compiled_release_id",
    "compiledRelease/awards/0/id":                  "award_id",
    "compiledRelease/awards/0/suppliers/0/id":      "proveedor_id",
    "compiledRelease/awards/0/suppliers/0/name":    "proveedor_nombre",
}


class OcdsAwardsTransformer:
    """
    Combina awards.csv y awa_suppliers.csv para producir
    el DataFrame de adjudicaciones listo para upsert.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def transform_awards(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Transforma un chunk de awards.csv.

        Args:
            chunk: DataFrame con columnas crudas de awards.csv + nro_licitacion + year.

        Returns:
            DataFrame con campos de adjudicaciones (sin proveedor aún).
        """
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in AWARDS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "award_id" not in df.columns:
            return None

        df = df.dropna(subset=["award_id"])

        # Casteos
        if "fecha_adjudicacion" in df.columns:
            df["fecha_adjudicacion"] = pd.to_datetime(
                df["fecha_adjudicacion"], errors="coerce", utc=True
            )
        if "monto_adjudicado" in df.columns:
            df["monto_adjudicado"] = pd.to_numeric(
                df["monto_adjudicado"], errors="coerce"
            )

        # Strings limpios
        for col in ["award_id", "compiled_release_id", "estado", "estado_detalle"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        cols = [c for c in [
            "award_id", "compiled_release_id", "nro_licitacion",
            "estado", "estado_detalle", "fecha_adjudicacion",
            "monto_adjudicado", "moneda", "invitation_id", "year",
        ] if c in df.columns]

        return df[cols].drop_duplicates(subset=["award_id"])

    @error_handling(default_return=None)
    def transform_suppliers(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Transforma un chunk de awa_suppliers.csv.

        Args:
            chunk: DataFrame con columnas crudas de awa_suppliers.csv.

        Returns:
            DataFrame (award_id, proveedor_id, proveedor_nombre).
        """
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in SUPPLIERS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "award_id" not in df.columns or "proveedor_id" not in df.columns:
            return None

        df = df.dropna(subset=["award_id", "proveedor_id"])

        for col in ["award_id", "proveedor_id", "proveedor_nombre"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        return df[["award_id", "proveedor_id", "proveedor_nombre"]].drop_duplicates(
            subset=["award_id"]  # una adjudicación → un proveedor principal
        )

    @error_handling(default_return=None)
    def merge(
        self,
        awards_df: Optional[pd.DataFrame],
        suppliers_df: Optional[pd.DataFrame],
    ) -> Optional[pd.DataFrame]:
        """
        Une awards con suppliers por award_id para producir
        el DataFrame final de adjudicaciones.

        Args:
            awards_df: Resultado de transform_awards acumulado.
            suppliers_df: Resultado de transform_suppliers acumulado.

        Returns:
            DataFrame listo para upsert en dncp.adjudicaciones.
        """
        if awards_df is None or awards_df.empty:
            return None

        if suppliers_df is not None and not suppliers_df.empty:
            result = awards_df.merge(suppliers_df, on="award_id", how="left")
        else:
            result = awards_df.copy()
            result["proveedor_id"] = None
            result["proveedor_nombre"] = None

        self.logger.info(
            "Awards merge: %d adjudicaciones, %d con proveedor",
            len(result),
            result["proveedor_id"].notna().sum() if "proveedor_id" in result else 0,
        )
        return result
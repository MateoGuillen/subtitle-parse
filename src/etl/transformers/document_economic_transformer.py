"""Transformer for document-level economic features."""

import numpy as np
import pandas as pd
from src.utils.logging_utils import setup_logger


class DocumentEconomicTransformer:
    """
    Computes derived economic features (ratios, flags) from the raw
    OCDS data loaded by :class:`DocumentEconomicExtractor`.

    **Computed fields:**

    * ``es_unico_oferente`` — ``cantidad_oferentes == 1``
    * ``contract_value_ratio`` — ``monto_contrato / monto_estimado``
    * ``enmienda_ratio`` — ``total_monto_enmiendas / monto_contrato``
    * ``precio_vs_estimado`` — ``monto_adjudicado / monto_estimado``
    * ``pago_vs_contrato_ratio`` — ``total_pagado / monto_contrato``
    * ``has_multas`` — ``total_multas > 0``
    * ``has_protesta`` — ``n_protestas > 0``
    * ``bidding_urgency`` — ``monto_estimado / duracion_oferta_dias``
    * ``oferentes_por_item`` — ``cantidad_oferentes / cantidad_items``
    * ``is_high_value_single_bidder`` — high value AND single bidder
    * ``overbudget_ratio`` — ``(monto_contrato - monto_estimado) / monto_estimado``
    * ``bidder_diversity`` — ``n_oferentes_distintos / cantidad_oferentes`` (0 if 0 oferentes)
    """

    DERIVED_FIELDS = [
        "es_unico_oferente",
        "contract_value_ratio",
        "enmienda_ratio",
        "precio_vs_estimado",
        "pago_vs_contrato_ratio",
        "has_multas",
        "has_protesta",
        "bidding_urgency",
        "oferentes_por_item",
        "is_high_value_single_bidder",
        "overbudget_ratio",
        "bidder_diversity",
    ]

    COLLUSION_FIELDS = [
        "winner_category_frequency",
        "winner_total_contracts",
        "is_repeat_winner",
    ]

    # Columns to drop before returning (raw cols not in target table)
    RAW_COLS_TO_DROP = [
        "convocante_id",
        "category_id",
    ]

    def __init__(self):
        self.logger = setup_logger(__name__)

    def compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute all derived economic features in-place."""
        self.logger.info("Computing derived economic features…")
        n0 = len(df)

        # Boolean flags
        df["es_unico_oferente"] = df["cantidad_oferentes"] == 1

        # Financial ratios (protect against division by zero)
        denom = df["monto_estimado"].replace(0, np.nan)
        df["contract_value_ratio"] = df["monto_contrato"] / denom

        denom2 = df["monto_contrato"].replace(0, np.nan)
        df["enmienda_ratio"] = df["total_monto_enmiendas"] / denom2
        df["pago_vs_contrato_ratio"] = df["total_pagado"] / denom2

        df["precio_vs_estimado"] = df["monto_adjudicado"] / denom
        df["overbudget_ratio"] = (df["monto_contrato"] - df["monto_estimado"]) / denom

        # Derived from aggregated counts
        df["has_multas"] = df["total_multas"].fillna(0) > 0
        df["has_protesta"] = df["n_protestas"].fillna(0) > 0

        # Efficiency / competition
        denom3 = df["duracion_oferta_dias"].replace(0, np.nan)
        df["bidding_urgency"] = df["monto_estimado"] / denom3

        denom4 = df["cantidad_items"].replace(0, np.nan)
        df["oferentes_por_item"] = df["cantidad_oferentes"] / denom4

        # High-value single bidder (top 10% value + single bidder)
        high_value_p90 = df["monto_estimado"].quantile(0.90)
        df["is_high_value_single_bidder"] = (
            df["monto_estimado"] > high_value_p90
        ) & df["es_unico_oferente"]

        # Collusion: bidder diversity (distinct bidders per item)
        denom5 = df["cantidad_items"].replace(0, np.nan)
        df["bidder_diversity"] = df["n_oferentes_distintos"] / denom5

        # Fill NaN from division by zero / missing data
        fill_cols = [c for c in self.DERIVED_FIELDS if c in df.columns]
        df[fill_cols] = df[fill_cols].fillna(0)

        # Fill nulls in raw columns
        for col in ["duracion_contrato_dias", "duracion_oferta_dias", "duracion_consultas_dias"]:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        for col in ["monto_contrato", "monto_adjudicado", "costo_pliego",
                     "total_monto_enmiendas", "total_pagado", "total_multas",
                     "avg_retencion", "garantia_porcentaje"]:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        for col in ["winner_proveedor_id", "winner_tipo_entidad",
                     "convocante_region", "convocante_localidad",
                     "contrato_estado", "metodo_contratacion",
                     "criterio_adjudicacion"]:
            if col in df.columns:
                df[col] = df[col].fillna("")

        # Drop raw cols not in target table
        cols_to_drop = [c for c in self.RAW_COLS_TO_DROP if c in df.columns]
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)

        # Cast n_protestas
        if "n_protestas" in df.columns:
            df["n_protestas"] = df["n_protestas"].fillna(0).astype(int)
        for col in ["n_enmiendas", "n_oferentes_distintos", "n_pagos"]:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        self.logger.info("Derived features computed: %d rows, %d columns.", len(df), len(df.columns))
        return df

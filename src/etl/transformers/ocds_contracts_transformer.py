"""Transformer para contracts.csv + con_amendments.csv + con_imp_transactions.csv.

Puebla:
  - dncp.contratos
  - dncp.enmiendas_contrato
  - dncp.pagos_contrato
"""

import pandas as pd
from typing import Optional, Tuple
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


CONTRACTS_COLS = {
    "compiledRelease/id":                       "compiled_release_id",
    "compiledRelease/contracts/0/id":           "contrato_id",
    "compiledRelease/contracts/0/awardID":      "award_id",
    "compiledRelease/contracts/0/status":       "estado",
    "compiledRelease/contracts/0/statusDetails":"estado_detalle",
    "compiledRelease/contracts/0/dateSigned":   "fecha_firma",
    "compiledRelease/contracts/0/period/startDate": "fecha_inicio",
    "compiledRelease/contracts/0/period/endDate":   "fecha_fin",
    "compiledRelease/contracts/0/value/amount": "monto_contrato",
    "compiledRelease/contracts/0/value/currency":"moneda",
    "compiledRelease/contracts/0/period/durationInDays": "duracion_dias",
}

AMENDMENTS_COLS = {
    "compiledRelease/id":                                       "compiled_release_id",
    "compiledRelease/contracts/0/id":                           "contrato_id",
    "compiledRelease/contracts/0/amendments/0/id":              "enmienda_id",
    "compiledRelease/contracts/0/amendments/0/date":            "fecha",
    "compiledRelease/contracts/0/amendments/0/description":     "descripcion",
    "compiledRelease/contracts/0/amendments/0/financialCode":   "codigo_financiero",
    "compiledRelease/contracts/0/amendments/0/amendsAmount/amount":   "monto_enmienda",
    "compiledRelease/contracts/0/amendments/0/amendsAmount/currency": "moneda",
}

TRANSACTIONS_COLS = {
    "compiledRelease/id":                                           "compiled_release_id",
    "compiledRelease/contracts/0/id":                               "contrato_id",
    "compiledRelease/contracts/0/implementation/transactions/0/id": "pago_id",
    "compiledRelease/contracts/0/implementation/transactions/0/sourceSystem": "sistema_origen",
    "compiledRelease/contracts/0/implementation/transactions/0/value/amount": "monto_pagado",
    "compiledRelease/contracts/0/implementation/transactions/0/value/currency": "moneda",
    "compiledRelease/contracts/0/implementation/transactions/0/date": "fecha_pago",
    "compiledRelease/contracts/0/implementation/transactions/0/requestDate": "fecha_solicitud",
    "compiledRelease/contracts/0/implementation/transactions/0/payer/id":   "pagador_id",
    "compiledRelease/contracts/0/implementation/transactions/0/payer/name": "pagador_nombre",
    "compiledRelease/contracts/0/implementation/transactions/0/payee/id":   "proveedor_id",
    "compiledRelease/contracts/0/implementation/transactions/0/payee/name": "proveedor_nombre",
    "compiledRelease/contracts/0/implementation/transactions/0/financialCode": "codigo_financiero",
}

# Columnas de financial obligations (factura)
OBLIGATIONS_COLS = {
    "compiledRelease/contracts/0/implementation/transactions/0/id": "pago_id",
    "compiledRelease/contracts/0/implementation/transactions/0/finantialObligations/0/bill/id":   "nro_factura",
    "compiledRelease/contracts/0/implementation/transactions/0/finantialObligations/0/bill/date": "fecha_factura",
    "compiledRelease/contracts/0/implementation/transactions/0/finantialObligations/0/bill/amount/amount": "monto_factura",
}

RETENTIONS_COLS = {
    "compiledRelease/contracts/0/implementation/transactions/0/id": "pago_id",
    "compiledRelease/contracts/0/implementation/transactions/0/finantialObligations/0/retentions/0/name":          "retencion_nombre",
    "compiledRelease/contracts/0/implementation/transactions/0/finantialObligations/0/retentions/0/amount/amount": "retencion_monto",
}


class OcdsContractsTransformer:
    """
    Transforma contracts.csv, con_amendments.csv y
    con_imp_transactions.csv en DataFrames para upsert.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def transform_contracts(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Transforma un chunk de contracts.csv."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in CONTRACTS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "contrato_id" not in df.columns:
            return None

        df = df.dropna(subset=["contrato_id"])

        for col in ["fecha_firma", "fecha_inicio", "fecha_fin"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        for col in ["monto_contrato", "duracion_dias"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in ["contrato_id", "award_id", "compiled_release_id",
                    "estado", "estado_detalle"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        cols = [c for c in [
            "contrato_id", "award_id", "compiled_release_id", "nro_licitacion",
            "estado", "estado_detalle", "fecha_firma", "fecha_inicio", "fecha_fin",
            "monto_contrato", "moneda", "duracion_dias", "year",
        ] if c in df.columns]

        return df[cols].drop_duplicates(subset=["contrato_id"])

    @error_handling(default_return=None)
    def transform_amendments(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Transforma un chunk de con_amendments.csv."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in AMENDMENTS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "enmienda_id" not in df.columns or "contrato_id" not in df.columns:
            return None

        df = df.dropna(subset=["enmienda_id", "contrato_id"])

        if "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce", utc=True)
        if "monto_enmienda" in df.columns:
            df["monto_enmienda"] = pd.to_numeric(df["monto_enmienda"], errors="coerce")

        for col in ["enmienda_id", "contrato_id", "descripcion", "codigo_financiero"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        cols = [c for c in [
            "enmienda_id", "contrato_id", "nro_licitacion",
            "fecha", "descripcion", "codigo_financiero", "monto_enmienda", "moneda",
        ] if c in df.columns]

        return df[cols].drop_duplicates(subset=["enmienda_id"])

    @error_handling(default_return=None)
    def transform_transactions(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Transforma un chunk de con_imp_transactions.csv."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in TRANSACTIONS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "pago_id" not in df.columns or "contrato_id" not in df.columns:
            return None

        df = df.dropna(subset=["pago_id", "contrato_id"])

        for col in ["fecha_pago", "fecha_solicitud"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        if "monto_pagado" in df.columns:
            df["monto_pagado"] = pd.to_numeric(df["monto_pagado"], errors="coerce")

        for col in ["pago_id", "contrato_id", "sistema_origen",
                    "proveedor_id", "pagador_id", "codigo_financiero"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        cols = [c for c in [
            "pago_id", "contrato_id", "nro_licitacion", "proveedor_id",
            "fecha_pago", "fecha_solicitud", "monto_pagado", "moneda",
            "pagador_id", "pagador_nombre", "codigo_financiero",
            "sistema_origen", "year",
        ] if c in df.columns]

        return df[cols].drop_duplicates(subset=["pago_id"])

    @error_handling(default_return=None)
    def transform_obligations(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Extrae nro_factura y monto_factura de con_imp_tra_finantialObliga.csv."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in OBLIGATIONS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "pago_id" not in df.columns:
            return None

        if "fecha_factura" in df.columns:
            df["fecha_factura"] = pd.to_datetime(
                df["fecha_factura"], errors="coerce", utc=True
            )
        if "monto_factura" in df.columns:
            df["monto_factura"] = pd.to_numeric(df["monto_factura"], errors="coerce")

        return df[["pago_id", "nro_factura", "fecha_factura", "monto_factura"]].drop_duplicates(
            subset=["pago_id"]
        ) if all(c in df.columns for c in ["pago_id"]) else None

    @error_handling(default_return=None)
    def transform_retentions(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Pivotea retenciones por tipo (IVA, RENTA, DNCP, MULTA)
        para añadirlas como columnas al pago.
        """
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in RETENTIONS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "pago_id" not in df.columns:
            return None

        df["retencion_monto"] = pd.to_numeric(
            df.get("retencion_monto", 0), errors="coerce"
        ).fillna(0)

        # Mapeo de nombres de retención a columnas
        retention_map = {
            "IVA": "retencion_iva",
            "RENTA": "retencion_renta",
            "RETENCION DNCP": "retencion_dncp",
            "MULTA": "multa",
            "RETENCION IVA": "retencion_iva",
        }

        df["col_name"] = df["retencion_nombre"].map(retention_map)
        df = df.dropna(subset=["col_name"])

        if df.empty:
            return None

        # Pivot: una fila por pago_id con columnas de retención
        pivot = df.pivot_table(
            index="pago_id",
            columns="col_name",
            values="retencion_monto",
            aggfunc="sum",
            fill_value=0,
        ).reset_index()

        pivot.columns.name = None
        return pivot

    def merge_payments(
        self,
        transactions_df: Optional[pd.DataFrame],
        obligations_df: Optional[pd.DataFrame],
        retentions_df: Optional[pd.DataFrame],
    ) -> Optional[pd.DataFrame]:
        """
        Une transacciones + obligaciones + retenciones en el
        DataFrame final de pagos_contrato.
        """
        if transactions_df is None or transactions_df.empty:
            return None

        result = transactions_df.copy()

        if obligations_df is not None and not obligations_df.empty:
            result = result.merge(obligations_df, on="pago_id", how="left")

        if retentions_df is not None and not retentions_df.empty:
            result = result.merge(retentions_df, on="pago_id", how="left")
            # Asegurar que las columnas de retención existan
            for col in ["retencion_iva", "retencion_renta", "retencion_dncp", "multa"]:
                if col not in result.columns:
                    result[col] = 0.0
                result[col] = result[col].fillna(0.0)

        self.logger.info("Payments merge: %d pagos", len(result))
        return result
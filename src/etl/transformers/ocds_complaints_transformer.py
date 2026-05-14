"""Transformer para complaints.csv + events.csv → dncp.protestas."""

import pandas as pd
from typing import Optional
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


COMPLAINTS_COLS = {
    "compiledRelease/id":               "compiled_release_id",
    "compiledRelease/complaints/0/id":  "protesta_id",
}

EVENTS_COLS = {
    "compiledRelease/id":                               "compiled_release_id",
    "compiledRelease/complaints/0/id":                  "protesta_id",
    "compiledRelease/complaints/0/events/0/id":         "evento_id",
    "compiledRelease/complaints/0/events/0/type":       "tipo_evento",
    "compiledRelease/complaints/0/events/0/description":"descripcion_evento",
    "compiledRelease/complaints/0/events/0/status":     "estado_evento",
    "compiledRelease/complaints/0/events/0/period/startDate": "fecha",
}


class OcdsComplaintsTransformer:
    """
    Combina complaints.csv y events.csv para producir
    el DataFrame de protestas.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def transform_complaints(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """complaints.csv → base de protestas (solo IDs)."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in COMPLAINTS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "protesta_id" not in df.columns:
            return None

        df = df.dropna(subset=["protesta_id"])

        for col in ["protesta_id", "compiled_release_id"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        cols = [c for c in [
            "protesta_id", "compiled_release_id", "nro_licitacion", "year",
        ] if c in df.columns]

        return df[cols].drop_duplicates(subset=["protesta_id"])

    @error_handling(default_return=None)
    def transform_events(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """events.csv → detalle de eventos por protesta."""
        if chunk is None or chunk.empty:
            return None

        available = {k: v for k, v in EVENTS_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "protesta_id" not in df.columns:
            return None

        df = df.dropna(subset=["protesta_id"])

        if "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce", utc=True)

        for col in ["protesta_id", "tipo_evento", "descripcion_evento", "estado_evento"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        cols = [c for c in [
            "protesta_id", "compiled_release_id",
            "tipo_evento", "descripcion_evento", "estado_evento", "fecha",
        ] if c in df.columns]

        # Un evento representativo por protesta (el primero cronológicamente)
        if "fecha" in df.columns:
            df = df.sort_values("fecha", na_position="last")

        return df[cols].drop_duplicates(subset=["protesta_id"])

    @error_handling(default_return=None)
    def merge(
        self,
        complaints_df: Optional[pd.DataFrame],
        events_df: Optional[pd.DataFrame],
    ) -> Optional[pd.DataFrame]:
        """Une complaints con events para el DataFrame final de protestas."""
        if complaints_df is None or complaints_df.empty:
            return None

        if events_df is not None and not events_df.empty:
            result = complaints_df.merge(
                events_df, on=["protesta_id", "compiled_release_id"], how="left"
            )
        else:
            result = complaints_df.copy()

        self.logger.info("Complaints merge: %d protestas", len(result))
        return result
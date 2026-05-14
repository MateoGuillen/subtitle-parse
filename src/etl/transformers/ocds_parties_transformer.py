"""Transformer para parties.csv.

parties.csv contiene todas las entidades del proceso:
  - buyers/procuringEntities → convocantes
  - suppliers/tenderers      → proveedores

Puebla:
  - dncp.convocantes  (enriquece con región, dirección)
  - dncp.proveedores  (datos completos del proveedor)
"""

import pandas as pd
from typing import Optional, Tuple
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


PARTIES_COLS = {
    "compiledRelease/id":                           "compiled_release_id",
    "compiledRelease/parties/0/id":                 "party_id",
    "compiledRelease/parties/0/name":               "nombre",
    "compiledRelease/parties/0/identifier/id":      "identifier_id",
    "compiledRelease/parties/0/identifier/scheme":  "identifier_scheme",
    "compiledRelease/parties/0/identifier/legalName":"nombre_legal",
    "compiledRelease/parties/0/roles":              "roles",
    "compiledRelease/parties/0/contactPoint/email": "email",
    "compiledRelease/parties/0/contactPoint/telephone": "telefono",
    "compiledRelease/parties/0/contactPoint/url":   "url_web",
    "compiledRelease/parties/0/address/region":     "region",
    "compiledRelease/parties/0/address/locality":   "localidad",
    "compiledRelease/parties/0/address/streetAddress": "direccion",
    "compiledRelease/parties/0/details/legalEntityTypeDetail": "tipo_entidad",
    "compiledRelease/parties/0/details/size":       "tamanio",
    "compiledRelease/parties/0/details/activityTypes": "tipo_actividad",
    "compiledRelease/parties/0/details/scale":      "escala",
    "compiledRelease/parties/0/details/level":      "nivel_institucional",
    "compiledRelease/parties/0/details/entityType": "tipo_entidad_detalle",
}

# Roles que identifican convocantes
BUYER_ROLES = {"buyer", "procuringEntity", "payer"}

# Roles que identifican proveedores/oferentes
SUPPLIER_ROLES = {"supplier", "tenderer", "notifiedSupplier"}


class OcdsPartiesTransformer:
    """
    Transforma chunks de parties.csv en DataFrames para
    dncp.convocantes y dncp.proveedores.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)

    @error_handling(default_return=(None, None))
    def transform(
        self, chunk: pd.DataFrame
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Separa el chunk en convocantes y proveedores según el campo 'roles'.

        Args:
            chunk: DataFrame con columnas crudas de parties.csv.

        Returns:
            Tuple (convocantes_df, proveedores_df).
        """
        if chunk is None or chunk.empty:
            return None, None

        available = {k: v for k, v in PARTIES_COLS.items() if k in chunk.columns}
        df = chunk.rename(columns=available).copy()

        if "party_id" not in df.columns:
            return None, None

        # Limpiar strings
        str_cols = ["party_id", "nombre", "nombre_legal", "roles",
                    "identifier_scheme", "identifier_id"]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", None)

        df = df.dropna(subset=["party_id"])
        df = df[df["party_id"].str.strip() != ""]

        # Separar por rol
        convocantes_df = self._extract_convocantes(df)
        proveedores_df = self._extract_proveedores(df)

        self.logger.info(
            "Parties transform: %d convocantes, %d proveedores",
            len(convocantes_df) if convocantes_df is not None else 0,
            len(proveedores_df) if proveedores_df is not None else 0,
        )
        return convocantes_df, proveedores_df

    # ─────────────────────────────────────────────────────────────

    def _has_role(self, roles_str: str, target_roles: set) -> bool:
        """Verifica si alguno de los roles target está en el string de roles."""
        if not roles_str or roles_str == "nan":
            return False
        parts = {r.strip() for r in roles_str.split(";")}
        return bool(parts & target_roles)

    def _extract_convocantes(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if "roles" not in df.columns:
            return None

        mask = df["roles"].apply(lambda r: self._has_role(str(r), BUYER_ROLES))
        conv = df[mask].copy()

        if conv.empty:
            return None

        result = pd.DataFrame({
            "convocante_id": conv.get("party_id"),
            "nombre":        conv.get("nombre"),
            "region":        conv.get("region"),
            "localidad":     conv.get("localidad"),
            "direccion":     conv.get("direccion"),
        })
        return result.drop_duplicates(subset=["convocante_id"]).dropna(
            subset=["convocante_id"]
        )

    def _extract_proveedores(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if "roles" not in df.columns:
            return None

        mask = df["roles"].apply(lambda r: self._has_role(str(r), SUPPLIER_ROLES))
        prov = df[mask].copy()

        if prov.empty:
            return None

        # El proveedor_id usa formato PY-RUC-XXXX
        # Si el scheme es PY-RUC ya está bien, si no construirlo
        def build_proveedor_id(row):
            pid = str(row.get("party_id", "") or "")
            if pid.startswith("PY-RUC-"):
                return pid
            scheme = str(row.get("identifier_scheme", "") or "")
            iid    = str(row.get("identifier_id", "") or "")
            if scheme and iid and iid != "nan":
                return f"{scheme}-{iid}"
            return pid if pid != "nan" else None

        prov["proveedor_id"] = prov.apply(build_proveedor_id, axis=1)

        # RUC limpio (sin prefijo)
        def extract_ruc(row):
            iid = str(row.get("identifier_id", "") or "")
            return iid if iid not in ("nan", "") else None

        prov["ruc"] = prov.apply(extract_ruc, axis=1)

        result = pd.DataFrame({
            "proveedor_id":       prov["proveedor_id"],
            "ruc":                prov.get("ruc"),
            "nombre_comercial":   prov.get("nombre"),
            "nombre_legal":       prov.get("nombre_legal"),
            "tipo_entidad":       prov.get("tipo_entidad"),
            "tamanio":            prov.get("tamanio"),
            "tipo_actividad":     prov.get("tipo_actividad"),
            "region":             prov.get("region"),
            "localidad":          prov.get("localidad"),
            "direccion":          prov.get("direccion"),
            "email":              prov.get("email"),
            "telefono":           prov.get("telefono"),
            "url_web":            prov.get("url_web"),
            "nivel_institucional":prov.get("nivel_institucional"),
            "tipo_entidad_detalle":prov.get("tipo_entidad_detalle"),
            "escala":             prov.get("escala"),
        })

        return result.drop_duplicates(subset=["proveedor_id"]).dropna(
            subset=["proveedor_id"]
        )
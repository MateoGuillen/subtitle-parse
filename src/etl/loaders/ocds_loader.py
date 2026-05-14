"""Loader para el pipeline OCDS.

Mismo patrón que el resto del proyecto:
  1. Escribir a parquet como checkpoint antes de tocar la DB.
  2. Upsert a PostgreSQL con ON CONFLICT DO NOTHING / DO UPDATE.
  3. Liberar memoria entre operaciones.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Optional
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


class OcdsLoader:
    """
    Loader para todos los DataFrames producidos por el pipeline OCDS.
    Escribe parquet y hace upsert a PostgreSQL.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)
        self._conn = None

    # ── Conexión ──────────────────────────────────────────────────

    def connect(self, dsn: str) -> None:
        self.logger.info("Conectando a PostgreSQL...")
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        self.logger.info("Conectado.")

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self.logger.info("Desconectado de PostgreSQL.")

    @contextmanager
    def _cursor(self):
        cur = self._conn.cursor()
        try:
            yield cur
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    # ── Parquet checkpoint ────────────────────────────────────────

    @error_handling(default_return=None)
    def append_parquet(self, df: pd.DataFrame, path: str) -> None:
        """Agrega filas a un parquet existente o lo crea."""
        if df is None or df.empty:
            return

        os.makedirs(os.path.dirname(path), exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)

        if os.path.exists(path):
            existing = pq.read_table(path)
            table = pa.concat_tables([existing, table])

        pq.write_table(table, path, compression="snappy")
        self.logger.info("Parquet actualizado: %s (%d filas)", path, len(df))

    # ── Upserts por tabla ─────────────────────────────────────────

    @error_handling(default_return=False)
    def upsert_convocantes(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        sql = """
            INSERT INTO dncp.convocantes (convocante_id, nombre, region, localidad, direccion)
            VALUES %s
            ON CONFLICT (convocante_id) DO UPDATE SET
                nombre    = COALESCE(EXCLUDED.nombre,    dncp.convocantes.nombre),
                region    = COALESCE(EXCLUDED.region,    dncp.convocantes.region),
                localidad = COALESCE(EXCLUDED.localidad, dncp.convocantes.localidad),
                direccion = COALESCE(EXCLUDED.direccion, dncp.convocantes.direccion)
        """
        rows = [
            (
                self._v(r, "convocante_id"),
                self._v(r, "nombre"),
                self._v(r, "region"),
                self._v(r, "localidad"),
                self._v(r, "direccion"),
            )
            for _, r in df.iterrows()
            if self._v(r, "convocante_id")
        ]
        self._execute_values(sql, rows, "convocantes")
        return True

    @error_handling(default_return=False)
    def upsert_licitaciones(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True

        cols = [
            "nro_licitacion", "compiled_release_id", "ocid",
            "titulo", "estado", "estado_detalle",
            "metodo_contratacion", "metodo_detalle",
            "categoria_principal", "categoria_detalle",
            "criterio_adjudicacion", "criterio_detalle",
            "metodo_entrega", "monto_estimado", "moneda",
            "fecha_publicacion", "fecha_apertura", "fecha_fin_consultas",
            "duracion_consultas_dias", "duracion_oferta_dias",
            "duracion_contrato_dias", "cantidad_oferentes",
            "tiene_consultas", "tiene_subasta", "tiene_acuerdo_marco",
            "criterio_elegibilidad", "convocante_id",
        ]
        available = [c for c in cols if c in df.columns]
        update_cols = [c for c in available if c != "nro_licitacion"]
        set_clause = ", ".join(
            f"{c} = COALESCE(EXCLUDED.{c}, dncp.licitaciones.{c})"
            for c in update_cols
        )
        sql = f"""
            INSERT INTO dncp.licitaciones ({', '.join(available)})
            VALUES %s
            ON CONFLICT (nro_licitacion) DO UPDATE SET {set_clause}
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "licitaciones")
        return True

    @error_handling(default_return=False)
    def upsert_proveedores(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "proveedor_id", "ruc", "nombre_comercial", "nombre_legal",
            "tipo_entidad", "tamanio", "tipo_actividad",
            "region", "localidad", "direccion", "email", "telefono",
            "url_web", "nivel_institucional", "tipo_entidad_detalle", "escala",
        ]
        available = [c for c in cols if c in df.columns]
        update_cols = [c for c in available if c != "proveedor_id"]
        set_clause = ", ".join(
            f"{c} = COALESCE(EXCLUDED.{c}, dncp.proveedores.{c})"
            for c in update_cols
        )
        sql = f"""
            INSERT INTO dncp.proveedores ({', '.join(available)})
            VALUES %s
            ON CONFLICT (proveedor_id) DO UPDATE SET {set_clause}
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "proveedor_id")
        ]
        self._execute_values(sql, rows, "proveedores")
        return True

    @error_handling(default_return=False)
    def upsert_adjudicaciones(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "award_id", "nro_licitacion", "compiled_release_id",
            "proveedor_id", "proveedor_nombre",
            "monto_adjudicado", "moneda",
            "fecha_adjudicacion", "estado", "estado_detalle", "invitation_id",
        ]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.adjudicaciones ({', '.join(available)})
            VALUES %s
            ON CONFLICT (award_id) DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "award_id") and self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "adjudicaciones")
        return True

    @error_handling(default_return=False)
    def upsert_contratos(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "contrato_id", "award_id", "nro_licitacion", "compiled_release_id",
            "estado", "estado_detalle", "fecha_firma", "fecha_inicio", "fecha_fin",
            "monto_contrato", "moneda", "duracion_dias",
        ]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.contratos ({', '.join(available)})
            VALUES %s
            ON CONFLICT (contrato_id) DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "contrato_id") and self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "contratos")
        return True

    @error_handling(default_return=False)
    def upsert_enmiendas(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "enmienda_id", "contrato_id", "nro_licitacion",
            "fecha", "descripcion", "codigo_financiero", "monto_enmienda", "moneda",
        ]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.enmiendas_contrato ({', '.join(available)})
            VALUES %s
            ON CONFLICT (enmienda_id) DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "enmienda_id") and self._v(r, "contrato_id")
        ]
        self._execute_values(sql, rows, "enmiendas_contrato")
        return True

    @error_handling(default_return=False)
    def upsert_pagos(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "pago_id", "contrato_id", "nro_licitacion", "proveedor_id",
            "fecha_pago", "fecha_solicitud", "fecha_factura",
            "nro_factura", "monto_factura", "monto_pagado", "moneda",
            "retencion_iva", "retencion_renta", "retencion_dncp", "multa",
            "codigo_financiero", "pagador_id", "pagador_nombre", "sistema_origen",
        ]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.pagos_contrato ({', '.join(available)})
            VALUES %s
            ON CONFLICT (pago_id) DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "pago_id") and self._v(r, "contrato_id")
        ]
        self._execute_values(sql, rows, "pagos_contrato")
        return True

    @error_handling(default_return=False)
    def upsert_oferentes(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = ["compiled_release_id", "nro_licitacion",
                "proveedor_id", "proveedor_nombre", "year"]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.oferentes ({', '.join(available)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "oferentes")
        return True

    @error_handling(default_return=False)
    def upsert_notificados(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = ["compiled_release_id", "nro_licitacion",
                "proveedor_id", "proveedor_nombre", "year"]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.proveedores_notificados ({', '.join(available)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "proveedores_notificados")
        return True

    @error_handling(default_return=False)
    def upsert_items(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "item_id", "nro_licitacion", "descripcion", "cantidad",
            "unidad_id", "unidad_nombre", "monto", "moneda",
            "clasificacion_id", "clasificacion_desc", "unspsc_id", "unspsc_desc",
        ]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.items_licitacion ({', '.join(available)})
            VALUES %s
            ON CONFLICT (item_id) DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "item_id") and self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "items_licitacion")
        return True

    @error_handling(default_return=False)
    def upsert_criterios(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "compiled_release_id", "nro_licitacion",
            "criterio_id", "criterio_titulo", "criterio_descripcion",
            "criterio_fuente", "grupo_id", "grupo_descripcion",
            "requisito_id", "requisito_titulo", "requisito_valor", "year",
        ]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.criterios_llamado ({', '.join(available)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "criterios_llamado")
        return True

    @error_handling(default_return=False)
    def upsert_consultas(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "consulta_id", "compiled_release_id", "nro_licitacion",
            "fecha", "titulo", "descripcion", "respuesta",
            "fecha_respuesta", "autor_id", "autor_nombre", "year",
        ]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.consultas_llamado ({', '.join(available)})
            VALUES %s
            ON CONFLICT (compiled_release_id, consulta_id) DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "consultas_llamado")
        return True

    @error_handling(default_return=False)
    def upsert_protestas(self, df: Optional[pd.DataFrame]) -> bool:
        if df is None or df.empty:
            return True
        cols = [
            "protesta_id", "nro_licitacion", "compiled_release_id",
            "fecha", "tipo_evento", "descripcion_evento", "estado_evento",
        ]
        available = [c for c in cols if c in df.columns]
        sql = f"""
            INSERT INTO dncp.protestas ({', '.join(available)})
            VALUES %s
            ON CONFLICT (protesta_id) DO NOTHING
        """
        rows = [
            tuple(self._v(r, c) for c in available)
            for _, r in df.iterrows()
            if self._v(r, "protesta_id") and self._v(r, "nro_licitacion")
        ]
        self._execute_values(sql, rows, "protestas")
        return True

    # ── Helper interno ────────────────────────────────────────────

    def _v(self, row, col):
        """Extrae valor de una fila de forma segura."""
        val = row.get(col)
        if val is None:
            return None
        if isinstance(val, float) and val != val:
            return None
        if str(val) in ("nan", "None", ""):
            return None
        import pandas as pd
        if pd.isna(val):
            return None
        return val

    def _execute_values(self, sql: str, rows: list, table_name: str) -> None:
        if not rows:
            self.logger.info("Sin filas para %s", table_name)
            return
        BATCH = 5_000
        total = 0
        for start in range(0, len(rows), BATCH):
            batch = rows[start: start + BATCH]
            with self._cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, batch, page_size=BATCH)
            total += len(batch)
        self.logger.info("Upsert %s: %d filas", table_name, total)
"""Extractor for document-level economic features."""

from typing import Optional
import pandas as pd
import psycopg2
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class DocumentEconomicExtractor:
    """
    Extracts economic, contractual, and supplier data from OCDS tables
    for the ``document_economic_features`` pipeline.

    Joins ``licitaciones``, ``contratos``, ``enmiendas_contrato``,
    ``adjudicaciones``, ``oferentes``, ``pagos_contrato``,
    ``protestas``, ``convocantes``, and ``proveedores``.
    """

    ECONOMIC_QUERY = """
    WITH
      base AS (
        SELECT
          l.nro_licitacion,
          l.cantidad_oferentes,
          l.monto_estimado,
          l.duracion_oferta_dias,
          l.duracion_consultas_dias,
          l.tiene_consultas,
          l.tiene_subasta,
          l.costo_pliego,
          l.cantidad_items,
          l.cantidad_lotes,
          l.garantia_porcentaje,
          l.metodo_contratacion,
          l.criterio_adjudicacion,
          l.fecha_publicacion,
          l.fecha_apertura,
          l.convocante_id,
          l.category_id
        FROM dncp.licitaciones l
        WHERE l.nro_licitacion IS NOT NULL
      ),
      conv AS (
        SELECT convocante_id, region, localidad FROM dncp.convocantes
      ),
      ctr AS (
        SELECT DISTINCT ON (c.nro_licitacion)
          c.nro_licitacion,
          c.monto_contrato,
          c.duracion_dias,
          c.estado as contrato_estado
        FROM dncp.contratos c
        ORDER BY c.nro_licitacion, c.fecha_firma DESC NULLS LAST
      ),
      enm AS (
        SELECT
          e.nro_licitacion,
          COUNT(*)::INTEGER AS n_enmiendas,
          COALESCE(SUM(e.monto_enmienda), 0) AS total_monto_enmiendas
        FROM dncp.enmiendas_contrato e
        GROUP BY e.nro_licitacion
      ),
      adj AS (
        SELECT DISTINCT ON (a.nro_licitacion)
          a.nro_licitacion,
          a.monto_adjudicado,
          a.proveedor_id AS winner_proveedor_id
        FROM dncp.adjudicaciones a
        ORDER BY a.nro_licitacion, a.monto_adjudicado DESC NULLS LAST
      ),
      ofe AS (
        SELECT
          o.nro_licitacion,
          COUNT(DISTINCT o.proveedor_id)::INTEGER AS n_oferentes_distintos
        FROM dncp.oferentes o
        GROUP BY o.nro_licitacion
      ),
      pag AS (
        SELECT
          p.nro_licitacion,
          COUNT(*)::INTEGER AS n_pagos,
          COALESCE(SUM(p.monto_pagado), 0) AS total_pagado,
          COALESCE(SUM(p.multa), 0) AS total_multas,
          AVG(COALESCE(p.retencion_iva, 0) + COALESCE(p.retencion_renta, 0)) AS avg_retencion
        FROM dncp.pagos_contrato p
        GROUP BY p.nro_licitacion
      ),
      prot AS (
        SELECT
          p.nro_licitacion,
          COUNT(*)::INTEGER AS n_protestas
        FROM dncp.protestas p
        GROUP BY p.nro_licitacion
      ),
      prov AS (
        SELECT DISTINCT ON (p.proveedor_id)
          p.proveedor_id,
          p.tipo_entidad AS winner_tipo_entidad
        FROM dncp.proveedores p
      ),

      -- 10. Winner frequency per category (collusion: dominance)
      win_freq AS (
        SELECT
          a.nro_licitacion,
          a.proveedor_id,
          ROW_NUMBER() OVER (
            PARTITION BY a.nro_licitacion
            ORDER BY a.monto_adjudicado DESC
          ) AS rn,
          COUNT(*) OVER (
            PARTITION BY a.proveedor_id, l.category_id
          ) - 1 AS winner_category_frequency,
          COUNT(*) OVER (
            PARTITION BY a.proveedor_id
          ) - 1 AS winner_total_contracts
        FROM dncp.adjudicaciones a
        JOIN dncp.licitaciones l ON a.nro_licitacion = l.nro_licitacion
      ),

      -- 11. Repeat winner (same convocante)
      repeat_win AS (
        SELECT DISTINCT ON (a.nro_licitacion)
          a.nro_licitacion,
          (COUNT(*) OVER (
            PARTITION BY a.proveedor_id, l.convocante_id
          ) > 1) AS is_repeat_winner
        FROM dncp.adjudicaciones a
        JOIN dncp.licitaciones l ON a.nro_licitacion = l.nro_licitacion
      )
    SELECT
      b.*,
      conv.region AS convocante_region,
      conv.localidad AS convocante_localidad,
      ctr.monto_contrato,
      ctr.duracion_dias AS duracion_contrato_dias,
      ctr.contrato_estado,
      enm.n_enmiendas,
      enm.total_monto_enmiendas,
      adj.monto_adjudicado,
      adj.winner_proveedor_id,
      ofe.n_oferentes_distintos,
      pag.n_pagos,
      pag.total_pagado,
      pag.total_multas,
      pag.avg_retencion,
      prot.n_protestas,
      prov.winner_tipo_entidad,
      COALESCE(wf.winner_category_frequency, 0)::INTEGER AS winner_category_frequency,
      COALESCE(wf.winner_total_contracts, 0)::INTEGER AS winner_total_contracts,
      COALESCE(rw.is_repeat_winner, FALSE) AS is_repeat_winner
    FROM base b
    LEFT JOIN conv ON b.convocante_id = conv.convocante_id
    LEFT JOIN ctr ON b.nro_licitacion = ctr.nro_licitacion
    LEFT JOIN enm ON b.nro_licitacion = enm.nro_licitacion
    LEFT JOIN adj ON b.nro_licitacion = adj.nro_licitacion
    LEFT JOIN ofe ON b.nro_licitacion = ofe.nro_licitacion
    LEFT JOIN pag ON b.nro_licitacion = pag.nro_licitacion
    LEFT JOIN prot ON b.nro_licitacion = prot.nro_licitacion
    LEFT JOIN prov ON adj.winner_proveedor_id = prov.proveedor_id
    LEFT JOIN win_freq wf ON b.nro_licitacion = wf.nro_licitacion AND wf.rn = 1
    LEFT JOIN repeat_win rw ON b.nro_licitacion = rw.nro_licitacion
    ORDER BY b.nro_licitacion
    """

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def load_economic_data(self) -> Optional[pd.DataFrame]:
        """Load all economic data with joins via chunked reading."""
        self.logger.info("Loading economic data from database (chunked)…")
        conn = psycopg2.connect(**self.db_params)
        chunks = []
        for i, chunk in enumerate(pd.read_sql(self.ECONOMIC_QUERY, conn, chunksize=50_000)):
            chunks.append(chunk)
            self.logger.info("  Loaded chunk %d (%d rows)", i + 1, len(chunk))
        conn.close()
        if not chunks:
            self.logger.warning("No economic data found.")
            return None
        df = pd.concat(chunks, ignore_index=True)
        self.logger.info("Loaded %d documents with economic data.", len(df))
        return df

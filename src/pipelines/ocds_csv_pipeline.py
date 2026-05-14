"""Pipeline orquestador para datos OCDS desde CSVs del DNCP.

Procesamiento por año:
  1. Descarga y extrae ZIP masivo.
  2. Construye mapa compiled_release_id → nro_licitacion.
  3. Transforma cada CSV → DataFrames limpios.
  4. Checkpoint parquet + upsert PostgreSQL.

Orden de carga respetando FK:
  convocantes → licitaciones → proveedores → adjudicaciones →
  contratos → enmiendas → pagos → oferentes → notificados →
  items → criterios → consultas → protestas
"""

import os
import gc
import pandas as pd
from typing import Optional

from src.etl.extractors.ocds_csv_extractor import OcdsCsvExtractor
from src.etl.transformers.ocds_records_transformer import OcdsRecordsTransformer
from src.etl.transformers.ocds_parties_transformer import OcdsPartiesTransformer
from src.etl.transformers.ocds_awards_transformer import OcdsAwardsTransformer
from src.etl.transformers.ocds_contracts_transformer import OcdsContractsTransformer
from src.etl.transformers.ocds_tender_transformer import OcdsTenderTransformer
from src.etl.transformers.ocds_complaints_transformer import OcdsComplaintsTransformer
from src.etl.loaders.ocds_loader import OcdsLoader
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


RECORDS_CSV = "records.csv"
PARTIES_CSV = "parties.csv"
AWARDS_CSV = "awards.csv"
AWARDS_SUPPLIERS_CSV = "awa_suppliers.csv"
CONTRACTS_CSV = "contracts.csv"
AMENDMENTS_CSV = "con_amendments.csv"
TRANSACTIONS_CSV = "con_imp_transactions.csv"
OBLIGATIONS_CSV = "con_imp_tra_finantialObliga.csv"
RETENTIONS_CSV = "con_imp_tra_fin_retentions.csv"
TENDERERS_CSV = "ten_tenderers.csv"
NOTIFIED_CSV = "ten_notifiedSuppliers.csv"
ITEMS_CSV = "ten_items.csv"
ITEMS_UNSPSC_CSV = "ten_ite_additionalClassific.csv"
CRITERIA_CSV = "ten_criteria.csv"
REQ_GROUPS_CSV = "ten_cri_requirementGroups.csv"
REQUIREMENTS_CSV = "ten_cri_req_requirements.csv"
ENQUIRIES_CSV = "ten_enquiries.csv"
COMPLAINTS_CSV = "complaints.csv"
EVENTS_CSV = "events.csv"


class OcdsCsvPipeline:
    """
    Orquesta el pipeline ETL completo para datos OCDS del DNCP desde CSVs.

    Attributes:
        config (dict): Configuración con work_dir, db_dsn, years, etc.
        extractor (OcdsCsvExtractor): Descarga/extrae CSVs.
        loader (OcdsLoader): Escribe parquet y upsert a PostgreSQL.
    """

    def __init__(self, config: dict):
        self.config = config
        self.work_dir = config["work_dir"]
        self.checkpoint_dir = config.get(
            "checkpoint_dir",
            os.path.join(self.work_dir, "checkpoint"),
        )
        self.logger = setup_logger(__name__)

        self.extractor = OcdsCsvExtractor(self.work_dir)
        self.records_transformer = OcdsRecordsTransformer()
        self.parties_transformer = OcdsPartiesTransformer()
        self.awards_transformer = OcdsAwardsTransformer()
        self.contracts_transformer = OcdsContractsTransformer()
        self.tender_transformer = OcdsTenderTransformer()
        self.complaints_transformer = OcdsComplaintsTransformer()
        self.loader = OcdsLoader()

        self._reset_accumulators()

    def _reset_accumulators(self):
        self._licitaciones = []
        self._convocantes = []
        self._proveedores = []
        self._adjudicaciones = []
        self._contratos = []
        self._enmiendas = []
        self._pagos = []
        self._oferentes = []
        self._notificados = []
        self._items = []
        self._criterios = []
        self._consultas = []
        self._protestas = []

    # ─────────────────────────────────────────────────────────────
    # Pipeline principal
    # ─────────────────────────────────────────────────────────────

    @error_handling(default_return=False)
    def run_year(self, year: int) -> bool:
        """Ejecuta el pipeline completo para un año."""
        self.logger.info("═" * 60)
        self.logger.info("Procesando año %d", year)
        self.logger.info("═" * 60)

        ckpt = os.path.join(self.checkpoint_dir, f"year_{year}.parquet")
        if self.extractor.is_year_processed(year, ckpt):
            self.logger.info("Año %d ya procesado. Saltando.", year)
            return True

        if not self.extractor.download_zip(year):
            self.logger.error("Fallo descarga ZIP año %d", year)
            return False
        if not self.extractor.extract_relevant_csvs(year):
            self.logger.error("Fallo extracción CSVs año %d", year)
            return False

        id_map = self.extractor.build_id_map(year)
        if not id_map:
            self.logger.error("Mapa de IDs vacío para año %d", year)
            return False

        self._reset_accumulators()
        self._process_records(year)
        self._process_parties(year)
        self._process_awards(year)
        self._process_contracts(year)
        self._process_tender(year)
        self._process_complaints(year)

        success = self._load_year(year)
        if success:
            self.extractor.mark_year_processed(year, ckpt)
            self.logger.info("Año %d completado sin errores.", year)
        else:
            self.logger.error(
                "Año %d tuvo errores en upsert — datos guardados en parquet pero NO en DB. "
                "Corregí el error, truncá la base y re-ejecutá.", year
            )
        return success

    def run(self):
        """Ejecuta el pipeline para todos los años configurados."""
        self.logger.info("Iniciando pipeline OCDS CSV...")

        db_dsn = self.config.get("db_dsn")
        if db_dsn:
            self.loader.connect(db_dsn)

        try:
            for year in self.config.get("years", []):
                self.run_year(year)
                gc.collect()
        finally:
            if db_dsn:
                self.loader.disconnect()

        self.logger.info("Pipeline OCDS CSV completado.")

    # ─────────────────────────────────────────────────────────────
    # Procesamiento por tipo de CSV
    # ─────────────────────────────────────────────────────────────

    def _process_records(self, year: int):
        """Transforma records.csv → licitaciones + convocantes."""
        self.logger.info("Procesando records.csv...")
        for chunk in self.extractor.iter_records(year):
            licit_df, conv_df = self.records_transformer.transform(chunk)
            if licit_df is not None and not licit_df.empty:
                self._licitaciones.append(licit_df)
            if conv_df is not None and not conv_df.empty:
                self._convocantes.append(conv_df)
            del chunk, licit_df, conv_df
        gc.collect()

    def _process_parties(self, year: int):
        """Transforma parties.csv → convocantes + proveedores."""
        self.logger.info("Procesando parties.csv...")
        for chunk in self.extractor.iter_csv(year, PARTIES_CSV):
            conv_df, prov_df = self.parties_transformer.transform(chunk)
            if conv_df is not None and not conv_df.empty:
                self._convocantes.append(conv_df)
            if prov_df is not None and not prov_df.empty:
                self._proveedores.append(prov_df)
            del chunk, conv_df, prov_df
        gc.collect()

    def _process_awards(self, year: int):
        """Transforma awards.csv + awa_suppliers.csv → adjudicaciones."""
        self.logger.info("Procesando awards...")
        awards_acc = []
        for chunk in self.extractor.iter_csv(year, AWARDS_CSV):
            df = self.awards_transformer.transform_awards(chunk)
            if df is not None and not df.empty:
                awards_acc.append(df)
            del chunk, df
        gc.collect()

        suppliers_acc = []
        for chunk in self.extractor.iter_csv(year, AWARDS_SUPPLIERS_CSV):
            df = self.awards_transformer.transform_suppliers(chunk)
            if df is not None and not df.empty:
                suppliers_acc.append(df)
            del chunk, df
        gc.collect()

        awards_all = pd.concat(awards_acc, ignore_index=True) if awards_acc else None
        suppliers_all = pd.concat(suppliers_acc, ignore_index=True) if suppliers_acc else None
        merged = self.awards_transformer.merge(awards_all, suppliers_all)
        if merged is not None and not merged.empty:
            self._adjudicaciones.append(merged)
        del awards_acc, suppliers_acc, awards_all, suppliers_all, merged
        gc.collect()

    def _process_contracts(self, year: int):
        """Transforma contracts.csv + amendments + transactions."""
        self.logger.info("Procesando contracts...")

        ctrl_acc = []
        for chunk in self.extractor.iter_csv(year, CONTRACTS_CSV):
            df = self.contracts_transformer.transform_contracts(chunk)
            if df is not None and not df.empty:
                ctrl_acc.append(df)
            del chunk, df
        gc.collect()

        amend_acc = []
        for chunk in self.extractor.iter_csv(year, AMENDMENTS_CSV):
            df = self.contracts_transformer.transform_amendments(chunk)
            if df is not None and not df.empty:
                amend_acc.append(df)
            del chunk, df
        gc.collect()

        txn_acc = []
        for chunk in self.extractor.iter_csv(year, TRANSACTIONS_CSV):
            df = self.contracts_transformer.transform_transactions(chunk)
            if df is not None and not df.empty:
                txn_acc.append(df)
            del chunk, df
        gc.collect()

        oblig_acc = []
        for chunk in self.extractor.iter_csv(year, OBLIGATIONS_CSV):
            df = self.contracts_transformer.transform_obligations(chunk)
            if df is not None and not df.empty:
                oblig_acc.append(df)
            del chunk, df
        gc.collect()

        ret_acc = []
        for chunk in self.extractor.iter_csv(year, RETENTIONS_CSV):
            df = self.contracts_transformer.transform_retentions(chunk)
            if df is not None and not df.empty:
                ret_acc.append(df)
            del chunk, df
        gc.collect()

        ctrl_all = pd.concat(ctrl_acc, ignore_index=True) if ctrl_acc else None
        amend_all = pd.concat(amend_acc, ignore_index=True) if amend_acc else None
        txn_all = pd.concat(txn_acc, ignore_index=True) if txn_acc else None
        oblig_all = pd.concat(oblig_acc, ignore_index=True) if oblig_acc else None
        ret_all = pd.concat(ret_acc, ignore_index=True) if ret_acc else None

        if ctrl_all is not None and not ctrl_all.empty:
            self._contratos.append(ctrl_all)
        if amend_all is not None and not amend_all.empty:
            self._enmiendas.append(amend_all)

        pagos_all = self.contracts_transformer.merge_payments(txn_all, oblig_all, ret_all)
        if pagos_all is not None and not pagos_all.empty:
            self._pagos.append(pagos_all)

        del (ctrl_acc, amend_acc, txn_acc, oblig_acc, ret_acc,
             ctrl_all, amend_all, txn_all, oblig_all, ret_all, pagos_all)
        gc.collect()

    def _process_tender(self, year: int):
        """Transforma CSVs del tender."""
        self.logger.info("Procesando tender CSVs...")

        for chunk in self.extractor.iter_csv(year, TENDERERS_CSV):
            df = self.tender_transformer.transform_tenderers(chunk)
            if df is not None and not df.empty:
                self._oferentes.append(df)
            del chunk, df
        gc.collect()

        for chunk in self.extractor.iter_csv(year, NOTIFIED_CSV):
            df = self.tender_transformer.transform_notified(chunk)
            if df is not None and not df.empty:
                self._notificados.append(df)
            del chunk, df
        gc.collect()

        items_acc = []
        for chunk in self.extractor.iter_csv(year, ITEMS_CSV):
            df = self.tender_transformer.transform_items(chunk)
            if df is not None and not df.empty:
                items_acc.append(df)
            del chunk, df
        gc.collect()

        unspsc_acc = []
        for chunk in self.extractor.iter_csv(year, ITEMS_UNSPSC_CSV):
            df = self.tender_transformer.transform_items_unspsc(chunk)
            if df is not None and not df.empty:
                unspsc_acc.append(df)
            del chunk, df
        gc.collect()

        items_all = pd.concat(items_acc, ignore_index=True) if items_acc else None
        unspsc_all = pd.concat(unspsc_acc, ignore_index=True) if unspsc_acc else None
        merged_items = self.tender_transformer.merge_items(items_all, unspsc_all)
        if merged_items is not None and not merged_items.empty:
            self._items.append(merged_items)
        del items_acc, unspsc_acc, items_all, unspsc_all, merged_items
        gc.collect()

        criteria_acc = []
        groups_acc = []
        reqs_acc = []
        for chunk in self.extractor.iter_csv(year, CRITERIA_CSV):
            criteria_acc.append(chunk)
        for chunk in self.extractor.iter_csv(year, REQ_GROUPS_CSV):
            groups_acc.append(chunk)
        for chunk in self.extractor.iter_csv(year, REQUIREMENTS_CSV):
            reqs_acc.append(chunk)

        if criteria_acc:
            criteria_all = pd.concat(criteria_acc, ignore_index=True)
            groups_all = pd.concat(groups_acc, ignore_index=True) if groups_acc else None
            reqs_all = pd.concat(reqs_acc, ignore_index=True) if reqs_acc else None
            crit_df = self.tender_transformer.transform_criteria(
                criteria_all, groups_all, reqs_all
            )
            if crit_df is not None and not crit_df.empty:
                self._criterios.append(crit_df)
            del criteria_all, groups_all, reqs_all, crit_df
        del criteria_acc, groups_acc, reqs_acc
        gc.collect()

        for chunk in self.extractor.iter_csv(year, ENQUIRIES_CSV):
            df = self.tender_transformer.transform_enquiries(chunk)
            if df is not None and not df.empty:
                self._consultas.append(df)
            del chunk, df
        gc.collect()

    def _process_complaints(self, year: int):
        """Transforma complaints.csv + events.csv → protestas."""
        self.logger.info("Procesando complaints...")

        comp_acc = []
        for chunk in self.extractor.iter_csv(year, COMPLAINTS_CSV):
            df = self.complaints_transformer.transform_complaints(chunk)
            if df is not None and not df.empty:
                comp_acc.append(df)
            del chunk, df
        gc.collect()

        evt_acc = []
        for chunk in self.extractor.iter_csv(year, EVENTS_CSV):
            df = self.complaints_transformer.transform_events(chunk)
            if df is not None and not df.empty:
                evt_acc.append(df)
            del chunk, df
        gc.collect()

        comp_all = pd.concat(comp_acc, ignore_index=True) if comp_acc else None
        evt_all = pd.concat(evt_acc, ignore_index=True) if evt_acc else None
        merged = self.complaints_transformer.merge(comp_all, evt_all)
        if merged is not None and not merged.empty:
            self._protestas.append(merged)
        del comp_acc, evt_acc, comp_all, evt_all, merged
        gc.collect()

    # ─────────────────────────────────────────────────────────────
    # Carga a parquet + PostgreSQL
    # ─────────────────────────────────────────────────────────────

    def _load_year(self, year: int) -> bool:
        """Concatena acumuladores, filtra FK huérfanas y carga a parquet + DB.
        Returns True si TODOS los upserts fueron exitosos.
        """
        parquet_dir = os.path.join(self.checkpoint_dir, "parquet", str(year))
        os.makedirs(parquet_dir, exist_ok=True)

        # ── Concatenar todos los acumuladores ────────────────────
        convocantes_df    = self._concat(self._convocantes)
        licitaciones_df   = self._concat(self._licitaciones)
        proveedores_df    = self._concat(self._proveedores)
        adjudicaciones_df = self._concat(self._adjudicaciones)
        contratos_df      = self._concat(self._contratos)
        enmiendas_df      = self._concat(self._enmiendas)
        pagos_df          = self._concat(self._pagos)
        oferentes_df      = self._concat(self._oferentes)
        notificados_df    = self._concat(self._notificados)
        items_df          = self._concat(self._items)
        criterios_df      = self._concat(self._criterios)
        consultas_df      = self._concat(self._consultas)
        protestas_df      = self._concat(self._protestas)

        # ── Deduplicar por PK ────────────────────────────────────
        pk_map = {
            "convocantes": "convocante_id",
            "licitaciones": "nro_licitacion",
            "proveedores": "proveedor_id",
            "adjudicaciones": "award_id",
            "contratos": "contrato_id",
            "enmiendas": "enmienda_id",
            "pagos": "pago_id",
        }
        # ── Construir lista de carga ─────────────────────────────
        loads = [
            ("convocantes", convocantes_df, self.loader.upsert_convocantes),
            ("licitaciones", licitaciones_df, self.loader.upsert_licitaciones),
            ("proveedores", proveedores_df, self.loader.upsert_proveedores),
            ("adjudicaciones", adjudicaciones_df, self.loader.upsert_adjudicaciones),
            ("contratos", contratos_df, self.loader.upsert_contratos),
            ("enmiendas", enmiendas_df, self.loader.upsert_enmiendas),
            ("pagos", pagos_df, self.loader.upsert_pagos),
            ("oferentes", oferentes_df, self.loader.upsert_oferentes),
            ("notificados", notificados_df, self.loader.upsert_notificados),
            ("items", items_df, self.loader.upsert_items),
            ("criterios", criterios_df, self.loader.upsert_criterios),
            ("consultas", consultas_df, self.loader.upsert_consultas),
            ("protestas", protestas_df, self.loader.upsert_protestas),
        ]

        has_errors = False
        for name, df, upsert_fn in loads:
            if df is None or df.empty:
                self.logger.info("Sin datos para %s año %d", name, year)
                continue

            pk = pk_map.get(name)
            if pk and pk in df.columns:
                df = df.drop_duplicates(subset=[pk])

            # Parquet checkpoint (siempre se escribe, incluso si DB falla)
            pq_path = os.path.join(parquet_dir, f"{name}.parquet")
            self.loader.append_parquet(df, pq_path)

            # Punto 4: upsert y tracking de errores
            ok = upsert_fn(df)
            if ok:
                self.logger.info("Cargados %d registros en %s año %d", len(df), name, year)
            else:
                has_errors = True
                self.logger.error(
                    "Fallo upsert %s año %d — %d registros solo en parquet, NO en DB",
                    name, year, len(df)
                )

        self._reset_accumulators()
        gc.collect()
        return not has_errors

    @staticmethod
    def _concat(dfs: list) -> Optional[pd.DataFrame]:
        if not dfs:
            return None
        return pd.concat(dfs, ignore_index=True)

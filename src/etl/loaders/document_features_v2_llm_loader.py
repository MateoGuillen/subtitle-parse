"""Promoción staging -> producción del fallback (plan de corrección F3; auditoría H7.1-H7.3).

Reglas (sesión 23):
  - Solo se promueve desde dncp.llm_fallback_runs_v2, por run_id y con lista EXPLÍCITA
    de campos (nunca "todos" por defecto).
  - Procedencia real: {campo}_source = 'rule' | 'llm' (antes siempre 'llm').
  - Conf por fuente, convencional y documentada (no es una probabilidad):
    regex 1.0 · rule 0.9 · llm 0.7.
  - Guardia: nunca pisa un valor con conf >= la nueva. Pisa si el actual es NULL o es
    el placeholder histórico (valor 0 con conf 0 = "regex no-match").
  - Cada celda cambiada deja una fila en dncp.document_features_v2_history
    (valor/conf/source/status viejos y nuevos) -> rollback_run() la revierte.
  - Si la tabla destino tiene {campo}_status (semántica NULL≠0 de F4), también se
    promueven las ausencias (valor NULL, status 'ausente'), sin pisar valores existentes.
"""
import re
from typing import Dict, Iterable, List

import pandas as pd
import psycopg2
from src.utils.logging_utils import setup_logger

CONF_BY_SOURCE = {"regex": 1.0, "rule": 0.9, "llm": 0.7}
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


class DocumentFeaturesV2LLMLoader:
    TARGET_TABLE = "dncp.document_features_v2"

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = setup_logger(__name__)
        self._conn = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(**self.db_params)
        self._conn.autocommit = False
        self.logger.info("Connected.")

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self.logger.info("Disconnected.")

    def ensure_llm_model_column(self, table: str = None) -> None:
        # Sin commit interno: el llamador maneja la transacción.
        with self._conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {table or self.TARGET_TABLE} "
                        "ADD COLUMN IF NOT EXISTS llm_model TEXT")

    # ------------------------------------------------------------------ v2
    def _columns(self, table: str) -> set:
        schema, name = table.split(".")
        with self._conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema=%s AND table_name=%s", (schema, name))
            return {r[0] for r in cur.fetchall()}

    def promote_run(self, run_id: str, fields: Iterable[str], table: str = None,
                    dry_run: bool = True) -> Dict[str, dict]:
        """Promueve los `fields` del `run_id`. dry_run=True no escribe nada.

        Devuelve {campo: {elegibles_valor, elegibles_ausencia, por_fuente, escritas}}.
        """
        table = table or self.TARGET_TABLE
        fields = list(fields)
        if not fields:
            raise ValueError("promote_run exige una lista explícita de campos")
        cols = self._columns(table)
        out = {}
        if not dry_run:
            self.ensure_llm_model_column(table)
        for field in fields:
            if not _IDENT.match(field) or field not in cols:
                raise ValueError(f"campo inválido o inexistente en {table}: {field!r}")
            val, conf, src = field, f"{field}_conf", f"{field}_source"
            status = f"{field}_status" if f"{field}_status" in cols else None
            guard_val = (f"(t.{conf} IS NULL OR t.{conf} < CASE s.source "
                         f"WHEN 'rule' THEN {CONF_BY_SOURCE['rule']} ELSE {CONF_BY_SOURCE['llm']} END "
                         f"OR t.{val} IS NULL OR (t.{val} = 0 AND t.{conf} = 0))")
            guard_abs = f"(t.{val} IS NULL OR (t.{val} = 0 AND t.{conf} = 0))"
            base = (f"FROM dncp.llm_fallback_runs_v2 s JOIN {table} t "
                    f"ON t.nro_licitacion = s.nro_licitacion "
                    f"WHERE s.run_id = %s AND s.field = %s AND s.status = 'done' ")
            elig_val = base + f"AND s.value IS NOT NULL AND s.source IN ('rule','llm') AND {guard_val}"
            elig_abs = (base + f"AND s.value IS NULL AND s.source IN ('rule','none') AND {guard_abs}"
                        if status else None)
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT s.source, count(*) {elig_val} GROUP BY 1", (run_id, field))
                por_fuente = dict(cur.fetchall())
                n_abs = 0
                if elig_abs:
                    cur.execute(f"SELECT count(*) {elig_abs}", (run_id, field))
                    n_abs = cur.fetchone()[0]
                written = 0
                if not dry_run:
                    old_status = f"t.{status}" if status else "NULL"
                    new_status_val = "'extraido'" if status else "NULL"
                    conf_expr = (f"CASE s.source WHEN 'rule' THEN {CONF_BY_SOURCE['rule']} "
                                 f"ELSE {CONF_BY_SOURCE['llm']} END")
                    cur.execute(
                        f"INSERT INTO dncp.document_features_v2_history (target_table, "
                        f"nro_licitacion, field, old_value, old_conf, old_source, old_status, "
                        f"new_value, new_conf, new_source, new_status, run_id) "
                        f"SELECT %s, s.nro_licitacion, %s, t.{val}, t.{conf}, t.{src}, "
                        f"{old_status}, s.value, {conf_expr}, s.source, {new_status_val}, "
                        f"s.run_id {elig_val}", (table, field, run_id, field))
                    set_status = f", {status} = 'extraido'" if status else ""
                    cur.execute(
                        f"UPDATE {table} t SET {val} = s.value, {conf} = {conf_expr}, "
                        f"{src} = s.source, llm_model = s.source || '@' || s.run_id{set_status} "
                        f"FROM dncp.llm_fallback_runs_v2 s WHERE t.nro_licitacion = s.nro_licitacion "
                        f"AND s.run_id = %s AND s.field = %s AND s.status = 'done' "
                        f"AND s.value IS NOT NULL AND s.source IN ('rule','llm') AND {guard_val}",
                        (run_id, field))
                    written = cur.rowcount
                    if elig_abs:
                        abs_src = "CASE s.source WHEN 'rule' THEN 'rule' ELSE 'llm' END"
                        cur.execute(
                            f"INSERT INTO dncp.document_features_v2_history (target_table, "
                            f"nro_licitacion, field, old_value, old_conf, old_source, old_status, "
                            f"new_value, new_conf, new_source, new_status, run_id) "
                            f"SELECT %s, s.nro_licitacion, %s, t.{val}, t.{conf}, t.{src}, "
                            f"t.{status}, NULL, NULL, {abs_src}, 'ausente', s.run_id {elig_abs}",
                            (table, field, run_id, field))
                        cur.execute(
                            f"UPDATE {table} t SET {val} = NULL, {conf} = NULL, "
                            f"{src} = {abs_src}, {status} = 'ausente', "
                            f"llm_model = s.source || '@' || s.run_id "
                            f"FROM dncp.llm_fallback_runs_v2 s WHERE t.nro_licitacion = s.nro_licitacion "
                            f"AND s.run_id = %s AND s.field = %s AND s.status = 'done' "
                            f"AND s.value IS NULL AND s.source IN ('rule','none') AND {guard_abs}",
                            (run_id, field))
                        written += cur.rowcount
            out[field] = dict(elegibles_valor=sum(por_fuente.values()), por_fuente=por_fuente,
                              elegibles_ausencia=n_abs, escritas=written)
            self.logger.info("promote %s %s %s: %s", "DRY" if dry_run else "REAL",
                             run_id, field, out[field])
        if not dry_run:
            self._conn.commit()
        return out

    def rollback_run(self, run_id: str, fields: List[str] = None, table: str = None,
                     dry_run: bool = True) -> Dict[str, int]:
        """Revierte las celdas promovidas por `run_id` a su valor previo (de la historia)."""
        table = table or self.TARGET_TABLE
        cols = self._columns(table)
        with self._conn.cursor() as cur:
            cur.execute("SELECT DISTINCT field FROM dncp.document_features_v2_history "
                        "WHERE run_id=%s AND target_table=%s AND rolled_back_at IS NULL",
                        (run_id, table))
            todo = [f for (f,) in cur.fetchall() if not fields or f in fields]
            out = {}
            for field in todo:
                if not _IDENT.match(field) or field not in cols:
                    raise ValueError(f"campo inválido: {field!r}")
                status = f"{field}_status" if f"{field}_status" in cols else None
                set_status = f", {status} = h.old_status" if status else ""
                cur.execute("SELECT count(*) FROM dncp.document_features_v2_history WHERE run_id=%s "
                            "AND field=%s AND target_table=%s AND rolled_back_at IS NULL",
                            (run_id, field, table))
                out[field] = cur.fetchone()[0]
                if dry_run:
                    continue
                cur.execute(
                    f"UPDATE {table} t SET {field} = h.old_value, {field}_conf = h.old_conf, "
                    f"{field}_source = h.old_source{set_status} "
                    f"FROM (SELECT DISTINCT ON (nro_licitacion) * FROM dncp.document_features_v2_history "
                    f"      WHERE run_id=%s AND field=%s AND target_table=%s AND rolled_back_at IS NULL "
                    f"      ORDER BY nro_licitacion, id) h "
                    f"WHERE t.nro_licitacion = h.nro_licitacion", (run_id, field, table))
                cur.execute("UPDATE dncp.document_features_v2_history SET rolled_back_at=now() "
                            "WHERE run_id=%s AND field=%s AND target_table=%s "
                            "AND rolled_back_at IS NULL", (run_id, field, table))
        if not dry_run:
            self._conn.commit()
        return out

    # ------------------------------------------------------------------ legado
    def upsert_guarded(self, df_results: pd.DataFrame, run_id: str) -> int:
        raise NotImplementedError(
            "Deprecado en sesión 23 (H7.1/H7.2: sin filtro por campo, source='llm' falso, "
            "sin historia). Usar promote_run() vía scripts/promote_fallback.py.")

    def update_field_batch(self, df_results: pd.DataFrame) -> int:
        raise NotImplementedError(
            "Deprecado en sesión 23: UPDATE directo a producción sin guardia ni historia. "
            "Usar promote_run() vía scripts/promote_fallback.py.")

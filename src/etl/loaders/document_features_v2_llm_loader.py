from typing import List
import pandas as pd
import psycopg2
from src.utils.logging_utils import setup_logger


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

    def ensure_llm_model_column(self) -> None:
        # Sin commit interno: el llamador maneja la transacción (un commit aquí
        # commitearía trabajo ajeno pendiente en la misma conexión).
        with self._conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE dncp.document_features_v2 "
                "ADD COLUMN IF NOT EXISTS llm_model TEXT"
            )
        self.logger.info("Ensured llm_model column exists.")

    def update_field_batch(self, df_results: pd.DataFrame) -> int:
        total = 0
        for field_name, grp in df_results.groupby("field_name"):
            n = self._update_single_field(field_name, grp)
            total += n
        self._conn.commit()
        self.logger.info(
            "LLM fallback updated %d cells across %d fields.",
            total, df_results["field_name"].nunique(),
        )
        return total

    def upsert_guarded(self, df_results: pd.DataFrame, run_id: str) -> int:
        """Staging→producción con regla de no-degradar: solo actualiza la fila si la
        confianza actual es MENOR que la nueva (o el valor actual es NULL).

        Requiere columnas: nro_licitacion, field_name, llm_value, llm_confidence,
        llm_model. Devuelve nº de celdas efectivamente actualizadas.
        """
        self.ensure_llm_model_column()
        total = 0
        for field_name, grp in df_results.groupby("field_name"):
            total += self._upsert_single_field_guarded(field_name, grp, run_id)
        self._conn.commit()
        self.logger.info(
            "Guarded upsert [%s]: %d cells across %d fields.",
            run_id, total, df_results["field_name"].nunique(),
        )
        return total

    def _upsert_single_field_guarded(
        self, field_name: str, df: pd.DataFrame, run_id: str,
    ) -> int:
        val_col, conf_col, src_col = (
            field_name, f"{field_name}_conf", f"{field_name}_source")
        updated = 0
        with self._conn.cursor() as cur:
            for _, row in df.iterrows():
                doc_id = row["nro_licitacion"]
                val = None if pd.isna(row["llm_value"]) else float(row["llm_value"])
                conf = float(row["llm_confidence"] or 0.0)
                model = row.get("llm_model", "unknown")
                cur.execute(
                    f"""UPDATE {self.TARGET_TABLE}
                        SET {val_col} = %s::NUMERIC,
                            {conf_col} = %s::REAL,
                            {src_col} = 'llm'::TEXT,
                            llm_model = %s::TEXT
                        WHERE nro_licitacion = %s
                          AND ({conf_col} IS NULL OR {conf_col} < %s
                               OR {val_col} IS NULL)""",
                    (val, conf, f"{model}@{run_id}", doc_id, conf),
                )
                updated += cur.rowcount or 0
        return updated

    def _update_single_field(self, field_name: str, df: pd.DataFrame) -> int:
        n = len(df)
        val_col = field_name
        conf_col = f"{field_name}_conf"
        src_col = f"{field_name}_source"

        cases_val = []
        cases_conf = []
        cases_src = []
        cases_model = []
        doc_ids = []

        for _, row in df.iterrows():
            doc_id = row["nro_licitacion"]
            val = row["llm_value"]
            conf = row["llm_confidence"]
            model = row.get("llm_model", "unknown")
            if pd.isna(val):
                cases_val.append(f"WHEN %s THEN NULL")
            else:
                cases_val.append(f"WHEN %s THEN %s::NUMERIC")
            cases_conf.append(f"WHEN %s THEN %s::REAL")
            cases_src.append(f"WHEN %s THEN 'llm'::TEXT")
            cases_model.append(f"WHEN %s THEN %s::TEXT")
            doc_ids.append(doc_id)

        if not doc_ids:
            return 0

        in_clause = ", ".join("%s" for _ in doc_ids)

        sql = f"""
            UPDATE {self.TARGET_TABLE}
            SET {val_col} = CASE nro_licitacion
                    {" ".join(cases_val)}
                    ELSE {val_col}
                END,
                {conf_col} = CASE nro_licitacion
                    {" ".join(cases_conf)}
                    ELSE {conf_col}
                END,
                {src_col} = CASE nro_licitacion
                    {" ".join(cases_src)}
                    ELSE {src_col}
                END,
                llm_model = CASE nro_licitacion
                    {" ".join(cases_model)}
                    ELSE llm_model
                END
            WHERE nro_licitacion IN ({in_clause})
        """

        params = []
        for _, row in df.iterrows():
            val = row["llm_value"]
            conf = row["llm_confidence"]
            model = row.get("llm_model", "unknown")
            doc_id = row["nro_licitacion"]
            if pd.isna(val):
                params.append(doc_id)
            else:
                params.extend([doc_id, val])
            params.extend([doc_id, conf])
            params.extend([doc_id])
            params.extend([doc_id, model])
        params.extend(doc_ids)

        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            updated = cur.rowcount

        self.logger.info(
            "  %s: updated %d rows (model=%s)",
            field_name, updated, df["llm_model"].iloc[0] if "llm_model" in df.columns else "?",
        )
        return updated

import json
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import psycopg2
from src.utils.logging_utils import setup_logger


class DocumentFeaturesV2LLMExtractor:
    def __init__(self, db_params: dict, schemas_path: str, conf_threshold: float = 0.8,
                 exclude_fields=None, features_table: str = "dncp.document_features_v2"):
        self.db_params = db_params
        self.conf_threshold = conf_threshold
        # Sesión 23: con dncp.document_features_v3 (NULL≠0) la población es
        # {campo}_status = 'no_extraido' (la conf de esas filas es NULL en v3).
        self.features_table = features_table
        self.exclude_fields = set(exclude_fields or [])
        self.logger = setup_logger(__name__)

        with Path(schemas_path).open(encoding="utf-8") as f:
            schemas = json.load(f)
        self.title_schemas = {k: v for k, v in schemas.items() if k != "meta"}

        self._field_to_title: Dict[str, str] = {}
        self._hybrid_fields: List[dict] = []
        excluded_count = 0
        for slug, schema in self.title_schemas.items():
            for f in schema["fields"]:
                self._field_to_title[f["name"]] = slug
                # spec-v1: llm_fallback=false saca el campo del fallback (reglas/regex o reclasificado)
                if f.get("extraction_method") == "hybrid" and f.get("llm_fallback", True):
                    if f["name"] in self.exclude_fields:
                        excluded_count += 1
                        continue
                    self._hybrid_fields.append(f)
        if excluded_count:
            self.logger.info(
                "Excluded %d hybrid fields per exclude_fields: %s",
                excluded_count, sorted(self.exclude_fields),
            )

    def get_hybrid_fields(self) -> List[dict]:
        return self._hybrid_fields

    def get_title_slug(self, field_name: str) -> Optional[str]:
        return self._field_to_title.get(field_name)

    def get_failed_extractions_grouped(self) -> Dict[str, pd.DataFrame]:
        """For each hybrid field, returns dict of {field_name: DataFrame}.

        Each DataFrame has columns: nro_licitacion, content_text, cluster_id,
        section_title, section_line_start, section_year (one section per doc).
        Only docs where ``{field}_conf < threshold`` are included.
        """
        self.logger.info(
            "Finding failed extractions (conf < %.1f) for %d hybrid fields…",
            self.conf_threshold, len(self._hybrid_fields),
        )

        groups: Dict[str, list] = {f["name"]: [] for f in self._hybrid_fields}
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        schema_name, table_name = self.features_table.split(".")
        cur.execute("SELECT count(*) FROM information_schema.columns WHERE table_schema=%s "
                    "AND table_name=%s AND column_name LIKE '%%\\_status'",
                    (schema_name, table_name))
        use_status = cur.fetchone()[0] > 0
        thr = 0.5 if use_status else self.conf_threshold

        try:
            for slug, schema in self.title_schemas.items():
                title_name = slug.replace("_", " ")
                hybrid = [f for f in schema["fields"]
                          if f.get("extraction_method") == "hybrid" and f.get("llm_fallback", True)]
                if not hybrid:
                    continue

                field_names = [f["name"] for f in hybrid]
                conf_cols = [f"{n}_conf" for n in field_names]
                if use_status:
                    # selector = 0 (pendiente) / 1 (resuelto): reutiliza la comparación < thr
                    sel_cols = [f"(CASE WHEN fv2.{n}_status = 'no_extraido' THEN 0.0 ELSE 1.0 END)"
                                for n in field_names]
                else:
                    sel_cols = [f"fv2.{c}" for c in conf_cols]
                where_clause = " OR ".join(f"{c} < %s" for c in sel_cols)

                # Sesión 23 (H1.5): una sola sección por doc, elegida de forma
                # determinista (la más larga; desempate por línea y año) y con su
                # clave (title, line_start, year) para que el resume sea exacto.
                sql = f"""
                    SELECT DISTINCT ON (fv2.nro_licitacion)
                           fv2.nro_licitacion, {', '.join(sel_cols)},
                           ps.content_text, ps.cluster_id,
                           ps.title, ps.line_start, ps.year
                    FROM {self.features_table} fv2
                    JOIN dncp.pliegos_secciones ps
                      ON fv2.nro_licitacion = ps.nro_licitacion
                     AND ps.title_normalized = %s
                    WHERE {where_clause}
                    ORDER BY fv2.nro_licitacion, ps.content_length DESC NULLS LAST,
                             ps.line_start, ps.year
                """
                params = [title_name] + [thr] * len(conf_cols)
                cur.execute(sql, params)
                rows = cur.fetchall()

                k = len(field_names)
                for row in rows:
                    doc_id = row[0]
                    text, cluster_id, title, line_start, year = row[1 + k:]
                    for i, fn in enumerate(field_names):
                        conf_val = row[1 + i]
                        if conf_val is not None and conf_val < thr:
                            groups[fn].append({
                                "nro_licitacion": doc_id,
                                "content_text": text,
                                "cluster_id": cluster_id,
                                "section_title": title,
                                "section_line_start": line_start,
                                "section_year": year,
                            })

                self.logger.info("  %s: %d rows checked", slug, len(rows))
        finally:
            cur.close()
            conn.close()

        result = {}
        for fn, items in groups.items():
            if items:
                result[fn] = pd.DataFrame(items).drop_duplicates(["nro_licitacion"])
                self.logger.info("  %s: %d docs need LLM", fn, len(result[fn]))
        return result

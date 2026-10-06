"""Recalcula UN campo regex en dncp.document_features_v3 con el extractor actual (plan F2/F4).

Usa exactamente la misma extracción que el pipeline v2 (secciones del título del campo
concatenadas por documento -> DocumentFeaturesV2Transformer._extract_field_text), pero
escribe con la semántica v3 (NULL ≠ 0 + {campo}_status) y deja historia en
dncp.document_features_v2_history (run_id = 'regex-recompute-<campo>-<git sha corto>').

Uso:
  /root/vllm-env/bin/python scripts/recompute_regex_field.py --field num_documentos_pago          # dry-run
  /root/vllm-env/bin/python scripts/recompute_regex_field.py --field num_documentos_pago --apply
"""
import argparse
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

import psycopg2
import psycopg2.extras

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402
from src.etl.transformers.document_features_v2_transformer import (  # noqa: E402
    DocumentFeaturesV2Transformer)

SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
TABLE = "dncp.document_features_v3"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--field", required=True)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    schemas = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    hit = [(k, f) for k, v in schemas.items() if k != "meta" for f in v["fields"]
           if f["name"] == a.field]
    if not hit:
        raise SystemExit(f"campo desconocido: {a.field}")
    slug, fschema = hit[0]
    title = slug.replace("_", " ")
    tr = DocumentFeaturesV2Transformer(str(SCHEMAS))
    sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                         text=True, cwd=ROOT).stdout.strip()
    run_id = f"regex-recompute-{a.field}-{sha}"

    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    cur.execute("SELECT nro_licitacion, string_agg(content_text, E'\\n' ORDER BY line_start) "
                "FROM dncp.pliegos_secciones WHERE title_normalized = %s "
                "AND content_text IS NOT NULL GROUP BY 1", (title,))
    new = {}
    for nro, text in cur.fetchall():
        v = tr._extract_field_text(text, slug, a.field, fschema["type"])
        new[str(nro)] = None if v is None else float(v)
    f = a.field
    cur.execute(f"SELECT nro_licitacion, {f}, {f}_status FROM {TABLE}")
    old = {str(n): (None if v is None else float(v), s) for n, v, s in cur.fetchall()}
    changes = []
    for nro, (ov, ost) in old.items():
        if ost == "sin_seccion" and nro not in new:
            continue
        nv = new.get(nro)
        nst = "extraido" if nv is not None else "no_extraido"
        if (nv, nst) != (ov, ost):
            changes.append((nro, ov, ost, nv, nst))
    print(f"{a.field}: {len(new)} docs con sección | cambios: {len(changes)}")
    print("  valores viejos (extraido):",
          Counter(ov for _, ov, ost, _, _ in changes if ost == "extraido").most_common(5))
    print("  valores nuevos:", Counter(nv for *_, nv, _ in changes).most_common(8))
    print("  transiciones de status:", Counter((ost, nst) for _, _, ost, _, nst in changes).most_common())
    if not a.apply:
        print("DRY-RUN: nada escrito (--apply para escribir)")
        return
    rows = [(n, nv, nst) for n, _, _, nv, nst in changes]
    cur.execute("CREATE TEMP TABLE _chg (nro TEXT, nv NUMERIC, nst TEXT) ON COMMIT DROP")
    psycopg2.extras.execute_values(cur, "INSERT INTO _chg VALUES %s", rows, page_size=5000)
    cur.execute(f"""
        INSERT INTO dncp.document_features_v2_history (target_table, nro_licitacion, field,
          old_value, old_conf, old_source, old_status, old_llm_model, new_value, new_conf,
          new_source, new_status, run_id)
        SELECT %s, c.nro, %s, t.{f}, t.{f}_conf, t.{f}_source, t.{f}_status, t.llm_model,
               c.nv, CASE WHEN c.nv IS NULL THEN 0 ELSE 1.0 END,
               CASE WHEN c.nv IS NULL THEN 'absent' ELSE 'regex' END, c.nst, %s
        FROM _chg c JOIN {TABLE} t ON t.nro_licitacion = c.nro""", (TABLE, f, run_id))
    cur.execute(f"""
        UPDATE {TABLE} t SET {f} = c.nv,
               {f}_conf = CASE WHEN c.nv IS NULL THEN NULL ELSE 1.0 END,
               {f}_source = CASE WHEN c.nv IS NULL THEN 'absent' ELSE 'regex' END,
               {f}_status = c.nst
        FROM _chg c WHERE t.nro_licitacion = c.nro""")
    print(f"APLICADO: {cur.rowcount} filas, historia run_id={run_id}")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()

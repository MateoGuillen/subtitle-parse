"""F4.1/F4.2 — dncp.document_features_v3: copia de v2 con semántica NULL ≠ 0 (auditoría H4.1).

Evidencia (sesión 23): en document_features_v2 ningún campo numérico tiene NULL; la
ausencia se codifica como 0 en los 26 campos int/float (12 hybrid + 14 regex), y en
5.266 etiquetas humanas de esos 26 campos (gt_por_campo) no hay NI UN 0. Por eso,
para los 26 campos numéricos:
    valor 0                      -> NULL
    {campo}_status (nuevo)       -> 'extraido' | 'sin_seccion' | 'no_extraido'
      extraido     valor != 0
      sin_seccion  el documento no tiene ninguna sección del título del campo
                   (en v2 era source='absent' con conf 1.0; para los regex sin
                   _source se determina con EXISTS sobre pliegos_secciones)
      no_extraido  la sección existe pero no hay valor (regex no-match): población
                   del fallback. Las promociones de F7 agregan 'ausente' (regla o
                   LLM dictaminan que el pliego no fija el valor).
    {campo}_conf                 -> NULL donde el valor pasa a NULL y la conf era 0
Binarios y categóricos NO se tocan (0 = "no" es un valor legítimo).
document_features_v2 queda INTACTA (reversible sin backup; nada que lea v2 se rompe).

Uso:
  /root/vllm-env/bin/python scripts/build_features_v3.py --sql   # solo imprime el SQL
  /root/vllm-env/bin/python scripts/build_features_v3.py         # crea/recrea v3 y verifica
"""
import argparse
import json
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402

SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
OUT_SQL = ROOT / "sql/s23_features_v3_null_semantics.sql"


def numeric_fields():
    """[(campo, title_normalized)] de los campos int/float del schema."""
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    return [(f["name"], k.replace("_", " ")) for k, v in s.items() if k != "meta"
            for f in v["fields"] if f["type"] in ("int", "float")]


def build_sql(cols: set) -> str:
    fields = [(f, t) for f, t in numeric_fields() if f in cols]
    lines = [
        "-- Generado por scripts/build_features_v3.py (sesión 23, H4.1). No editar a mano.",
        "BEGIN;",
        "DROP TABLE IF EXISTS dncp.document_features_v3;",
        "CREATE TABLE dncp.document_features_v3 AS SELECT * FROM dncp.document_features_v2;",
        "ALTER TABLE dncp.document_features_v3 ADD PRIMARY KEY (nro_licitacion);",
        "ALTER TABLE dncp.document_features_v3 ADD COLUMN IF NOT EXISTS llm_model TEXT;",
    ]
    for f, title in fields:
        has_conf = f"{f}_conf" in cols
        lines.append(f"ALTER TABLE dncp.document_features_v3 ADD COLUMN {f}_status TEXT;")
        lines.append(
            f"UPDATE dncp.document_features_v3 v SET {f}_status = CASE "
            f"WHEN {f} IS NOT NULL AND {f} <> 0 THEN 'extraido' "
            f"WHEN NOT EXISTS (SELECT 1 FROM dncp.pliegos_secciones s "
            f"  WHERE s.nro_licitacion = v.nro_licitacion AND s.title_normalized = '{title}') "
            f"THEN 'sin_seccion' ELSE 'no_extraido' END;")
        conf_set = (f", {f}_conf = CASE WHEN {f}_conf = 0 THEN NULL ELSE {f}_conf END"
                    if has_conf else "")
        lines.append(f"UPDATE dncp.document_features_v3 SET {f} = NULL{conf_set} WHERE {f} = 0;")
    lines.append("COMMIT;")
    return "\n".join(lines) + "\n", fields


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql", action="store_true")
    a = ap.parse_args()
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='dncp' AND table_name='document_features_v2'")
    cols = {r[0] for r in cur.fetchall()}
    sql, fields = build_sql(cols)
    OUT_SQL.write_text(sql, encoding="utf-8")
    if a.sql:
        print(sql)
        return
    cur.execute(sql)
    conn.commit()
    # ---- verificación
    bad = 0
    print(f"{'campo':36s} {'extraido':>8s} {'sin_secc':>8s} {'no_extr':>8s} | ceros  nulls_sin_status  v2_no0==v3_extraido")
    for f, _ in fields:
        cur.execute(f"""SELECT count(*) FILTER (WHERE {f}_status='extraido'),
                               count(*) FILTER (WHERE {f}_status='sin_seccion'),
                               count(*) FILTER (WHERE {f}_status='no_extraido'),
                               count(*) FILTER (WHERE {f} = 0),
                               count(*) FILTER (WHERE {f} IS NULL AND {f}_status IS NULL)
                        FROM dncp.document_features_v3""")
        ext, aus, noe, zeros, orphan = cur.fetchone()
        cur.execute(f"SELECT count(*) FROM dncp.document_features_v2 WHERE {f} <> 0")
        v2_nonzero = cur.fetchone()[0]
        ok = zeros == 0 and orphan == 0 and ext == v2_nonzero
        bad += not ok
        print(f"{f:36s} {ext:8d} {aus:8d} {noe:8d} | {zeros:5d} {orphan:17d} {str(ext == v2_nonzero):>21s}")
    cur.execute("SELECT (SELECT count(*) FROM dncp.document_features_v3), "
                "(SELECT count(*) FROM dncp.document_features_v2)")
    n3, n2 = cur.fetchone()
    print(f"filas v3={n3} v2={n2} | campos migrados={len(fields)} | "
          f"{'OK' if not bad and n3 == n2 else 'FALLA'} ({bad} campos con problema)")
    print(f"SQL -> {OUT_SQL.relative_to(ROOT)}")
    conn.close()
    sys.exit(0 if not bad and n3 == n2 else 1)


if __name__ == "__main__":
    main()

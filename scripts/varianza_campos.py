"""F4.6 — Varianza por campo: ¿qué campos aportan señal a la detección de anomalías? (H4.2)

Sobre la tabla de features final (default dncp.document_features_v3, NULL≠0) calcula por
campo numérico: cobertura, valores distintos, % del valor modal, entropía (bits) y, para
los campos con texto fuente, cuántos textos distintos lo sostienen. Un campo con valor
modal >= 99% de los extraídos no discrimina como magnitud: se reclasifica como binario
"cláusula estándar presente" (p. ej. fuerza mayor = 7, aceptación de factura = 15).

Salida: docs/varianza_campos.md + data/processed/llm_benchmark/varianza_campos.csv
Uso: /root/vllm-env/bin/python scripts/varianza_campos.py [--table dncp.document_features_v3]
"""
import argparse
import json
import math
import sys
from pathlib import Path

import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402

SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
MODAL_BINARIO = 0.99


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="dncp.document_features_v3")
    ap.add_argument("--overlay-run", default="",
                    help="proyecta el estado post-promote: COALESCE(tabla, staging del run)")
    a = ap.parse_args()
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    fields = [(f["name"], f["type"], f["extraction_method"], k.replace("_", " "))
              for k, v in s.items() if k != "meta" for f in v["fields"]
              if f["type"] in ("int", "float")]
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg, options="-c default_transaction_read_only=on")
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM {a.table}")
    n_docs = cur.fetchone()[0]
    rows = []
    for f, t, m, title in fields:
        if a.overlay_run:
            cur.execute(f"""SELECT COALESCE(t.{f}, s.value) v, count(*) FROM {a.table} t
                LEFT JOIN dncp.llm_fallback_runs_v2 s ON s.nro_licitacion = t.nro_licitacion
                 AND s.field = %s AND s.run_id = %s AND s.status = 'done'
                WHERE COALESCE(t.{f}, s.value) IS NOT NULL GROUP BY 1""", (f, a.overlay_run))
        else:
            cur.execute(f"SELECT {f}, count(*) FROM {a.table} WHERE {f} IS NOT NULL GROUP BY 1")
        dist = {float(v): c for v, c in cur.fetchall()}
        n = sum(dist.values())
        if n:
            modal_v, modal_c = max(dist.items(), key=lambda x: x[1])
            ent = -sum((c / n) * math.log2(c / n) for c in dist.values())
        else:
            modal_v, modal_c, ent = None, 0, 0.0
        src = {}
        cur.execute("SELECT column_name FROM information_schema.columns WHERE "
                    "table_schema=%s AND table_name=%s AND column_name=%s",
                    (*a.table.split("."), f"{f}_source"))
        if cur.fetchone():
            cur.execute(f"SELECT {f}_source, count(*) FROM {a.table} WHERE {f} IS NOT NULL "
                        f"GROUP BY 1")
            src = dict(cur.fetchall())
        cur.execute("SELECT count(DISTINCT md5(content_text)) FROM dncp.pliegos_secciones "
                    "WHERE title_normalized = %s", (title,))
        textos = cur.fetchone()[0]
        frac_modal = modal_c / n if n else None
        clase = ("sin datos" if not n else
                 "binario (cláusula estándar)" if frac_modal >= MODAL_BINARIO else
                 "baja variabilidad" if ent < 1.0 else "magnitud")
        rows.append(dict(campo=f, tipo=t, metodo=m, extraidos=n,
                         cobertura=round(n / n_docs, 4), valores_distintos=len(dist),
                         valor_modal=modal_v, frac_modal=None if n == 0 else round(frac_modal, 4),
                         entropia_bits=round(ent, 3), textos_distintos_seccion=textos,
                         fuentes=json.dumps(src), clase=clase))
    conn.close()
    df = pd.DataFrame(rows).sort_values(["clase", "entropia_bits"], ascending=[True, False])
    out_csv = ROOT / "data/processed/llm_benchmark/varianza_campos.csv"
    df.to_csv(out_csv, index=False)
    md = ["# Varianza por campo (F4.6, auditoría H4.2)", "",
          f"> Generado por `scripts/varianza_campos.py` sobre `{a.table}` ({n_docs} docs, NULL≠0)"
          + (f" + proyección del run `{a.overlay_run}` (estado post-promote)." if a.overlay_run else "."),
          f"> `binario`: el valor modal es ≥{MODAL_BINARIO:.0%} de los extraídos → como magnitud no "
          "discrimina; usar como indicador de cláusula estándar presente.", "",
          "| campo | método | extraídos | cobertura | distintos | modal (frac) | entropía | textos sección | fuentes | clase |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for r in df.itertuples():
        modal = "—" if r.valor_modal is None else f"{r.valor_modal:g} ({r.frac_modal:.1%})"
        md.append(f"| {r.campo} | {r.metodo} | {r.extraidos} | {r.cobertura:.1%} | "
                  f"{r.valores_distintos} | {modal} | {r.entropia_bits} | "
                  f"{r.textos_distintos_seccion} | {r.fuentes} | {r.clase} |")
    (ROOT / "docs/varianza_campos.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(df[["campo", "extraidos", "valores_distintos", "frac_modal", "entropia_bits",
              "clase"]].to_string(index=False))


if __name__ == "__main__":
    main()

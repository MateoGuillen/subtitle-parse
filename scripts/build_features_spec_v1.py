"""I3 — Features de la especificación de campos v1.0 (docs/especificacion_campos.md, 08-oct-2026).

Calcula desde dncp.pliegos_secciones (texto por documento = secciones del título concatenadas por
line_start, igual que el pipeline v2):
  1. Columnas NUEVAS (regex/flags, sin LLM): tipo_indice_reajuste, 3 binarios del grupo C y 7 flags
     (field_rules_v1.NEW_COLUMNS). Con --apply se agregan a dncp.document_features_v3.
  2. Valores de las REGLAS v1 para los 8 campos con ficha (A1-A6, B1, B2) con su estado
     ('valor' | 'ausente' | 'llm' = residual para el fallback). Con --apply van a la tabla de staging
     dncp.spec_v1_rule_values. NO se tocan los valores actuales de esos campos en v3: cambiarlos es
     un promote (F7), que requiere la validación fuera de muestra (I6/I7) y la aprobación del usuario.

Uso:
  /root/vllm-env/bin/python scripts/build_features_spec_v1.py            # dry-run: resumen + reportes CSV
  /root/vllm-env/bin/python scripts/build_features_spec_v1.py --apply    # escribe columnas nuevas + staging
"""
import argparse
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402
from src.etl.transformers import field_rules_v1 as fr  # noqa: E402
from src.etl.transformers.run_fingerprint import sha_file  # noqa: E402

TABLE = "dncp.document_features_v3"
STAGING = "dncp.spec_v1_rule_values"
OUT = ROOT / "data/processed/spec_v1"
RULE_FIELDS = {  # campo -> slug de la sección
    "dias_pago_maximo": "formas_y_condiciones_de_pago",
    "dias_validez_garantia": "periodo_de_validez_de_la_garantia_de_los_bienes",
    "dias_pago_anticipo": "solicitud_de_pago_de_anticipo",
    "dias_solicitud_anticipo": "solicitud_de_pago_de_anticipo",
    "anos_experiencia_minima_tecnica": "capacidad_tecnica",
    "pct_monto_referencia_tecnica": "capacidad_tecnica",
    "ratio_liquidez_minimo": "capacidad_financiera",
    "ratio_endeudamiento_maximo": "capacidad_financiera",
}


def load_texts(cur, slugs):
    titles = [s.replace("_", " ") for s in slugs]
    cur.execute("SELECT nro_licitacion, title_normalized, string_agg(content_text, E'\\n' ORDER BY line_start) "
                "FROM dncp.pliegos_secciones WHERE title_normalized = ANY(%s) AND content_text IS NOT NULL "
                "GROUP BY 1, 2", (titles,))
    out = {s: {} for s in slugs}
    for nro, title, text in cur.fetchall():
        out[title.replace(" ", "_")][str(nro)] = text
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    cur.execute(f"SELECT nro_licitacion FROM {TABLE}")
    docs = [str(r[0]) for r in cur.fetchall()]
    slugs = sorted(set(fr.NEW_COLUMNS) | set(RULE_FIELDS.values()))
    texts = load_texts(cur, slugs)
    code_sha = sha_file(str(ROOT / "src/etl/transformers/field_rules_v1.py"))[:16]
    git = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                         cwd=ROOT).stdout.strip()

    # ---- 1. columnas nuevas
    cols = {c: t for d in fr.NEW_COLUMNS.values() for c, t in d.items()}
    cols.update({"pct_respaldo_financiero_monto": "NUMERIC", "exige_respaldo_financiero": "SMALLINT"})
    newvals = {c: {} for c in cols}
    # A8 (variable nueva): respaldo financiero proporcional al monto, sobre varias secciones
    cur.execute("SELECT nro_licitacion, string_agg(content_text, E'\\n' ORDER BY title_normalized, line_start) "
                "FROM dncp.pliegos_secciones WHERE title_normalized = ANY(%s) AND content_text IS NOT NULL "
                "GROUP BY 1", (list(fr.SOLVENCIA_TITLES),))
    solv = {str(n): fr.pct_respaldo_financiero_monto(t) for n, t in cur.fetchall()}
    for nro in docs:
        v = solv.get(nro)
        newvals["pct_respaldo_financiero_monto"][nro] = v
        newvals["exige_respaldo_financiero"][nro] = None if nro not in solv else int(v is not None)
    for slug, cdefs in fr.NEW_COLUMNS.items():
        tx = texts[slug]
        for nro in docs:
            feats = fr.section_features(slug, tx[nro]) if nro in tx else \
                {c: fr.NO_SECTION_DEFAULT.get(c) for c in cdefs}
            for c in cdefs:
                newvals[c][nro] = feats.get(c)
    OUT.mkdir(parents=True, exist_ok=True)
    resumen = []
    print(f"== Columnas nuevas ({len(cols)}) sobre {len(docs)} docs de {TABLE}")
    for c in cols:
        cnt = Counter(newvals[c].values())
        nulls = cnt.pop(None, 0)
        top = cnt.most_common(8)
        print(f"  {c:36s} NULL={nulls:6d}  {top}")
        resumen.append({"columna": c, "tipo": cols[c], "nulls": nulls, "distribucion": dict(top)})
    pd.DataFrame(resumen).to_csv(OUT / "columnas_nuevas_resumen.csv", index=False)

    # ---- 2. reglas v1 vs v3 actual
    rule_rows, comp = [], []
    for f, slug in RULE_FIELDS.items():
        cur.execute(f"SELECT nro_licitacion, {f}, {f}_status FROM {TABLE}")
        cur_v3 = {str(n): (None if v is None else float(v), st) for n, v, st in cur.fetchall()}
        tx = texts[slug]
        st_cnt, same, changed, newly, lost = Counter(), 0, 0, 0, 0
        for nro in docs:
            if nro not in tx:
                continue
            v = fr.RULES_V1[f](tx[nro])
            status = "llm" if v is fr.NO_RULE else ("ausente" if v is None else "valor")
            val = None if status != "valor" else float(v)
            st_cnt[status] += 1
            rule_rows.append((nro, f, val, status))
            old = cur_v3.get(nro, (None, None))[0]
            if status == "llm":
                continue
            if old is None and val is None or (old is not None and val is not None and abs(old - val) < 1e-9):
                same += 1
            elif old is None:
                newly += 1
            elif val is None:
                lost += 1
            else:
                changed += 1
        n = sum(st_cnt.values())
        comp.append({"campo": f, "docs_con_seccion": n, **{f"estado_{k}": st_cnt[k] for k in ("valor", "ausente", "llm")},
                     "igual_a_v3": same, "valor_distinto": changed, "nuevo_valor": newly, "v1_ausente_v3_valor": lost})
        print(f"  {f:34s} secc={n:6d} valor={st_cnt['valor']:6d} ausente={st_cnt['ausente']:6d} llm={st_cnt['llm']:4d} | "
              f"=v3 {same:6d}  distinto {changed:5d}  nuevo {newly:5d}  v1-ausente/v3-valor {lost:5d}")
    pd.DataFrame(comp).to_csv(OUT / "reglas_v1_vs_v3.csv", index=False)
    print(f"Reportes -> {OUT.relative_to(ROOT)}/  (code_sha={code_sha}, git={git})")

    if not a.apply:
        print("DRY-RUN: nada escrito (--apply para agregar columnas a v3 y llenar el staging)")
        return
    # ---- escritura
    for c, t in cols.items():
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS {c} {t}")
    cur.execute("CREATE TEMP TABLE _nv (nro TEXT PRIMARY KEY, " +
                ", ".join(f"{c} {t}" for c, t in cols.items()) + ") ON COMMIT DROP")
    psycopg2.extras.execute_values(
        cur, f"INSERT INTO _nv (nro, {', '.join(cols)}) VALUES %s",
        [(nro, *[newvals[c][nro] for c in cols]) for nro in docs], page_size=5000)
    cur.execute(f"UPDATE {TABLE} v SET " + ", ".join(f"{c} = n.{c}" for c in cols) +
                " FROM _nv n WHERE v.nro_licitacion::text = n.nro")
    print(f"columnas nuevas escritas en {cur.rowcount} filas de {TABLE}")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {STAGING} (
        nro_licitacion TEXT NOT NULL, field TEXT NOT NULL, value NUMERIC, status TEXT NOT NULL,
        ruleset TEXT NOT NULL, code_sha TEXT NOT NULL, git_sha TEXT, computed_at TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (nro_licitacion, field, ruleset))""")
    cur.execute(f"DELETE FROM {STAGING} WHERE ruleset = 'spec-v1'")
    psycopg2.extras.execute_values(
        cur, f"INSERT INTO {STAGING} (nro_licitacion, field, value, status, ruleset, code_sha, git_sha) VALUES %s",
        [(n, f, v, s, "spec-v1", code_sha, git) for n, f, v, s in rule_rows], page_size=5000)
    print(f"{len(rule_rows)} filas en {STAGING} (ruleset=spec-v1)")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()

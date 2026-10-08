"""I5 — Calibración v2 de la especificación de campos v1.0 (docs/especificacion_campos.md, plan I5).

60 ítems ciegos (A1-A6 × 10) de documentos que NO están en el GT de agosto, ni en la muestra del pseudo-GT
Opus, ni en la calibración v1. Estratos por campo según el valor de la regla spec-v1 (dncp.spec_v1_rule_values):
  no_modal 5 (valor distinto del modal: donde el criterio importa) · modal 2 · ausente 2 · residual LLM 1
(si un estrato no alcanza, completa con no_modal). Un texto distinto por ítem.

Dos anotaciones independientes, ambas ciegas al valor del sistema y guiadas solo por la ficha
(extraction_hint de master_schemas_v2.json, spec v1.0):
  - usuario: planillas/calibracion_v2.csv (+ .xlsx si hay openpyxl), columnas valor_anotador / nota
  - Opus (subagente sin acceso a las reglas): planillas/calibracion_v2_opus.csv
Clave (no mostrar al anotador): planillas/calibracion_v2_clave.csv. Semilla fija.
Después: scripts/evaluar_calibracion_v2.py → κ usuario-Opus y acuerdo con la regla por campo
(κ < 0,8 → se corrige la ficha).
"""
import hashlib
import json
import random
import re
import sys
from pathlib import Path

import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402

SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
PSEUDO = ROOT / "data/processed/llm_benchmark/pseudo_gt_opus/muestra.csv"
OUT = ROOT / "data/processed/llm_benchmark/planillas"
FIELDS = {  # campo -> sección
    "dias_pago_maximo": "formas y condiciones de pago",
    "dias_validez_garantia": "periodo de validez de la garantia de los bienes",
    "dias_pago_anticipo": "solicitud de pago de anticipo",
    "dias_solicitud_anticipo": "solicitud de pago de anticipo",
    "anos_experiencia_minima_tecnica": "capacidad tecnica",
    "pct_monto_referencia_tecnica": "capacidad tecnica",
}
QUOTA = {"no_modal": 5, "modal": 2, "ausente": 2, "residual": 1}
MAX_CHARS = 8000
SEED = 1008


def norm(t):
    return hashlib.sha256(re.sub(r"\s+", " ", t or "").strip().lower().encode()).hexdigest()


def main():
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    hint = {f["name"]: f.get("extraction_hint") or f.get("description", "")
            for k, v in s.items() if k != "meta" for f in v["fields"]}
    used = set()
    for p in GT_DIR.glob("*.csv"):
        if not p.name.startswith("errores_"):
            used |= set(pd.read_csv(p, usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    used |= set(pd.read_csv(PSEUDO, usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    used |= set(pd.read_csv(OUT / "calibracion_v1_clave.csv", usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    cur = psycopg2.connect(**cfg, options="-c default_transaction_read_only=on").cursor()
    rng = random.Random(SEED)
    sheet, key, seen = [], [], set()
    for f, title in FIELDS.items():
        cur.execute("""
            SELECT r.nro_licitacion, r.value, r.status,
                   string_agg(s.content_text, E'\\n' ORDER BY s.line_start)
            FROM dncp.spec_v1_rule_values r
            JOIN dncp.pliegos_secciones s ON s.nro_licitacion::text = r.nro_licitacion AND s.title_normalized = %s
            WHERE r.field = %s AND r.ruleset = 'spec-v1' AND s.content_text IS NOT NULL
            GROUP BY 1, 2, 3""", (title, f))
        by_text = {}
        for nro, val, st, text in cur.fetchall():
            if nro in used or len(text) > MAX_CHARS or len(text.strip()) < 30:
                continue
            by_text.setdefault(norm(text), (nro, None if val is None else float(val), st, text))
        vals = [v for _, v, st, _ in by_text.values() if st == "valor"]
        modal = max(set(vals), key=vals.count) if vals else None
        strata = {k: [] for k in QUOTA}
        for h, (nro, v, st, text) in by_text.items():
            k = {"llm": "residual", "ausente": "ausente"}.get(st) or ("modal" if v == modal else "no_modal")
            strata[k].append((h, nro, v, st, text))
        picked = []
        for k in QUOTA:
            rng.shuffle(strata[k])
        for k, q in QUOTA.items():
            picked += [(k, x) for x in strata[k] if (f, x[0]) not in seen][:q]
        falta = 10 - len(picked)
        if falta > 0:
            picked += [("no_modal", x) for x in strata["no_modal"][QUOTA["no_modal"]:]][:falta]
        for k, (h, nro, v, st, text) in picked:
            seen.add((f, h))
            iid = hashlib.sha256(f"cal2|{f}|{h}".encode()).hexdigest()[:10]
            sheet.append(dict(id=iid, campo=f, guia=hint.get(f, ""), texto=re.sub(r"[ \t]+", " ", text).strip(),
                              valor_anotador="", nota=""))
            key.append(dict(id=iid, campo=f, nro_licitacion=nro, valor_regla=v, estado_regla=st, estrato=k,
                            modal_campo=modal))
        cnt = {k: len(v) for k, v in strata.items()}
        print(f"  {f:34s} textos={len(by_text):6d} modal={modal} estratos={cnt} -> {len(picked)}")
    df = pd.DataFrame(sheet).sample(frac=1.0, random_state=SEED).reset_index(drop=True)
    df.to_csv(OUT / "calibracion_v2.csv", index=False)
    pd.DataFrame(key).to_csv(OUT / "calibracion_v2_clave.csv", index=False)
    print(f"{len(df)} ítems -> {OUT.relative_to(ROOT)}/calibracion_v2.csv (CIEGA) + clave")


if __name__ == "__main__":
    main()

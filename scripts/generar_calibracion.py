"""Muestra de calibración para la especificación de campos (docs/especificacion_campos.md).

10 ítems por campo de los grupos A y B (10 campos), de documentos que NO están en gt_por_campo ni en
la muestra del pseudo-GT Opus. Sobremuestrea el estrato "no modal": 6 ítems cuyo valor actual
(regla/regex en v3 o staging) difiere del modal del campo, 2 con valor modal, 2 sin valor (NULL).
Así la calibración se hace donde el criterio importa, no en la plantilla.
Salida ciega: planillas/calibracion_v1.csv (id, campo, texto, valor_anotador, estado, nota) + clave
calibracion_v1_clave.csv (id, nro, valor_sistema, estrato). Semilla fija.
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
RUN = "llmfb-qwen3-8b-fp8-3fe595a69e69bfd4"
FIELDS = ["dias_pago_maximo", "dias_validez_garantia", "dias_pago_anticipo", "dias_solicitud_anticipo",
          "anos_experiencia_minima_tecnica", "pct_monto_referencia_tecnica",
          "ratio_liquidez_minimo", "ratio_endeudamiento_maximo", "num_componentes_formula"]
QUOTA = {"no_modal": 6, "modal": 2, "null": 2}
SEED = 808


def norm(t):
    return hashlib.sha256(re.sub(r"\s+", " ", t or "").strip().lower().encode()).hexdigest()


def main():
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    meta = {f["name"]: (k.replace("_", " "), f) for k, v in s.items() if k != "meta" for f in v["fields"]}
    used = set()
    for p in GT_DIR.glob("*.csv"):
        if not p.name.startswith("errores_"):
            used |= set(pd.read_csv(p, usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    used |= set(pd.read_csv(PSEUDO, usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg, options="-c default_transaction_read_only=on")
    cur = conn.cursor()
    rng = random.Random(SEED)
    sheet, key = [], []
    for f in FIELDS:
        title, fs = meta[f]
        # valor vigente: v3 si extraído, si no el del staging (regla/LLM); NULL si ninguno
        cur.execute(f"""
            SELECT v.nro_licitacion, COALESCE(v.{f}, r.value) AS val, s.content_text
            FROM dncp.document_features_v3 v
            JOIN dncp.pliegos_secciones s ON s.nro_licitacion = v.nro_licitacion
                 AND s.title_normalized = %s
            LEFT JOIN dncp.llm_fallback_runs_v2 r ON r.nro_licitacion = v.nro_licitacion
                 AND r.field = %s AND r.run_id = %s AND r.status = 'done'
            WHERE s.content_text IS NOT NULL AND length(s.content_text) > 30""", (title, f, RUN))
        by_text = {}
        for nro, val, text in cur.fetchall():
            if str(nro) in used:
                continue
            by_text.setdefault(norm(text), (str(nro), None if val is None else float(val), text))
        vals = [v for _, v, _ in by_text.values() if v is not None]
        modal = max(set(vals), key=vals.count) if vals else None
        strata = {"no_modal": [], "modal": [], "null": []}
        for h, (nro, v, text) in by_text.items():
            strata["null" if v is None else "modal" if v == modal else "no_modal"].append((h, nro, v, text))
        for st, lst in strata.items():
            rng.shuffle(lst)
            for h, nro, v, text in lst[:QUOTA[st]]:
                iid = hashlib.sha256(f"cal|{f}|{h}".encode()).hexdigest()[:10]
                sheet.append(dict(id=iid, campo=f, descripcion=fs.get("description", ""),
                                  texto=text[:6000], valor_anotador="", estado="", nota=""))
                key.append(dict(id=iid, campo=f, nro_licitacion=nro, valor_sistema=v, estrato=st,
                                modal_campo=modal))
        print(f"  {f:34s} textos={len(by_text):6d} modal={modal} "
              f"no_modal={len(strata['no_modal'])} null={len(strata['null'])}")
    conn.close()
    df = pd.DataFrame(sheet).sample(frac=1.0, random_state=SEED).reset_index(drop=True)
    df.to_csv(OUT / "calibracion_v1.csv", index=False)
    pd.DataFrame(key).to_csv(OUT / "calibracion_v1_clave.csv", index=False)
    print(f"{len(df)} ítems -> {OUT / 'calibracion_v1.csv'} (CIEGA) + clave")


if __name__ == "__main__":
    main()

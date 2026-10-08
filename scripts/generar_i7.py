"""I7 — GT fuera de muestra de la especificación v1.0 (docs/especificacion_campos.md, plan I7).

Diseño (propuesto tras I6 y aprobado por el usuario 08-oct-2026): la referencia se concentra donde decide el
LLM (residuales de producción: la regla v1 devuelve NO_RULE) y se completa con una muestra de lo que decide la
regla, para medir la precisión de las dos fuentes por campo y estrato. Por campo A1-A6:
  residual_discrepa  hasta 25 residuales donde los 4 modelos de I6 no coinciden (lo que el GT desempata)
  residual_coincide  hasta 5 residuales donde los 4 coinciden (¿el consenso es correcto?)
  regla_no_modal 10 · regla_modal 4 · regla_ausente 4   (la regla aplica; valida la regla fuera de muestra)
Excluye todo documento del GT de agosto, del pseudo-GT Opus y de las calibraciones v1/v2. Un texto distinto por
ítem y campo. El texto es el mismo que ven los modelos (sección completa, truncada a 20.000 caracteres; para los
estratos de regla solo se toman textos que entran enteros, así regla, modelos y anotador ven lo mismo).

Salida (planillas/): i7_gt.csv (CIEGA: id, campo, guia, texto, valor_anotador, nota) + i7_gt_clave.csv
(estrato, tamaño del estrato en la base para ponderar, nro, valor de la regla, predicciones I6 de los
residuales). Semilla fija.
Después: anotación Opus ciega (i7_gt_opus.csv), anotación del usuario, scripts/benchmark_i6.py --i7 y
scripts/evaluar_i7.py.

--lote b (I7b, aprobado 08-oct tras I7): valida fuera de muestra las correcciones de I7 (reglas A4/A5 y hint A5
v1.1) y amplía los estratos de regla para que el IC del promote sea informativo. Excluye además los documentos de
I7. Cuotas QUOTA_B; residual = todo NO_RULE vigente (sin depender de I6). Salida i7b_gt.csv / i7b_gt_clave.csv.
"""
import argparse
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
sys.path.insert(0, str(ROOT / "scripts"))
from config.settings import DB_CONFIG  # noqa: E402
from src.etl.transformers import field_rules_v1 as fr  # noqa: E402

SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
LB = ROOT / "data/processed/llm_benchmark"
OUT = LB / "planillas"
MODELS_I6 = ["gemma-4-12b-qat-w4a16", "qwen3-14b-awq", "qwen3-8b-fp8", "gemma-4-26b-a4b-gguf"]
FIELDS = {
    "dias_pago_maximo": "formas y condiciones de pago",
    "dias_validez_garantia": "periodo de validez de la garantia de los bienes",
    "dias_pago_anticipo": "solicitud de pago de anticipo",
    "dias_solicitud_anticipo": "solicitud de pago de anticipo",
    "anos_experiencia_minima_tecnica": "capacidad tecnica",
    "pct_monto_referencia_tecnica": "capacidad tecnica",
}
QUOTA = {"residual_discrepa": 25, "residual_coincide": 5, "regla_no_modal": 10, "regla_modal": 4, "regla_ausente": 4}
MAX_CHARS = 20000
SEED = 1007
QUOTA_B = {"residual": 10, "regla_no_modal": 16, "regla_modal": 16, "regla_ausente": 16}
QUOTA_B_RESIDUAL = {"anos_experiencia_minima_tecnica": 20}
SEED_B = 1009


def norm(t):
    return hashlib.sha256(re.sub(r"\s+", " ", t or "").strip().lower().encode()).hexdigest()


def same(a, b):
    return (a is None and b is None) or (a is not None and b is not None and abs(a - b) < 1e-9)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lote", choices=["a", "b"], default="a")
    lote = ap.parse_args().lote
    quota, seed, pref = (QUOTA, SEED, "i7") if lote == "a" else (QUOTA_B, SEED_B, "i7b")
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    hint = {f["name"]: f.get("extraction_hint") or f.get("description", "")
            for k, v in s.items() if k != "meta" for f in v["fields"]}
    used = set()
    for p in GT_DIR.glob("*.csv"):
        if not p.name.startswith("errores_"):
            used |= set(pd.read_csv(p, usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    used |= set(pd.read_csv(LB / "pseudo_gt_opus/muestra.csv", usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    for v in ("v1", "v2"):
        used |= set(pd.read_csv(OUT / f"calibracion_{v}_clave.csv", usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    if lote == "b":
        used |= set(pd.read_csv(OUT / "i7_gt_clave.csv", usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    # predicciones crudas de I6 sobre los residuales
    pred = {m: {(it["field"], it["nro"]): it["llm"]
                for it in json.loads((LB / f"I6_{m}.json").read_text(encoding="utf-8"))["items"]
                if it["set"] == "residual"} for m in MODELS_I6}
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    cur = psycopg2.connect(**cfg, options="-c default_transaction_read_only=on").cursor()
    rng = random.Random(seed)
    sheet, key = [], []
    for f, title in FIELDS.items():
        cur.execute("""
            SELECT r.nro_licitacion, string_agg(s.content_text, E'\\n' ORDER BY s.line_start)
            FROM dncp.spec_v1_rule_values r
            JOIN dncp.pliegos_secciones s ON s.nro_licitacion::text = r.nro_licitacion AND s.title_normalized = %s
            WHERE r.field = %s AND r.ruleset = 'spec-v1' AND s.content_text IS NOT NULL
            GROUP BY 1""", (title, f))
        strata, by_text, vals = {k: [] for k in quota}, {}, []
        for nro, text in cur.fetchall():
            if nro in used or len(text.strip()) < 30:
                continue
            v = fr.RULES_V1[f](text)  # regla vigente sobre el texto completo
            if v is not fr.NO_RULE and len(text) > MAX_CHARS:
                continue
            h = norm(text[:MAX_CHARS])
            if h in by_text:
                continue
            by_text[h] = (nro, v, text[:MAX_CHARS])
            if v is not fr.NO_RULE and v is not None:
                vals.append(float(v))
        modal = max(set(vals), key=vals.count) if vals else None
        for h, (nro, v, text) in by_text.items():
            if v is fr.NO_RULE and lote == "b":
                k = "residual"
            elif v is fr.NO_RULE:
                p = [pred[m].get((f, nro), "falta") for m in MODELS_I6]
                if "falta" in p:  # residual nuevo (posterior a I6): sin predicciones, no entra
                    continue
                k = "residual_coincide" if all(same(x, p[0]) for x in p) else "residual_discrepa"
            elif v is None:
                k = "regla_ausente"
            else:
                k = "regla_modal" if float(v) == modal else "regla_no_modal"
            strata[k].append((nro, v, text))
        picked = []
        for k, q in quota.items():
            if k == "residual":
                q = QUOTA_B_RESIDUAL.get(f, q)
            rng.shuffle(strata[k])
            picked += [(k, x) for x in strata[k][:q]]
        for k, (nro, v, text) in picked:
            iid = hashlib.sha256(f"{pref}|{f}|{nro}".encode()).hexdigest()[:10]
            sheet.append(dict(id=iid, campo=f, guia=hint.get(f, ""), texto=re.sub(r"[ \t]+", " ", text).strip(),
                              valor_anotador="", nota=""))
            row = dict(id=iid, campo=f, nro_licitacion=nro, estrato=k, pob_estrato=len(strata[k]), modal_campo=modal,
                       valor_regla=None if (v is fr.NO_RULE or v is None) else float(v),
                       estado_regla="llm" if v is fr.NO_RULE else "ausente" if v is None else "valor")
            row.update({f"i6_{m}": pred[m].get((f, nro)) for m in MODELS_I6} if lote == "a" else {})
            key.append(row)
        cnt = {k: len(v) for k, v in strata.items()}
        print(f"  {f:34s} textos={len(by_text):6d} modal={modal} estratos={cnt} -> {len(picked)}")
    df = pd.DataFrame(sheet).sample(frac=1.0, random_state=seed).reset_index(drop=True)
    df.to_csv(OUT / f"{pref}_gt.csv", index=False)
    pd.DataFrame(key).to_csv(OUT / f"{pref}_gt_clave.csv", index=False)
    print(f"{len(df)} ítems -> {OUT.relative_to(ROOT)}/{pref}_gt.csv (CIEGA) + {pref}_gt_clave.csv")


if __name__ == "__main__":
    main()

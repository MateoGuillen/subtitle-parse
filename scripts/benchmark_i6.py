"""I6 — Benchmark reducido de la especificación v1.0 (docs/especificacion_campos.md, plan I6).

Modelos (aprobados por el usuario 08-oct-2026): gemma-4-12b-qat-w4a16, qwen3-14b-awq, qwen3-8b-fp8 y
gemma-4-26b-a4b-gguf (llama-server, KV q8, 2 slots) (configuración y backends de scripts/ablacion_gt.py). Campos A1-A6. Prompt de PRODUCCIÓN (extraction_hint v1.0
de master_schemas_v2.json) con decodificación restringida (número o NULL).

Conjuntos (una sola carga del modelo por corrida):
  gt_v1      GT de agosto re-alineado en I4 (gt_por_campo_v1, 200/campo). Reglas desarrolladas aquí: in-sample.
  pseudo_v1  pseudo-GT Opus re-alineado (anotaciones_v1.json, ambiguos fuera; fuera de muestra de agosto).
  calib_v2   calibración v2 anotada por Opus a ciegas (60; la referencia humana llega con I5).
  residual   TODOS los casos campo-documento de la base donde la regla v1 devuelve NO_RULE (sin referencia):
             lo que el LLM decide de verdad en producción. Se mide acuerdo entre modelos.
Brazos por ítem: regla (spec-v1 sola), llm (salida CRUDA del modelo, sin reglas), sistema (rule_first).
Salida: data/processed/llm_benchmark/I6_<modelo>.json ; resumen con scripts/benchmark_i6.py --resumen
Uso: /root/vllm-env/bin/python scripts/benchmark_i6.py --model gemma-4-12b-qat-w4a16
"""
import argparse
import json
import sys
import time
from collections import Counter
from itertools import combinations
from pathlib import Path

import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import ablacion_gt as ab  # noqa: E402
from config.settings import DB_CONFIG  # noqa: E402
from src.etl.transformers import field_rules_v1 as fr  # noqa: E402
from src.etl.transformers.document_features_v2_llm_transformer import (  # noqa: E402
    DocumentFeaturesV2LLMTransformer)
from src.etl.transformers.extraction_postprocess import resolve  # noqa: E402

OUT = ROOT / "data/processed/llm_benchmark"
PL = OUT / "planillas"
MODELS_I6 = ["gemma-4-12b-qat-w4a16", "qwen3-14b-awq", "qwen3-8b-fp8", "gemma-4-26b-a4b-gguf"]
FIELDS = {
    "dias_pago_maximo": "formas y condiciones de pago",
    "dias_validez_garantia": "periodo de validez de la garantia de los bienes",
    "dias_pago_anticipo": "solicitud de pago de anticipo",
    "dias_solicitud_anticipo": "solicitud de pago de anticipo",
    "anos_experiencia_minima_tecnica": "capacidad tecnica",
    "pct_monto_referencia_tecnica": "capacidad tecnica",
}
MAX_CHARS = 20000  # ~6K tokens: entra en max_model_len 8192 con el prompt


def load_items():
    items = []
    for p in sorted((ROOT / "data/processed/alolab/gt_por_campo_v1").glob("*_GT*.csv")):
        g = pd.read_csv(p, dtype={"nro_licitacion": str})
        f = g.campo.iloc[0]
        if f in FIELDS:
            items += [dict(set="gt_v1", field=f, nro=r.nro_licitacion, text=str(r.text) if pd.notna(r.text) else "",
                           gt=ab.num(r.ground_truth)) for r in g.itertuples()]
    ps = OUT / "pseudo_gt_opus"
    ann = json.loads((ps / "anotaciones_v1.json").read_text(encoding="utf-8"))
    for r in pd.read_csv(ps / "muestra.csv", dtype={"nro_licitacion": str}).itertuples():
        a = ann.get(str(r.id))
        if r.campo in FIELDS and a is not None and a[1] != "ambiguo":
            items.append(dict(set="pseudo_v1", field=r.campo, nro=r.nro_licitacion, text=str(r.texto),
                              gt=None if a[0] is None else float(a[0]), id=r.id))
    cal = pd.read_csv(PL / "calibracion_v2.csv", dtype={"id": str}).set_index("id")
    op = pd.read_csv(PL / "calibracion_v2_opus.csv", dtype={"id": str}).set_index("id")
    key = pd.read_csv(PL / "calibracion_v2_clave.csv", dtype={"id": str, "nro_licitacion": str}).set_index("id")
    for i in cal.index:
        items.append(dict(set="calib_v2", field=cal.campo[i], nro=key.nro_licitacion[i], text=cal.texto[i],
                          gt=ab.num(op.valor[i]), id=i))
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    cur = psycopg2.connect(**cfg, options="-c default_transaction_read_only=on").cursor()
    for f, title in FIELDS.items():
        cur.execute("""SELECT r.nro_licitacion, string_agg(s.content_text, E'\\n' ORDER BY s.line_start)
            FROM dncp.spec_v1_rule_values r JOIN dncp.pliegos_secciones s
              ON s.nro_licitacion::text = r.nro_licitacion AND s.title_normalized = %s
            WHERE r.field = %s AND r.status = 'llm' AND r.ruleset = 'spec-v1' AND s.content_text IS NOT NULL
            GROUP BY 1""", (title, f))
        for nro, text in cur.fetchall():
            if fr.RULES_V1[f](text) is fr.NO_RULE:  # regla vigente (el staging puede ser de un código anterior)
                items.append(dict(set="residual", field=f, nro=nro, text=text, gt=None))
    for it in items:
        it["text"] = it["text"][:MAX_CHARS]
    return items


def run(model_name):
    cfg = ab.MODELS[model_name]
    fs = ab.field_schemas()
    items = load_items()
    print(f"{model_name}: {len(items)} ítems {dict(Counter(it['set'] for it in items))}", flush=True)
    tr = DocumentFeaturesV2LLMTransformer(str(ab.SCHEMAS), llm_provider=None)
    if cfg["backend"] == "vllm":
        from src.etl.transformers.llm_provider import VLLMOfflineProvider
        model = VLLMOfflineProvider(model_path=cfg["id"], quantization=None, max_model_len=cfg["max_len"],
                                    gpu_memory_utilization=cfg["gpu_mem"], dtype="bfloat16",
                                    guided_regex=ab.GUIDED["float"])
        model._lazy_init()

        def gen(msgs, t):
            model.guided_regex = ab.GUIDED[t]
            try:
                return model.generate_batch_messages(msgs, max_tokens=16)
            except Exception:  # noqa: BLE001
                out = []
                for m in msgs:
                    try:
                        out.append(model.generate_batch_messages([m], max_tokens=16)[0])
                    except Exception as e:  # noqa: BLE001
                        out.append(e)
                return out
    else:
        model = ab.HttpModel(cfg["gguf"], cfg["server_flags"], cfg.get("np", ab.NP))

        def gen(msgs, t):
            return model.generate(msgs, ab.GBNF[t])
    t0 = time.time()
    try:
        for f in FIELDS:
            t = ab.ftype(fs, f)
            sub = [it for it in items if it["field"] == f]
            raws = gen([tr._build_messages(fs[f], it["text"], None) for it in sub], t)
            for it, raw in zip(sub, raws):
                it["raw"] = "ERROR" if isinstance(raw, Exception) else raw
                rr = resolve(f, it["raw"], it["text"], mode="rule_first", ftype=t)
                it["regla_aplica"] = rr.rule_applies
                it["regla"] = None if rr.rule_value is None else float(rr.rule_value)
                it["llm"] = None if rr.llm_value is None else float(rr.llm_value)
                it["sistema"] = None if rr.value is None else float(rr.value)
                it["sistema_source"] = rr.source
            print(f"  {f}: {len(sub)} en {time.time() - t0:.0f}s "
                  f"(errores {sum(it['raw'] == 'ERROR' for it in sub)})", flush=True)
    finally:
        if cfg["backend"] == "http":
            model.close()
    meta = {"model": cfg.get("id") or cfg.get("gguf"), "backend": cfg["backend"], "spec": "v1.0",
            "elapsed_s": round(time.time() - t0, 1)}
    (OUT / f"I6_{model_name}.json").write_text(json.dumps(
        {**meta, "items": [{k: v for k, v in it.items() if k != "text"} for it in items]},
        ensure_ascii=False, indent=0), encoding="utf-8")
    print(f"-> I6_{model_name}.json ({meta['elapsed_s']}s)")


def _n(x):
    return None if x is None or (isinstance(x, float) and x != x) else x  # NaN de pandas = NULL


def ok(a, b, f):
    return ab.eq(_n(a), _n(b), f)


def resumen():
    data = {m: json.loads((OUT / f"I6_{m}.json").read_text(encoding="utf-8"))["items"]
            for m in MODELS_I6 if (OUT / f"I6_{m}.json").exists()}
    rows = []
    for m, its in data.items():
        df = pd.DataFrame(its)
        for (st, f), g in df[df.set != "residual"].groupby(["set", "field"]):
            vals = g["gt"].dropna()
            modal = vals.mode().iloc[0] if len(vals) else None
            nm = g[g["gt"].notna() & (g["gt"] != modal)]
            au = g[g["gt"].isna()]
            res = g[~g.regla_aplica]
            r = dict(modelo=m, conjunto=st, campo=f, n=len(g))
            for arm in ("regla", "llm", "sistema"):
                r[f"{arm}"] = round(sum(ok(a, b, f) for a, b in zip(g[arm], g["gt"])) / len(g), 3)
            r["llm_no_modal"] = f"{sum(ok(a, b, f) for a, b in zip(nm.llm, nm["gt"]))}/{len(nm)}"
            r["llm_ausencias"] = f"{sum(ok(a, b, f) for a, b in zip(au.llm, au["gt"]))}/{len(au)}"
            r["llm_en_residual"] = f"{sum(ok(a, b, f) for a, b in zip(res.llm, res["gt"]))}/{len(res)}"
            r["errores"] = int((g.raw == "ERROR").sum())
            rows.append(r)
    res = pd.DataFrame(rows)
    res.to_csv(OUT / "I6_resumen.csv", index=False)
    with pd.option_context("display.width", 250, "display.max_rows", 500):
        print(res.to_string(index=False))
        # crudo agregado por conjunto
        agg = res.assign(k=res.llm * res.n).groupby(["conjunto", "modelo"]).agg(n=("n", "sum"), k=("k", "sum"))
        agg["llm_crudo"] = (agg.k / agg.n).round(3)
        print("\nLLM crudo (sin reglas) por conjunto:\n", agg[["n", "llm_crudo"]].to_string())
    # residuales de producción: acuerdo entre modelos
    if len(data) >= 2:
        pred = {m: {(it["field"], it["nro"]): it["llm"] for it in its if it["set"] == "residual"}
                for m, its in data.items()}
        keys = set.intersection(*(set(p) for p in pred.values()))
        a0 = next(iter(pred))
        print(f"\nResiduales de producción (sin referencia): {len(keys)} casos campo-documento")
        for a, b in combinations(pred, 2):
            k = sum(ok(pred[a][x], pred[b][x], x[0]) for x in keys)
            print(f"  acuerdo {a} vs {b}: {k}/{len(keys)} = {k / len(keys):.3f}")
        allk = sum(all(ok(pred[m][x], pred[a0][x], x[0]) for m in pred) for x in keys)
        print(f"  los {len(pred)} modelos coinciden: {allk}/{len(keys)} = {allk / len(keys):.3f}")
        for m in pred:
            print(f"  {m}: NULL en {sum(pred[m][x] is None for x in keys)}/{len(keys)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", choices=MODELS_I6 + [m for m in ab.MODELS if m not in MODELS_I6])
    ap.add_argument("--resumen", action="store_true")
    a = ap.parse_args()
    if a.resumen:
        resumen()
    else:
        run(a.model)

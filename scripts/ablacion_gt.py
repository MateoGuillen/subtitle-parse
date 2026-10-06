"""F4.5/F6.1 — Ablación de 3 brazos contra GT humano en los 12 campos hybrid (auditoría H6.3).

Para cada campo hybrid con GT (gt_por_campo/*_GT.csv, 200 filas c/u) corre el modelo con el
prompt y guided de PRODUCCIÓN sobre el texto y mide contra la etiqueta humana:
  regla    resolve(mode="rule_only")   la regla sola (NULL donde no aplica)
  llm      resolve(mode="llm_only")    el modelo solo (sin reglas): mide al MODELO
  sistema  resolve(mode="rule_first")  lo que produce el pipeline de producción
Caveat obligatorio (H2.2): las reglas fueron desarrolladas sobre estos mismos archivos
GT -> el brazo "regla" (y por ende "sistema") es una cota OPTIMISTA in-sample; el brazo
"llm" es limpio (el modelo nunca vio estas etiquetas). La validación fuera de muestra de
las reglas es la anotación del estratificado-75 (F5).

Salida: data/processed/llm_benchmark/F4_ablacion_gt_<modelo>.json (+ .csv agregado)
Uso: /root/vllm-env/bin/python scripts/ablacion_gt.py --model qwen3-8b-fp8
"""
import argparse
import importlib.util
import json
import math
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.etl.transformers.document_features_v2_llm_transformer import (  # noqa: E402
    DocumentFeaturesV2LLMTransformer)
from src.etl.transformers.extraction_postprocess import resolve  # noqa: E402

GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
OUT = ROOT / "data/processed/llm_benchmark"
SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"

spec = importlib.util.spec_from_file_location("rf", ROOT / "scripts/run_llm_fallback.py")
rf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rf)


def num(v):
    try:
        x = float(str(v).strip().replace(",", "."))
        return None if math.isnan(x) else x
    except (ValueError, TypeError):
        return None


def eq(a, b, field):
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    return field.startswith("pct_") and (abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6)


def wilson(k, n, z=1.96):
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="qwen3-8b-fp8", choices=list(rf.MODELS))
    a = ap.parse_args()
    fschemas = rf.field_schemas()
    hybrid = sorted(f for f, s in fschemas.items() if s.get("extraction_method") == "hybrid")
    items = []
    for f in hybrid:
        cands = [p for p in GT_DIR.glob("*.csv") if not p.name.startswith("errores_")]
        path = next(p for p in cands if pd.read_csv(p, nrows=1)["campo"].iloc[0] == f)
        g = pd.read_csv(path)
        for r in g.itertuples():
            items.append(dict(field=f, nro=str(r.nro_licitacion), text=str(r.text),
                              gt=num(r.ground_truth)))
    print(f"{len(items)} ítems GT en {len(hybrid)} campos hybrid")

    mcfg = rf.MODELS[a.model]
    from src.etl.transformers.llm_provider import VLLMOfflineProvider
    provider = VLLMOfflineProvider(model_path=mcfg["model_id"], quantization=mcfg["quant"],
                                   max_model_len=mcfg["max_len"],
                                   gpu_memory_utilization=mcfg["gpu_mem"], dtype="bfloat16",
                                   guided_regex=rf.GUIDED["float"])
    tr = DocumentFeaturesV2LLMTransformer(str(SCHEMAS), provider)
    t0 = time.time()
    for f in hybrid:
        ftype = rf._ftype(fschemas, f)
        provider.guided_regex = rf.GUIDED[ftype]
        sub = [it for it in items if it["field"] == f]
        raws = rf._generate(provider, [tr._build_messages(fschemas[f], it["text"], None)
                                       for it in sub])
        for it, raw in zip(sub, raws):
            it["raw"] = "ERROR" if isinstance(raw, Exception) else raw
            for arm, mode in (("regla", "rule_only"), ("llm", "llm_only"), ("sistema", "rule_first")):
                res = resolve(f, it["raw"], it["text"], mode=mode, ftype=ftype)
                it[arm] = None if res.value is None else float(res.value)
                it[f"ok_{arm}"] = eq(it[arm], it["gt"], f)
            it["sistema_source"] = resolve(f, it["raw"], it["text"], mode="rule_first",
                                           ftype=ftype).source
        print(f"  {f}: {len(sub)} en {time.time()-t0:.0f}s", flush=True)
    del provider
    tag = a.model
    (OUT / f"F4_ablacion_gt_{tag}.json").write_text(
        json.dumps({"model": mcfg["model_id"], "items": [{k: v for k, v in it.items()
                                                          if k != "text"} for it in items]},
                   ensure_ascii=False, indent=0), encoding="utf-8")
    rows = []
    for f in hybrid:
        sub = [it for it in items if it["field"] == f]
        n = len(sub)
        r = dict(campo=f, n=n, gt_null=sum(it["gt"] is None for it in sub))
        for arm in ("regla", "llm", "sistema"):
            k = sum(it[f"ok_{arm}"] for it in sub)
            lo, hi = wilson(k, n)
            r[f"{arm}_k"], r[f"{arm}_p"], r[f"{arm}_lo"], r[f"{arm}_hi"] = k, round(k / n, 3), round(lo, 3), round(hi, 3)
        r["sistema_rule"] = sum(it["sistema_source"] == "rule" for it in sub)
        r["llm_corrige_regla"] = sum(it["ok_llm"] and not it["ok_regla"] for it in sub)
        r["regla_corrige_llm"] = sum(it["ok_regla"] and not it["ok_llm"] for it in sub)
        rows.append(r)
    df = pd.DataFrame(rows)
    df.to_csv(OUT / f"F4_ablacion_gt_{tag}.csv", index=False)
    print(df[["campo", "n", "gt_null", "regla_k", "llm_k", "sistema_k", "sistema_rule",
              "llm_corrige_regla", "regla_corrige_llm"]].to_string(index=False))
    tot = {arm: int(df[f"{arm}_k"].sum()) for arm in ("regla", "llm", "sistema")}
    print(f"TOTAL n={int(df.n.sum())}: {tot}")


if __name__ == "__main__":
    main()

"""F4.5/F6.1 — Ablación contra GT humano con protocolo ÚNICO para todos los modelos (auditoría H6.3).

Para cada campo (12 hybrid + pct_monto_referencia_tecnica) con GT (gt_por_campo, 200 filas c/u)
corre el modelo con el prompt de PRODUCCIÓN y decodificación restringida al mismo contrato
(número o NULL; int sin decimales) y mide contra la etiqueta humana:
  regla    resolve(mode="rule_only")   la regla sola (NULL donde no aplica)
  llm      resolve(mode="llm_only")    el modelo solo (sin reglas): mide al MODELO
  sistema  resolve(mode="rule_first")  lo que produce el pipeline de producción
Backends:
  vllm  -> guided regex (xgrammar), igual que producción
  http  -> llama-server (GGUF) con gramática GBNF equivalente al regex (mismo contrato)
Se guarda db_value para reconstruir el subconjunto C3 (GT∩regex con valor) de los 4 dias + pct.
Caveat obligatorio (H2.2): reglas/regex se desarrollaron sobre estos GT -> cotas in-sample; el
brazo "llm" es limpio.

Salida: data/processed/llm_benchmark/F4_ablacion_gt_<modelo>.json (+ .csv agregado)
Uso: /root/vllm-env/bin/python scripts/ablacion_gt.py --model qwen3-8b-fp8
"""
import argparse
import json
import math
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.etl.transformers.document_features_v2_llm_transformer import (  # noqa: E402
    DocumentFeaturesV2LLMTransformer)
from src.etl.transformers.extraction_postprocess import resolve  # noqa: E402

GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
OUT = ROOT / "data/processed/llm_benchmark"
SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
EXTRA_FIELDS = ["pct_monto_referencia_tecnica"]
GUIDED = {"int": r"^(\d{1,4}|NULL)$", "float": r"^(-?\d+(?:[.,]\d+)?|NULL)$"}
GBNF = {"int": 'root ::= "NULL" | [0-9] | [0-9][0-9] | [0-9][0-9][0-9] | [0-9][0-9][0-9][0-9]',
        "float": 'root ::= "NULL" | "-"? [0-9]+ ([.,] [0-9]+)?'}
LLAMA_SERVER = "/root/llama.cpp-src/build/bin/llama-server"
MODELS = {
    # vLLM (mismas configs que el benchmark original; max_len ampliado donde entra)
    "qwen3-8b-fp8": dict(backend="vllm", id="Qwen/Qwen3-8B-FP8", gpu_mem=0.80, max_len=8192),
    "llama-fp8": dict(backend="vllm", id="RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8",
                      gpu_mem=0.70, max_len=8192),
    "exaone-3.5-7.8b-awq": dict(backend="vllm", id="LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct-AWQ",
                                gpu_mem=0.70, max_len=8192),
    "qwen3-14b-awq": dict(backend="vllm", id="Qwen/Qwen3-14B-AWQ", gpu_mem=0.92, max_len=8192),
    # GGUF vía llama-server (flags del benchmark original, docs/reporte_benchmark_completo.md)
    "gemma-4-12b-gguf": dict(backend="http", gguf="models/gguf/gemma-4-12B-it-qat-UD-Q4_K_XL.gguf",
                             server_flags=["--reasoning", "off"], extra={}),
    "ministral-3-14b-gguf": dict(backend="http",
                                 gguf="models/gguf/Ministral-3-14B-Instruct-2512-Q4_K_M.gguf",
                                 server_flags=[], extra={}),
    # 27B: FP8 (30,9 GB) y AWQ (21 GB) no entran en 16 GB; UD-Q3_K_XL (13,15 GB) sí, con
    # KV en q8 y 2 slots de 8K. Misma cuantización que la corrida de agosto (97/100 GT-100).
    # ---- Ronda 2 (sesión 24): candidatos 2025-2026 que entran en 16 GB (scripts/descargar_candidatos.py)
    "gemma-4-12b-qat-w4a16": dict(backend="vllm", id="google/gemma-4-12B-it-qat-w4a16-ct",
                                  gpu_mem=0.85, max_len=8192),
    "qwen3.5-9b-fp8": dict(backend="vllm", id="RedHatAI/Qwen3.5-9B-FP8-dynamic",
                           gpu_mem=0.85, max_len=8192),
    "qwen3.5-9b-gguf": dict(backend="http", gguf="/root/models/gguf/Qwen3.5-9B-Q8_0.gguf",
                            server_flags=["--reasoning", "off"], extra={}),
    "ministral-3-8b-gguf": dict(backend="http", gguf="/root/models/gguf/Ministral-3-8B-Instruct-2512-Q8_0.gguf",
                                server_flags=[], extra={}),
    "ling-3.0-tiny-gguf": dict(backend="http", gguf="/root/models/gguf/Ling-3.0-tiny-Q8_0.gguf",
                               server_flags=["--reasoning", "off"], extra={}),
    "mimo-v2.6-9b-gguf": dict(backend="http", gguf="/root/models/gguf/MiMo-V2.6-Distill-Qwen-9B-Q8_0.gguf",
                              server_flags=["--reasoning", "off"], extra={}),
    "clef-flash-gguf": dict(backend="http", gguf="/root/models/gguf/Clef-Flash-Q8_0.gguf",
                            server_flags=["--reasoning", "off"], extra={}),
    "gemma-4-26b-a4b-gguf": dict(backend="http", gguf="/root/models/gguf/gemma-4-26B-A4B-it-qat-UD-Q4_K_XL.gguf",
                                 server_flags=["--reasoning", "off", "--cache-type-k", "q8_0",
                                               "--cache-type-v", "q8_0"], np=2, extra={}),
    "qwen3.6-35b-a3b-gguf": dict(backend="http", gguf="/root/models/gguf/Qwen3.6-35B-A3B-UD-IQ3_XXS.gguf",
                                 server_flags=["--reasoning", "off", "--cache-type-k", "q8_0",
                                               "--cache-type-v", "q8_0"], np=2, extra={}),
    "qwen3.8-27b-gguf": dict(backend="http", gguf="models/gguf/Qwen3.8-27B-UD-Q3_K_XL.gguf",
                             server_flags=["--reasoning", "off", "--cache-type-k", "q8_0",
                                           "--cache-type-v", "q8_0"], np=2, extra={}),
}
NP = 4  # slots llama-server


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


def field_schemas():
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    return {f["name"]: f for k, v in s.items() if k != "meta" for f in v["fields"]}


def ftype(fs, f):
    return "int" if fs[f]["type"] in ("int", "binary") else "float"


class HttpModel:
    """llama-server con NP slots; gramática GBNF por tipo (mismo contrato que el guided)."""

    def __init__(self, gguf, flags, np_=NP):
        self.np = np_
        self.proc = subprocess.Popen(
            [LLAMA_SERVER, "-m", str(ROOT / gguf), "--port", "1234", "-ngl", "999",
             "--ctx-size", str(8192 * np_), "-np", str(np_), "--jinja", *flags],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(240):
            try:
                if requests.get("http://127.0.0.1:1234/health", timeout=2).status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(2)
        raise RuntimeError("llama-server no arrancó")

    def generate(self, msgs_list, grammar):
        def one(m):
            try:
                r = requests.post("http://127.0.0.1:1234/v1/chat/completions", timeout=300, json={
                    "messages": m, "temperature": 0.0, "max_tokens": 16, "grammar": grammar})
                r.raise_for_status()
                return (r.json()["choices"][0]["message"].get("content") or "").strip()
            except Exception as e:  # noqa: BLE001
                return e
        with ThreadPoolExecutor(self.np) as ex:
            return list(ex.map(one, msgs_list))

    def close(self):
        self.proc.terminate()
        self.proc.wait(timeout=60)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, choices=list(MODELS))
    a = ap.parse_args()
    cfg = MODELS[a.model]
    fs = field_schemas()
    fields = sorted(f for f, s in fs.items() if s.get("extraction_method") == "hybrid") + EXTRA_FIELDS
    items = []
    cands = [p for p in GT_DIR.glob("*.csv") if not p.name.startswith("errores_")]
    for f in fields:
        path = next(p for p in cands if pd.read_csv(p, nrows=1)["campo"].iloc[0] == f)
        for r in pd.read_csv(path).itertuples():
            items.append(dict(field=f, nro=str(r.nro_licitacion), text=str(r.text),
                              gt=num(r.ground_truth), db=num(r.db_value)))
    print(f"{a.model}: {len(items)} ítems GT en {len(fields)} campos", flush=True)

    tr = DocumentFeaturesV2LLMTransformer(str(SCHEMAS), llm_provider=None)
    if cfg["backend"] == "vllm":
        from src.etl.transformers.llm_provider import VLLMOfflineProvider
        model = VLLMOfflineProvider(model_path=cfg["id"], quantization=None,
                                    max_model_len=cfg["max_len"], gpu_memory_utilization=cfg["gpu_mem"],
                                    dtype="bfloat16", guided_regex=GUIDED["float"])
        # Cargar ANTES del bucle: si el modelo no entra (OOM), abortar aquí. Sin esto, el
        # reintento ítem por ítem de gen() recargaba el modelo en cada ítem (bug visto con
        # Qwen3.5-9B-FP8: 26 recargas fallidas).
        model._lazy_init()

        def gen(msgs, t):
            model.guided_regex = GUIDED[t]
            try:
                return model.generate_batch_messages(msgs, max_tokens=16)
            except Exception:
                out = []
                for m in msgs:
                    try:
                        out.append(model.generate_batch_messages([m], max_tokens=16)[0])
                    except Exception as e:  # noqa: BLE001
                        out.append(e)
                return out
    else:
        model = HttpModel(cfg["gguf"], cfg["server_flags"], cfg.get("np", NP))

        def gen(msgs, t):
            return model.generate(msgs, GBNF[t])

    t0 = time.time()
    try:
        for f in fields:
            t = ftype(fs, f)
            sub = [it for it in items if it["field"] == f]
            raws = gen([tr._build_messages(fs[f], it["text"], None) for it in sub], t)
            for it, raw in zip(sub, raws):
                it["raw"] = "ERROR" if isinstance(raw, Exception) else raw
                for arm, mode in (("regla", "rule_only"), ("llm", "llm_only"),
                                  ("sistema", "rule_first")):
                    res = resolve(f, it["raw"], it["text"], mode=mode, ftype=t)
                    it[arm] = None if res.value is None else float(res.value)
                    it[f"ok_{arm}"] = eq(it[arm], it["gt"], f)
                it["sistema_source"] = resolve(f, it["raw"], it["text"], mode="rule_first",
                                               ftype=t).source
            print(f"  {f}: {len(sub)} en {time.time()-t0:.0f}s "
                  f"(errores {sum(it['raw'] == 'ERROR' for it in sub)})", flush=True)
    finally:
        if cfg["backend"] == "http":
            model.close()
    meta = {"model": cfg.get("id") or cfg.get("gguf"), "backend": cfg["backend"],
            "guided": "xgrammar regex" if cfg["backend"] == "vllm" else "llama-server GBNF",
            "elapsed_s": round(time.time() - t0, 1)}
    (OUT / f"F4_ablacion_gt_{a.model}.json").write_text(json.dumps(
        {**meta, "items": [{k: v for k, v in it.items() if k != "text"} for it in items]},
        ensure_ascii=False, indent=0), encoding="utf-8")
    rows = []
    for f in fields:
        sub = [it for it in items if it["field"] == f]
        r = dict(campo=f, n=len(sub), gt_null=sum(it["gt"] is None for it in sub),
                 errores=sum(it["raw"] == "ERROR" for it in sub))
        for arm in ("regla", "llm", "sistema"):
            k = sum(it[f"ok_{arm}"] for it in sub)
            lo, hi = wilson(k, len(sub))
            r.update({f"{arm}_k": k, f"{arm}_p": round(k / len(sub), 3), f"{arm}_lo": round(lo, 3),
                      f"{arm}_hi": round(hi, 3)})
        rows.append(r)
    df = pd.DataFrame(rows)
    df.to_csv(OUT / f"F4_ablacion_gt_{a.model}.csv", index=False)
    print(df[["campo", "n", "errores", "regla_k", "llm_k", "sistema_k"]].to_string(index=False))


if __name__ == "__main__":
    main()

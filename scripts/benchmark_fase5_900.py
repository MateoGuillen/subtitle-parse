"""
Fase 5: 900 pseudo-GT (5 campos x 180 docs) en los 10 modelos descargados.
Pseudo-truth = valor regex de produccion (conf>=0.9, no nulo, !=0) en dncp.document_features_v2.
Metrica honesta: agreement con el regex + latencia P50/P95 (NO accuracy humana).

Usage:
  python scripts/benchmark_fase5_900.py --build-samples          # congela el set (1 vez)
  python scripts/benchmark_fase5_900.py --backend vllm --idx 0   # un modelo vLLM
  python scripts/benchmark_fase5_900.py --backend http --name X --gguf f.gguf --out Fase5_X.json
"""
import sys
import json
import time
import argparse
import importlib.util
import statistics
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

GT5 = Path("data/processed/llm_benchmark/ground_truth_5_campos.csv")
SAMPLES = Path("data/processed/llm_benchmark/pseudo900.json")
OUT = Path("data/processed/llm_benchmark/multimodel_results")
OUT.mkdir(parents=True, exist_ok=True)

FIELDS = ["dias_validez_garantia", "pct_monto_referencia_tecnica",
          "anos_experiencia_minima_tecnica", "dias_pago_anticipo",
          "dias_solicitud_anticipo"]
HYBRID = {f for f in FIELDS if f != "pct_monto_referencia_tecnica"}
PER_FIELD = 180
# pct_monto_referencia_tecnica es raro en produccion (solo ~147 docs con valor):
# se toman todos los disponibles (take-all, minimo 100).
TAKE_ALL_MIN = {"pct_monto_referencia_tecnica": 100}
SEED = 42


def _load_mod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build_samples():
    import pandas as pd
    from config.settings import DB_CONFIG
    import psycopg2
    mm = _load_mod("scripts/benchmark_multi_vs_gt.py", "mmgt")

    gt = pd.read_csv(GT5, encoding="utf-8-sig")
    excluded = {(str(int(float(r["nro_licitacion"]))), r["campo"]) for _, r in gt.iterrows()}

    cfg = DB_CONFIG.copy()
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    cur.execute("SELECT setseed(%s)", (SEED / 100.0,))

    samples = []
    for field in FIELDS:
        cond = f'"{field}" IS NOT NULL AND "{field}" <> 0'
        if field in HYBRID:
            cond += f' AND "{field}_conf" >= 0.9'
        cur.execute(
            f'SELECT nro_licitacion, "{field}" FROM dncp.document_features_v2'
            f" WHERE {cond} ORDER BY RANDOM() LIMIT 600")
        rows = [(str(r[0]), float(r[1])) for r in cur.fetchall()
                if (str(r[0]), field) not in excluded]
        want = PER_FIELD if field not in TAKE_ALL_MIN else len(rows)
        picked = rows[:want]
        print(f"  {field}: {len(picked)} docs (candidatos {len(rows)})")
        if field in TAKE_ALL_MIN:
            assert len(picked) >= TAKE_ALL_MIN[field], f"muy pocas para {field}"
        else:
            assert len(picked) == PER_FIELD, f"sin suficientes para {field}"
        title = mm.TITLE_MAP[field].replace("_", " ")
        nros = [n for n, _ in picked]
        ph = ",".join("%s" for _ in nros)
        cur.execute(
            "SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones"
            f" WHERE nro_licitacion IN ({ph}) AND title_normalized = %s",
            nros + [title])
        texts = {str(r[0]): r[1] or "" for r in cur.fetchall()}
        missing = [n for n in nros if not texts.get(n)]
        if missing:
            print(f"    WARN {len(missing)} sin texto en '{title}', se reemplazan")
            cur.execute(
                f'SELECT nro_licitacion, "{field}" FROM dncp.document_features_v2'
                f" WHERE {cond} ORDER BY RANDOM() LIMIT 600")
            extra = [(str(r[0]), float(r[1])) for r in cur.fetchall()
                     if (str(r[0]), field) not in excluded
                     and str(r[0]) not in {n for n, _ in picked}]
            repl = 0
            for n, v in extra:
                if repl >= len(missing):
                    break
                cur.execute(
                    "SELECT content_text FROM dncp.pliegos_secciones"
                    " WHERE nro_licitacion = %s AND title_normalized = %s",
                    (n, title))
                t = cur.fetchone()
                if t and t[0]:
                    idx = nros.index(missing[repl])
                    picked[idx] = (n, v)
                    texts[n] = t[0]
                    repl += 1
            print(f"    reemplazados {repl}/{len(missing)}")
        for n, v in picked:
            samples.append({"nro": n, "field": field, "pseudo": v,
                            "text": texts.get(n, "")})
    conn.close()
    with open(SAMPLES, "w", encoding="utf-8") as f:
        json.dump({"seed": SEED, "per_field": PER_FIELD,
                   "note": "pseudo-truth=regex prod conf>=0.9, excluye 100 GT humanas",
                   "samples": samples}, f, ensure_ascii=False)
    print(f"Guardado {SAMPLES} ({len(samples)} muestras)")


def vram_peak():
    try:
        out = subprocess.run(["nvidia-smi", "--query-gpu=memory.used",
                              "--format=csv,nounits"],
                             capture_output=True, text=True, timeout=10).stdout
        return int(out.strip().split("\n")[-1].split()[0])
    except Exception:
        return None


def summarize(results):
    by_field = {}
    for r in results:
        by_field.setdefault(r["field"], []).append(r["agree"])
    for fld in FIELDS:
        vals = by_field.get(fld, [])
        if vals:
            print(f"    -> {fld}: {sum(vals)}/{len(vals)} = {sum(vals)/len(vals)*100:.1f}%")
    lats = sorted(r["lat"] for r in results)
    p50 = lats[len(lats) // 2] if lats else 0
    p95 = lats[int(len(lats) * 0.95)] if lats else 0
    ok = sum(1 for r in results if r["agree"])
    print(f"  TOTAL agree: {ok}/{len(results)} = {ok/len(results)*100:.1f}%  "
          f"P50={p50:.2f}s P95={p95:.2f}s")


def run_vllm(idx):
    from vllm import LLM, SamplingParams
    from vllm.sampling_params import StructuredOutputsParams
    from transformers import AutoTokenizer
    mm = _load_mod("scripts/benchmark_multi_vs_gt.py", "mmgt")
    cfg = mm.MODELS[idx]
    print(f"Modelo vLLM: {cfg['name']}")

    with open(SAMPLES, encoding="utf-8") as f:
        samples = json.load(f)["samples"]

    hf_tok = AutoTokenizer.from_pretrained(cfg.get("tokenizer_path", cfg["path"]),
                                           trust_remote_code=True)
    llm_kwargs = dict(model=cfg["path"], max_model_len=cfg["max_len"],
                      max_num_seqs=cfg.get("max_seqs", 4 if cfg["bf16"] else 8),
                      gpu_memory_utilization=cfg["gpu_mem"],
                      enforce_eager=True, trust_remote_code=True)
    if cfg.get("cpu_offload"):
        llm_kwargs["cpu_offload_gb"] = cfg["cpu_offload"]
    if cfg["quant"] == "gguf":
        llm_kwargs["dtype"] = "float16"
        if cfg.get("tokenizer_path"):
            llm_kwargs["tokenizer"] = cfg["tokenizer_path"]
    elif cfg["quant"]:
        llm_kwargs["quantization"] = cfg["quant"]
        llm_kwargs["dtype"] = "float16"
    else:
        llm_kwargs["dtype"] = "bfloat16"
    llm = LLM(**llm_kwargs)
    print(f"  VRAM peak post-carga: {vram_peak()} MiB")
    sp = SamplingParams(max_tokens=64, temperature=0.0,
                        structured_outputs=StructuredOutputsParams(
                            regex=r'^(-?\d+(?:[.,]\d+)?|NULL)$'))

    safe = cfg["name"].replace(" ", "_").replace(".", "")
    out_file = OUT / f"Fase5_{safe}.json"
    results, done = [], set()
    if out_file.exists():
        try:
            prev = json.load(open(out_file, encoding="utf-8"))
            results = prev.get("results", [])
            done = {(r["nro"], r["field"]) for r in results}
            print(f"  [resume] {len(results)} muestras")
        except (ValueError, KeyError):
            pass

    t0 = time.time()
    for s in samples:
        if (s["nro"], s["field"]) in done:
            continue
        msgs = mm.build_prompt(s["field"], s["text"], cfg["merge_system"])
        tmpl = {"enable_thinking": False} if cfg["thinking_fix"] else {}
        rendered = hf_tok.apply_chat_template(msgs, tokenize=False,
                                              add_generation_prompt=True, **tmpl)
        t1 = time.time()
        try:
            out = llm.generate([rendered], sp)[0].outputs[0].text.strip()
        except Exception as e:  # ctx overflow u otro: cuenta como desacuerdo
            out = f"ERROR:{type(e).__name__}"
        lat = time.time() - t1
        parsed = mm.parse(out, field=s["field"], text=s["text"])
        pv = s["pseudo"]
        agree = (parsed == pv) or (parsed is not None and abs(parsed - pv) < 1e-9)
        results.append({"nro": s["nro"], "field": s["field"], "pseudo": pv,
                        "llm_raw": out[:80], "llm_value": parsed,
                        "agree": agree, "lat": round(lat, 3)})
        if len(results) % 25 == 0:
            json.dump({"model": cfg["name"], "results": results,
                       "elapsed": time.time() - t0},
                      open(out_file, "w", encoding="utf-8"),
                      ensure_ascii=False, indent=1)
            print(f"    {len(results)}/{len(samples)}...", flush=True)
    json.dump({"model": cfg["name"], "results": results,
               "elapsed": time.time() - t0},
              open(out_file, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"  [{cfg['name']}] ({time.time()-t0:.0f}s)")
    summarize(results)


def run_http(name, out_name, extra):
    import requests
    gh = _load_mod("scripts/benchmark_gguf_http.py", "ggufh")
    print(f"Modelo HTTP: {name}")
    with open(SAMPLES, encoding="utf-8") as f:
        samples = json.load(f)["samples"]
    out_file = OUT / out_name
    results, done = [], set()
    if out_file.exists():
        try:
            prev = json.load(open(out_file, encoding="utf-8"))
            results = prev.get("results", [])
            done = {(r["nro"], r["field"]) for r in results}
            print(f"  [resume] {len(results)} muestras")
        except (ValueError, KeyError):
            pass
    t0 = time.time()
    for s in samples:
        if (s["nro"], s["field"]) in done:
            continue
        t1 = time.time()
        try:
            raw = gh.call_llm(gh.build_prompt(s["field"], s["text"]), extra,
                              max_tokens=64)
        except Exception as e:
            raw = f"ERROR:{type(e).__name__}"
        lat = time.time() - t1
        parsed = gh.parse_response(raw, field=s["field"], text=s["text"],
                                   strict=True)
        pv = s["pseudo"]
        agree = (parsed == pv) or (parsed is not None and abs(parsed - pv) < 1e-9)
        results.append({"nro": s["nro"], "field": s["field"], "pseudo": pv,
                        "llm_raw": raw[:80], "llm_value": parsed,
                        "agree": agree, "lat": round(lat, 3)})
        if len(results) % 25 == 0:
            json.dump({"model": name, "results": results,
                       "elapsed": time.time() - t0},
                      open(out_file, "w", encoding="utf-8"),
                      ensure_ascii=False, indent=1)
            print(f"    {len(results)}/{len(samples)}...", flush=True)
    json.dump({"model": name, "results": results,
               "elapsed": time.time() - t0},
              open(out_file, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"  [{name}] ({time.time()-t0:.0f}s)")
    summarize(results)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-samples", action="store_true")
    ap.add_argument("--backend", choices=["vllm", "http"], default="vllm")
    ap.add_argument("--idx", type=int, default=0)
    ap.add_argument("--name", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--extra-params", default="{}")
    args = ap.parse_args()
    if args.build_samples:
        build_samples()
    elif args.backend == "vllm":
        run_vllm(args.idx)
    else:
        run_http(args.name, args.out, json.loads(args.extra_params))


if __name__ == "__main__":
    main()

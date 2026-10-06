"""Concurrencia online: sirve un modelo (vLLM serve o servidor existente) y lo
ataca con C hilos paralelos. Mide latencia P50/P95, req/s, tok/s, errores,
agreement y pico de VRAM/util GPU por nivel.

Uso vLLM (levanta `vllm serve` solo con los flags del manifest):
  /root/vllm-env/bin/python scripts/benchmark_concurrency.py --backend vllm --idx 3 \\
      --levels 1,4,8,16,32 --prompts 64 --out Conc_Llama_FP8.json

Uso HTTP (servidor ya corriendo, p.ej. llama-server en :1234):
  /root/vllm-env/bin/python scripts/benchmark_concurrency.py --backend http \\
      --endpoint http://127.0.0.1:1234 --name Granite --levels 1,2,4,8 \\
      --prompts 32 --out Conc_Granite.json
"""
import sys
import json
import time
import argparse
import importlib.util
import statistics
import subprocess
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
OUT = ROOT / "data" / "processed" / "llm_benchmark" / "multimodel_results"
SAMPLES = ROOT / "data" / "processed" / "llm_benchmark" / "pseudo900.json"
GUIDED_RE = r'^(-?\d+(?:[.,]\d+)?|NULL)$'


def _load_mod(path, name):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def gpu_sample():
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.used,utilization.gpu",
             "--format=csv,nounits,noheader"],
            capture_output=True, text=True, timeout=10).stdout.strip()
        mem, util = out.split(",")
        return int(mem.strip()), int(util.strip())
    except Exception:
        return None, None


class Sampler(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.stop_ev = threading.Event()
        self.peak_mem, self.peak_util = 0, 0

    def run(self):
        while not self.stop_ev.wait(0.5):
            m, u = gpu_sample()
            if m is not None:
                self.peak_mem = max(self.peak_mem, m)
                self.peak_util = max(self.peak_util, u)

    def stop(self):
        self.stop_ev.set()
        self.join(timeout=5)
        return self.peak_mem, self.peak_util


def build_workset(n):
    import random
    samples = json.load(open(SAMPLES, encoding="utf-8"))["samples"]
    by_field = {}
    for s in samples:
        by_field.setdefault(s["field"], []).append(s)
    rng = random.Random(1234)
    work = []
    fields = sorted(by_field)
    per, rem = divmod(n, len(fields))
    for i, f in enumerate(fields):
        pool = by_field[f][:]
        rng.shuffle(pool)
        work += pool[:per + (1 if i < rem else 0)]
    rng.shuffle(work)
    return work


def wait_ready(endpoint, timeout=900):
    import requests
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = requests.get(endpoint + "/v1/models", timeout=10)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


def start_vllm_serve(cfg, port, max_seqs):
    cmd = [sys.executable, "-m", "vllm.entrypoints.openai.api_server",
           "--model", cfg["path"], "--host", "127.0.0.1", "--port", str(port),
           "--gpu-memory-utilization", str(cfg["gpu_mem"]),
           "--max-model-len", str(cfg["max_len"]),
           "--max-num-seqs", str(max_seqs),
           "--enforce-eager", "--trust-remote-code"]
    if cfg.get("cpu_offload"):
        cmd += ["--cpu-offload-gb", str(cfg["cpu_offload"])]
    if cfg["quant"] == "gguf":
        cmd += ["--dtype", "float16"]
        if cfg.get("tokenizer_path"):
            cmd += ["--tokenizer", cfg["tokenizer_path"]]
    elif cfg["quant"]:
        cmd += ["--quantization", cfg["quant"], "--dtype", "float16"]
    else:
        cmd += ["--dtype", "bfloat16"]
    log = open(f"/tmp/conc_serve_{port}.log", "w")
    import os
    env = dict(os.environ)
    # el Small-24B tarda >600s en cargar shards+cuantizar: no abortar el arranque
    env.setdefault("VLLM_ENGINE_READY_TIMEOUT_S", "2400")
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
    return proc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=["vllm", "http"], required=True)
    ap.add_argument("--idx", type=int, default=0)
    ap.add_argument("--endpoint", default="http://127.0.0.1:1234")
    ap.add_argument("--name", default="")
    ap.add_argument("--levels", default="1,4,8,16,32")
    ap.add_argument("--prompts", type=int, default=64)
    ap.add_argument("--max-seqs", type=int, default=0)
    ap.add_argument("--timeout", type=int, default=300)
    ap.add_argument("--truncate-chars", type=int, default=0,
                    help="recorta el texto de la sección a N caracteres "
                         "(para servidores con slot_ctx chico)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    import requests
    mm = _load_mod("scripts/benchmark_multi_vs_gt.py", "mmgt")
    levels = [int(x) for x in args.levels.split(",")]
    work = build_workset(args.prompts)
    print(f"workset: {len(work)} prompts")

    proc = None
    guided = True
    render_tok = None
    merge_system = False
    thinking_fix = False
    serve_model = None  # id de modelo que espera el servidor (vLLM sirve el path HF)
    if args.backend == "vllm":
        cfg = mm.MODELS[args.idx]
        model_name = cfg["name"]
        serve_model = cfg["path"]
        merge_system = cfg["merge_system"]
        thinking_fix = cfg["thinking_fix"]
        max_seqs = args.max_seqs or max(max(levels), cfg.get("max_seqs", 8))
        proc = start_vllm_serve(cfg, 8000, max_seqs)
        endpoint = "http://127.0.0.1:8000"
        print(f"sirviendo {model_name} (max_seqs={max_seqs})...")
        assert wait_ready(endpoint), "serve no levantó (ver /tmp/conc_serve_8000.log)"
        from transformers import AutoTokenizer
        render_tok = AutoTokenizer.from_pretrained(
            cfg.get("tokenizer_path", cfg["path"]), trust_remote_code=True)
    else:
        model_name = args.name or "http-model"
        endpoint = args.endpoint.rstrip("/")
        guided = False  # llama-server: sin guided fiable -> plain + parse
        print(f"usando servidor existente {endpoint} (sin guided)")

    # pre-render de payloads
    payloads = []
    ntrunc = 0
    for s in work:
        text = s["text"]
        if args.truncate_chars and len(text) > args.truncate_chars:
            text = text[:args.truncate_chars]
            ntrunc += 1
        if render_tok is not None:
            msgs = mm.build_prompt(s["field"], text, merge_system)
            prompt = render_tok.apply_chat_template(
                msgs, tokenize=False, add_generation_prompt=True, **tmpl)
            body = {"model": serve_model or model_name, "prompt": prompt,
                    "max_tokens": 64,
                    "temperature": 0.0,
                    "extra_body": {"guided_regex": GUIDED_RE}} if guided else None
            url = endpoint + "/v1/completions"
            if body is None:  # pragma: no cover
                raise AssertionError
        else:
            gh = _load_mod("scripts/benchmark_gguf_http.py", "ggufh")
            msgs = gh.build_prompt(s["field"], text)
            body = {"model": model_name, "messages": msgs, "max_tokens": 64,
                    "temperature": 0.0}
            url = endpoint + "/v1/chat/completions"
        payloads.append((url, body, s))
    if ntrunc:
        print(f"truncados a {args.truncate_chars} chars: {ntrunc}/{len(work)}")

    tls = threading.local()

    def one(job):
        url, body, s = job
        if not hasattr(tls, "sess"):
            tls.sess = requests.Session()
        t1 = time.time()
        try:
            r = tls.sess.post(url, json=body, timeout=args.timeout)
            r.raise_for_status()
            d = r.json()
            ch = d["choices"][0]
            raw = (ch.get("text") if "text" in ch
                   else ch["message"]["content"]) or ""
            comp_tok = (d.get("usage") or {}).get("completion_tokens", 0)
            return {"ok": True, "lat": time.time() - t1, "raw": raw.strip(),
                    "ctok": comp_tok, "field": s["field"],
                    "pseudo": s["pseudo"], "text": s["text"]}
        except Exception as e:
            return {"ok": False, "lat": time.time() - t1,
                    "raw": f"ERROR:{type(e).__name__}", "ctok": 0,
                    "field": s["field"], "pseudo": s["pseudo"],
                    "text": s.get("text", "")}

    out_file = OUT / args.out
    all_res = {}
    try:
        for C in levels:
            samp = Sampler()
            samp.start()
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=C) as ex:
                res = list(ex.map(one, payloads))
            wall = time.time() - t0
            peak_mem, peak_util = samp.stop()
            lats = sorted(r["lat"] for r in res if r["ok"])
            errs = sum(1 for r in res if not r["ok"])
            ok_lats = lats or [0]
            ctoks = sum(r["ctok"] for r in res if r["ok"])
            # agreement (secundario bajo concurrencia)
            agr = 0
            for r in res:
                if not r["ok"]:
                    continue
                try:
                    pv = (mm.parse(r["raw"], field=r["field"], text=r["text"])
                          if render_tok is not None else None)
                    if pv is None:
                        gh2 = _load_mod("scripts/benchmark_gguf_http.py", "ggufh2")
                        pv = gh2.parse_response(r["raw"], field=r["field"],
                                                text=r["text"], strict=True)
                    if (pv == r["pseudo"]
                            or (pv is not None
                                and abs(pv - r["pseudo"]) < 1e-9)):
                        agr += 1
                except Exception:
                    pass
            all_res[str(C)] = {
                "n": len(res), "errors": errs,
                "agree": agr,
                "wall_s": round(wall, 1),
                "req_per_s": round(len(res) / wall, 2),
                "out_tok_per_s": round(ctoks / wall, 1),
                "p50_s": round(ok_lats[len(ok_lats) // 2], 2),
                "p95_s": round(ok_lats[int(len(ok_lats) * 0.95)], 2),
                "mean_s": round(statistics.mean(ok_lats), 2),
                "vram_peak_mib": peak_mem, "gpu_util_peak": peak_util,
            }
            print(f"  C={C}: req/s={len(res)/wall:.2f} tok/s={ctoks/wall:.1f} "
                  f"P50={ok_lats[len(ok_lats)//2]:.2f}s P95="
                  f"{ok_lats[int(len(ok_lats)*0.95)]:.2f}s err={errs} "
                  f"agree={agr}/{len(res)} VRAM={peak_mem}MiB U={peak_util}%",
                  flush=True)
            json.dump({"model": model_name, "backend": args.backend,
                       "prompts": len(work), "levels": all_res},
                      open(out_file, "w", encoding="utf-8"),
                      ensure_ascii=False, indent=1)
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=120)
            except Exception:
                proc.kill()
    print(f"OK -> {out_file}")


if __name__ == "__main__":
    main()

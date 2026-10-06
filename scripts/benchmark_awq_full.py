"""Full AWQ benchmark: test all top 4 models with awq_marlin, 60 samples each."""
import os, sys, time, json, random, gc, traceback

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
# HF_TOKEN se lee del entorno (.env vía config.settings); nunca hardcodear tokens

from vllm import LLM, SamplingParams
from transformers import AutoTokenizer
from config.settings import DB_CONFIG
import psycopg2, psycopg2.extras

TITLES = [
    "Fraude Y Corrupcion", "Confidencialidad De La Informacion", "Fuerza Mayor",
    "Aclaracion De Las Ofertas", "Interpretacion", "Formato Y Firma De La Oferta",
]
TITLE_MAP = {t: t.lower() for t in TITLES}
N_PER_TITLE = 10

SYSTEM_PROMPT = (
    "Eres un extractor de datos estructurados. Analiza el texto de la seccion "
    "del pliego de licitacion y extrae los campos indicados en el schema. "
    "Responde UNICAMENTE con un JSON valido con la estructura {\"campos\": {...}}."
)

MODELS = [
    {
        "name": "Mistral 7B v0.3 AWQ",
        "repo": "solidrust/Mistral-7B-Instruct-v0.3-AWQ",
        "quant": "awq_marlin",
        "needs_system_merge": True,
        "needs_thinking_fix": False,
    },
    {
        "name": "Qwen3-8B AWQ",
        "repo": "Qwen/Qwen3-8B-AWQ",
        "quant": "awq",
        "needs_system_merge": False,
        "needs_thinking_fix": True,
    },
    {
        "name": "Phi-4 14B AWQ",
        "repo": "curiousmind147/microsoft-phi-4-AWQ-4bit-GEMM",
        "quant": "awq",
        "needs_system_merge": False,
        "needs_thinking_fix": False,
    },
    {
        "name": "Qwen3-14B AWQ",
        "repo": "Qwen/Qwen3-14B-AWQ",
        "quant": "awq",
        "needs_system_merge": False,
        "needs_thinking_fix": True,
    },
]

def sample_sections():
    conn = psycopg2.connect(**DB_CONFIG)
    samples = []
    for master_title, db_title in TITLE_MAP.items():
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT nro_licitacion, content_text
                FROM dncp.pliegos_secciones TABLESAMPLE SYSTEM(1)
                WHERE LOWER(title_normalized) = %s
                  AND content_text IS NOT NULL AND LENGTH(content_text) > 50
                LIMIT %s
            """, (db_title, N_PER_TITLE * 2))
            rows = cur.fetchall()
            if len(rows) < N_PER_TITLE:
                cur.execute("""
                    SELECT nro_licitacion, content_text
                    FROM dncp.pliegos_secciones
                    WHERE LOWER(title_normalized) = %s
                      AND content_text IS NOT NULL AND LENGTH(content_text) > 50
                    LIMIT %s
                """, (db_title, N_PER_TITLE))
                rows = cur.fetchall()
            random.shuffle(rows)
            for r in rows[:N_PER_TITLE]:
                samples.append((master_title, str(r["nro_licitacion"]), r["content_text"][:2000]))
        print(f"  {master_title}: {min(len(rows), N_PER_TITLE)} sampled")
    conn.close()
    return samples

def parse_response(text):
    text = text.strip()
    for marker in ["```json", "```"]:
        if marker in text:
            text = text.split(marker)[1]
            if "```" in text:
                text = text.split("```")[0]
            break
    try:
        data = json.loads(text)
        return data.get("campos", data)
    except:
        return None

def validate(values, schema):
    if not values:
        return False
    for fname, fdef in schema.items():
        if fname not in values:
            return False
        v = values[fname]
        if fdef["type"] == "binary" and v not in (0, 1):
            return False
        if fdef["type"] == "numeric" and not isinstance(v, (int, float)):
            return False
    return True

def build_user_prompt(titulo, nro, texto, schema):
    schema_extra = {k: {"type": v["type"], "extraction_hint": v.get("extraction_hint", v.get("description", ""))}
                    for k, v in schema.items()}
    return (
        f"TITULO DE LA SECCION: {titulo}\nDOCUMENTO: {nro}\n\n"
        f"SCHEMA A EXTRAER:\n{json.dumps(schema_extra, ensure_ascii=False, indent=2)}\n\n"
        f"TEXTO DE LA SECCION:\n{texto}\n\n"
        f"Extrae los campos del schema siguiendo exactamente cada extraction_hint."
    )

if __name__ == "__main__":
    print("Loading master schemas...")
    master = json.loads(open("data/processed/section_clustering/master_schemas.json", encoding="utf-8").read())

    print("Sampling sections from DB...")
    samples = sample_sections()
    n_samples = len(samples)
    print(f"Total: {n_samples} samples\n")

    sampling_params = SamplingParams(temperature=0, max_tokens=512)
    results = {}

    for cfg in MODELS:
        name = cfg["name"]
        repo = cfg["repo"]
        quant = cfg["quant"]

        print(f"\n{'='*70}")
        print(f"  Loading {name}")
        print(f"  Repo: {repo}")
        print(f"  Quant: {quant}")
        print(f"{'='*70}")

        # Load tokenizer
        t0 = time.time()
        try:
            hf_tok = AutoTokenizer.from_pretrained(repo, trust_remote_code=True)
        except Exception as e:
            print(f"  [SKIP] Tokenizer error: {e}")
            results[name] = {"ok": 0, "total": n_samples, "error": str(e)[:100], "avg_time": 0}
            continue

        # Load model
        try:
            llm = LLM(
                model=repo, quantization=quant,
                max_model_len=4096, max_num_seqs=32,
                gpu_memory_utilization=0.85,
                kv_cache_dtype="fp8",
                enforce_eager=True,
                trust_remote_code=True,
            )
        except Exception as e:
            print(f"  [FAIL] Load error with {quant}: {e}")
            # Fall back to awq
            if quant == "awq_marlin":
                print(f"  Retrying with quantization=awq...")
                try:
                    llm = LLM(
                        model=repo, quantization="awq",
                        max_model_len=4096, max_num_seqs=32,
                        gpu_memory_utilization=0.85,
                        kv_cache_dtype="fp8",
                        enforce_eager=True,
                        trust_remote_code=True,
                    )
                except Exception as e2:
                    print(f"  [FAIL] Load error with awq: {e2}")
                    results[name] = {"ok": 0, "total": n_samples, "error": str(e2)[:100], "avg_time": 0}
                    del hf_tok; gc.collect(); import torch; torch.cuda.empty_cache(); time.sleep(3)
                    continue
            else:
                results[name] = {"ok": 0, "total": n_samples, "error": str(e)[:100], "avg_time": 0}
                del hf_tok; gc.collect(); import torch; torch.cuda.empty_cache(); time.sleep(3)
                continue

        load_time = time.time() - t0
        print(f"  Loaded in {load_time:.1f}s")

        ok = 0
        total_time = 0.0
        per_title = {t: {"ok": 0, "total": 0, "time": 0.0} for t in TITLES}

        for i, (title, nro, text) in enumerate(samples):
            schema = master.get(title, {}).get("json_schema", {})
            if not schema:
                continue

            user = build_user_prompt(title, nro, text, schema)

            # Build messages based on model requirements
            if cfg["needs_system_merge"]:
                # Mistral: merge system into user (alternating roles requirement)
                messages = [{"role": "user", "content": f"{user}\n\nIMPORTANTE: {SYSTEM_PROMPT}"}]
            else:
                messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]

            # Render with optional thinking fix
            kwargs = {"tokenize": False, "add_generation_prompt": True}
            if cfg["needs_thinking_fix"]:
                kwargs["enable_thinking"] = False
            rendered = hf_tok.apply_chat_template(messages, **kwargs)

            t1 = time.time()
            outputs = llm.generate([rendered], sampling_params=sampling_params)
            resp = outputs[0].outputs[0].text
            elapsed = time.time() - t1
            total_time += elapsed
            per_title[title]["time"] += elapsed
            per_title[title]["total"] += 1

            values = parse_response(resp)
            valid = validate(values, schema)
            if valid:
                ok += 1
                per_title[title]["ok"] += 1
                status = "OK"
            else:
                status = "FAIL"

            print(f"  [{i+1}/{n_samples}] {title[:30]:30s} {nro} {status} ({elapsed:.2f}s)", flush=True)

        avg_time = total_time / max(n_samples, 1)
        results[name] = {
            "ok": ok, "total": n_samples, "pct": 100 * ok / max(n_samples, 1),
            "total_time": total_time, "avg_time": round(avg_time, 2),
            "per_title": {t: f"{per_title[t]['ok']}/{per_title[t]['total']} ({per_title[t]['time']/max(per_title[t]['total'],1):.1f}s avg)"
                         for t in TITLES},
        }
        print(f"\n  >> {name}: {ok}/{n_samples} = {results[name]['pct']:.1f}%, avg {avg_time:.2f}s")
        for t in TITLES:
            d = per_title[t]
            print(f"     {t[:35]:35s} {d['ok']}/{d['total']}  avg {d['time']/max(d['total'],1):.1f}s")

        # Free memory
        del llm; del hf_tok; gc.collect()
        import torch; torch.cuda.empty_cache()
        time.sleep(3)

    # Final comparison
    print(f"\n\n{'='*70}")
    print(f"  FINAL COMPARISON — AWQ FORMAT")
    print(f"{'='*70}")
    print(f"  {'Modelo':35s} {'OK/N':>8s} {'%':>6s} {'Avg(s)':>8s} {'Total(s)':>10s}")
    print(f"  {'-'*35} {'-'*8} {'-'*6} {'-'*8} {'-'*10}")
    for name, r in sorted(results.items(), key=lambda x: x[1].get("pct", 0), reverse=True):
        if "error" in r:
            print(f"  {name:35s} {'ERROR':>8s} {r['error']}")
        else:
            print(f"  {name:35s} {r['ok']}/{r['total']:>2d} {r['pct']:>5.1f}% {r['avg_time']:>7.2f}s {r['total_time']:>8.0f}s")

    print(f"\n  Reference (GGUF format from previous benchmark):")
    print(f"  {'Mistral 7B v0.3 GGUF':35s} 60/60 100.0%    4.90s      294s")
    print(f"  {'Qwen3-8B GGUF':35s} 60/60 100.0%    4.50s      270s")
    print(f"  {'Phi-4 14B GGUF':35s} 60/60 100.0%    6.09s      365s")

"""Benchmark all 5 candidate models — fixed: pre-rendered prompts + local tokenizers."""
import os, sys, time, json, random, gc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ["VLLM_USE_V2_MODEL_RUNNER"] = "0"
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import transformers.modeling_gguf_pytorch_utils as gguf_utils
original_load = gguf_utils.load_gguf_checkpoint
def patched_load(*args, **kwargs):
    try:
        return original_load(*args, **kwargs)
    except ValueError as e:
        if "is not supported yet" in str(e):
            arch = str(e).split("architecture ")[1].split(" is")[0]
            print(f"  [PATCH] Unsupported arch: {arch}")
            return {"config": {"model_type": "qwen2", "architectures": ["Qwen2ForCausalLM"],
                "hidden_size": 3584, "intermediate_size": 18944,
                "num_attention_heads": 28, "num_hidden_layers": 28,
                "num_key_value_heads": 4, "rope_theta": 1000000,
                "vocab_size": 152064, "max_position_embeddings": 32768,
                "torch_dtype": "bfloat16"}, "tensor_count": 0}
        raise
gguf_utils.load_gguf_checkpoint = patched_load

from vllm import LLM, SamplingParams
from transformers import AutoTokenizer
from config.settings import DB_CONFIG
import psycopg2, psycopg2.extras

TITLES = [
    "Fraude Y Corrupcion", "Confidencialidad De La Informacion", "Fuerza Mayor",
    "Aclaracion De Las Ofertas", "Interpretacion", "Formato Y Firma De La Oferta",
]
TITLE_MAP = {t: t.lower() for t in TITLES}

MODELS = [
    ("Mistral 7B v0.3 Q5_K_M", "/mnt/d/llmstudio-models/bartowski/Mistral-7B-Instruct-v0.3-GGUF/Mistral-7B-Instruct-v0.3-Q5_K_M.gguf",
     "/mnt/d/llmstudio-models/bartowski/Mistral-7B-Instruct-v0.3-GGUF"),
    ("Phi-4 14B Q4_K_M", "/mnt/d/llmstudio-models/bartowski/phi-4-GGUF/phi-4-Q4_K_M.gguf",
     "/mnt/d/llmstudio-models/bartowski/phi-4-GGUF"),
    ("Qwen3 8B Q5_K_M", "/mnt/d/llmstudio-models/Qwen/Qwen3-8B-GGUF/Qwen3-8B-Q5_K_M.gguf",
     "/mnt/d/llmstudio-models/Qwen/Qwen3-8B-GGUF"),
    ("Llama 3.1 8B Q5_K_M", "/mnt/d/llmstudio-models/bartowski/Meta-Llama-3.1-8B-Instruct-GGUF/Meta-Llama-3.1-8B-Instruct-Q5_K_M.gguf",
     "/mnt/d/llmstudio-models/bartowski/Meta-Llama-3.1-8B-Instruct-GGUF"),
    ("Gemma2 27B Q4_K_M", "/mnt/d/llmstudio-models/bartowski/gemma-2-27b-it-GGUF/gemma-2-27b-it-Q4_K_M.gguf",
     "/mnt/d/llmstudio-models/bartowski/gemma-2-27b-it-GGUF"),
]

SYSTEM_PROMPT = (
    "Eres un extractor de datos estructurados. Analiza el texto de la seccion "
    "del pliego de licitacion y extrae los campos indicados en el schema. "
    "Responde UNICAMENTE con un JSON valido con la estructura {\"campos\": {...}}."
)

def sample_sections(n_per_title=10):
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
            """, (db_title, n_per_title * 2))
            rows = cur.fetchall()
            if len(rows) < n_per_title:
                cur.execute("""
                    SELECT nro_licitacion, content_text
                    FROM dncp.pliegos_secciones
                    WHERE LOWER(title_normalized) = %s
                      AND content_text IS NOT NULL AND LENGTH(content_text) > 50
                    LIMIT %s
                """, (db_title, n_per_title))
                rows = cur.fetchall()
            random.shuffle(rows)
            for r in rows[:n_per_title]:
                samples.append((master_title, str(r["nro_licitacion"]), r["content_text"][:2000]))
        print(f"  {master_title}: {min(len(rows), n_per_title)} sampled")
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
    samples = sample_sections(10)
    n_samples = len(samples)
    print(f"Total: {n_samples} samples\n")

    sampling_params = SamplingParams(temperature=0, max_tokens=512)
    results = {}

    for name, model_path, tok_dir in MODELS:
        print(f"\n{'='*70}")
        print(f"  Loading {name}")
        print(f"  Model: {model_path}")
        print(f"  Tokenizer dir: {tok_dir}")
        print(f"{'='*70}")

        # Load HF tokenizer from local directory
        if not os.path.isdir(tok_dir):
            print(f"  [SKIP] Tokenizer dir not found: {tok_dir}")
            results[name] = {"ok": 0, "total": n_samples, "error": "tokenizer_dir_not_found", "avg_time": 0}
            continue
        try:
            hf_tok = AutoTokenizer.from_pretrained(tok_dir, trust_remote_code=True)
        except Exception as e:
            print(f"  [SKIP] Tokenizer load error: {e}")
            results[name] = {"ok": 0, "total": n_samples, "error": f"tokenizer_load: {e}", "avg_time": 0}
            continue

        # Load vLLM model
        t_load = time.time()
        try:
            llm = LLM(
                model=model_path, tokenizer=tok_dir,
                max_model_len=4096, max_num_seqs=32,
                gpu_memory_utilization=0.85, enforce_eager=True,
                kv_cache_dtype="fp8", enable_prefix_caching=True,
                trust_remote_code=True,
            )
        except Exception as e:
            print(f"  [FAIL] Load error: {e}")
            results[name] = {"ok": 0, "total": n_samples, "error": f"load: {e}", "avg_time": 0}
            del hf_tok; gc.collect(); import torch; torch.cuda.empty_cache(); time.sleep(2)
            continue

        load_time = time.time() - t_load
        print(f"  Loaded in {load_time:.1f}s")

        ok = 0
        total_time = 0.0
        for i, (title, nro, text) in enumerate(samples):
            schema = master.get(title, {}).get("json_schema", {})
            if not schema:
                continue

            user = build_user_prompt(title, nro, text, schema)
            messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]

            # Pre-render with enable_thinking=False (critical for Qwen3)
            kwargs = {"tokenize": False, "add_generation_prompt": True}
            if hasattr(hf_tok, "enable_thinking") or "enable_thinking" in str(hf_tok.chat_template):
                kwargs["enable_thinking"] = False
            rendered = hf_tok.apply_chat_template(messages, **kwargs)

            t1 = time.time()
            outputs = llm.generate([rendered], sampling_params=sampling_params)
            resp = outputs[0].outputs[0].text
            elapsed = time.time() - t1
            total_time += elapsed

            values = parse_response(resp)
            valid = validate(values, schema)
            if valid:
                ok += 1
                status = f"OK ({len(values)} fields)"
            else:
                status = "FAIL"

            print(f"  [{i+1}/{n_samples}] {title[:30]:30s} {nro} {status} ({elapsed:.1f}s)", flush=True)

        avg_time = total_time / max(n_samples, 1)
        results[name] = {
            "ok": ok, "total": n_samples, "pct": 100 * ok / max(n_samples, 1),
            "total_time": total_time, "avg_time": round(avg_time, 2),
        }
        print(f"  >> {name}: {ok}/{n_samples} = {results[name]['pct']:.1f}%, avg {avg_time:.2f}s")

        # Free memory
        del llm; del hf_tok; gc.collect()
        import torch; torch.cuda.empty_cache()
        time.sleep(3)

    print(f"\n{'='*70}")
    print(f"  FINAL COMPARISON")
    print(f"{'='*70}")
    print(f"  {'Modelo':35s} {'OK/N':>8s} {'%':>6s} {'Avg(s)':>8s} {'Total(s)':>10s}")
    print(f"  {'-'*35} {'-'*8} {'-'*6} {'-'*8} {'-'*10}")
    for name, r in sorted(results.items(), key=lambda x: x[1].get("pct", 0), reverse=True):
        if "error" in r:
            print(f"  {name:35s} {'ERROR':>8s} {r['error']}")
        else:
            print(f"  {name:35s} {r['ok']}/{r['total']:>2d} {r['pct']:>5.1f}% {r['avg_time']:>7.2f}s {r['total_time']:>8.0f}s")
    print(f"\n  Baseline Qwen3-14B: 60/60 = 100.0%, avg 5.10s")

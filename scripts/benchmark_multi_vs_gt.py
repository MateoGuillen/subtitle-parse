"""
Multi-model benchmark against human-annotated ground truth (5 campos, 100 muestras).
Tests all available AWQ models with the improved prompts + post-processing.

Each model runs in an isolated subprocess so VRAM is fully released between runs.
Usage:
  python scripts/benchmark_multi_vs_gt.py                 # run all models (orchestrator)
  python scripts/benchmark_multi_vs_gt.py --model <idx>   # run single model (spawned)
"""
import sys, re, gc, time, json, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from transformers import AutoTokenizer
from config.settings import DB_CONFIG
import psycopg2
import os

# DB_CONFIG uses "database" key but psycopg2 needs "dbname"
cfg_db = DB_CONFIG.copy()
if "database" in cfg_db:
    cfg_db["dbname"] = cfg_db.pop("database")
if cfg_db.get("dbname") is None:
    cfg_db["dbname"] = os.getenv("DB_NAME", "dncp")
from vllm import LLM, SamplingParams
from vllm.sampling_params import StructuredOutputsParams

GT = Path("data/processed/llm_benchmark/ground_truth_5_campos.csv")
SCHEMAS = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
TEXTS_CACHE = Path("data/processed/llm_benchmark/gt_texts.json")
OUT = Path("data/processed/llm_benchmark/multimodel_results")

MODELS = [
    {"name": "Llama 3.1 8B-AWQ", "path": "thesven/Meta-Llama-3.1-8B-Instruct-AWQ", "quant": None, "thinking_fix": False, "bf16": False, "gpu_mem": 0.70, "max_len": 8192, "merge_system": False},
    {"name": "Mistral 7B v0.3-AWQ", "path": "solidrust/Mistral-7B-Instruct-v0.3-AWQ", "quant": None, "thinking_fix": False, "bf16": False, "gpu_mem": 0.70, "max_len": 8192, "merge_system": True},
    {"name": "Qwen3-8B-AWQ", "path": "Qwen/Qwen3-8B-AWQ", "quant": None, "thinking_fix": True, "bf16": False, "gpu_mem": 0.70, "max_len": 8192, "merge_system": False},
    {"name": "Qwen3-14B-AWQ", "path": "Qwen/Qwen3-14B-AWQ", "quant": None, "thinking_fix": True, "bf16": False, "gpu_mem": 0.92, "max_len": 4096, "merge_system": False},
    {"name": "Qwen2.5-14B-AWQ", "path": "Qwen/Qwen2.5-14B-Instruct-AWQ", "quant": "awq", "thinking_fix": False, "bf16": False, "gpu_mem": 0.92, "max_len": 4096, "merge_system": False},
    {"name": "ALIA legal 7B-Q8_0", "path": "mradermacher/ALIA-es-legal-administrative-7B-Instruct-GGUF", "quant": "gguf", "gguf_file": "ALIA-es-legal-administrative-7B-Instruct.Q8_0.gguf", "tokenizer_path": "SINAI/ALIA-es-legal-administrative-7B-Instruct", "thinking_fix": False, "bf16": True, "gpu_mem": 0.90, "max_len": 8192, "merge_system": False},
    {"name": "Llama 3.1 8B-FP8", "path": "RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8", "quant": None, "thinking_fix": False, "bf16": False, "gpu_mem": 0.70, "max_len": 8192, "merge_system": False},
    {"name": "Qwen3-8B-FP8", "path": "Qwen/Qwen3-8B-FP8", "quant": None, "thinking_fix": True, "bf16": False, "gpu_mem": 0.80, "max_len": 4096, "merge_system": False},
    {"name": "EXAONE-3.5-7.8B-AWQ", "path": "LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct-AWQ", "quant": None, "thinking_fix": False, "bf16": False, "gpu_mem": 0.70, "max_len": 8192, "merge_system": False},
    {"name": "Mistral Small 24B-AWQ", "path": "stelterlab/Mistral-Small-24B-Instruct-2501-AWQ", "quant": None, "thinking_fix": False, "bf16": False, "gpu_mem": 0.95, "max_len": 2048, "merge_system": False, "limit": 10, "cpu_offload": 5, "max_seqs": 2},
]

SYSTEM_PROMPT = (
    "Eres un extractor de datos de pliegos de licitaciones p\u00fablicas paraguayas (DNCP).\n"
    "Extrae el valor del campo solicitado siguiendo exactamente la gu\u00eda de extracci\u00f3n.\n\n"
    "REGLAS ESTRICTAS:\n"
    "- No confundas tipos: si el campo pide d\u00edas, no extraigas porcentajes (10%) ni montos.\n"
    "- Verifica que el n\u00famero est\u00e9 asociado a la unidad correcta (d\u00edas, %, a\u00f1os).\n"
    "- Los porcentajes tienen el s\u00edmbolo '%' o texto 'por ciento' junto al n\u00famero.\n"
    "- No extraigas n\u00fameros de cl\u00e1usulas, art\u00edculos legales, ni listas numeradas.\n"
    "- Si el \u00fanico n\u00famero en el texto corresponde a otro concepto (otro campo), responde NULL.\n"
    "- La gu\u00eda de extracci\u00f3n tiene prioridad sobre estas reglas generales.\n\n"
    "FORMATO DE RESPUESTA:\n"
    "- Para int/float: responde SOLO con el n\u00famero (usa punto decimal, sin unidades).\n"
    "- Para binary: responde SOLO con 1 (si se cumple) o 0 (si no).\n"
    "- Para categorical: responde SOLO con la categor\u00eda exacta.\n"
    "- Si no encuentras el valor o no es v\u00e1lido, responde exactamente: NULL\n\n"
    "IMPORTANTE: Escribe el n\u00famero o NULL en la PRIMERA l\u00ednea de tu respuesta.\n"
    "Luego puedes explicar en l\u00edneas siguientes si es necesario."
)

TITLE_MAP = {
    "pct_diferencia_precio_bajo": "analisis de precios ofertados",
    "pct_diferencia_precio_alto": "analisis de precios ofertados",
    "pct_multa_diaria": "porcentaje de multas",
    "dias_notificacion_fuerza_mayor": "fuerza mayor",
    "pct_garantia_cumplimiento": "porcentaje de garantia de fiel cumplimiento de contrato",
    "dias_presentacion_garantia": "porcentaje de garantia de fiel cumplimiento de contrato",
    "anos_experiencia_minima": "experiencia requerida",
    "num_referencias_minimo": "experiencia requerida",
    "pct_monto_referencia_minimo": "experiencia requerida",
    "dias_reposicion": "plazo de reposicion de bienes",
    "num_categorias_documentos": "documentacion requerida para la firma del contrato",
    "dias_validez_garantia": "periodo de validez de la garantia de los bienes",
    "anos_experiencia_minima_tecnica": "capacidad tecnica",
    "num_referencias_tecnicas_minimo": "capacidad tecnica",
    "pct_monto_referencia_tecnica": "capacidad tecnica",
    "dias_pago_maximo": "formas y condiciones de pago",
    "dias_aceptacion_rechazo_factura": "formas y condiciones de pago",
    "num_documentos_pago": "formas y condiciones de pago",
    "ratio_liquidez_minimo": "capacidad financiera",
    "ratio_endeudamiento_maximo": "capacidad financiera",
    "num_componentes_formula": "reajuste",
    "pct_anticipo": "forma de instrumentacion de garantia de anticipo",
    "num_pasos_verificacion": "requisitos de calificacion",
    "dias_solicitud_anticipo": "solicitud de pago de anticipo",
    "dias_pago_anticipo": "solicitud de pago de anticipo",
    "pct_interes_mora_diario": "tasa de interes por mora",
}

from src.etl.transformers.extraction_postprocess import (
    NO_RULE, KNOWN_DEFAULTS, deterministic_pct, postprocess, parse_response,
)

_postprocess_shared = postprocess

def get_hint(field_name):
    with open(SCHEMAS, encoding="utf-8") as f:
        schemas = json.load(f)
    for slug, schema in schemas.items():
        if slug == "meta": continue
        for f in schema["fields"]:
            if f["name"] == field_name:
                return f
    return {}

def build_prompt(field_name, text, merge_system=False):
    fs = get_hint(field_name)
    hint = fs.get("extraction_hint", "")
    user = [f"Campo a extraer: {fs.get('description', '')}",
            f"Tipo esperado: {fs.get('type', '')}"]
    if hint and hint not in ("", "None"):
        user.append(f"Gu\u00eda de extracci\u00f3n: {hint}")
    user.append(f"\nTexto del pliego:\n{text}")
    if merge_system:
        return [{"role": "user", "content": "\n".join([SYSTEM_PROMPT] + user)}]
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": "\n".join(user)}]

def postprocess(field, v, text):
    return _postprocess_shared(field, v, text)

def parse(raw, field="", text=""):
    return parse_response(raw, field=field, text=text, strict=True)

def load_gt_texts():
    gt = pd.read_csv(GT, encoding="utf-8-sig")
    if TEXTS_CACHE.exists():
        with open(TEXTS_CACHE, encoding="utf-8") as f:
            texts = json.load(f)
        return gt, texts
    conn = psycopg2.connect(**cfg_db)
    texts = {}
    cur = conn.cursor()
    for field in gt["campo"].unique():
        title = TITLE_MAP[field]
        nros = [str(int(x)) for x in gt[gt["campo"] == field]["nro_licitacion"]]
        ph = ",".join("%s" for _ in nros)
        cur.execute(f"SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones WHERE nro_licitacion IN ({ph}) AND title_normalized = %s", nros + [title.replace("_", " ")])
        texts.update({str(row[0]): row[1] or "" for row in cur.fetchall()})
    cur.close()
    conn.close()
    with open(TEXTS_CACHE, "w", encoding="utf-8") as f:
        json.dump(texts, f, ensure_ascii=False)
    return gt, texts

def run_model(cfg, gt, texts):
    print(f"\n{'='*60}")
    print(f"  Model: {cfg['name']}")
    print(f"{'='*60}")

    hf_tok_path = cfg.get("tokenizer_path", cfg["path"])
    hf_tok = AutoTokenizer.from_pretrained(hf_tok_path, trust_remote_code=True)
    model_path = cfg["path"]
    if cfg["quant"] == "gguf":
        # GGUF needs the absolute path to the .gguf file
        gguf = cfg.get("gguf_file")
        import glob as _glob
        matches = _glob.glob(f"/root/.cache/huggingface/**/models--*{cfg['path'].split('/')[1]}*/snapshots/*/{gguf}", recursive=True)
        if matches:
            model_path = matches[0]
        else:
            # fallback to repo dir + file
            model_path = str(Path(cfg["path"]) / gguf)
    llm_kwargs = dict(model=model_path, max_model_len=cfg["max_len"],
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
    sp = SamplingParams(max_tokens=64, temperature=0.0,
                         structured_outputs=StructuredOutputsParams(regex=r'^(-?\d+(?:[.,]\d+)?|NULL)$'))

    FIELDS = ["dias_validez_garantia", "pct_monto_referencia_tecnica",
              "anos_experiencia_minima_tecnica", "dias_pago_anticipo", "dias_solicitud_anticipo"]

    t0 = time.time()
    all_results = []
    for field in FIELDS:
        sub = gt[gt["campo"] == field]
        if cfg.get("limit"):
            sub = sub.head(cfg["limit"])
        for _, row in sub.iterrows():
            nro = str(int(row["nro_licitacion"]))
            text = texts.get(nro, "")
            msgs = build_prompt(field, text, cfg["merge_system"])
            tmpl_kwargs = {"enable_thinking": False} if cfg["thinking_fix"] else {}
            rendered = hf_tok.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True, **tmpl_kwargs)
            out = llm.generate([rendered], sp)[0].outputs[0].text.strip()
            parsed = parse(out, field=field, text=text)
            gt_val = None if pd.isna(row["ground_truth"]) else float(row["ground_truth"])
            ok = (parsed == gt_val) or (parsed is None and gt_val is None)
            all_results.append({"nro": nro, "field": field, "gt": row["ground_truth"], "llm_value": parsed, "llm_raw": out[:80], "match": ok})

        ok = sum(1 for x in all_results if x["field"] == field and x["match"])
        print(f"    {field:35s}: {ok}/{len(sub)} = {ok/len(sub)*100:.0f}%")

    elapsed = time.time() - t0
    ok = sum(1 for x in all_results if x["match"])
    print(f"    {'TOTAL':35s}: {ok}/{len(all_results)} = {ok/len(all_results)*100:.0f}%  ({elapsed:.0f}s)")

    OUT.mkdir(parents=True, exist_ok=True)
    fname = OUT / f"{cfg['name'].replace(' ', '_').replace('.', '')}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump({"model": cfg["name"], "score": ok, "elapsed": elapsed, "results": all_results}, f, ensure_ascii=False, indent=1)
    return ok, elapsed, all_results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=int, default=None)
    args = parser.parse_args()

    gt, texts = load_gt_texts()

    if args.model is not None:
        run_model(MODELS[args.model], gt, texts)
        return

    results = {}
    for i, cfg in enumerate(MODELS):
        import subprocess
        env = os.environ.copy()
        r = subprocess.run([sys.executable, str(Path(__file__).resolve()),
                            "--model", str(i)], env=env, check=False)
        if r.returncode != 0:
            results[cfg["name"]] = {"score": None, "elapsed": None, "error": "crashed"}
            continue
        fname = OUT / f"{cfg['name'].replace(' ', '_').replace('.', '')}.json"
        if fname.exists():
            with open(fname, encoding="utf-8") as f:
                data = json.load(f)
            results[cfg["name"]] = {"score": data["score"], "elapsed": data["elapsed"]}
        time.sleep(5)

    print(f"\n{'='*60}")
    print(f"  FINAL RANKING (vs GT, 100 samples)")
    print(f"{'='*60}")
    for name, r in sorted(results.items(), key=lambda x: -(x[1]["score"] or 0)):
        score = f"{r['score']}%" if r["score"] is not None else "ERROR"
        print(f"  {name:25s}: {score}  ({r['elapsed']:.0f}s)" if r["elapsed"] else f"  {name:25s}: {score}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()

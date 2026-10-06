"""
Benchmark multiple LLMs for Phase 5 fallback extraction.

Loads each model once via vLLM offline API, runs extraction on N random
samples per hybrid field (where regex conf < threshold), collects metrics.

Usage::

    python scripts/benchmark_llm_fallback.py [--samples 20] [--fields "campo1,campo2"]

    # Test a single model:
    python scripts/benchmark_llm_fallback.py --models 0

Output:
    ``data/processed/llm_benchmark/results_{model_name}.csv`` — per-field CSVs
    ``data/processed/llm_benchmark/comparison.csv`` — aggregated comparison
"""

import sys
import re
import gc
import time
import json
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import dataclasses

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd


# ---------------------------------------------------------------------------
# Model registry
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class ModelConfig:
    index: int
    name: str
    path: str
    quantization: str = "awq"
    needs_system_merge: bool = False
    needs_thinking_fix: bool = False
    dtype: str = "auto"
    max_model_len: int = 4096


MODEL_REGISTRY: List[ModelConfig] = [
    ModelConfig(
        index=0, name="Llama 3.1 8B-AWQ",
        path="thesven/Meta-Llama-3.1-8B-Instruct-AWQ",
        quantization="awq_marlin", max_model_len=8192,
    ),
    ModelConfig(
        index=1, name="Qwen3-8B-AWQ",
        path="Qwen/Qwen3-8B-AWQ",
        quantization="awq", needs_thinking_fix=True, max_model_len=8192,
    ),
    ModelConfig(
        index=2, name="Qwen2.5-14B-AWQ",
        path="Qwen/Qwen2.5-14B-Instruct-AWQ",
        quantization="awq", max_model_len=8192,
    ),
    ModelConfig(
        index=3, name="Mistral 7B v0.3-AWQ",
        path="solidrust/Mistral-7B-Instruct-v0.3-AWQ",
        quantization="awq_marlin", needs_system_merge=True, max_model_len=8192,
    ),
]

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_PATH = str(
    PROJECT_ROOT / "data" / "processed" / "section_clustering_v2" / "master_schemas_v2.json"
)
FEW_SHOT_DB_PATH = str(
    PROJECT_ROOT / "data" / "processed" / "section_clustering_v2" / "few_shot_db.json"
)
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "llm_benchmark"

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "Eres un extractor de datos de pliegos de licitaciones p\u00fablicas paraguayas (DNCP).\n"
    "Extrae el valor del campo solicitado siguiendo exactamente la gu\u00eda de extracci\u00f3n.\n"
    "\n"
    "REGLAS ESTRICTAS:\n"
    "- No confundas tipos: si el campo pide d\u00edas, no extraigas porcentajes (10%) ni montos.\n"
    "- Verifica que el n\u00famero est\u00e9 asociado a la unidad correcta (d\u00edas, %, a\u00f1os).\n"
    "- Los porcentajes tienen el s\u00edmbolo '%' o texto 'por ciento' junto al n\u00famero.\n"
    "- No extraigas n\u00fameros de cl\u00e1usulas, art\u00edculos legales, ni listas numeradas.\n"
    "- Si el \u00fanico n\u00famero en el texto corresponde a otro concepto (otro campo), responde NULL.\n"
    "- La gu\u00eda de extracci\u00f3n tiene prioridad sobre estas reglas generales.\n"
    "\n"
    "FORMATO DE RESPUESTA:\n"
    "- Para int/float: responde SOLO con el n\u00famero (usa punto decimal, sin unidades).\n"
    "- Para binary: responde SOLO con 1 (si se cumple) o 0 (si no).\n"
    "- Para categorical: responde SOLO con la categor\u00eda exacta.\n"
    "- Si no encuentras el valor o no es v\u00e1lido, responde exactamente: NULL\n"
    "\n"
    "IMPORTANTE: Escribe el n\u00famero o NULL en la PRIMERA l\u00ednea de tu respuesta.\n"
    "Luego puedes explicar en l\u00edneas siguientes si es necesario."
)

GUIDED_SYSTEM_PROMPT = (
    "Eres un extractor de datos de pliegos de licitaciones p\u00fablicas paraguayas (DNCP).\n"
    "Extrae el valor del campo solicitado siguiendo exactamente la gu\u00eda de extracci\u00f3n.\n"
    "\n"
    "REGLAS ESTRICTAS:\n"
    "- No confundas tipos: si el campo pide d\u00edas, no extraigas porcentajes (10%) ni montos.\n"
    "- Verifica que el n\u00famero est\u00e9 asociado a la unidad correcta (d\u00edas, %, a\u00f1os).\n"
    "- Los porcentajes tienen el s\u00edmbolo '%' o texto 'por ciento' junto al n\u00famero.\n"
    "- No extraigas n\u00fameros de cl\u00e1usulas, art\u00edculos legales, ni listas numeradas.\n"
    "- Si el \u00fanico n\u00famero en el texto corresponde a otro concepto (otro campo), responde NULL.\n"
    "- La gu\u00eda de extracci\u00f3n tiene prioridad sobre estas reglas generales.\n"
    "\n"
    "Si no encuentras el valor o no es v\u00e1lido, responde NULL."
)

GUIDED_REGEX_NUMERIC = r'^(-?\d+(?:[.,]\d+)?|NULL)$'
GUIDED_REGEX_BINARY = r'^(0|1)$'


# Load few-shot DB (cluster-aware examples)
_few_shot_db: Dict = {}
if Path(FEW_SHOT_DB_PATH).exists():
    with open(FEW_SHOT_DB_PATH, encoding="utf-8") as f:
        _few_shot_db = json.load(f)

# Build field-to-title mapping from schemas
_field_to_title: Dict[str, str] = {}
with open(SCHEMAS_PATH, encoding="utf-8") as f:
    _schemas_data = json.load(f)
for slug, schema in _schemas_data.items():
    if slug == "meta":
        continue
    for f in schema["fields"]:
        _field_to_title[f["name"]] = slug


def get_few_shot_examples(field_name: str, cluster_id) -> List[str]:
    slug = _field_to_title.get(field_name)
    if not slug or not _few_shot_db:
        return []
    try:
        cid_str = str(int(cluster_id))
    except (ValueError, TypeError):
        cid_str = None
    examples = []
    slug_data = _few_shot_db.get(slug, {})
    if cid_str and cid_str in slug_data:
        for ex in slug_data[cid_str].get(field_name, [])[:3]:
            text = ex.get("text", "")[:500]
            val = ex.get("value", "")
            examples.append(f"Ejemplo (cluster {cid_str}): valor={val}\n{text}")
    return examples


def build_messages(
    field_schema: dict, text: str, needs_system_merge: bool = False,
    cluster_id=None, field_name: str = "",
) -> List[Dict[str, str]]:
    hint = field_schema.get("extraction_hint", "")
    system_prompt = SYSTEM_PROMPT
    user_parts = [
        f"Campo a extraer: {field_schema['description']}",
        f"Tipo esperado: {field_schema['type']}",
    ]
    if hint and hint not in ("", "None"):
        user_parts.append(f"Gu\u00eda de extracci\u00f3n: {hint}")
    few_shot = get_few_shot_examples(field_name, cluster_id)
    if few_shot:
        user_parts.append("\nEjemplos de documentos similares:")
        for ex in few_shot:
            user_parts.append(f"---\n{ex}")
    user_parts.append(f"\nTexto del pliego:\n{text}")

    if needs_system_merge:
        return [{"role": "user", "content": "\n".join([system_prompt] + user_parts)}]
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "\n".join(user_parts)},
    ]


def parse_response(raw: str, ftype: str, guided: bool = False) -> Tuple[Optional[float], float]:
    if guided:
        if raw in ("", "NULL", "NONE", "N/A", "NA", "NOT FOUND"):
            return None, 0.0
        raw = raw.strip()
        if raw.upper() in ("NULL", "NONE", "N/A", "NOT FOUND", ""):
            return None, 0.0
        if not re.match(GUIDED_REGEX_NUMERIC, raw):
            return None, 0.0
        try:
            val_str = raw.replace(",", ".")
            val = int(float(val_str)) if ftype == "int" else float(val_str)
            return val, 1.0
        except (ValueError, TypeError):
            return None, 0.0

    # Non-guided: first-line priority parsing
    if raw in ("", "NULL", "NONE", "N/A", "NA", "NOT FOUND"):
        return None, 0.0
    cleaned = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    if not cleaned:
        cleaned = raw
    first_line = cleaned.split('\n')[0].strip()
    if first_line.upper() in ('NULL', 'NONE', 'N/A', 'NOT FOUND', ''):
        return None, 0.0
    m_line = re.search(r'^(-?\d+[.,]?\d*)$', first_line.replace(' ', ''))
    if m_line:
        try:
            val_str = m_line.group(0).replace(",", ".")
            val = int(float(val_str)) if ftype == "int" else float(val_str)
            return val, 1.0
        except (ValueError, TypeError):
            pass
    m = re.search(r'(?:respuesta|valor|resultado|resulta|es\s*[:=])\s*(?:de\s*)?(-?\d+[.,]?\d*)', cleaned, re.IGNORECASE)
    if m:
        try:
            val_str = m.group(1).replace(",", ".")
            val = int(float(val_str)) if ftype == "int" else float(val_str)
            return val, 1.0
        except (ValueError, TypeError):
            pass
    if re.search(r'\bNULL\b', cleaned[-200:], re.IGNORECASE):
        return None, 0.0
    all_nums = re.findall(r'-?\d+[.,]?\d*', cleaned.replace(' ', ''))
    if all_nums:
        last = all_nums[-1]
        try:
            val_str = last.replace(",", ".")
            val = int(float(val_str)) if ftype == "int" else float(val_str)
            return val, 1.0
        except (ValueError, TypeError):
            pass
    return None, 0.0


# ---------------------------------------------------------------------------
# Sampling: use real failed extractions from DB
# ---------------------------------------------------------------------------

def sample_failed_fields(
    db_params: dict, n_per_field: int,
    field_filter: Optional[set] = None,
    seed: int = 42,
) -> Dict[str, pd.DataFrame]:
    """Get N random samples per hybrid field where regex conf < 0.8."""
    from src.etl.extractors.document_features_v2_llm_extractor import DocumentFeaturesV2LLMExtractor

    extractor = DocumentFeaturesV2LLMExtractor(db_params, SCHEMAS_PATH, 0.8)
    failed = extractor.get_failed_extractions_grouped()

    result = {}
    for field_name, df_all in sorted(failed.items()):
        if field_filter and field_name not in field_filter:
            continue
        n = min(n_per_field, len(df_all))
        if n == 0:
            continue
        df_samp = df_all.sample(n=n, random_state=seed)
        result[field_name] = df_samp
        print(f"  {field_name:35s} {n:4d} / {len(df_all):5d} available")
    return result


# ---------------------------------------------------------------------------
# Per-model runner
# ---------------------------------------------------------------------------

def run_model(
    cfg: ModelConfig,
    samples_by_field: Dict[str, pd.DataFrame],
    field_schemas: Dict[str, dict],
    guided: bool = False,
) -> Optional[pd.DataFrame]:
    """Load model once, run all fields, return per-field metrics DataFrame."""
    from vllm import LLM, SamplingParams
    from vllm.sampling_params import StructuredOutputsParams
    from transformers import AutoTokenizer

    print(f"\n  Loading {cfg.name} ({cfg.path})...", end=" ")
    sys.stdout.flush()
    t0 = time.time()

    try:
        hf_tok = AutoTokenizer.from_pretrained(cfg.path, trust_remote_code=True)
    except Exception as e:
        print(f"\n  [SKIP] Tokenizer error: {e}")
        return None

    quant = cfg.quantization
    llm = None
    for attempt_quant in [quant, "awq"] if quant == "awq_marlin" else [quant]:
        try:
            llm = LLM(
                model=cfg.path, quantization=attempt_quant,
                max_model_len=cfg.max_model_len, max_num_seqs=8,
                gpu_memory_utilization=0.70, enforce_eager=True,
                trust_remote_code=True, dtype=cfg.dtype,
            )
            break
        except Exception as e:
            if attempt_quant != quant:
                print(f"\n    {quant} failed, retrying awq: {e}")
            else:
                print(f"\n  [FAIL] Load error with {attempt_quant}: {e}")
                return None

    if llm is None:
        return None

    load_time = time.time() - t0
    print(f"done ({load_time:.1f}s)    guided={'ON' if guided else 'OFF'}")

    all_results = []
    model_metrics = []

    for field_name, df_samp in sorted(samples_by_field.items()):
        fs = field_schemas.get(field_name)
        if not fs:
            continue
        ftype = fs["type"]
        base_params = {"temperature": 0, "max_tokens": 64}

        if guided and ftype in ("int", "float"):
            sp = SamplingParams(
                structured_outputs=StructuredOutputsParams(regex=GUIDED_REGEX_NUMERIC),
                **base_params,
            )
        elif guided and ftype == "binary":
            sp = SamplingParams(
                structured_outputs=StructuredOutputsParams(regex=GUIDED_REGEX_BINARY),
                **base_params,
            )
        else:
            sp = SamplingParams(max_tokens=256, **base_params)

        field_results = []
        t_start = time.time()

        for idx, row in df_samp.iterrows():
            text = row.get("content_text") or ""
            if not text.strip():
                continue
            cluster_id = row.get("cluster_id")
            messages = build_messages(fs, text, cfg.needs_system_merge, cluster_id, field_name)
            render_kw = {"tokenize": False, "add_generation_prompt": True}
            if cfg.needs_thinking_fix:
                render_kw["enable_thinking"] = False
            rendered = hf_tok.apply_chat_template(messages, **render_kw)

            t_req = time.time()
            outputs = llm.generate([rendered], sp)
            elapsed = time.time() - t_req
            resp = outputs[0].outputs[0].text.strip()

            value, conf = parse_response(resp, ftype, guided=guided)
            field_results.append({
                "nro_licitacion": row["nro_licitacion"],
                "field": field_name,
                "value": value,
                "confidence": conf,
                "response_raw": resp[:200],
                "time_s": round(elapsed, 3),
            })

        total_time = time.time() - t_start
        n_total = len(field_results)
        n_found = sum(1 for r in field_results if r["confidence"] > 0)
        pct = round(n_found / max(n_total, 1) * 100, 1)

        all_results.extend(field_results)
        model_metrics.append({
            "field": field_name, "docs": n_total, "found": n_found,
            "pct": pct, "time_s": round(total_time, 1),
        })
        print(f"    {field_name:35s} {n_found:4d}/{n_total:4d} ({pct:4.0f}%) | {total_time:5.1f}s ({n_total/max(total_time,0.01):.1f} docs/s)")

    # Save detailed CSV
    df_detail = pd.DataFrame(all_results)
    safe_name = cfg.name.replace(" ", "_").replace("/", "_")
    csv_path = OUTPUT_DIR / f"results_{safe_name}.csv"
    df_detail.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"  Saved: {csv_path}")

    # Free GPU memory
    del llm; del hf_tok; gc.collect()
    import torch
    torch.cuda.empty_cache()

    return pd.DataFrame(model_metrics)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Benchmark LLMs for Phase 5 fallback extraction")
    parser.add_argument("--samples", type=int, default=20,
                        help="Samples per field per model")
    parser.add_argument("--fields", type=str, default=None,
                        help="Comma-separated field names (default: all hybrid)")
    parser.add_argument("--models", type=str, default=None,
                        help="Comma-separated model indices (default: all)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--guided", action="store_true",
                        help="Use vLLM guided_regex to constrain output to number or NULL")
    args = parser.parse_args()

    from config.settings import DB_CONFIG

    # Resolve models
    models_to_run = MODEL_REGISTRY
    if args.models:
        indices = {int(i) for i in args.models.split(",")}
        models_to_run = [m for m in models_to_run if m.index in indices]

    field_filter = set(args.fields.split(",")) if args.fields else None
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load field schemas
    with open(SCHEMAS_PATH, encoding="utf-8") as f:
        schemas = json.load(f)
    field_schemas = {}
    for slug, schema in schemas.items():
        if slug == "meta":
            continue
        for f in schema["fields"]:
            field_schemas[f["name"]] = f

    # Sample failed extractions once (shared across models)
    print("\n=== Sampling failed extractions from DB ===")
    print(f"  Threshold: conf < 0.8 | Samples/field: {args.samples}")
    samples_by_field = sample_failed_fields(DB_CONFIG, args.samples, field_filter, args.seed)
    if not samples_by_field:
        print("No fields to benchmark.")
        return

    field_names = sorted(samples_by_field.keys())
    total_samples = sum(len(v) for v in samples_by_field.values())
    print(f"\n  Total: {len(field_names)} fields, {total_samples} samples")

    # Run benchmarks
    all_metrics: Dict[str, pd.DataFrame] = {}
    for cfg in models_to_run:
        print(f"\n{'='*70}")
        print(f"  MODEL [{cfg.index}]: {cfg.name}")
        print(f"{'='*70}")
        print(f"  Path: {cfg.path}")
        print(f"  Quant: {cfg.quantization} | System merge: {cfg.needs_system_merge} | Thinking fix: {cfg.needs_thinking_fix}")
        metrics = run_model(cfg, samples_by_field, field_schemas, guided=args.guided)
        all_metrics[cfg.name] = metrics

    # Comparison table
    print(f"\n\n{'='*70}")
    print(f"  BENCHMARK COMPARISON")
    print(f"  {len(models_to_run)} models x {len(field_names)} fields x {args.samples} samples")
    print(f"{'='*70}")
    print(f"  {'Model':30s} {'Found':>7s} {'Docs':>7s} {'%':>6s} {'T(s)':>8s} {'doc/s':>7s}")
    print(f"  {'-'*30} {'-'*7} {'-'*7} {'-'*6} {'-'*8} {'-'*7}")

    comparison_rows = []
    for cfg in models_to_run:
        metrics = all_metrics.get(cfg.name)
        if metrics is None or metrics.empty:
            print(f"  {cfg.name:30s} {'FAILED':>7s}")
            continue
        total = int(metrics["docs"].sum())
        found = int(metrics["found"].sum())
        ttime = metrics["time_s"].sum()
        pct = found / max(total, 1) * 100
        dps = total / max(ttime, 0.01)
        print(f"  {cfg.name:30s} {found:7d} {total:7d} {pct:5.1f}% {ttime:7.1f}s {dps:6.2f}")
        comparison_rows.append({
            "model": cfg.name,
            "path": cfg.path,
            "samples": total,
            "found": found,
            "pct": round(pct, 1),
            "time_s": round(ttime, 1),
            "docs_per_s": round(dps, 2),
        })

    if comparison_rows:
        comp_df = pd.DataFrame(comparison_rows)
        comp_csv = OUTPUT_DIR / "comparison.csv"
        comp_df.to_csv(comp_csv, index=False, encoding="utf-8")
        print(f"\n  Comparison saved: {comp_csv}")

    # Per-field comparison table
    print(f"\n{'='*70}")
    print(f"  PER-FIELD COMPARISON (% FOUND)")
    print(f"{'='*70}")
    col_w = max(18, max(len(c.name) for c in models_to_run))
    header = f"  {'Field':35s}"
    for cfg in models_to_run:
        header += f" {cfg.name[:col_w]:>{col_w}s}"
    print(header)
    print(f"  {'-'*35} " + "-" * (col_w * max(1, len(models_to_run))))

    for fn in field_names:
        row = f"  {fn:35s}"
        for cfg in models_to_run:
            metrics = all_metrics.get(cfg.name)
            if metrics is None or metrics.empty:
                row += f" {'FAILED':>{col_w}s}"
            else:
                m = metrics[metrics["field"] == fn]
                if m.empty:
                    row += f" {'N/A':>{col_w}s}"
                else:
                    pct = m.iloc[0]["pct"]
                    row += f" {pct:>{col_w-1}.0f}%"
        print(row)

    print(f"\n  All outputs: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()

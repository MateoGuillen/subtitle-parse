"""Evaluate extraction quality across multiple SambaNova models.

Usage:
    python scripts/evaluate_extraction.py                                   # full eval (3 models, 20/title)
    python scripts/evaluate_extraction.py --samples-per-title 5             # quick test
    python scripts/evaluate_extraction.py --models gemma-4-31B-it          # single model
    python scripts/evaluate_extraction.py --prompt-variant few-shot        # test a prompt variant

Pipeline:
    1. Sample sections from DB → eval/sample_sections.json
    2. Run each model on sampled sections → eval/raw_results/
    3. Generate pseudo-GT (majority vote / median)
    4. Compute metrics per model vs pseudo-GT
    5. Self-consistency: re-run best model x2
    6. Test prompt variants on winner
    7. Report → eval/comparison_report.md
"""

import argparse
import json
import os
import random
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.etl.transformers.llm_provider import LLMProvider, TokengoProvider, LocalLLMProvider, OpenRouterProvider
from src.utils.prompt_utils import EXTRACTION_SYSTEM_PROMPT, TITLE_DESCRIPTIONS

# --- Paths ---
EVAL_DIR = Path("eval")
RAW_DIR = EVAL_DIR / "raw_results"
SAMPLES_PATH = EVAL_DIR / "sample_sections.json"
GT_PATH = EVAL_DIR / "ground_truth.json"
REPORT_PATH = EVAL_DIR / "comparison_report.md"

# --- SambaNova config ---
BASE_URL = "https://api.sambanova.ai/v1"
API_KEY_ENV = "SAMBANOVA_API_KEY"

# --- Models to evaluate ---
DEFAULT_MODELS = [
    "gemma-4-31B-it",
    "DeepSeek-V3.2",
    "gpt-oss-120b",
]

# --- Prompt variants ---
PROMPT_VARIANTS: Dict[str, str] = {
    "base": EXTRACTION_SYSTEM_PROMPT,

    "few-shot": EXTRACTION_SYSTEM_PROMPT + """

EJEMPLO:
Texto: "El proveedor deberá presentar la oferta en sobre cerrado. No se aceptarán ofertas electrónicas."
Schema: {"usa_sobre_cerrado": {"type": "binary", "extraction_hint": "Buscar 'sobre cerrado'. Retornar 1 si aparece."}}
Salida esperada: {"campos": {"usa_sobre_cerrado": 1}}
---""",

    "strict": EXTRACTION_SYSTEM_PROMPT + """

REGLAS ADICIONALES:
- No omitas ningún campo del schema. Todos deben aparecer en el JSON de salida.
- Si no estás seguro, usa 0 para binarios y 0 para numéricos.
- El JSON de salida debe tener EXACTAMENTE la estructura {"campos": {...}}.
- No agregues comentarios, explicaciones ni texto fuera del JSON.""",

    "edge": EXTRACTION_SYSTEM_PROMPT + """

CASOS ESPECIALES:
- Si el texto de la sección está vacío o solo contiene el título: retorna 0 para todos los campos binarios, 0 para todos los numéricos.
- Si el texto es muy corto (< 50 caracteres): prioriza extraction_method "literal" sobre "semantic".
- Si no encuentras evidencia clara para un campo, retorna 0 (no infieras presencia).""",
}

# --- Título normalization mapping ---
# master_schemas.json keys -> title_normalized in DB
TITLE_MAP = {
    "Fraude Y Corrupcion": "fraude y corrupcion",
    "Confidencialidad De La Informacion": "confidencialidad de la informacion",
    "Fuerza Mayor": "fuerza mayor",
    "Aclaracion De Las Ofertas": "aclaracion de las ofertas",
    "Interpretacion": "interpretacion",
    "Formato Y Firma De La Oferta": "formato y firma de la oferta",
    "Impuestos Y Derechos": "impuestos y derechos",
    "Derechos Intelectuales": "derechos intelectuales",
    "Retiro Sustitucion Y Modificacion De Las Ofertas": "retiro sustitucion y modificacion de las ofertas",
    "Causales De Terminacion Del Contrato": "causales de terminacion del contrato",
}


@dataclass
class EvalSample:
    title: str
    nro_licitacion: str
    text: str
    title_normalized: str


@dataclass
class ExtractionResult:
    title: str
    nro_licitacion: str
    model: str
    prompt_variant: str
    success: bool
    response_time: float
    values: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    raw_response: str = ""
    n_tokens_input: int = 0
    n_tokens_output: int = 0


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def load_db_config() -> dict:
    """Load DB config from settings."""
    from config.settings import DB_CONFIG
    return DB_CONFIG


def sample_sections(db_params: dict, samples_per_title: int = 20) -> List[EvalSample]:
    """Sample sections for each validated title from the DB using TABLESAMPLE for speed."""
    conn = psycopg2.connect(**db_params)
    samples = []
    for master_title, db_title in TITLE_MAP.items():
        master = json.loads(open("data/processed/section_clustering/master_schemas.json", encoding="utf-8").read())
        if master.get(master_title, {}).get("n_fields", 0) < 3:
            print(f"  [SKIP] {master_title} — no valid schema")
            continue

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Use SYSTEM sampling for speed instead of ORDER BY RANDOM()
            cur.execute(
                """
                SELECT nro_licitacion, content_text, title_normalized
                FROM dncp.pliegos_secciones TABLESAMPLE SYSTEM(1)
                WHERE LOWER(title_normalized) = %s
                  AND content_text IS NOT NULL
                  AND LENGTH(content_text) > 50
                LIMIT %s
                """,
                (db_title, samples_per_title * 2),
            )
            rows = cur.fetchall()
            # Fallback if not enough rows from sample
            if len(rows) < samples_per_title:
                cur.execute(
                    """
                    SELECT nro_licitacion, content_text, title_normalized
                    FROM dncp.pliegos_secciones
                    WHERE LOWER(title_normalized) = %s
                      AND content_text IS NOT NULL
                      AND LENGTH(content_text) > 50
                    LIMIT %s
                    """,
                    (db_title, samples_per_title),
                )
                rows = cur.fetchall()

            random.shuffle(rows)
            for r in rows[:samples_per_title]:
                samples.append(EvalSample(
                    title=master_title,
                    nro_licitacion=str(r["nro_licitacion"]),
                    text=r["content_text"][:2000],
                    title_normalized=r["title_normalized"],
                ))
        print(f"  [OK] {master_title}: {len([r for r in rows if r])} found, {min(len(rows), samples_per_title)} sampled")

    conn.close()
    return samples


# ---------------------------------------------------------------------------
# Extraction prompt builder
# ---------------------------------------------------------------------------

def build_extraction_prompt(
    titulo: str, nro_licitacion: str, texto: str,
    schema: dict, system_prompt: str,
) -> Tuple[str, str]:
    """Build system + user prompt. Same logic as SectionClusteringTransformer.build_section_extraction_prompt."""
    schema_para_extractor = {}
    for field_name, field_def in schema.items():
        entry = {
            "type": field_def["type"],
            "extraction_hint": field_def.get("extraction_hint", field_def.get("description", "")),
        }
        if "extraction_method" in field_def:
            entry["extraction_method"] = field_def["extraction_method"]
        schema_para_extractor[field_name] = entry

    user_prompt = (
        f"TÍTULO DE LA SECCIÓN: {titulo}\n"
        f"DOCUMENTO: {nro_licitacion}\n\n"
        f"SCHEMA A EXTRAER:\n"
        f"{json.dumps(schema_para_extractor, ensure_ascii=False, indent=2)}\n\n"
        f"TEXTO DE LA SECCIÓN:\n"
        f"{texto}\n\n"
        f"Extrae los campos del schema siguiendo exactamente cada extraction_hint."
    )
    return system_prompt, user_prompt


def parse_extraction_response(response_text: str) -> Optional[Dict[str, Any]]:
    """Parse JSON from LLM response, handling {campos: {...}} or flat structure."""
    text = response_text.strip()
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()
    try:
        data = json.loads(text)
        if "campos" in data:
            return data["campos"]
        return data
    except json.JSONDecodeError:
        return None


def validate_extraction(values: Dict[str, Any], schema: dict) -> List[str]:
    """Validate extracted values against schema. Returns errors."""
    errors = []
    for field_name, field_def in schema.items():
        if field_name not in values:
            errors.append(f"Missing field: {field_name}")
            continue
        val = values[field_name]
        ftype = field_def["type"]
        if ftype == "binary":
            if val not in (0, 1):
                errors.append(f"{field_name}: expected binary (0/1), got {val}")
        elif ftype == "numeric":
            if not isinstance(val, (int, float)):
                errors.append(f"{field_name}: expected numeric, got {type(val).__name__}")
    return errors


# ---------------------------------------------------------------------------
# Run model on all samples
# ---------------------------------------------------------------------------

def run_extraction(
    model_id: str,
    samples: List[EvalSample],
    master_schemas: dict,
    prompt_variant: str = "base",
    provider: Optional[LLMProvider] = None,
    max_retries: int = 2,
) -> List[ExtractionResult]:
    """Run extraction for all samples through a single model."""
    print(f"\n{'='*60}")
    print(f"  Model: {model_id} | Prompt variant: {prompt_variant}")
    print(f"  Samples: {len(samples)}")
    print(f"{'='*60}")

    system_prompt = PROMPT_VARIANTS.get(prompt_variant, PROMPT_VARIANTS["base"])
    results = []

    for idx, sample in enumerate(samples):
        schema = master_schemas.get(sample.title, {}).get("json_schema", {})
        if not schema:
            results.append(ExtractionResult(
                title=sample.title, nro_licitacion=sample.nro_licitacion,
                model=model_id, prompt_variant=prompt_variant,
                success=False, response_time=0, errors=["No schema"],
            ))
            continue

        _, user_prompt = build_extraction_prompt(
            sample.title, sample.nro_licitacion, sample.text, schema, system_prompt,
        )
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        # Show progress before each API call
        print(f"    [{idx+1}/{len(samples)}] {sample.title[:30]:30s} {sample.nro_licitacion}...", end=" ", flush=True)

        t0 = time.time()
        response_text = ""
        for attempt in range(max_retries + 1):
            resp = provider.generate(messages, response_format={"type": "json_object"}, max_tokens=1024)
            if not resp:
                print(f"empty response", end=" ", flush=True)
                continue
            values = parse_extraction_response(resp)
            if values and validate_extraction(values, schema) == []:
                response_text = resp
                break
            else:
                errs = validate_extraction(values, schema) if values else ["parse failed"]
                print(f"attempt {attempt+1} failed: {errs[0][:40]}", end=" ", flush=True)
                if attempt < max_retries:
                    print("retrying...", end=" ", flush=True)

        elapsed = time.time() - t0

        if response_text:
            values = parse_extraction_response(response_text)
            result = ExtractionResult(
                title=sample.title, nro_licitacion=sample.nro_licitacion,
                model=model_id, prompt_variant=prompt_variant,
                success=True, response_time=elapsed,
                values=values, raw_response=response_text,
            )
            n_fields = len(values) if values else 0
            print(f"OK ({n_fields} fields, {elapsed:.1f}s)", flush=True)
        else:
            result = ExtractionResult(
                title=sample.title, nro_licitacion=sample.nro_licitacion,
                model=model_id, prompt_variant=prompt_variant,
                success=False, response_time=elapsed,
                errors=["Failed after retries"],
            )
            print(f"FAIL ({elapsed:.1f}s)", flush=True)

        results.append(result)

    ok = sum(1 for r in results if r.success)
    print(f"  [{model_id}] Done: {ok}/{len(results)} OK ({100*ok/len(results):.1f}%)")
    return results


# ---------------------------------------------------------------------------
# Pseudo-GT generation
# ---------------------------------------------------------------------------

def generate_pseudo_gt(all_results: Dict[str, List[ExtractionResult]]) -> Dict[str, Any]:
    """Generate pseudo-ground truth by majority vote (binary) and median (numeric).

    Returns dict keyed by (title, nro_licitacion) -> {field: value}.
    """
    # Group by (title, nro_licitacion)
    groups: Dict[Tuple[str, str], List[ExtractionResult]] = defaultdict(list)
    for model_id, results in all_results.items():
        for r in results:
            if r.success and r.values:
                groups[(r.title, r.nro_licitacion)].append(r)

    gt: Dict[str, Any] = {}
    for (title, nro), results_list in groups.items():
        # Collect all field values
        field_values: Dict[str, List] = defaultdict(list)
        for r in results_list:
            for fname, fval in r.values.items():
                field_values[fname].append(fval)

        entry: Dict[str, Any] = {}
        for fname, vals in field_values.items():
            # Determine type from any model's schema
            is_binary = all(v in (0, 1) for v in vals)
            if is_binary:
                # Majority vote, break ties with 0
                ones = sum(v for v in vals if v == 1)
                zeros = len(vals) - ones
                entry[fname] = 1 if ones > zeros else 0
            else:
                # Numeric: median
                sorted_vals = sorted(vals)
                mid = len(sorted_vals) // 2
                entry[fname] = sorted_vals[mid] if len(sorted_vals) % 2 else (sorted_vals[mid-1] + sorted_vals[mid]) / 2

        key = f"{title}||{nro}"
        gt[key] = {
            "title": title,
            "nro_licitacion": nro,
            "values": entry,
            "n_models": len(results_list),
        }

    print(f"\n[GT] Generated pseudo-ground truth for {len(gt)} sections ({len(all_results)} models)")
    return gt


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class ModelMetrics:
    model: str = ""
    prompt_variant: str = ""
    n_ok: int = 0
    n_total: int = 0
    accuracy_binary: float = 0.0
    precision_binary: float = 0.0
    recall_binary: float = 0.0
    f1_binary: float = 0.0
    mae_numeric: float = 0.0
    agreement_rate: float = 0.0
    avg_time: float = 0.0
    total_time: float = 0.0


def compute_metrics(
    results: List[ExtractionResult],
    pseudo_gt: Dict[str, Any],
) -> ModelMetrics:
    """Compute metrics comparing a model's results against pseudo-GT."""
    m = ModelMetrics()
    m.n_total = len(results)
    m.n_ok = sum(1 for r in results if r.success)

    total_time = sum(r.response_time for r in results if r.success)
    m.total_time = round(total_time, 1)
    m.avg_time = round(total_time / max(m.n_ok, 1), 2)

    # Per-field metrics
    tp = fp = tn = fn = 0
    mae_sum = 0.0
    mae_count = 0
    agreements = 0
    total_comparable = 0

    for r in results:
        if not r.success or not r.values:
            continue
        key = f"{r.title}||{r.nro_licitacion}"
        gt_entry = pseudo_gt.get(key)
        if not gt_entry:
            continue

        gt_vals = gt_entry["values"]
        for fname, gt_val in gt_vals.items():
            pred_val = r.values.get(fname)
            if pred_val is None:
                continue

            total_comparable += 1
            if isinstance(gt_val, int) and gt_val in (0, 1):
                # Binary
                if pred_val == gt_val:
                    agreements += 1
                    if gt_val == 1:
                        tp += 1
                    else:
                        tn += 1
                else:
                    if gt_val == 1:
                        fn += 1
                    else:
                        fp += 1
            else:
                # Numeric
                diff = abs(float(pred_val) - float(gt_val))
                mae_sum += diff
                mae_count += 1
                if abs(diff) < 0.5:
                    agreements += 1

    m.agreement_rate = round(agreements / max(total_comparable, 1), 4)
    m.accuracy_binary = round((tp + tn) / max(tp + tn + fp + fn, 1), 4)
    m.precision_binary = round(tp / max(tp + fp, 1), 4)
    m.recall_binary = round(tp / max(tp + fn, 1), 4)
    m.f1_binary = round(2 * m.precision_binary * m.recall_binary / max(m.precision_binary + m.recall_binary, 0.001), 4)
    m.mae_numeric = round(mae_sum / max(mae_count, 1), 4)

    return m


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def generate_report(
    all_metrics: Dict[str, ModelMetrics],
    self_consistency_metrics: Optional[ModelMetrics] = None,
    prompt_variant_metrics: Optional[Dict[str, ModelMetrics]] = None,
) -> str:
    """Generate markdown comparison report."""
    lines = []
    lines.append("# Evaluación de Modelos — SambaNova Extraction")
    lines.append(f"\nGenerado: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"\n## Resumen por modelo (prompt: base)")
    lines.append(f"\n| Modelo | OK/N | Accuracy | Precision | Recall | F1 | MAE | Agreement | Tiempo |")
    lines.append(f"|--------|:----:|:--------:|:---------:|:-----:|:--:|:---:|:---------:|:------:|")

    sorted_models = sorted(all_metrics.items(), key=lambda x: x[1].f1_binary if x[1].f1_binary > 0 else 0, reverse=True)
    best_model = ""
    best_f1 = 0.0
    for mid, m in sorted_models:
        if m.f1_binary > best_f1:
            best_f1 = m.f1_binary
            best_model = m.model
        lines.append(f"| {m.model:40s} | {m.n_ok}/{m.n_total} | {m.accuracy_binary:.2%} | {m.precision_binary:.2%} | {m.recall_binary:.2%} | {m.f1_binary:.2%} | {m.mae_numeric:.3f} | {m.agreement_rate:.2%} | {m.total_time:.0f}s |")

    if best_model:
        lines.append(f"\n### Ganador: {best_model}")
        lines.append(f"F1: {best_f1:.2%}")

    # Self-consistency
    if self_consistency_metrics:
        lines.append(f"\n## Self-consistency ({self_consistency_metrics.model})")
        lines.append(f"| Métrica | Run 2 vs GT | Run 3 vs GT |")
        lines.append(f"|---------|:-----------:|:-----------:|")
        models_f1 = [v.f1_binary for k, v in all_metrics.items() if k != "pseudo"]
        if models_f1:
            lines.append(f"| F1 promedio | {models_f1[0]:.2%} | ... |")

    # Prompt variants
    if prompt_variant_metrics:
        lines.append(f"\n## Prompt variants ({best_model})")
        lines.append(f"\n| Variante | OK/N | Accuracy | F1 | MAE | Agreement |")
        lines.append(f"|----------|:----:|:--------:|:--:|:---:|:---------:|")
        for vname, vm in sorted(prompt_variant_metrics.items(), key=lambda x: x[1].f1_binary if x[1].f1_binary > 0 else 0, reverse=True):
            lines.append(f"| {vname:20s} | {vm.n_ok}/{vm.n_total} | {vm.accuracy_binary:.2%} | {vm.f1_binary:.2%} | {vm.mae_numeric:.3f} | {vm.agreement_rate:.2%} |")

    lines.append(f"\n## Detalle de errores")
    lines.append(f"\nCada modelo contra pseudo-GT (mayoría de votos):")

    lines.append(f"\n## Recomendación")
    if best_model:
        lines.append(f"\nUsar **{best_model}** en SambaNova para Pipeline 2.")
    lines.append(f"\n---")
    lines.append(f"\nCosto estimado: **~$0.50** para {len(sorted_models)} modelos × 200 secciones")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Evaluate extraction quality across SambaNova models")
    parser.add_argument("--models", nargs="*", default=None,
                        help=f"Models to evaluate (default: {DEFAULT_MODELS})")
    parser.add_argument("--samples-per-title", type=int, default=20,
                        help="Sections to sample per title (default: 20)")
    parser.add_argument("--prompt-variant", default="base",
                        choices=list(PROMPT_VARIANTS.keys()),
                        help="Prompt variant to use (default: base)")
    parser.add_argument("--skip-sample", action="store_true",
                        help="Skip DB sampling, reuse existing sample_sections.json")
    parser.add_argument("--self-consistency", type=int, default=2,
                        help="Extra runs for self-consistency on best model (default: 2)")
    parser.add_argument("--prompt-variants", nargs="*",
                        choices=list(PROMPT_VARIANTS.keys()),
                        help="Test prompt variants on best model")
    parser.add_argument("--provider", default="sambanova",
                        choices=["sambanova", "vllm", "openrouter", "local"],
                        help="LLM provider to use (default: sambanova)")
    args = parser.parse_args()

    models = args.models or DEFAULT_MODELS
    EVAL_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Provider config
    import os
    from config.settings import SAMBANOVA_API_KEY

    def make_provider(model_id: str) -> LLMProvider:
        if args.provider == "sambanova":
            if not SAMBANOVA_API_KEY:
                raise RuntimeError("SAMBANOVA_API_KEY not configured")
            return TokengoProvider(api_key=SAMBANOVA_API_KEY, model=model_id, base_url=BASE_URL)
        elif args.provider == "vllm":
            return LocalLLMProvider(
                base_url="http://localhost:8000/v1",
                model="/mnt/d/llmstudio-models/lmstudio-community/Qwen2.5-14B-Instruct-Q5_K_L/Qwen2.5-14B-Instruct-Q5_K_L.gguf",
            )
        elif args.provider == "openrouter":
            or_key = os.getenv("OPEN_ROUTER_API_KEY") or os.getenv("OPENROUTER_API_KEY", "")
            if not or_key:
                raise RuntimeError("OPEN_ROUTER_API_KEY not configured in .env")
            return OpenRouterProvider(api_key=or_key, model=model_id)
        elif args.provider == "local":
            return LocalLLMProvider(base_url="http://localhost:1234/v1", model="")
        raise ValueError(f"Unknown provider: {args.provider}")

    print(f"Provider: {args.provider}")

    # Load master schemas
    master_path = Path("data/processed/section_clustering/master_schemas.json")
    master_schemas = json.loads(master_path.read_text(encoding="utf-8"))

    # --- Step 1: Sample sections ---
    samples: List[EvalSample] = []
    if args.skip_sample and SAMPLES_PATH.exists():
        print(f"\n=== Loading cached samples from {SAMPLES_PATH} ===")
        raw = json.loads(SAMPLES_PATH.read_text(encoding="utf-8"))
        samples = [EvalSample(**s) for s in raw]
        print(f"  Loaded {len(samples)} samples")
    else:
        print(f"\n=== Sampling sections from DB ({args.samples_per_title}/title) ===")
        db_params = load_db_config()
        samples = sample_sections(db_params, args.samples_per_title)
        # Save
        raw = [{"title": s.title, "nro_licitacion": s.nro_licitacion, "text": s.text, "title_normalized": s.title_normalized} for s in samples]
        SAMPLES_PATH.write_text(json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Saved {len(samples)} samples to {SAMPLES_PATH}")

    # --- Step 2: Run models ---
    all_results: Dict[str, List[ExtractionResult]] = {}
    for model_id in models:
        cache_path = RAW_DIR / f"{model_id.replace('/', '_')}_{args.prompt_variant}.json"
        if cache_path.exists() and not args.skip_sample:
            print(f"\n  [CACHED] Loading {cache_path}")
            raw = json.loads(cache_path.read_text(encoding="utf-8"))
            all_results[model_id] = [ExtractionResult(**r) for r in raw]
            continue

        prov = make_provider(model_id)
        results = run_extraction(model_id, samples, master_schemas, args.prompt_variant, prov)
        all_results[model_id] = results
        # Save raw results
        raw = [{"title": r.title, "nro_licitacion": r.nro_licitacion,
                "model": r.model, "prompt_variant": r.prompt_variant,
                "success": r.success, "response_time": r.response_time,
                "values": r.values, "errors": r.errors, "raw_response": r.raw_response}
               for r in results]
        cache_path.write_text(json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8")

    # --- Step 3: Generate pseudo-GT ---
    print(f"\n{'='*60}")
    print(f"  Generating pseudo-ground truth...")
    print(f"{'='*60}")
    pseudo_gt = generate_pseudo_gt(all_results)
    GT_PATH.write_text(json.dumps(pseudo_gt, indent=2, ensure_ascii=False), encoding="utf-8")

    # --- Step 4: Compute metrics ---
    print(f"\n{'='*60}")
    print(f"  Computing metrics...")
    print(f"{'='*60}")
    all_metrics: Dict[str, ModelMetrics] = {}
    for model_id, results in all_results.items():
        m = compute_metrics(results, pseudo_gt)
        m.model = model_id
        m.prompt_variant = args.prompt_variant
        all_metrics[model_id] = m
        print(f"  {model_id:40s} F1={m.f1_binary:.2%} Acc={m.accuracy_binary:.2%} MAE={m.mae_numeric:.3f}")

    # Find best model
    best_model = max(all_metrics.keys(), key=lambda k: all_metrics[k].f1_binary)
    print(f"\n  => Best model: {best_model} (F1={all_metrics[best_model].f1_binary:.2%})")

    # --- Step 5: Self-consistency ---
    sc_metrics = None
    if args.self_consistency > 0:
        print(f"\n{'='*60}")
        print(f"  Self-consistency: {args.self_consistency} extra runs of {best_model}")
        print(f"{'='*60}")
        sc_results = []
        for run_idx in range(args.self_consistency):
            prov_sc = make_provider(best_model)
            results = run_extraction(best_model, samples, master_schemas, args.prompt_variant, prov_sc)
            sc_results.extend(results)
            # Save
            cache_path = RAW_DIR / f"{best_model.replace('/', '_')}_sc_run{run_idx+1}.json"
            raw = [{"title": r.title, "nro_licitacion": r.nro_licitacion,
                    "model": r.model, "prompt_variant": r.prompt_variant,
                    "success": r.success, "response_time": r.response_time,
                    "values": r.values, "errors": r.errors, "raw_response": r.raw_response}
                   for r in results]
            cache_path.write_text(json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8")

        all_results[f"{best_model}_sc"] = sc_results
        sc_m = compute_metrics(sc_results, pseudo_gt)
        sc_m.model = f"{best_model} (self-consistency)"
        sc_metrics = sc_m

    # --- Step 6: Prompt variants ---
    pv_metrics: Dict[str, ModelMetrics] = {}
    if args.prompt_variants:
        print(f"\n{'='*60}")
        print(f"  Testing prompt variants on {best_model}: {args.prompt_variants}")
        print(f"{'='*60}")
        for variant in args.prompt_variants:
            cache_path = RAW_DIR / f"{best_model.replace('/', '_')}_pv_{variant}.json"
            if cache_path.exists():
                raw = json.loads(cache_path.read_text(encoding="utf-8"))
                results = [ExtractionResult(**r) for r in raw]
            else:
                prov_pv = make_provider(best_model)
                results = run_extraction(best_model, samples, master_schemas, variant, prov_pv)
                raw = [{"title": r.title, "nro_licitacion": r.nro_licitacion,
                        "model": r.model, "prompt_variant": r.prompt_variant,
                        "success": r.success, "response_time": r.response_time,
                        "values": r.values, "errors": r.errors, "raw_response": r.raw_response}
                       for r in results]
                cache_path.write_text(json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8")
            vm = compute_metrics(results, pseudo_gt)
            vm.model = best_model
            vm.prompt_variant = variant
            pv_metrics[variant] = vm
            print(f"  {variant:20s} F1={vm.f1_binary:.2%} Acc={vm.accuracy_binary:.2%}")

    # --- Step 7: Report ---
    print(f"\n{'='*60}")
    print(f"  Generating report...")
    print(f"{'='*60}")
    report = generate_report(all_metrics, sc_metrics, pv_metrics if pv_metrics else None)
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(f"  Report saved to {REPORT_PATH}")
    print(f"  Ground truth saved to {GT_PATH}")

    # Summary
    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}")
    for mid, m in all_metrics.items():
        if mid == "pseudo":
            continue
        print(f"  {m.model:45s} OK={m.n_ok}/{m.n_total} F1={m.f1_binary:.1%} Acc={m.accuracy_binary:.1%} MAE={m.mae_numeric:.3f} ({m.total_time:.0f}s)")
    print(f"\n  Best model: {best_model}")
    print(f"  To run full Pipeline 2:")
    print(f"    python scripts/run_section_extraction_pipeline.py --model \"{best_model}\"")


if __name__ == "__main__":
    main()

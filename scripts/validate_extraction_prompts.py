"""Validate extraction prompts against real section samples.

Reads a schema from master_schemas.json, builds extraction prompts
for real section samples, and calls a local LLM to measure extraction
accuracy. Supports multiple runs with different samples for variance
measurement.

Usage:
    # Single run (default):
    python scripts/validate_extraction_prompts.py --title "idioma de la oferta" --samples 16

    # Double validation (recommended):
    python scripts/validate_extraction_prompts.py --title "idioma de la oferta" --samples 16 --runs 2

    # With OpenRouter:
    python scripts/validate_extraction_prompts.py --title "idioma de la oferta" --samples 16 --runs 2 --provider openrouter
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure project root is on sys.path so src/ and config/ resolve
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

MASTER_SCHEMAS_PATH = Path("data/processed/section_clustering/master_schemas.json")

EXTRACTION_SYSTEM_PROMPT = """Eres un extractor de informaci\u00f3n estructurada de documentos legales paraguayos.

Recibes el texto de una secci\u00f3n de un pliego de licitaci\u00f3n p\u00fablica y un schema
con campos a extraer. Tu tarea es extraer el valor de cada campo siguiendo
exactamente las instrucciones del extraction_hint de ese campo.

REGLAS:
1. Extrae SOLO los campos del schema. No agregues campos adicionales.
2. Para campos binarios: retorna exactamente 0 o 1 (entero, no booleano).
3. Para campos num\u00e9ricos: retorna el n\u00famero exacto seg\u00fan el hint. Si no aplica, retorna 0.
4. Si el texto est\u00e1 vac\u00edo o es ilegible, retorna el valor por defecto (0 para binarios, 0 para num\u00e9ricos).
5. Para campos con extraction_method "literal": busca coincidencia textual exacta, no infieras.
6. Para campos con extraction_method "semantic": razona sobre el significado del texto.
7. Para campos con extraction_method "structural": analiza la estructura (longitud, cantidad).
8. No expliques tu razonamiento. Retorna solo el JSON de salida.

Formato de salida DEBE SER EXACTAMENTE:
{
  "campos": {
    "nombre_campo": valor,
    ...
  }
}"""


def build_extraction_prompt(
    title: str, nro_licitacion: str, texto: str,
    schema: dict, importance_threshold: int = 0
) -> Tuple[str, str]:
    """Build system + user prompt for extracting fields from a single section.

    Replicates SectionClusteringTransformer.build_section_extraction_prompt()
    logic so this script has zero dependencies on the pipeline code.
    """
    schema_para_extractor = {}
    for field_name, field_def in schema.items():
        imp = field_def.get("importance", 0)
        if isinstance(imp, str):
            try:
                imp = int(imp)
            except (ValueError, TypeError):
                imp = 0
        if imp >= importance_threshold:
            entry = {
                "type": field_def.get("type", "binary"),
                "extraction_hint": field_def.get(
                    "extraction_hint", field_def.get("description", "")
                ),
            }
            if "extraction_method" in field_def:
                entry["extraction_method"] = field_def["extraction_method"]
            schema_para_extractor[field_name] = entry

    user_prompt = (
        f"T\u00cdTULO DE LA SECCI\u00d3N: {title}\n"
        f"DOCUMENTO: {nro_licitacion}\n\n"
        f"SCHEMA A EXTRAER:\n"
        f"{json.dumps(schema_para_extractor, ensure_ascii=False, indent=2)}\n\n"
        f"TEXTO DE LA SECCI\u00d3N:\n"
        f"{texto}\n\n"
        f"Extrae los campos del schema siguiendo exactamente cada extraction_hint."
    )
    return EXTRACTION_SYSTEM_PROMPT, user_prompt


def load_master_schema(title: str) -> dict:
    """Load schema data for a title from master_schemas.json."""
    if not MASTER_SCHEMAS_PATH.exists():
        print(f"[X] {MASTER_SCHEMAS_PATH} not found")
        sys.exit(1)

    master = json.loads(MASTER_SCHEMAS_PATH.read_text(encoding="utf-8"))

    if title not in master:
        print(f"[X] Title '{title}' not found")
        print(f"  Available: {list(master.keys())}")
        sys.exit(1)

    return master[title]


def collect_samples(
    title_data: dict, n_samples: int, exclude_nros: Optional[set] = None
) -> List[dict]:
    """Collect samples from master_schemas.json, stratified by cluster.

    Args:
        exclude_nros: set of nro_licitacion strings to skip (for re-sampling
                      across runs to avoid duplicates).
    """
    samples_pool = title_data.get("samples_per_cluster", {})
    if not samples_pool:
        print("[X] No samples_per_cluster found in master_schemas.json")
        sys.exit(1)

    if exclude_nros is None:
        exclude_nros = set()

    # Stratified: up to ceil(n_samples / n_clusters) per cluster
    n_clusters = max(1, len(samples_pool))
    per_cluster = max(1, n_samples // n_clusters)
    selected = []
    seen_nros = set(exclude_nros)
    for cid_str in sorted(samples_pool.keys(), key=lambda x: int(x)):
        cluster_selected = 0
        for s in samples_pool[cid_str]:
            nro = str(s.get("nro_licitacion", ""))
            if nro not in seen_nros:
                s_copy = dict(s)
                s_copy["cluster_id"] = cid_str
                selected.append(s_copy)
                seen_nros.add(nro)
                cluster_selected += 1
                if cluster_selected >= per_cluster:
                    break

    # Trim to exact n_samples
    selected = selected[:n_samples]
    print(f"  Collected {len(selected)} samples across {n_clusters} clusters")
    return selected


def map_cluster_to_expected(
    cluster_id: str, cluster_values: dict,
    sample_overrides: Optional[dict] = None, nro_licitacion: Optional[str] = None,
) -> dict:
    """Map a raw cluster id to expected values using cluster_values.

    cluster_values keys are like 'cluster_0_1_3_7_9_10_11_13_14_15_16_17_18'.
    Returns dict of field -> expected_value.

    If sample_overrides is provided and nro_licitacion matches a key,
    those values take precedence over cluster_values (per-sample ground truth).
    """
    # Start with cluster-level values
    expected = {}
    cluster_int = int(cluster_id)
    for field_name, cluster_map in cluster_values.items():
        val = None
        for merged_key, merged_val in cluster_map.items():
            # merged_key format: cluster_0_1_3_7_9_10_11_13_14_15_16_17_18
            parts = merged_key.replace("cluster_", "").split("_")
            if str(cluster_int) in parts:
                val = merged_val
                break
        if val is not None:
            expected[field_name] = val

    # Override with per-sample values if available
    if sample_overrides and nro_licitacion:
        overrides = sample_overrides.get(str(nro_licitacion), {})
        if overrides:
            expected.update(overrides)

    return expected


def try_parse_llm_response(raw: str) -> Optional[dict]:
    """Try to parse LLM response, handling various formats."""
    raw = raw.strip()

    # Remove markdown code fences
    if raw.startswith("```"):
        lines = raw.split("\n")
        cleaned = []
        in_code = False
        for line in lines:
            if line.strip().startswith("```"):
                in_code = not in_code
                continue
            if in_code:
                cleaned.append(line)
        if cleaned:
            raw = "\n".join(cleaned)

    # Try direct JSON parse
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Try to find JSON object in the text
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            try:
                data = json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                return None
        else:
            return None

    # Normalize: the response might have "campos" wrapper or direct fields
    if "campos" in data and isinstance(data["campos"], dict):
        return data["campos"]
    # Direct field mapping (no wrapper)
    field_keys = [k for k in data if k != "justification"]
    if field_keys:
        return {k: data[k] for k in field_keys}
    return None


def compute_accuracy(
    predicted: dict, expected: dict
) -> Tuple[int, int, int, int, float]:
    """Compare predicted vs expected, return TP, TN, FP, FN, accuracy."""
    tp = tn = fp = fn = 0
    for field, exp_val in expected.items():
        pred_val = predicted.get(field)
        if pred_val is None:
            fn += 1
            continue
        if exp_val == 1 and pred_val == 1:
            tp += 1
        elif exp_val == 0 and pred_val == 0:
            tn += 1
        elif exp_val == 1 and pred_val == 0:
            fn += 1
        elif exp_val == 0 and pred_val == 1:
            fp += 1
        else:
            # Numeric or other: check approximate match
            try:
                if float(pred_val) == float(exp_val):
                    tp += 1
                else:
                    fn += 1
            except (ValueError, TypeError):
                fn += 1

    total = tp + tn + fp + fn
    accuracy = tp / total if total > 0 else 0.0
    return tp, tn, fp, fn, accuracy



def run_validation_runs(
    title_data: dict, schema: dict, title: str,
    cluster_values: dict, provider,
    n_samples: int, n_runs: int,
    sample_overrides: Optional[dict] = None,
) -> dict:
    """Run N validation runs with different samples, report final verdict.

    Returns dict with keys: runs (list of per-run accuracy dicts),
    min_accuracy, max_accuracy, avg_accuracy, passed (bool).
    """
    all_run_accuracies = []
    all_run_field_stats = []
    all_run_failed_fields = []

    for run_idx in range(n_runs):
        print(f"\n{'#'*60}")
        print(f"  RUN {run_idx + 1}/{n_runs}")
        print(f"{'#'*60}")

        # Collect samples, excluding nros from previous runs
        exclude_nros = set()
        for prev in all_run_accuracies:
            exclude_nros.update(prev.get("sample_nros", []))

        samples = collect_samples(title_data, n_samples, exclude_nros=exclude_nros)

        if len(samples) == 0:
            print(f"  [WARN] No more unique samples available for run {run_idx + 1}")
            print(f"  Reusing samples from previous run")
            samples = collect_samples(title_data, n_samples)

        # Run auto validation for this run
        run_result = _run_single_auto_mode(
            samples, schema, title, cluster_values, provider,
            sample_overrides=sample_overrides,
        )

        run_accuracy = run_result["avg_accuracy"]
        run_failed = run_result["failed_fields"]
        run_sample_nros = [s.get("nro_licitacion", "") for s in samples]

        all_run_accuracies.append({
            "run": run_idx + 1,
            "accuracy": run_accuracy,
            "sample_nros": run_sample_nros,
            "failed_fields": run_failed,
        })
        all_run_field_stats.append(run_result["field_stats"])

        print(f"\n  RUN {run_idx + 1} RESULT: {run_accuracy:.1%} accuracy")
        if run_failed:
            print(f"  Fields < 85%: {', '.join(run_failed)}")

    # Compute final verdict
    accuracies = [r["accuracy"] for r in all_run_accuracies]
    min_acc = min(accuracies) if accuracies else 0.0
    max_acc = max(accuracies) if accuracies else 0.0
    avg_acc = sum(accuracies) / len(accuracies) if accuracies else 0.0

    # Merge failed fields across runs
    all_failed = set()
    for r in all_run_accuracies:
        all_failed.update(r.get("failed_fields", []))

    # APTO if ALL runs >= 85%
    passed = min_acc >= 0.85

    # Final report
    print(f"\n{'='*60}")
    print(f"  DOUBLE VALIDATION RESULT: {title}")
    print(f"{'='*60}")
    print(f"  Runs: {n_runs} | Samples per run: {n_samples}")
    print()
    for r in all_run_accuracies:
        status = "PASS" if r["accuracy"] >= 0.85 else "FAIL"
        print(f"  RUN {r['run']}: {r['accuracy']:.1%}  [{status}]")
    print()
    print(f"  Min:  {min_acc:.1%}")
    print(f"  Max:  {max_acc:.1%}")
    print(f"  Avg:  {avg_acc:.1%}")
    if all_failed:
        print(f"  Fields < 85% in any run: {', '.join(sorted(all_failed))}")
    else:
        print(f"  Fields < 85% in any run: ninguno")
    print()
    if passed:
        print(f"  [OK] Schema APTO (min accuracy {min_acc:.1%} >= 85%)")
    else:
        print(f"  [WARN] Schema NO APTO (min accuracy {min_acc:.1%} < 85%)")
        print(f"  Revisar extraction_hints de los campos con menor accuracy")

    return {
        "runs": all_run_accuracies,
        "min_accuracy": min_acc,
        "max_accuracy": max_acc,
        "avg_accuracy": avg_acc,
        "passed": passed,
        "failed_fields": sorted(all_failed),
    }


def _run_single_auto_mode(
    samples: List[dict], schema: dict, title: str,
    cluster_values: dict, provider,
    sample_overrides: Optional[dict] = None,
) -> dict:
    """Run a single auto validation pass. Returns per-field stats and accuracy."""
    field_stats: Dict[str, dict] = {}
    results = []

    for i, sample in enumerate(samples):
        nro = sample.get("nro_licitacion", "?")
        cid = sample.get("cluster_id", "?")
        texto = sample.get("text", "")
        print(f"  [{i+1}/{len(samples)}] nro={nro} cluster={cid}...", end="", flush=True)

        system_prompt, user_prompt = build_extraction_prompt(
            title, nro, texto, schema, importance_threshold=0
        )
        expected = map_cluster_to_expected(
            cid, cluster_values, sample_overrides, nro
        ) if cluster_values else {}

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        raw_response = provider.generate(
            messages, response_format=None, temperature=0.0,
        )
        parsed = try_parse_llm_response(raw_response)

        result = {
            "nro": nro, "cluster": cid, "expected": expected,
            "raw_response": raw_response, "parsed": parsed,
            "success": parsed is not None,
        }
        results.append(result)

        if parsed and expected:
            for field, exp_val in expected.items():
                pred_val = parsed.get(field)
                if field not in field_stats:
                    field_stats[field] = {"tp": 0, "tn": 0, "fp": 0, "fn": 0}
                if pred_val is None:
                    field_stats[field]["fn"] += 1
                elif exp_val == 1 and pred_val == 1:
                    field_stats[field]["tp"] += 1
                elif exp_val == 0 and pred_val == 0:
                    field_stats[field]["tn"] += 1
                elif exp_val == 1 and pred_val == 0:
                    field_stats[field]["fn"] += 1
                elif exp_val == 0 and pred_val == 1:
                    field_stats[field]["fp"] += 1
                else:
                    field_def = schema.get(field, {})
                    is_numeric = field_def.get("type") == "numeric"
                    try:
                        if is_numeric and abs(float(pred_val) - float(exp_val)) <= 15:
                            field_stats[field]["tp"] += 1
                        elif float(pred_val) == float(exp_val):
                            field_stats[field]["tp"] += 1
                        else:
                            field_stats[field]["fn"] += 1
                    except (ValueError, TypeError):
                        field_stats[field]["fn"] += 1

        successes_run = sum(1 for r in results if r["success"])
        print(f" {'OK' if parsed else 'FAIL'} ({successes_run}/{i+1} OK)")

    # Print per-field summary
    successes = sum(1 for r in results if r["success"])
    print(f"\n  Total: {len(samples)} | OK: {successes} | Fail: {len(samples) - successes}")

    if field_stats:
        print(f"\n  {'Field':<45} {'Acc':>6}")
        print(f"  {'-'*45} {'-'*6}")

        total_acc = 0.0
        failed_fields = []
        for field in sorted(field_stats.keys()):
            s = field_stats[field]
            total = s["tp"] + s["tn"] + s["fp"] + s["fn"]
            acc = (s["tp"] + s["tn"]) / total if total > 0 else 0.0
            total_acc += acc
            truncated = field[:44]
            print(f"  {truncated:<45} {acc:>5.1%}")
            if acc < 0.85:
                failed_fields.append(field)

        avg_acc = total_acc / len(field_stats) if field_stats else 0.0
        print(f"  {'-'*45} {'-'*6}")
        print(f"  {'AVERAGE':<45} {avg_acc:>5.1%}")
    else:
        avg_acc = 0.0
        failed_fields = []

    return {
        "avg_accuracy": avg_acc,
        "field_stats": field_stats,
        "failed_fields": failed_fields,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Validate extraction prompts against real section samples"
    )
    parser.add_argument(
        "--title", required=True,
        help="Title to validate (e.g. 'idioma de la oferta')"
    )
    parser.add_argument(
        "--samples", type=int, default=16,
        help="Number of samples per run (default: 16)"
    )
    parser.add_argument(
        "--runs", type=int, default=1,
        help="Number of validation runs with different samples (default: 1, recommended: 2)"
    )
    parser.add_argument(
        "--provider", choices=["local", "openrouter"], default="local",
        help="LLM provider (default: local)"
    )
    parser.add_argument(
        "--llm-base-url", default="http://localhost:1234/v1",
        help="Local LLM base URL (default: http://localhost:1234/v1)"
    )
    parser.add_argument(
        "--model", default="",
        help="Local LLM model name (default: LM Studio's loaded model)"
    )
    parser.add_argument(
        "--threshold", type=int, default=0,
        help="Importance threshold filter (default: 0 = all fields)"
    )
    args = parser.parse_args()

    # Load schema
    title_data = load_master_schema(args.title)
    schema = title_data.get("json_schema", {})
    cluster_values = title_data.get("cluster_values", {})
    sample_overrides = title_data.get("sample_overrides", {})

    if not schema:
        print(f"[X] No json_schema found for '{args.title}'")
        sys.exit(1)

    print(f"\n  Title: {args.title}")
    print(f"  Schema: {len(schema)} fields")
    print(f"  Samples per run: {args.samples}")
    print(f"  Runs: {args.runs}")
    if args.threshold > 0:
        print(f"  Importance threshold: {args.threshold}")

    # Initialize LLM provider
    if args.provider == "local":
        from src.etl.transformers.llm_provider import LocalLLMProvider
        provider = LocalLLMProvider(base_url=args.llm_base_url, model=args.model)
    else:
        from src.etl.transformers.llm_provider import OpenRouterProvider
        import os
        api_key = os.environ.get("OPENROUTER_API_KEY", "")
        if not api_key:
            try:
                from dotenv import load_dotenv
                load_dotenv()
                api_key = os.environ.get("OPENROUTER_API_KEY", "")
            except ImportError:
                pass
        if not api_key:
            print("[X] OPENROUTER_API_KEY not found")
            print("  Set it in .env or as environment variable")
            sys.exit(1)
        provider = OpenRouterProvider(api_key=api_key)

    if args.runs > 1:
        run_validation_runs(
            title_data, schema, args.title, cluster_values, provider,
            n_samples=args.samples, n_runs=args.runs,
            sample_overrides=sample_overrides,
        )
    else:
        samples = collect_samples(title_data, args.samples)
        _run_single_auto_mode(
            samples, schema, args.title, cluster_values, provider,
            sample_overrides=sample_overrides,
        )


if __name__ == "__main__":
    main()

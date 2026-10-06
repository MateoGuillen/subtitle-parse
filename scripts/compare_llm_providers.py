"""Compare two models via Tokengo API for Pipeline 1 schema generation.

Usage:
    python scripts/compare_llm_providers.py                          # GLM-5.1 vs DeepSeek V3.2
    python scripts/compare_llm_providers.py --model-a z-ai/glm-5.1 --model-b deepseek/deepseek-v4-pro
    python scripts/compare_llm_providers.py --reasoning              # both with reasoning_enabled
    python scripts/compare_llm_providers.py --populate best          # run + populate master_schemas.json

Output:
    data/external/llm_responses/<model_a_name>/<title>.json
    data/external/llm_responses/<model_b_name>/<title>.json
    data/external/llm_responses/comparison_report.md
    data/external/llm_responses/summary.json
"""

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config.settings  # noqa: F401 — loads .env via load_dotenv()
from src.etl.transformers.llm_provider import TokengoProvider

PROMPTS_PATH = Path("data/processed/section_clustering/llm_chat_prompts.json")
MASTER_PATH = Path("data/processed/section_clustering/master_schemas.json")
OUT_DIR = Path("data/external/llm_responses")

VALID_METHODS = {"literal", "semantic", "structural"}
ALLOWED_FIELD_KEYS = {"type", "description", "importance", "extraction_hint", "extraction_method"}


@dataclass
class SchemaMetrics:
    """Metrics for a single schema generation result."""
    title: str = ""
    success: bool = False
    n_attempts: int = 0
    n_fields: int = 0
    n_binary: int = 0
    n_numeric: int = 0
    has_justification: bool = False
    justification_len: int = 0
    has_cluster_values: bool = False
    validation_errors: List[str] = field(default_factory=list)
    field_names: List[str] = field(default_factory=list)
    extraction_methods: Dict[str, int] = field(default_factory=dict)
    has_importance: bool = True
    missing_importance: int = 0
    response_time: float = 0.0
    raw_response: str = ""
    schema_data: dict = field(default_factory=dict)


def parse_schema(response_text: str) -> Optional[dict]:
    """Parse JSON from LLM response, handling markdown code fences."""
    json_str = response_text.strip()
    if "```json" in json_str:
        json_str = json_str.split("```json")[1].split("```")[0].strip()
    elif "```" in json_str:
        json_str = json_str.split("```")[1].split("```")[0].strip()
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def validate_schema(result: dict) -> List[str]:
    """Replicate _validate_schema logic. Returns list of errors (empty = valid)."""
    errors = []
    if "schema" not in result or "justification" not in result:
        errors.append("Missing 'schema' or 'justification' key")
        return errors

    schema = result["schema"]
    if not isinstance(schema, dict):
        errors.append("'schema' must be a dict")
        return errors
    if len(schema) == 0:
        errors.append("Empty schema")
        return errors
    if len(schema) > 12:
        errors.append(f"Too many fields ({len(schema)}), max 12")

    for field_name, field_def in schema.items():
        if not isinstance(field_def, dict):
            errors.append(f"Field '{field_name}' is not a dict")
            continue
        ftype = field_def.get("type")
        if ftype not in ("binary", "numeric"):
            errors.append(f"Field '{field_name}': type='{ftype}' (must be binary/numeric)")
        if not field_def.get("description", "").strip():
            errors.append(f"Field '{field_name}': empty description")
        method = field_def.get("extraction_method")
        if method is not None and method not in VALID_METHODS:
            errors.append(f"Field '{field_name}': extraction_method='{method}' invalid")
        if "extraction_hint" not in field_def:
            errors.append(f"Field '{field_name}': missing extraction_hint")
        extra = set(field_def.keys()) - ALLOWED_FIELD_KEYS
        if extra:
            errors.append(f"Field '{field_name}': extra keys: {extra}")

    if not result.get("justification", "").strip():
        errors.append("Empty justification")

    return errors


def measure_schema(label: str, title: str, schema_data: dict, n_attempts: int,
                   response_time: float, raw_response: str) -> SchemaMetrics:
    """Measure quality metrics from a parsed schema response."""
    m = SchemaMetrics(
        title=title,
        n_attempts=n_attempts,
        response_time=response_time,
        raw_response=raw_response,
        schema_data=schema_data,
    )

    if not schema_data or "schema" not in schema_data:
        m.validation_errors.append("No schema in response")
        return m

    errors = validate_schema(schema_data)
    m.validation_errors = errors
    m.success = len(errors) == 0

    schema = schema_data["schema"]
    m.n_fields = len(schema)
    m.field_names = list(schema.keys())

    for fname, fdef in schema.items():
        if fdef.get("type") == "binary":
            m.n_binary += 1
        elif fdef.get("type") == "numeric":
            m.n_numeric += 1
        meth = fdef.get("extraction_method")
        if meth:
            m.extraction_methods[meth] = m.extraction_methods.get(meth, 0) + 1
        if "importance" not in fdef:
            m.has_importance = False
            m.missing_importance += 1

    m.has_justification = bool(schema_data.get("justification", "").strip())
    m.justification_len = len(schema_data.get("justification", ""))
    m.has_cluster_values = "cluster_values" in schema_data

    return m


def save_response(provider_dir: Path, title: str, response_text: str, metrics: SchemaMetrics):
    """Save raw response + metadata for a single title."""
    provider_dir.mkdir(parents=True, exist_ok=True)
    safe = title.lower().replace(" ", "_").replace("/", "_")
    path = provider_dir / f"{safe}.json"
    data = {
        "title": title,
        "success": metrics.success,
        "n_attempts": metrics.n_attempts,
        "response_time_s": round(metrics.response_time, 2),
        "n_fields": metrics.n_fields,
        "validation_errors": metrics.validation_errors,
        "field_names": metrics.field_names,
        "raw_response": response_text,
        "schema_data": metrics.schema_data,
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def run_provider(
    provider: TokengoProvider,
    label: str,
    prompts_data: list,
    titles_to_run: Optional[List[str]] = None,
    use_cache: bool = True,
) -> Dict[str, SchemaMetrics]:
    """Run schema generation for all titles through a provider.
    
    If use_cache=True and a cached response exists in data/external/llm_responses/<label>/,
    the API call is skipped and cached results are used.
    """
    out_dir = OUT_DIR / label
    out_dir.mkdir(parents=True, exist_ok=True)
    results: Dict[str, SchemaMetrics] = {}

    for entry in prompts_data:
        title = entry["title"]
        if titles_to_run and title not in titles_to_run:
            continue

        # Check cache
        safe = title.lower().replace(" ", "_").replace("/", "_")
        cache_path = out_dir / f"{safe}.json"
        cached_hit = False

        if use_cache and cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if cached.get("success") and cached.get("schema_data", {}).get("schema"):
                    m = SchemaMetrics(
                        title=title,
                        success=True,
                        n_attempts=cached.get("n_attempts", 1),
                        response_time=0,
                        raw_response=cached.get("raw_response", ""),
                        schema_data=cached.get("schema_data", {}),
                    )
                    m.n_fields = len(cached.get("schema_data", {}).get("schema", {}))
                    m.validation_errors = []
                    results[title] = m
                    print(f"  [{label}] {title}... CACHED ({m.n_fields} fields)")
                    cached_hit = True
            except (json.JSONDecodeError, KeyError):
                pass

        if cached_hit:
            continue

        print(f"  [{label}] {title}...", end=" ", flush=True)
        messages = [
            {"role": "system", "content": entry["prompt"]["system"]},
            {"role": "user", "content": entry["prompt"]["user"]},
        ]

        t0 = time.time()
        response_text = ""
        n_attempts = 0
        max_attempts = 3

        for attempt in range(max_attempts):
            n_attempts = attempt + 1
            resp = provider.generate(
                messages, response_format={"type": "json_object"}
            )
            if not resp:
                continue
            schema_data = parse_schema(resp)
            if not schema_data or "schema" not in schema_data:
                continue
            errors = validate_schema(schema_data)
            if len(errors) == 0:
                response_text = resp
                break
            else:
                print(f"\n    Attempt {attempt+1} failed validation: {errors[0][:60]}")
                if attempt < max_attempts - 1:
                    print(f"    Retrying...", end=" ", flush=True)

        elapsed = time.time() - t0

        if not response_text:
            print(f"FAILED ({n_attempts} attempts)")
            m = SchemaMetrics(title=title, success=False, n_attempts=n_attempts,
                             response_time=elapsed, validation_errors=["No valid response"])
        else:
            schema_data = parse_schema(response_text)
            m = measure_schema(label, title, schema_data, n_attempts, elapsed, response_text)
            status = "OK" if m.success else f"VALIDATION_FAILED ({len(m.validation_errors)} errors)"
            print(f"{status} ({m.n_fields} fields, {elapsed:.1f}s, {n_attempts} attempt(s))")

        results[title] = m
        save_response(out_dir, title, response_text, m)

    return results


def generate_report(
    results_a: Dict[str, SchemaMetrics],
    results_b: Dict[str, SchemaMetrics],
    label_a: str,
    label_b: str,
) -> str:
    """Generate markdown comparison report."""
    lines = []
    lines.append(f"# Comparación de LLM Providers — {label_a} vs {label_b}")
    lines.append(f"\nGenerado: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"\n## Resumen")

    a_ok = sum(1 for m in results_a.values() if m.success)
    a_fail = sum(1 for m in results_a.values() if not m.success)
    b_ok = sum(1 for m in results_b.values() if m.success)
    b_fail = sum(1 for m in results_b.values() if not m.success)

    a_fields = sum(m.n_fields for m in results_a.values())
    b_fields = sum(m.n_fields for m in results_b.values())
    a_time = sum(m.response_time for m in results_a.values())
    b_time = sum(m.response_time for m in results_b.values())

    lines.append(f"\n| Métrica | {label_a} | {label_b} |")
    lines.append(f"|---------|{'-'*len(label_a)}|{'-'*len(label_b)}|")
    lines.append(f"| Validación OK | {a_ok}/{len(results_a)} | {b_ok}/{len(results_b)} |")
    lines.append(f"| Fallos | {a_fail} | {b_fail} |")
    lines.append(f"| Campos totales | {a_fields} | {b_fields} |")
    lines.append(f"| Tiempo total | {a_time:.1f}s | {b_time:.1f}s |")

    lines.append(f"\n## Detalle por título")
    lines.append(f"\n| Título | {label_a} | | {label_b} | |")
    lines.append(f"|--------|{'-'*len(label_a)}|--------|{'-'*len(label_b)}|--------|")
    lines.append(f"| | Campos | Estado | Campos | Estado |")

    for title in results_a:
        a = results_a.get(title)
        b = results_b.get(title)
        a_status = "✅" if a and a.success else "❌"
        b_status = "✅" if b and b.success else "❌"
        a_f = str(a.n_fields) if a else "-"
        b_f = str(b.n_fields) if b else "-"
        lines.append(f"| {title} | {a_f} | {a_status} | {b_f} | {b_status} |")

    lines.append(f"\n## Errores de validación ({label_a})")
    for title, m in results_a.items():
        if m.validation_errors:
            lines.append(f"\n### {title}")
            for e in m.validation_errors:
                lines.append(f"- {e}")

    lines.append(f"\n## Errores de validación ({label_b})")
    for title, m in results_b.items():
        if m.validation_errors:
            lines.append(f"\n### {title}")
            for e in m.validation_errors:
                lines.append(f"- {e}")

    lines.append(f"\n## Análisis de campos")
    lines.append(f"\n### {label_a} — Methods distribution")
    a_methods: Dict[str, int] = {}
    for m in results_a.values():
        for meth, cnt in m.extraction_methods.items():
            a_methods[meth] = a_methods.get(meth, 0) + cnt
    for meth, cnt in sorted(a_methods.items()):
        lines.append(f"- {meth}: {cnt}")

    lines.append(f"\n### {label_b} — Methods distribution")
    b_methods: Dict[str, int] = {}
    for m in results_b.values():
        for meth, cnt in m.extraction_methods.items():
            b_methods[meth] = b_methods.get(meth, 0) + cnt
    for meth, cnt in sorted(b_methods.items()):
        lines.append(f"- {meth}: {cnt}")

    return "\n".join(lines)


def populate_master(results: Dict[str, SchemaMetrics], prompts_data: list, provider_label: str):
    """Populate master_schemas.json with a provider's schemas."""
    master = json.loads(MASTER_PATH.read_text(encoding="utf-8"))
    count = 0
    for entry in prompts_data:
        title = entry["title"]
        m = results.get(title)
        if not m or not m.success or not m.schema_data:
            print(f"  [X] No valid schema for '{title}' from {provider_label}, skipping")
            continue
        sd = m.schema_data
        master[title]["json_schema"] = sd["schema"]
        master[title]["schema_justification"] = sd.get("justification", "")
        if sd.get("cluster_values"):
            master[title]["cluster_values"] = sd["cluster_values"]
        master[title]["llm_provider"] = provider_label.lower().replace(" ", "_")
        master[title]["schema_version"] = "v1"
        master[title]["n_fields"] = len(sd["schema"])
        count += 1

    MASTER_PATH.write_text(json.dumps(master, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n  [OK] Populated master_schemas.json with {count}/{len(prompts_data)} schemas from {provider_label}")


def make_label(model: str) -> str:
    """Convert model ID to a short label."""
    name = model.split("/")[-1] if "/" in model else model
    return name.replace("-", "_").replace(".", "_")


def main():
    parser = argparse.ArgumentParser(
        description="Compare two models via Tokengo API for Pipeline 1 schema generation"
    )
    parser.add_argument("--model-a", default="z-ai/glm-5.1",
                        help="First model ID (default: z-ai/glm-5.1)")
    parser.add_argument("--model-b", default="deepseek/deepseek-v3.2",
                        help="Second model ID (default: deepseek/deepseek-v3.2)")
    parser.add_argument("--reasoning", action="store_true",
                        help="Enable reasoning mode for both models")
    parser.add_argument("--api-key-env", default="TOKENGO_API_KEY",
                        help="Env var name for the API key (default: TOKENGO_API_KEY)")
    parser.add_argument("--base-url", default="https://api.tokengo.com/v1",
                        help="Base URL for the API (default: https://api.tokengo.com/v1)")
    parser.add_argument("--populate", choices=["a", "b", "best"], default=None,
                        help="Populate master_schemas.json with winner's schemas")
    parser.add_argument("--titles", nargs="*", default=None,
                        help="Specific titles to process (default: all)")
    parser.add_argument("--no-cache", action="store_true",
                        help="Bypass file cache and re-run all API calls")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not PROMPTS_PATH.exists():
        print(f"[X] {PROMPTS_PATH} not found. Run clustering pipeline first.")
        return

    # Load API key from specified env var
    api_key = os.environ.get(args.api_key_env, "")
    if not api_key:
        print(f"[X] {args.api_key_env} not configured. Set it in .env")
        return

    prompts_data = json.loads(PROMPTS_PATH.read_text(encoding="utf-8"))
    print(f"Loaded {len(prompts_data)} prompts")
    print(f"Using API: {args.base_url} (key from {args.api_key_env})")

    # Build providers
    models = [
        (args.model_a, "a"),
        (args.model_b, "b"),
    ]
    all_results: Dict[str, Dict[str, SchemaMetrics]] = {}

    for model_id, tag in models:
        label = make_label(model_id)
        print(f"\n=== Running {model_id} ===")
        provider = TokengoProvider(
            api_key=api_key,
            model=model_id,
            reasoning_enabled=args.reasoning,
            base_url=args.base_url,
        )
        all_results[tag] = run_provider(provider, label, prompts_data, args.titles, use_cache=not args.no_cache)

    # Build nice display labels
    label_a = make_label(args.model_a)
    label_b = make_label(args.model_b)

    # Comparison report
    report = generate_report(all_results["a"], all_results["b"], label_a, label_b)
    report_path = OUT_DIR / "comparison_report.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"\n[DONE] Comparison report saved to {report_path}")

    # Winner
    a_ok = sum(1 for m in all_results["a"].values() if m.success)
    b_ok = sum(1 for m in all_results["b"].values() if m.success)
    print(f"\n  {label_a}: {a_ok}/{len(all_results['a'])} valid schemas")
    print(f"  {label_b}: {b_ok}/{len(all_results['b'])} valid schemas")
    if a_ok > b_ok:
        print(f"  => Winner: {label_a} (model {args.model_a})")
        winner_tag = "a"
    elif b_ok > a_ok:
        print(f"  => Winner: {label_b} (model {args.model_b})")
        winner_tag = "b"
    else:
        print(f"  => Tie: both have {a_ok} valid schemas")
        winner_tag = "a"  # default tiebreaker

    # Populate
    if args.populate:
        target = winner_tag if args.populate == "best" else args.populate
        target_label = label_a if target == "a" else label_b
        populate_master(all_results[target], prompts_data, target_label)

    # Summary
    summary = {"winner": winner_tag, "winner_model": args.model_a if winner_tag == "a" else args.model_b}
    for tag, results in all_results.items():
        model_name = args.model_a if tag == "a" else args.model_b
        summary[tag] = {
            "model": model_name,
            "ok": sum(1 for m in results.values() if m.success),
            "fail": sum(1 for m in results.values() if not m.success),
            "total_fields": sum(m.n_fields for m in results.values()),
            "total_time_s": round(sum(m.response_time for m in results.values()), 1),
        }
    summary_path = OUT_DIR / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\n  Summary: {summary_path}")


if __name__ == "__main__":
    main()

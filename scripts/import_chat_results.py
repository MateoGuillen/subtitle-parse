"""Import LLM web chat responses and merge into master_schemas.json.

Reads response files from data/external/llm_web_responses/, parses the
JSON schema + justification from each, compares across providers, and
optionally updates master_schemas.json with the best or merged schema.

Usage:
    python scripts/import_chat_results.py                          # dry-run: show diffs only
    python scripts/import_chat_results.py --apply                  # update master_schemas.json
    python scripts/import_chat_results.py --provider claude        # only import from specific provider
    python scripts/import_chat_results.py --title "fraude y corrupcion"  # only specific title
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

RESPONSES_DIR = Path("data/external/llm_web_responses")
MASTER_SCHEMAS_PATH = Path("data/processed/section_clustering/master_schemas.json")

PROVIDER_MAP = {
    "claude": "claude",
    "deepseek": "deepseek",
    "gpt": "gpt",
}

PROVIDER_LABELS = {
    "claude": "Claude",
    "deepseek": "DeepSeek",
    "gpt": "GPT",
}


def _parse_title_from_prompt(prompt: str) -> str:
    """Extract the title from a schema-design prompt."""
    m = re.search(
        r"TÍTULO DE LA SECCIÓN:\s*(.+?)(?:\n|$)", prompt, re.IGNORECASE
    )
    if m:
        return m.group(1).strip().lower()
    m = re.search(
        r"(?:title|título|sección)[:\s]+(.+?)(?:\n|$)", prompt, re.IGNORECASE
    )
    if m:
        return m.group(1).strip().lower()
    return "unknown"


def parse_response_file(filepath: Path) -> List[dict]:
    """Parse a single web LLM response file.

    The expected format is:
        {json block with schema + justification}
        (optional feedback text)

    Returns a list of parsed entries (usually 1).
    """
    text = filepath.read_text(encoding="utf-8")

    provider = _detect_provider(filepath.name)

    entries = []

    # Try to find JSON block at the beginning of the file
    json_match = re.search(r"(\{[\s\S]*?\n\})", text)
    if not json_match:
        print(f"  ⚠ No JSON found in {filepath.name}")
        return entries

    raw_json = json_match.group(1)
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        print(f"  ⚠ Invalid JSON in {filepath.name}, trying to repair...")
        data = _try_repair_json(raw_json)

    if not data:
        return entries

    schema = data.get("schema", {})
    justification = data.get("justification", "")
    cluster_values = data.get("cluster_values", {})

    if not schema:
        print(f"  ⚠ No 'schema' key in {filepath.name}")
        return entries

    # Determine the title from the prompt (not available in the response directly)
    # We'll need to map responses to titles based on the provider's context
    # For now, mark as "fraude y corrupcion" since all current responses are for that
    title = "fraude y corrupcion"

    entries.append({
        "title": title,
        "provider": provider,
        "provider_label": PROVIDER_LABELS.get(provider, provider),
        "schema": schema,
        "justification": justification,
        "cluster_values": cluster_values,
        "source_file": filepath.name,
    })

    return entries


def _detect_provider(filename: str) -> str:
    name = filename.lower()
    if "claude" in name:
        return "claude"
    if "deepseek" in name:
        return "deepseek"
    if "gpt" in name:
        return "gpt"
    return "unknown"


def _try_repair_json(text: str) -> dict:
    text = re.sub(r",\s*([}\]])", r"\1", text)
    text = re.sub(r"\{\s*,", "{", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def load_all_responses() -> Dict[str, Dict[str, dict]]:
    """Load all web LLM responses grouped by title -> provider -> entry."""
    result: dict[str, dict[str, dict]] = {}

    files = sorted(RESPONSES_DIR.glob("*.txt"))
    if not files:
        print(f"No response files found in {RESPONSES_DIR}")
        return result

    for fpath in files:
        entries = parse_response_file(fpath)
        for entry in entries:
            title = entry["title"]
            provider = entry["provider"]
            result.setdefault(title, {})[provider] = entry

    return result


def get_existing_schema(title: str) -> Optional[dict]:
    """Get the existing json_schema for a title from master_schemas.json."""
    if not MASTER_SCHEMAS_PATH.exists():
        return None
    try:
        master = json.loads(MASTER_SCHEMAS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, FileNotFoundError):
        return None
    title_data = master.get(title)
    if title_data:
        return title_data.get("json_schema", {})
    return None


def compare_schemas(
    title: str, responses: Dict[str, dict], existing_schema: Optional[dict]
) -> dict:
    """Compare schemas from different providers and generate a comparison report."""
    report: dict = {
        "title": title,
        "n_providers": len(responses),
        "providers": {},
        "existing_schema_fields": list(existing_schema.keys()) if existing_schema else [],
        "differences": {},
    }

    all_fields: Dict[str, set] = {}
    for provider, entry in responses.items():
        fields = set(entry["schema"].keys())
        report["providers"][provider] = {
            "n_fields": len(fields),
            "fields": sorted(fields),
        }
        for f in fields:
            all_fields.setdefault(f, set()).add(provider)

    # Find fields not in all providers (differences)
    n_providers = len(responses)
    for field, providers_set in sorted(all_fields.items()):
        if len(providers_set) < n_providers:
            report["differences"][field] = {
                "present_in": sorted(providers_set),
                "absent_in": sorted(
                    [p for p in responses if p not in providers_set]
                ),
            }

    return report


def select_best_schema(
    title: str, responses: Dict[str, dict], existing_schema: Optional[dict]
) -> tuple:
    """Select the best schema based on field count and diversity.

    Returns (schema, justification, provider_name).
    """
    if not responses:
        return (existing_schema or {}, "", "none")

    # Prefer the provider with the most fields, then Claude as tiebreaker
    providers_by_fields = sorted(
        responses.items(),
        key=lambda x: (len(x[1]["schema"]), x[0] == "claude"),
        reverse=True,
    )

    best_provider = providers_by_fields[0][0]
    best_entry = responses[best_provider]

    return (
        best_entry["schema"],
        best_entry["justification"],
        best_provider,
    )


def apply_import(
    title: str, schema: dict, justification: str, provider: str
) -> bool:
    """Update the master_schemas.json with the imported schema."""
    if not MASTER_SCHEMAS_PATH.exists():
        print(f"  [X] {MASTER_SCHEMAS_PATH} not found")
        return False

    master = json.loads(MASTER_SCHEMAS_PATH.read_text(encoding="utf-8"))

    if title not in master:
        print(f"  [X] Title '{title}' not found in master_schemas.json")
        return False

    title_data = master[title]
    title_data["json_schema"] = schema
    title_data["schema_justification"] = justification
    title_data["llm_provider"] = provider
    title_data["imported_from"] = "llm_web_responses"

    MASTER_SCHEMAS_PATH.write_text(
        json.dumps(master, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return True


def print_report(
    report: dict, responses: dict[str, dict], best_provider: str
):
    """Print a human-readable comparison report."""
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"  Title: {report['title']}")
    print(f"  Providers: {report['n_providers']}")
    print(f"{sep}")

    for provider, info in report["providers"].items():
        label = PROVIDER_LABELS.get(provider, provider)
        marker = " [BEST]" if provider == best_provider else ""
        print(f"\n  [{label}]{marker}")
        print(f"    Fields ({info['n_fields']}):")
        for f in info["fields"]:
            print(f"      * {f}")

    if report["differences"]:
        print(f"\n  -- Differences across providers --")
        for field, diff in report["differences"].items():
            print(
                f"    * {field}: only in {diff['present_in']}"
            )

    if report["existing_schema_fields"]:
        existing_set = set(report["existing_schema_fields"])
        new_fields = set()
        for provider_info in report["providers"].values():
            new_fields.update(provider_info["fields"])
        new_fields -= existing_set
        missing_from_new = existing_set
        for provider_info in report["providers"].values():
            missing_from_new -= set(provider_info["fields"])

        if new_fields:
            print(f"\n  -- New fields not in master_schemas.json --")
            for f in sorted(new_fields):
                print(f"    + {f}")
        if missing_from_new:
            print(f"\n  -- Fields in master_schemas.json not in any response --")
            for f in sorted(missing_from_new):
                print(f"    - {f}")


def main():
    parser = argparse.ArgumentParser(
        description="Import LLM web chat responses into master_schemas.json"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes to master_schemas.json (default: dry-run only)",
    )
    parser.add_argument(
        "--provider",
        choices=["claude", "deepseek", "gpt"],
        help="Only import from a specific provider",
    )
    parser.add_argument(
        "--title",
        help="Only process a specific title",
    )
    args = parser.parse_args()

    if not RESPONSES_DIR.exists():
        print(f"Error: responses directory not found: {RESPONSES_DIR}")
        sys.exit(1)

    # Load all responses grouped by title -> provider
    all_responses = load_all_responses()

    if not all_responses:
        print("No responses found.")
        sys.exit(0)

    print(f"Found {sum(len(v) for v in all_responses.values())} response(s) across {len(all_responses)} title(s)")

    total_applied = 0
    total_errors = 0

    for title in sorted(all_responses.keys()):
        if args.title and title != args.title:
            continue

        responses = all_responses[title]
        if args.provider:
            responses = {k: v for k, v in responses.items() if k == args.provider}
            if not responses:
                continue

        existing_schema = get_existing_schema(title)

        report = compare_schemas(title, responses, existing_schema)
        best_schema, best_just, best_provider = select_best_schema(
            title, responses, existing_schema
        )

        print_report(report, responses, best_provider)

        if args.apply:
            success = apply_import(
                title, best_schema, best_just, best_provider
            )
            if success:
                print(f"\n  [OK] Applied schema from {PROVIDER_LABELS.get(best_provider, best_provider)} to '{title}'")
                total_applied += 1
            else:
                total_errors += 1

    sep = "=" * 60
    if args.apply:
        print(f"\n{sep}")
        print(f"  Applied: {total_applied}, Errors: {total_errors}")
    else:
        print(f"\n{sep}")
        print("  Dry-run completed. Use --apply to write changes.")
    print()


if __name__ == "__main__":
    main()

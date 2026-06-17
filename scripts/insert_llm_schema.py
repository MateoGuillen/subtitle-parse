"""Insert an LLM web response schema into master_schemas.json.

Usage:
    python scripts/insert_llm_schema.py --title "idioma de la oferta" --provider deepseek --file data/external/llm_web_responses/deepseek_response_v4.txt
    python scripts/insert_llm_schema.py --title "idioma de la oferta" --provider claude --file data/external/llm_web_responses/claude_response_v4.txt
    python scripts/insert_llm_schema.py --title "fraude y corrupcion" --provider deepseek --file data/external/llm_web_responses/deepseek_reponse.txt
"""

import argparse
import json
import re
import sys
from pathlib import Path


MASTER_SCHEMAS_PATH = Path("data/processed/section_clustering/master_schemas.json")


def parse_response_file(filepath: Path) -> dict:
    """Parse an LLM response file and extract schema + justification + cluster_values."""
    text = filepath.read_text(encoding="utf-8")

    # Find JSON block (the response starts with it before any feedback)
    json_match = re.search(r"(\{[\s\S]*?\n\})", text)
    if not json_match:
        print(f"  [X] No JSON found in {filepath.name}")
        return {}

    raw_json = json_match.group(1)
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        # Try cleaning common issues
        cleaned = re.sub(r",\s*([}\]])", r"\1", raw_json)
        cleaned = re.sub(r"\{\s*,", "{", cleaned)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            print(f"  [X] Invalid JSON in {filepath.name}: {e}")
            return {}

    if "schema" not in data:
        print(f"  [X] No 'schema' key found in {filepath.name}")
        return {}

    return {
        "json_schema": data["schema"],
        "schema_justification": data.get("justification", ""),
        "cluster_values": data.get("cluster_values", {}),
    }


def validate_schema(schema: dict) -> list[str]:
    """Validate schema fields have required params. Return list of warnings."""
    warnings = []
    for fname, fdef in schema.items():
        if "type" not in fdef:
            warnings.append(f"  Field '{fname}': missing 'type'")
        elif fdef["type"] not in ("binary", "numeric"):
            warnings.append(f"  Field '{fname}': invalid type '{fdef['type']}'")
        if "extraction_hint" not in fdef:
            warnings.append(f"  Field '{fname}': missing 'extraction_hint'")
        if "extraction_method" not in fdef:
            warnings.append(f"  Field '{fname}': missing 'extraction_method'")
        if "importance" not in fdef:
            warnings.append(f"  Field '{fname}': missing 'importance'")
    return warnings


def main():
    parser = argparse.ArgumentParser(
        description="Insert an LLM web response schema into master_schemas.json"
    )
    parser.add_argument("--title", required=True, help="Title to update (e.g. 'idioma de la oferta')")
    parser.add_argument("--provider", required=True, choices=["deepseek", "claude", "gpt"], help="LLM provider")
    parser.add_argument("--file", required=True, type=Path, help="Path to response file")
    parser.add_argument("--version", default="v4", help="Schema version label")
    args = parser.parse_args()

    # Parse the response file
    print(f"\nParsing {args.file}...")
    parsed = parse_response_file(args.file)
    if not parsed:
        sys.exit(1)

    schema = parsed["json_schema"]
    n_fields = len(schema)
    print(f"  Fields: {n_fields}")
    print(f"  Has justification: {bool(parsed['schema_justification'])}")
    print(f"  Has cluster_values: {bool(parsed['cluster_values'])}")
    print(f"  Field names: {list(schema.keys())}")

    # Validate schema
    warnings = validate_schema(schema)
    if warnings:
        print(f"\n  [WARN] Validation issues ({len(warnings)}):")
        for w in warnings:
            print(f"    {w}")
    else:
        print(f"\n  [OK] All fields valid")

    # Read master_schemas.json
    if not MASTER_SCHEMAS_PATH.exists():
        print(f"\n  [X] {MASTER_SCHEMAS_PATH} not found")
        sys.exit(1)

    master = json.loads(MASTER_SCHEMAS_PATH.read_text(encoding="utf-8"))

    if args.title not in master:
        print(f"\n  [X] Title '{args.title}' not found in master_schemas.json")
        print(f"  Available titles: {list(master.keys())}")
        sys.exit(1)

    # Update the entry
    title_data = master[args.title]
    title_data["json_schema"] = schema
    title_data["schema_justification"] = parsed["schema_justification"]
    if parsed["cluster_values"]:
        title_data["cluster_values"] = parsed["cluster_values"]
    title_data["llm_provider"] = args.provider
    title_data["schema_version"] = args.version
    title_data["imported_from"] = args.file.name

    # Write back
    MASTER_SCHEMAS_PATH.write_text(
        json.dumps(master, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\n  [OK] Schema inserted for '{args.title}' ({n_fields} fields from {args.provider})")

    # Show summary
    print(f"\n  Summary:")
    print(f"    Old schema: 2 fields (default fallback)")
    print(f"    New schema: {n_fields} fields")
    print(f"    Binary:     {sum(1 for f in schema.values() if f.get('type') == 'binary')}")
    print(f"    Numeric:    {sum(1 for f in schema.values() if f.get('type') == 'numeric')}")
    print(f"    Literal:    {sum(1 for f in schema.values() if f.get('extraction_method') == 'literal')}")
    print(f"    Semantic:   {sum(1 for f in schema.values() if f.get('extraction_method') == 'semantic')}")
    print(f"    Structural: {sum(1 for f in schema.values() if f.get('extraction_method') == 'structural')}")
    if parsed["cluster_values"]:
        n_clusters = len(list(parsed["cluster_values"].values())[0]) if parsed["cluster_values"] else 0
        print(f"    Cluster mappings: {n_clusters}")


if __name__ == "__main__":
    main()

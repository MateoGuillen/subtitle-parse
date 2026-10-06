"""Import Claude/DeepSeek responses and compile into ground truth.
Reads JSON responses from gt_responses/ and consolidates into gt_labels_web.json."""

import json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VAL_DIR = ROOT / "data/processed/validation"
RESP_DIR = VAL_DIR / "gt_responses"
RESP_DIR.mkdir(parents=True, exist_ok=True)

OUT_PATH = VAL_DIR / "gt_labels_web.json"

# Load master schemas for validation
master = json.loads((ROOT / "data/processed/section_clustering/master_schemas.json").read_text(encoding="utf-8"))

# Load samples (for title/nro mapping by index)
raw_path = VAL_DIR / "raw_Llama-3.1-8B-AWQ_base.json"
samples = json.loads(raw_path.read_text(encoding="utf-8"))
seen, samples_dedup = set(), []
for s in samples:
    key = (s["title"], s["nro"])
    if key not in seen:
        seen.add(key)
        samples_dedup.append(s)

by_title = {}
for s in samples_dedup:
    by_title.setdefault(s["title"], []).append(s)

GT_FILE_ORDER = ["000_system_prompt.txt"]
for p in sorted(RESP_DIR.glob("*.json")):
    GT_FILE_ORDER.append(p.name)

print(f"=== GT Import Tool ===")
print(f"Response files expected: {len([p for p in sorted(RESP_DIR.glob('*.json'))])}")
print(f"Titles: {len(by_title)}")

# Help: generate placeholder response files
def generate_placeholders():
    """Create empty template response files if none exist."""
    for idx, (title, ss) in enumerate(sorted(by_title.items()), 1):
        n = min(30, len(ss))
        resp_path = RESP_DIR / f"{idx:03d}_{title.replace(' ', '_')}_respuesta.json"
        if not resp_path.exists():
            placeholder = {str(i): {} for i in range(1, n + 1)}
            resp_path.write_text(json.dumps(placeholder, indent=2), encoding="utf-8")
            print(f"  Created placeholder: {resp_path.name}")

if len([p for p in sorted(RESP_DIR.glob("*.json"))]) == 0:
    print("\nNo response files found. Generating placeholders...")
    generate_placeholders()
    print(f"\nRun: pega las respuestas Claude en:")
    for p in sorted(RESP_DIR.glob("*.json")):
        print(f"  {p}")
    print("\nLuego corre este script de nuevo para compilar.")
    sys.exit(0)


def parse_response(text: str):
    """Extract JSON dict from Claude's response."""
    text = text.strip()
    # Remove markdown fences
    for fence in ["```json", "```"]:
        if fence in text:
            parts = text.split(fence)
            text = parts[1] if len(parts) > 1 else text
            if "```" in text:
                text = text.split("```")[0]
    try:
        data = json.loads(text)
        return data
    except json.JSONDecodeError:
        return None


def find_json_in_text(text: str):
    """Try to find any JSON object/array in raw text."""
    # Try {...}
    for match in re.finditer(r'\{[^{}]*\}', text, re.DOTALL):
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


gt_labels = {}
errors = []
total_fields = 0
match_rate = 0
total_predicted = 0

for idx, (title, ss) in enumerate(sorted(by_title.items()), 1):
    n = min(30, len(ss))
    resp_path = RESP_DIR / f"{idx:03d}_{title.replace(' ', '_')}_respuesta.json"
    if not resp_path.exists():
        print(f"  [SKIP] {title}: {resp_path.name} not found")
        continue

    raw = resp_path.read_text(encoding="utf-8")
    data = parse_response(raw)
    if data is None:
        print(f"  [PARSE FAIL] {title}: Invalid JSON")
        errors.append({"title": title, "error": "PARSE_FAIL", "raw_preview": raw[:200]})
        continue

    schema = master.get(title, {}).get("json_schema", {})
    section_entries = [(str(i+1), ss[i]) for i in range(n) if str(i+1) in data]
    title_ok = 0
    title_total = 0

    for key, s in section_entries:
        predicted = data.get(key, {})
        if not isinstance(predicted, dict):
            errors.append({"title": title, "nro": s["nro"], "error": f"ENTRY_NOT_DICT:{key}={type(predicted).__name__}"})
            continue

        gt_fields = {}
        entry_ok = 0
        entry_total = 0
        for fname, fdef in schema.items():
            val = predicted.get(fname)
            if val is None:
                errors.append({"title": title, "nro": s["nro"], "error": f"MISSING_FIELD:{fname}"})
                continue
            entry_total += 1
            gt_fields[fname] = val
            # Validate type
            expected_type = fdef.get("type", "binary")
            if expected_type == "binary" and val not in (0, 1, 0.0, 1.0):
                errors.append({"title": title, "nro": s["nro"], "field": fname,
                                "error": f"TYPE:{val} (expected binary 0/1)"})
            elif expected_type == "numeric" and not isinstance(val, (int, float)):
                errors.append({"title": title, "nro": s["nro"], "field": fname,
                                "error": f"TYPE:{val} (expected numeric)"})
            else:
                entry_ok += 1

        key_gt = f"{title}||{s['nro']}"
        gt_labels[key_gt] = {
            "title": title,
            "nro": s["nro"],
            "values": gt_fields,
            "fields_valid": entry_ok,
            "fields_total": entry_total,
        }
        title_ok += entry_ok
        title_total += entry_total

    total_fields += title_total
    match_rate += title_ok
    print(f"  [{idx}] {title[:40]:40s} fields OK: {title_ok}/{title_total} ({title_ok/max(title_total,1):.0%})")

# Stats
print(f"\n=== SUMMARY ===")
print(f"Total sections labeled: {len(gt_labels)}")
print(f"Total fields: {total_fields}")
print(f"Fields valid: {match_rate} ({match_rate/max(total_fields,1):.1%})")
print(f"Errors: {len(errors)}")

# Compare against local models (if metrics.json exists)
metrics_path = VAL_DIR / "metrics.json"
if metrics_path.exists():
    local_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    best_local = max(local_metrics.values(), key=lambda m: m.get("f1", 0))
    print(f"\n  Local best: {best_local['model']}/{best_local['prompt']} F1={best_local['f1']:.1%}")
else:
    best_local = None

# Save GT
output = {
    "source": "claude_web_manual",
    "total_sections": len(gt_labels),
    "total_fields": total_fields,
    "fields_valid": match_rate,
    "samples": gt_labels,
    "errors": errors,
    "local_best": best_local,
}
OUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"\nGT saved to {OUT_PATH}")

# Generate quick comparison report
lines = []
lines.append("# GT Validation Report — Ground Truth from Web LLM")
lines.append(f"\n- Source: Claude/DeepSeek web responses")
lines.append(f"- Sections labeled: {len(gt_labels)}")
lines.append(f"- Total fields: {total_fields}")
lines.append(f"- Fields valid (type-checked): {match_rate} ({match_rate/max(total_fields,1):.1%})")
lines.append(f"- Parse errors: {len([e for e in errors if e.get('error') == 'PARSE_FAIL'])}")
lines.append(f"- Field errors: {len([e for e in errors if e.get('error') != 'PARSE_FAIL'])}")
lines.append(f"- Errors detail: {json.dumps(errors[:20], indent=2)}")

# Compare fields per model if raw files exist
lines.append("\n## Accuracy vs Local Models\n")
lines.append("| Model | Prompt | Fields | Match vs GT | Agreement |")
lines.append("|-------|--------|:-----:|:-----------:|:---------:|")

for raw_file in sorted(VAL_DIR.glob("raw_*.json")):
    name_prompt = raw_file.stem.replace("raw_", "")
    if "|" in name_prompt:
        name, prompt = name_prompt.split("|", 1)
    else:
        name, prompt = name_prompt, ""

    local_data = json.loads(raw_file.read_text(encoding="utf-8"))
    matched = 0
    total = 0
    for r in local_data:
        k = f"{r['title']}||{r['nro']}"
        gt_entry = gt_labels.get(k)
        if not gt_entry or not r.get("values"):
            continue
        for fname, gt_val in gt_entry["values"].items():
            pred = r["values"].get(fname)
            if pred is not None:
                total += 1
                if pred == gt_val:
                    matched += 1

    if total > 0:
        rate = matched / total
        lines.append(f"| {name:20s} | {prompt:10s} | {total} | {matched} | {rate:.1%} |")
        print(f"  {name:20s} {prompt:10s}: {matched}/{total} = {rate:.1%} vs GT web")

report_path = VAL_DIR / "gt_validation_report.md"
report_path.write_text("\n".join(lines), encoding="utf-8")
print(f"\nReport: {report_path}")
print("\n=== DONE ===")

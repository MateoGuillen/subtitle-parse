"""Generate batch prompts for manual GT labeling via Claude/DeepSeek web.
Creates one .txt per title with 30 sections ready to copy-paste."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

VAL_DIR = ROOT / "data/processed/validation"
OUT_DIR = VAL_DIR / "gt_prompts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Load samples from raw files (they have text + title + nro)
raw_path = VAL_DIR / "raw_Llama-3.1-8B-AWQ_base.json"
if not raw_path.exists():
    print(f"ERROR: {raw_path} not found. Run validation first.")
    exit(1)

samples = json.loads(raw_path.read_text(encoding="utf-8"))
print(f"Loaded {len(samples)} samples")

# Deduplicate by (title, nro)
seen = set()
samples_dedup = []
for s in samples:
    key = (s["title"], s["nro"])
    if key not in seen:
        seen.add(key)
        samples_dedup.append(s)
print(f"Deduplicated to {len(samples_dedup)} unique samples")

# Group by title
by_title = {}
for s in samples_dedup:
    by_title.setdefault(s["title"], []).append(s)

# Load master schemas
master = json.loads((ROOT / "data/processed/section_clustering/master_schemas.json").read_text(encoding="utf-8"))

# Filter to titles that have >= 10 samples
by_title = {t: ss for t, ss in by_title.items() if len(ss) >= 10}
print(f"Titles with samples: {len(by_title)}")
for t, ss in by_title.items():
    print(f"  {t}: {len(ss)} samples")

# System prompt (paste once at start of conversation)
system_prompt = (
    "Eres un extractor de datos estructurados de pliegos de licitaciones publicas de Paraguay. "
    "Te voy a enviar 9 lotes (1 por titulo), cada uno con un schema y 30 secciones de texto.\n\n"
    "REGLAS:\n"
    "- Responde UNICAMENTE con JSON valido, sin texto adicional\n"
    "- Formato exacto: {\"1\": {...}, \"2\": {...}, ..., \"30\": {...}}\n"
    "- Cada entrada debe contener TODOS los campos del schema de ese titulo\n"
    "- Campos binarios: 1 = si hay evidencia clara en el texto, 0 = si no hay evidencia\n"
    "- Campos numericos: el valor numerico exacto o 0 si no aparece\n"
    "- NO omitas campos. Si no hay evidencia, usa 0\n"
    "- NO agregas explicaciones, comentarios, ni texto fuera del JSON\n\n"
    "Confirma que entendiste con un 'OK'. Luego te envio el primer titulo."
)

# Write system prompt
(system_prompt_path := OUT_DIR / "000_system_prompt.txt").write_text(
    system_prompt, encoding="utf-8"
)
print(f"\nWrote {system_prompt_path}")

# Write one file per title
for idx, (title, ss) in enumerate(sorted(by_title.items()), 1):
    schema = master.get(title, {}).get("json_schema", {})
    n_samples = min(30, len(ss))

    lines = []
    lines.append(f"=== TITULO {idx}/{len(by_title)}: {title} ===\n")
    lines.append(f"SCHEMA ({len(schema)} campos):")
    lines.append(json.dumps(schema, ensure_ascii=False, indent=2))
    lines.append("")

    for i, s in enumerate(ss[:n_samples], 1):
        text = s.get("text", "")[:2000]
        lines.append(f"--- Seccion {i}/{n_samples} ---")
        lines.append(f"NRO: {s['nro']}")
        lines.append(f"TEXTO:")
        lines.append(text)
        lines.append("")

    lines.append(f"=== RESPUESTA ESPERADA ===")
    lines.append(f"Formato JSON con {n_samples} entradas numeradas del 1 al {n_samples}:")
    lines.append("{")
    for i in range(1, min(4, n_samples+1)):
        example = ", ".join(f'"{k}": 0' for k in list(schema.keys())[:3])
        lines.append(f'  "{i}": {{{example}, ...}},')
    if n_samples > 3:
        lines.append(f'  "... hasta {n_samples}": {{...}}')
    lines.append("}")

    file_path = OUT_DIR / f"{idx:03d}_{title.replace(' ', '_')}.txt"
    file_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  [{idx}] {title}: {n_samples} sections -> {file_path.name}")

print(f"\n=== INSTRUCCIONES ===")
print(f"1. Abri Claude/DeepSeek en el navegador -> nueva conversacion")
print(f"2. Pega el contenido de: {system_prompt_path}")
print(f"   El asistente respondera 'OK'")
print(f"3. Por cada archivo 001_*.txt a 009_*.txt:")
print(f"   - Ctrl+A -> Ctrl+C del archivo .txt")
print(f"   - Ctrl+V en el chat -> Enter")
print(f"   - Espera la respuesta JSON")
print(f"   - Ctrl+A -> Ctrl+C de la respuesta")
print(f"   - Pega en: data/processed/validation/gt_responses/{idx:03d}_respuesta.json")
print(f"4. Corre 'python scripts/import_gt_responses.py' para compilar el GT")
print(f"\nTiempo estimado: ~30-45 min para 9 titulos")

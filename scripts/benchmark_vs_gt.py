"""
Benchmark LLM against ground truth CSV (5 problematic fields).
Modified hint is loaded from master_schemas_v2.json.

Usage:
    python scripts/benchmark_vs_gt.py [--phase 1|2|3|4] [--field campo_x]
"""

import sys, re, gc, time, json, csv, io, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from transformers import AutoTokenizer
from vllm import LLM, SamplingParams
from vllm.sampling_params import StructuredOutputsParams
from config.settings import DB_CONFIG
import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_PATH = PROJECT_ROOT / "data" / "processed" / "section_clustering_v2" / "master_schemas_v2.json"
GT_PATH = PROJECT_ROOT / "data" / "processed" / "llm_benchmark" / "ground_truth_5_campos.csv"

MODEL_PATH = "thesven/Meta-Llama-3.1-8B-Instruct-AWQ"
MODEL_QUANT = "awq_marlin"

SYSTEM_PROMPT = (
    "Eres un extractor de datos de pliegos de licitaciones públicas paraguayas (DNCP).\n"
    "Extrae el valor del campo solicitado siguiendo exactamente la guía de extracción.\n"
    "\n"
    "REGLAS ESTRICTAS:\n"
    "- No confundas tipos: si el campo pide días, no extraigas porcentajes (10%) ni montos.\n"
    "- Verifica que el número esté asociado a la unidad correcta (días, %, años).\n"
    "- Los porcentajes tienen el símbolo '%' o texto 'por ciento' junto al número.\n"
    "- No extraigas números de cláusulas, artículos legales, ni listas numeradas.\n"
    "- Si el único número en el texto corresponde a otro concepto (otro campo), responde NULL.\n"
    "- La guía de extracción tiene prioridad sobre estas reglas generales.\n"
    "\n"
    "FORMATO DE RESPUESTA:\n"
    "- Para int/float: responde SOLO con el número (usa punto decimal, sin unidades).\n"
    "- Para binary: responde SOLO con 1 (si se cumple) o 0 (si no).\n"
    "- Para categorical: responde SOLO con la categoría exacta.\n"
    "- Si no encuentras el valor o no es válido, responde exactamente: NULL\n"
    "\n"
    "IMPORTANTE: Escribe el número o NULL en la PRIMERA línea de tu respuesta.\n"
    "Luego puedes explicar en líneas siguientes si es necesario."
)

GUIDED_REGEX_NUMERIC = r'^(-?\d+(?:[.,]\d+)?|NULL)$'

FIELDS_ORDER = [
    "dias_pago_anticipo",
    "dias_solicitud_anticipo",
    "anos_experiencia_minima_tecnica",
    "dias_validez_garantia",
    "pct_monto_referencia_tecnica",
]

TITLE_MAP = {
    "dias_pago_anticipo": "solicitud de pago de anticipo",
    "dias_solicitud_anticipo": "solicitud de pago de anticipo",
    "dias_validez_garantia": "periodo de validez de la garantia de los bienes",
    "anos_experiencia_minima_tecnica": "capacidad tecnica",
    "pct_monto_referencia_tecnica": "capacidad tecnica",
}

def load_schemas():
    with open(SCHEMAS_PATH, encoding="utf-8") as f:
        return json.load(f)

def get_field_schema(field_name: str, schemas: dict) -> dict:
    for slug, schema in schemas.items():
        if slug == "meta":
            continue
        for f in schema["fields"]:
            if f["name"] == field_name:
                return f
    return {}

def build_messages(field_schema: dict, text: str) -> list:
    hint = field_schema.get("extraction_hint", "")
    user_parts = [
        f"Campo a extraer: {field_schema['description']}",
        f"Tipo esperado: {field_schema['type']}",
    ]
    if hint and hint not in ("", "None"):
        user_parts.append(f"Guía de extracción: {hint}")
    user_parts.append(f"\nTexto del pliego:\n{text}")
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "\n".join(user_parts)},
    ]

def parse_guided(raw: str):
    raw = raw.strip()
    if raw.upper() in ("", "NULL", "NONE", "N/A", "NOT FOUND"):
        return None
    if not re.match(r'^(-?\d+(?:[.,]\d+)?|NULL)$', raw):
        return None
    try:
        val_str = raw.replace(",", ".")
        if "." in val_str:
            return float(val_str)
        return int(float(val_str))
    except (ValueError, TypeError):
        return None

def load_gt():
    df = pd.read_csv(GT_PATH, encoding="utf-8-sig")
    def parse_gt(v):
        if pd.isna(v) or str(v).strip().upper() in ("NULL", ""):
            return None
        s = str(v).strip()
        if "." in s:
            return float(s)
        return int(float(s))
    df["gt_parsed"] = df["ground_truth"].apply(parse_gt)
    return df

def fetch_texts(conn, nros: list, title: str):
    """Batch fetch texts, returns dict {nro: text}."""
    cur = conn.cursor()
    placeholders = ",".join("%s" for _ in nros)
    cur.execute(
        f"SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones WHERE nro_licitacion IN ({placeholders}) AND title_normalized = %s",
        tuple(nros) + (title,)
    )
    result = {row[0]: row[1] or "" for row in cur.fetchall()}
    cur.close()
    return result

def run_benchmark(fields_filter=None):
    gt_df = load_gt()
    if fields_filter:
        gt_df = gt_df[gt_df["campo"].isin(fields_filter)]

    schemas = load_schemas()
    conn = psycopg2.connect(**DB_CONFIG)

    # Load tokenizer + model
    print(f"Loading tokenizer + model {MODEL_PATH}...")
    hf_tok = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    llm = LLM(
        model=MODEL_PATH, quantization=MODEL_QUANT,
        max_model_len=8192, max_num_seqs=8,
        gpu_memory_utilization=0.70, enforce_eager=True,
        trust_remote_code=True, dtype="float16",
    )
    sp = SamplingParams(
        max_tokens=64, temperature=0.0,
        structured_outputs=StructuredOutputsParams(regex=GUIDED_REGEX_NUMERIC),
    )

    all_results = []
    for field_name in FIELDS_ORDER:
        subset = gt_df[gt_df["campo"] == field_name]
        if subset.empty:
            continue
        title = TITLE_MAP[field_name]
        fs = get_field_schema(field_name, schemas)
        print(f"\n--- {field_name} ({len(subset)} samples) ---")

        # Batch fetch all texts for this field
        nros = [str(int(r["nro_licitacion"])) for _, r in subset.iterrows()]
        texts = fetch_texts(conn, nros, title)

        field_results = []
        t_start = time.time()
        for _, row in subset.iterrows():
            nro = str(int(row["nro_licitacion"]))
            text = texts.get(nro, "")
            if not text.strip():
                continue
            messages = build_messages(fs, text)
            rendered = hf_tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

            t_req = time.time()
            outputs = llm.generate([rendered], sp)
            resp = outputs[0].outputs[0].text.strip()

            parsed = parse_guided(resp)
            gt_val = row["gt_parsed"]
            is_ok = (parsed == gt_val) or (parsed is None and gt_val is None)

            field_results.append({
                "nro_licitacion": nro,
                "campo": field_name,
                "ground_truth": row["ground_truth"],
                "llm_parsed": parsed,
                "llm_raw": resp[:200],
                "match": "SI" if is_ok else "NO",
            })

        total_time = time.time() - t_start
        n_total = len(field_results)
        n_ok = sum(1 for r in field_results if r["match"] == "SI")
        pct = n_ok / max(n_total, 1) * 100
        print(f"  {n_ok:3d}/{n_total:3d} = {pct:.0f}% | {total_time:.1f}s")
        all_results.extend(field_results)

    conn.close()

    # Print summary
    df = pd.DataFrame(all_results)
    print(f"\n{'='*60}")
    print("  FIELD                         OK/TOTAL     %")
    print(f"{'='*60}")
    for field in FIELDS_ORDER:
        sub = df[df["campo"] == field]
        if sub.empty:
            continue
        ok = (sub["match"] == "SI").sum()
        total = len(sub)
        pct = ok / total * 100
        bar = "#" * int(pct / 5) + " " * (20 - int(pct / 5))
        print(f"  {field:35s} {ok:3d}/{total:<3d}  {pct:5.1f}% |{bar}|")
    total_ok = (df["match"] == "SI").sum()
    total_all = len(df)
    total_pct = total_ok / total_all * 100
    bar = "#" * int(total_pct / 5) + " " * (20 - int(total_pct / 5))
    print(f"  {'─' * 48}")
    print(f"  {'TOTAL':35s} {total_ok:3d}/{total_all:<3d}  {total_pct:5.1f}% |{bar}|")

    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", type=int, choices=[1,2,3,4], help="Run specific phase")
    parser.add_argument("--field", type=str, help="Run single field")
    args = parser.parse_args()

    if args.field:
        fields_filter = [args.field]
    elif args.phase == 1:
        fields_filter = ["dias_validez_garantia"]
    elif args.phase == 2:
        fields_filter = ["pct_monto_referencia_tecnica"]
    elif args.phase == 3:
        fields_filter = ["anos_experiencia_minima_tecnica"]
    elif args.phase == 4:
        fields_filter = ["dias_pago_anticipo", "dias_solicitud_anticipo"]
    else:
        fields_filter = FIELDS_ORDER

    run_benchmark(fields_filter)

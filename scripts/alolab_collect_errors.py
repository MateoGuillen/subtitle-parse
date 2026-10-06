"""
AloLab handcrafted: collect extraction errors against the 12 hybrid GTs (200/campo).

Runs Llama 3.1 8B-AWQ (champion, 100% on 5-campo GT) over the 200-sample GT CSVs
in `data/processed/alolab/gt_por_campo/`. Compares LLM + postprocess to the human
ground truth, exports a JSON report of *errors only* with full pliego text,
ground truth, raw model output, and the current extraction hint.

This is the input for the human/meta-analyst agent to rewrite extraction hints
in `master_schemas_v2.json` (methodology AloLab: collect -> diagnose -> rewrite
hint -> re-run).

Usage (WSL, vLLM 0.23.0, offline API):

    VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/alolab_collect_errors.py
    VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/alolab_collect_errors.py --round 2
    VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/alolab_collect_errors.py --field dias_solicitud_anticipo
    VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/alolab_collect_errors.py --fields dias_solicitud_anticipo,dias_pago_anticipo

Output:
    ``data/processed/alolab/errors_round<N>.json`` — errors with full context
    ``data/processed/alolab/alolab_round<N>.csv`` — per-field summary (aciertos, errores, acc)
"""
import sys, re, json, time, argparse, math, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# vLLM V2 runner no funciona en WSL2 (UVA unavailable) — debe estar seteado
# ANTES de importar vllm.
os.environ.setdefault("VLLM_USE_V2_MODEL_RUNNER", "0")

import pandas as pd
from src.etl.transformers.extraction_postprocess import parse_response

SCHEMAS = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
GT_DIR = Path("data/processed/alolab/gt_por_campo")
OUT_DIR = Path("data/processed/alolab")

# 12 campos hybrid del schema (no queremos los 3 irresolubles por default,
# pero el script los incluye si se piden explícitamente con --fields).
HYBRID_FIELDS = [
    "dias_notificacion_fuerza_mayor",
    "dias_validez_garantia",
    "anos_experiencia_minima_tecnica",
    "dias_pago_maximo",
    "dias_aceptacion_rechazo_factura",
    "num_documentos_pago",
    "ratio_liquidez_minimo",
    "ratio_endeudamiento_maximo",
    "num_componentes_formula",
    "num_pasos_verificacion",
    "dias_solicitud_anticipo",
    "dias_pago_anticipo",
]

# Llama 3.1 8B-AWQ (champion Sesión 14, 100% en 5-campo GT).
MODEL_CFG = {
    "name": "Llama 3.1 8B-AWQ",
    "path": "thesven/Meta-Llama-3.1-8B-Instruct-AWQ",
    "quant": "awq_marlin",
    "thinking_fix": False,
    "merge_system": False,
    "gpu_mem": 0.70,
    "max_len": 8192,
    "dtype": "float16",
}

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


def load_schemas():
    with open(SCHEMAS, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for slug, schema in data.items():
        if slug == "meta":
            continue
        for f in schema["fields"]:
            out[f["name"]] = f
    return out


def get_hint(field_name, field_schemas):
    fs = field_schemas.get(field_name, {})
    return {
        "description": fs.get("description", ""),
        "type": fs.get("type", ""),
        "extraction_hint": fs.get("extraction_hint", ""),
    }


def build_prompt(field_name, text, field_schemas, merge_system=False):
    fs = field_schemas.get(field_name, {})
    hint = fs.get("extraction_hint", "")
    user = [
        f"Campo a extraer: {fs.get('description', '')}",
        f"Tipo esperado: {fs.get('type', '')}",
    ]
    if hint and hint not in ("", "None"):
        user.append(f"Gu\u00eda de extracci\u00f3n: {hint}")
    user.append(f"\nTexto del pliego:\n{text}")
    if merge_system:
        return [{"role": "user", "content": "\n".join([SYSTEM_PROMPT] + user)}]
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "\n".join(user)},
    ]


def to_num(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def match(gt, llm, flex=True):
    if gt is None and llm is None:
        return True
    if gt is None or llm is None:
        return False
    if gt == llm:
        return True
    if flex and gt != 0 and llm != 0 and (abs(gt / llm) == 100.0 or abs(llm / gt) == 100.0):
        return True
    return False


def find_gt_csv(field_name):
    """Busca el CSV del campo en GT_DIR, tolerando sufijos _GT, _gt, _GT (1), etc."""
    if not GT_DIR.exists():
        return None
    # Caso exacto primero
    for suffix in ("_GT.csv", "_gt.csv"):
        p = GT_DIR / f"{field_name}{suffix}"
        if p.exists():
            return p
    # Regex tolerante a ' (1)' u otros sufijos
    for p in GT_DIR.glob("*.csv"):
        if p.name.startswith("errores_"):
            continue
        base = re.sub(r"_GT( \(1\))?\.csv$|_gt( \(1\))?\.csv$", "", p.name, flags=re.I)
        if base == field_name:
            return p
    return None


def main():
    parser = argparse.ArgumentParser(description="AloLab: collect errors over 12 hybrid fields (200 GTs/campo)")
    parser.add_argument("--round", type=int, default=1, help="Número de ronda (default 1)")
    parser.add_argument("--field", type=str, default=None, help="Solo un campo (optional)")
    parser.add_argument("--fields", type=str, default=None, help="Campos separados por coma")
    parser.add_argument("--max-tokens", type=int, default=64, help="Max output tokens (default 64, guided)")
    parser.add_argument("--gpu-mem", type=float, default=None, help="Override gpu_memory_utilization")
    parser.add_argument("--max-num-seqs", type=int, default=32, help="vLLM max_num_seqs (continuous batching)")
    parser.add_argument("--max-len", type=int, default=None, help="Override max_model_len")
    args = parser.parse_args()

    if args.field:
        fields = [args.field]
    elif args.fields:
        fields = [f.strip() for f in args.fields.split(",") if f.strip()]
    else:
        fields = list(HYBRID_FIELDS)

    fields_to_run = []
    for f in fields:
        gt_csv = find_gt_csv(f)
        if gt_csv is None:
            print(f"WARN: no GT CSV encontrado para {f}, skipping")
            continue
        fields_to_run.append((f, gt_csv))

    if not fields_to_run:
        print("ERROR: ningun GT disponible para los campos pedidos")
        sys.exit(2)

    print(f"=== AloLab Ronda {args.round} ===")
    print(f"  Modelo: {MODEL_CFG['name']} ({MODEL_CFG['path']})")
    print(f"  Campos ({len(fields_to_run)}): {', '.join(f for f, _ in fields_to_run)}")
    print(f"  Guided decoding: regex ^(\\d+(?:[.,]\\d+)?|NULL)$")
    print(f"  Output: {OUT_DIR}/errors_round{args.round}.json + alolab_round{args.round}.csv")
    print()

    field_schemas = load_schemas()

    # Cargar GT (text ya viene en el CSV, no hace falta consultar DB)
    gt_data = {}  # campo -> DataFrame
    for field, gt_csv in fields_to_run:
        df = pd.read_csv(gt_csv, encoding="utf-8-sig")
        needed = {"text", "ground_truth", "nro_licitacion"}
        if not needed.issubset(set(df.columns)):
            print(f"ERROR: {gt_csv} no tiene cols necesarias (necesita {needed}, tiene {set(df.columns)})")
            sys.exit(3)
        df["text_clean"] = df["text"].fillna("").astype(str)
        gt_data[field] = df
        n_val = sum(1 for v in df["ground_truth"] if not pd.isna(v))
        n_null = sum(1 for v in df["ground_truth"] if pd.isna(v))
        print(f"  {field:40s}: {len(df):3d} GTs ({n_val} val, {n_null} null) <- {gt_csv.name}")
    print()

    # Inicializar modelo vLLM (offline API, single GPU)
    print("[vLLM] Cargando modelo...")
    from transformers import AutoTokenizer
    from vllm import LLM, SamplingParams
    from vllm.sampling_params import StructuredOutputsParams

    cfg = MODEL_CFG
    hf_tok = AutoTokenizer.from_pretrained(cfg["path"], trust_remote_code=True)
    gpu_mem = args.gpu_mem if args.gpu_mem is not None else cfg["gpu_mem"]

    llm_kwargs = dict(
        model=cfg["path"],
        quantization=cfg["quant"],
        max_model_len=args.max_len if args.max_len else cfg["max_len"],
        max_num_seqs=args.max_num_seqs,
        gpu_memory_utilization=gpu_mem,
        enforce_eager=True,
        trust_remote_code=True,
        dtype=cfg["dtype"],
    )
    llm = LLM(**llm_kwargs)
    sp = SamplingParams(
        max_tokens=args.max_tokens,
        temperature=0.0,
        structured_outputs=StructuredOutputsParams(
            regex=r"^(-?\d+(?:[.,]\d+)?|NULL)$"
        ),
    )
    print(f"[vLLM] Listo. max_num_seqs={args.max_num_seqs}, gpu_mem={gpu_mem}")
    print()

    # Procesar campo por campo. Para cada campo, renderizar TODOS los prompts y
    # correrlos en un único llm.generate() (continuous batching interno de vLLM).
    all_errors = []
    summary_rows = []
    t_total = time.time()

    for field, gt_csv in fields_to_run:
        df = gt_data[field]
        n_total = len(df)
        print(f"[{field}] {n_total} muestras, renderizando prompts...")

        prompts = []
        for _, row in df.iterrows():
            text = row["text_clean"]
            msgs = build_prompt(field, text, field_schemas, merge_system=cfg["merge_system"])
            tmpl_kwargs = {"enable_thinking": False} if cfg["thinking_fix"] else {}
            rendered = hf_tok.apply_chat_template(
                msgs, tokenize=False, add_generation_prompt=True, **tmpl_kwargs,
            )
            prompts.append(rendered)

        # Generar TODOS en un único batch (vLLM hace continuous batching internamente)
        t_field = time.time()
        outputs = llm.generate(prompts, sp)
        elapsed_field = time.time() - t_field

        n_ok = 0
        n_null_match = 0
        n_val_match = 0
        n_val_total = 0
        n_null_total = 0
        field_errors = []

        for i, (_, row) in enumerate(df.iterrows()):
            out = outputs[i].outputs[0].text.strip()
            text = row["text_clean"]
            parsed = parse_response(out, field=field, text=text, strict=True)
            gt_val = to_num(row["ground_truth"])
            ok = match(gt_val, parsed, flex=True)
            if ok:
                n_ok += 1
                if gt_val is None:
                    n_null_match += 1
                else:
                    n_val_match += 1
            if gt_val is not None:
                n_val_total += 1
            else:
                n_null_total += 1
            if not ok:
                field_errors.append({
                    "field": field,
                    "nro": str(row["nro_licitacion"]),
                    "gt": row["ground_truth"],
                    "llm_raw": out[:200],
                    "llm_value": parsed,
                    "current_hint": field_schemas.get(field, {}).get("extraction_hint", ""),
                    "text": text,
                })

        acc = n_ok / n_total * 100 if n_total else 0
        val_acc = n_val_match / n_val_total * 100 if n_val_total else 0
        null_acc = n_null_match / n_null_total * 100 if n_null_total else 0
        fps = n_null_total - n_null_match  # FPs: GT null pero LLM no-null
        fns = n_val_total - n_val_match  # FNs: GT no-null pero LLM null/distinto

        summary_rows.append({
            "campo": field,
            "n": n_total,
            "gt_valor": n_val_total,
            "gt_null": n_null_total,
            "aciertos": n_ok,
            "acc": round(acc, 1),
            "val_acc": round(val_acc, 1),
            "null_acc": round(null_acc, 1),
            "FP": fps,
            "FN": fns,
            "errores": len(field_errors),
            "time_s": round(elapsed_field, 1),
        })

        print(
            f"  {field:40s}: {n_ok}/{n_total} = {acc:5.1f}% (val={val_acc:.0f}% ({n_val_match}/{n_val_total}), null={null_acc:.0f}% ({n_null_match}/{n_null_total})) | "
            f"FP={fps} FN={fns} | {elapsed_field:.0f}s"
        )
        all_errors.extend(field_errors)

    elapsed_total = time.time() - t_total
    total_n = sum(r["n"] for r in summary_rows)
    total_ok = sum(r["aciertos"] for r in summary_rows)
    overall_acc = total_ok / total_n * 100 if total_n else 0

    # Save JSON report (errors only)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUT_DIR / f"errors_round{args.round}.json"
    report = {
        "round": args.round,
        "model": cfg["name"],
        "field_count": len(fields_to_run),
        "total_samples": total_n,
        "total_correct": total_ok,
        "overall_acc": round(overall_acc, 1),
        "elapsed_s": round(elapsed_total, 1),
        "max_tokens": args.max_tokens,
        "errors": all_errors,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)

    # Save CSV summary
    summary_csv = OUT_DIR / f"alolab_round{args.round}.csv"
    pd.DataFrame(summary_rows).to_csv(summary_csv, index=False, encoding="utf-8")

    print()
    print("=" * 80)
    print(f"  TOTAL: {total_ok}/{total_n} = {overall_acc:.1f}% | {len(all_errors)} errores | {elapsed_total:.0f}s")
    print(f"  Reporte errores: {report_path}")
    print(f"  Resumen CSV:     {summary_csv}")
    print("=" * 80)
    print()
    print("  Resumen por campo:")
    for r in sorted(summary_rows, key=lambda x: -x["acc"]):
        print(
            f"    {r['campo']:40s}: {r['acc']:5.1f}% ({r['aciertos']:3d}/{r['n']:3d}) | "
            f"FP={r['FP']:3d} FN={r['FN']:3d}"
        )


if __name__ == "__main__":
    main()

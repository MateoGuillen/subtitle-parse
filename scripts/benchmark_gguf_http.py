"""
Benchmark GGUF (llama-server :1234) vs 100 GT humanas (5 campos).
Harness HTTP OpenAI-compatible, reutilizable por modelo.
Usage:
  python scripts/benchmark_gguf_http.py --name "Granite-4.2-8B" --out Granite_42_8B.json [--extra-params '{"enable_thinking": false}']
"""
import sys
import json
import csv
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests

GT = Path("data/processed/llm_benchmark/ground_truth_5_campos.csv")
SCHEMAS = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
TEXTS_CACHE = Path("data/processed/llm_benchmark/gt_texts.json")
OUT = Path("data/processed/llm_benchmark/multimodel_results")
OUT.mkdir(parents=True, exist_ok=True)

BASE_URL = "http://127.0.0.1:1234/v1/chat/completions"

SYSTEM_PROMPT = (
    "Eres un extractor de datos de pliegos de licitaciones públicas paraguayas (DNCP).\n"
    "Extrae el valor del campo solicitado siguiendo exactamente la guía de extracción.\n\n"
    "REGLAS ESTRICTAS:\n"
    "- No confundas tipos: si el campo pide días, no extraigas porcentajes (10%) ni montos.\n"
    "- Verifica que el número esté asociado a la unidad correcta (días, %, años).\n"
    "- Los porcentajes tienen el símbolo '%' o texto 'por ciento' junto al número.\n"
    "- No extraigas números de cláusulas, artículos legales, ni listas numeradas.\n"
    "- Si el único número en el texto corresponde a otro concepto (otro campo), responde NULL.\n"
    "- La guía de extracción tiene prioridad sobre estas reglas generales.\n\n"
    "FORMATO DE RESPUESTA:\n"
    "- Para int/float: responde SOLO con el número (usa punto decimal, sin unidades).\n"
    "- Para binary: responde SOLO con 1 (si se cumple) o 0 (si no).\n"
    "- Para categorical: responde SOLO con la categoría exacta.\n"
    "- Si no encuentras el valor o no es válido, responde exactamente: NULL\n\n"
    "IMPORTANTE: Escribe el número o NULL en la PRIMERA línea de tu respuesta.\n"
    "Luego puedes explicar en líneas siguientes si es necesario."
)

from src.etl.transformers.extraction_postprocess import parse_response


def get_hint(field_name):
    with open(SCHEMAS, encoding="utf-8") as f:
        schemas = json.load(f)
    for slug, schema in schemas.items():
        if slug == "meta":
            continue
        for ff in schema["fields"]:
            if ff["name"] == field_name:
                return ff
    return {}


def build_prompt(field_name, text):
    fs = get_hint(field_name)
    hint = fs.get("extraction_hint", "")
    user = [f"Campo a extraer: {fs.get('description', '')}",
            f"Tipo esperado: {fs.get('type', '')}"]
    if hint and hint not in ("", "None"):
        user.append(f"Guía de extracción: {hint}")
    user.append(f"\nTexto del pliego:\n{text}")
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "\n".join(user)},
    ]


def call_llm(messages, extra, max_tokens=64, retries=3):
    body = {"messages": messages, "max_tokens": max_tokens,
            "temperature": 0, "cache_prompt": False}
    body.update(extra)
    for attempt in range(retries):
        try:
            r = requests.post(BASE_URL, json=body, timeout=300)
            r.raise_for_status()
            j = r.json()
            return j["choices"][0]["message"].get("content", "").strip()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == retries - 1:
                raise
            print(f"      [retry {attempt+1}/{retries} {e.__class__.__name__}]", flush=True)
            time.sleep(5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--extra-params", default="{}")
    args = ap.parse_args()
    extra = json.loads(args.extra_params)

    rows = []
    with open(GT, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            gt_raw = row["ground_truth"].strip()
            gt_val = None
            if gt_raw not in ("NULL", ""):
                try:
                    gt_val = float(gt_raw)
                except ValueError:
                    gt_val = None
            rows.append({"nro": row["nro_licitacion"], "field": row["campo"],
                         "gt": gt_val, "gt_raw": gt_raw})

    with open(TEXTS_CACHE, encoding="utf-8") as f:
        texts = json.load(f)

    FIELDS = ["dias_validez_garantia", "pct_monto_referencia_tecnica",
              "anos_experiencia_minima_tecnica", "dias_pago_anticipo",
              "dias_solicitud_anticipo"]

    out_file = OUT / args.out
    all_results, done = [], set()
    if out_file.exists():
        try:
            prev = json.load(open(out_file, encoding="utf-8"))
            all_results = prev.get("results", [])
            done = {(r["nro"], r["field"]) for r in all_results}
            print(f"  [resume] {len(all_results)} muestras ya procesadas")
        except (ValueError, KeyError):
            pass

    t0 = time.time()
    for field in FIELDS:
        sub = [r for r in rows if r["field"] == field]
        for row in sub:
            nro = str(int(float(row["nro"])))
            if (nro, field) in done:
                continue
            text = texts.get(nro, "")
            raw = call_llm(build_prompt(field, text), extra, max_tokens=64)
            parsed = parse_response(raw, field=field, text=text, strict=True)
            gt_val = row["gt"]
            ok = ((parsed is None and gt_val is None)
                  or (parsed is not None and gt_val is not None
                      and (parsed == gt_val or abs(parsed - gt_val) < 1e-9)))
            all_results.append({"nro": nro, "field": field, "gt": row["gt_raw"],
                                "llm_raw": raw[:80], "llm_value": parsed, "match": ok})
            done.add((nro, field))
            if len(all_results) % 5 == 0:
                json.dump({"model": args.name,
                           "score": sum(1 for x in all_results if x["match"]),
                           "elapsed": time.time() - t0,
                           "results": all_results},
                          open(out_file, "w", encoding="utf-8"),
                          ensure_ascii=False, indent=1)
            print(f"    {nro} {field[:28]:28s} gt={row['gt_raw']:8s} llm={str(parsed):8s} {'OK' if ok else 'FAIL'}",
                  flush=True)
        ok_f = sum(1 for r in all_results if r["field"] == field and r["match"])
        print(f"    -> {field}: {ok_f}/{len(sub)} = {ok_f/len(sub)*100:.0f}%")

    elapsed = time.time() - t0
    total_ok = sum(1 for x in all_results if x["match"])
    print(f"  TOTAL: {total_ok}/{len(all_results)} = {total_ok/len(all_results)*100:.0f}%  ({elapsed:.0f}s)")
    json.dump({"model": args.name, "score": total_ok, "elapsed": elapsed,
               "results": all_results},
              open(out_file, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()

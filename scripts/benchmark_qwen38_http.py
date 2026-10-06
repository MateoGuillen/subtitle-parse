"""
Benchmark Qwen3.8-27B (llama-server Windows) vs 100 GT humanas (5 campos).
Usa HTTP OpenAI-compatible en localhost:1234, razonamiento desactivado.
"""
import sys, json, csv, time, re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests

GT = Path("data/processed/llm_benchmark/ground_truth_5_campos.csv")
SCHEMAS = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
TEXTS_CACHE = Path("data/processed/llm_benchmark/gt_texts.json")
OUT = Path("data/processed/llm_benchmark/multimodel_results")
OUT.mkdir(parents=True, exist_ok=True)

BASE_URL = "http://127.0.0.1:1234/v1/chat/completions"
MODEL = "qwen38"

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

from src.etl.transformers.extraction_postprocess import parse_response

def get_hint(field_name):
    with open(SCHEMAS, encoding="utf-8") as f:
        schemas = json.load(f)
    for slug, schema in schemas.items():
        if slug == "meta": continue
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
        user.append(f"Gu\u00eda de extracci\u00f3n: {hint}")
    user.append(f"\nTexto del pliego:\n{text}")
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "\n".join(user)}
    ]

def call_llm(messages, max_tokens=64, retries=5):
    body = {
        "model": MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0,
        "reasoning_effort": "none",
        "cache_prompt": False,
    }
    for attempt in range(retries):
        try:
            r = requests.post(BASE_URL, json=body, timeout=120)
            r.raise_for_status()
            j = r.json()
            return j["choices"][0]["message"].get("content","").strip()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == retries - 1:
                raise
            print(f"      [retry {attempt+1}/{retries} after {e.__class__.__name__} - reiniciando servidor]")
            time.sleep(5)
            # intentar reiniciar servidor si está caído
            try:
                requests.get("http://127.0.0.1:1234/health", timeout=3)
            except:
                import subprocess
                print("      [servidor caído, reiniciando...]")
                subprocess.run(["powershell","-Command","Get-Process -Name llama-server -ErrorAction SilentlyContinue | Stop-Process -Force; Start-Sleep 3"], timeout=10)
                subprocess.Popen([
                    "powershell","-Command",
                    "$a=@('-m','C:\\models\\qwen38\\Qwen3.8-27B-UD-Q3_K_XL.gguf','-ngl','999','--parallel','1','-c','8192','--port','1234','--host','127.0.0.1'); Start-Process -FilePath 'C:\\models\\llama_cuda\\llama-server.exe' -ArgumentList $a -WindowStyle Hidden"
                ])
                time.sleep(35)

def main():
    # Load GT
    rows = []
    with open(GT, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # ground_truth may be "NULL" or numeric string
            gt_raw = row["ground_truth"].strip()
            if gt_raw == "NULL" or gt_raw == "":
                gt_val = None
            else:
                try:
                    gt_val = float(gt_raw)
                except:
                    gt_val = None
            rows.append({"nro": row["nro_licitacion"], "field": row["campo"], "gt": gt_val, "gt_raw": gt_raw})

    with open(TEXTS_CACHE, encoding="utf-8") as f:
        texts = json.load(f)

    FIELDS = ["dias_validez_garantia", "pct_monto_referencia_tecnica",
              "anos_experiencia_minima_tecnica", "dias_pago_anticipo", "dias_solicitud_anticipo"]

    out_file = OUT / "Qwen38-27B-UD-Q3_K_XL.json"
    # resume: cargar resultados previos si existen
    done = set()
    all_results = []
    if out_file.exists():
        try:
            with open(out_file, encoding="utf-8") as f:
                prev = json.load(f)
                all_results = prev.get("results", [])
                done = {(r["nro"], r["field"]) for r in all_results}
                print(f"  [resume] {len(all_results)} muestras ya procesadas")
        except:
            all_results = []
            done = set()
    else:
        # buscar log parcial
        partial = Path("C:/models/bench_qwen38.out.log")
        if partial.exists():
            print(f"  [checkpoint] log parcial existe, continuando desde 0")

    t0 = time.time()
    for field in FIELDS:
        sub = [r for r in rows if r["field"] == field]
        print(f"\n  {field}: {len(sub)} muestras")
        ok_field = sum(1 for r in all_results if r["field"]==field and r["match"])
        # contar ya hechos para este campo
        already = len([r for r in all_results if r["field"]==field])
        if already:
            print(f"    [skip {already} ya hechos]")
        for row in sub:
            nro = str(int(float(row["nro"])) if "." in row["nro"] else row["nro"])
            if (nro, field) in done:
                continue
            # try both keys
            text = texts.get(nro, "") or texts.get(str(int(row["nro"])), "")
            if not text:
                text = texts.get(row["nro"], "")
            msgs = build_prompt(field, text)
            raw = call_llm(msgs, max_tokens=32)
            parsed = parse_response(raw, field=field, text=text, strict=True)
            time.sleep(0.15)
            gt_val = row["gt"]
            if parsed is None and gt_val is None:
                ok = True
            elif parsed is not None and gt_val is not None:
                ok = (parsed == gt_val) or (abs(parsed - gt_val) < 1e-9)
            else:
                ok = False
            if ok:
                ok_field += 1
            all_results.append({"nro": nro, "field": field, "gt": row["gt_raw"], "llm_raw": raw[:80], "llm_value": parsed, "match": ok})
            done.add((nro, field))
            # guardado incremental cada 5
            if len(all_results) % 5 == 0:
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump({"model": "Qwen3.8-27B UD-Q3_K_XL (llama.cpp, no-thinking)", "score": sum(1 for x in all_results if x["match"]), "elapsed": time.time()-t0, "results": all_results}, f, ensure_ascii=False, indent=1)
            status = "OK" if ok else "FAIL"
            print(f"    {nro} gt={row['gt_raw']:8s} llm={str(parsed):8s} raw={repr(raw[:40]):50s} {status}")
        # recalcular ok_field total para el campo
        ok_field = sum(1 for r in all_results if r["field"]==field and r["match"])
        total_field = len([r for r in rows if r["field"]==field])
        print(f"    -> {field}: {ok_field}/{total_field} = {ok_field/total_field*100:.0f}%")

    elapsed = time.time() - t0
    total_ok = sum(1 for x in all_results if x["match"])
    print(f"\n{'='*60}")
    print(f"  TOTAL: {total_ok}/100 = {total_ok}%  ({elapsed:.0f}s)")
    print(f"{'='*60}")

    out_file = OUT / "Qwen38-27B-UD-Q3_K_XL.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"model": "Qwen3.8-27B UD-Q3_K_XL (llama.cpp, no-thinking)", "score": total_ok, "elapsed": elapsed, "results": all_results}, f, ensure_ascii=False, indent=1)
    print(f"Guardado: {out_file}")

if __name__ == "__main__":
    main()

"""Benchmark 12 hybrid raw vs PP con reinicio por batch"""
import sys, json, csv, time, re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests
from src.etl.transformers.extraction_postprocess import parse_response

GT5 = Path("data/processed/llm_benchmark/ground_truth_5_campos.csv")
FULL460 = Path("data/processed/llm_benchmark/full_460_with_context.csv")
SCHEMAS = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
GT_TEXTS = Path("data/processed/llm_benchmark/gt_texts.json")
OUT = Path("data/processed/llm_benchmark/multimodel_results/Qwen38-12hybrid-rawpp.json")

BASE_URL = "http://127.0.0.1:1234/v1/chat/completions"
hybrid12 = ['dias_notificacion_fuerza_mayor','dias_validez_garantia','anos_experiencia_minima_tecnica','dias_pago_maximo','dias_aceptacion_rechazo_factura','num_documentos_pago','ratio_liquidez_minimo','ratio_endeudamiento_maximo','num_componentes_formula','num_pasos_verificacion','dias_solicitud_anticipo','dias_pago_anticipo']

import json as js
with open(SCHEMAS, encoding="utf-8") as f:
    schemas=js.load(f)
def get_hint(field):
    for slug,sc in schemas.items():
        if slug=="meta": continue
        for ff in sc["fields"]:
            if ff["name"]==field: return ff
    return {}
SYSTEM_PROMPT = ("Eres un extractor de datos de pliegos de licitaciones p\u00fablicas paraguayas (DNCP).\nExtrae el valor del campo solicitado siguiendo exactamente la gu\u00eda de extracci\u00f3n.\n\nREGLAS ESTRICTAS:\n- No confundas tipos: si el campo pide d\u00edas, no extraigas porcentajes (10%) ni montos.\n- Verifica que el n\u00famero est\u00e9 asociado a la unidad correcta (d\u00edas, %, a\u00f1os).\n- Los porcentajes tienen el s\u00edmbolo '%' o texto 'por ciento' junto al n\u00famero.\n- No extraigas n\u00fameros de cl\u00e1usulas, art\u00edculos legales, ni listas numeradas.\n- Si el \u00fanico n\u00famero en el texto corresponde a otro concepto (otro campo), responde NULL.\n- La gu\u00eda de extracci\u00f3n tiene prioridad sobre estas reglas generales.\n\nFORMATO DE RESPUESTA:\n- Para int/float: responde SOLO con el n\u00famero (usa punto decimal, sin unidades).\n- Para binary: responde SOLO con 1 (si se cumple) o 0 (si no).\n- Para categorical: responde SOLO con la categor\u00eda exacta.\n- Si no encuentras el valor o no es v\u00e1lido, responde exactamente: NULL\n\nIMPORTANTE: Escribe el n\u00famero o NULL en la PRIMERA l\u00ednea de tu respuesta.\nLuego puedes explicar en l\u00edneas siguientes si es necesario.")
def build(field,text):
    fs=get_hint(field)
    hint=fs.get("extraction_hint","")
    user=[f"Campo a extraer: {fs.get('description','')}",f"Tipo esperado: {fs.get('type','')}"]
    if hint and hint not in ("","None"):
        user.append(f"Gu\u00eda de extracci\u00f3n: {hint}")
    user.append(f"\nTexto del pliego:\n{text}")
    return [{"role":"system","content":SYSTEM_PROMPT},{"role":"user","content":"\n".join(user)}]
def raw_parse(s):
    s=s.strip().split('\n')[0].strip()
    if s=='NULL': return None
    m=re.search(r'-?\d+(?:[.,]\d+)?', s)
    if m:
        try: return float(m.group().replace(',','.'))
        except: return None
    return None
def call_llm(msgs, retries=5):
    body={"model":"qwen38","messages":msgs,"max_tokens":32,"temperature":0,"reasoning_effort":"none","cache_prompt":False}
    for attempt in range(retries):
        try:
            r=requests.post(BASE_URL, json=body, timeout=60)
            r.raise_for_status()
            return r.json()["choices"][0]["message"].get("content","").strip()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt==retries-1: raise
            print(f"      [retry {attempt+1} reiniciando servidor]")
            import subprocess
            subprocess.run(["powershell","-Command","Get-Process -Name llama-server -ErrorAction SilentlyContinue | Stop-Process -Force; Start-Sleep 2"], timeout=10)
            subprocess.Popen(["powershell","-Command","$a=@('-m','C:\\models\\qwen38\\Qwen3.8-27B-UD-Q3_K_XL.gguf','-ngl','999','--parallel','1','-c','8192','--port','1234','--host','127.0.0.1'); Start-Process -FilePath 'C:\\models\\llama_cuda\\llama-server.exe' -ArgumentList $a -WindowStyle Hidden"], shell=False)
            time.sleep(35)
def restart_server():
    import subprocess, time
    print("[reinicio servidor batch]")
    subprocess.run(["powershell","-Command","Get-Process -Name llama-server -ErrorAction SilentlyContinue | Stop-Process -Force; Start-Sleep 2"], timeout=10)
    subprocess.Popen(["powershell","-Command","$a=@('-m','C:\\models\\qwen38\\Qwen3.8-27B-UD-Q3_K_XL.gguf','-ngl','999','--parallel','1','-c','8192','--port','1234','--host','127.0.0.1'); Start-Process -FilePath 'C:\\models\\llama_cuda\\llama-server.exe' -ArgumentList $a -WindowStyle Hidden"], shell=False)
    time.sleep(35)

# cargar GT5
gt5_rows=[]
with open(GT5, encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
        gt=None if row["ground_truth"]=="NULL" else float(row["ground_truth"])
        gt5_rows.append((row["campo"], row["nro_licitacion"], gt))
texts=json.load(open(GT_TEXTS, encoding="utf-8"))
# cargar full460 para los 7 faltantes (proxy GT = db_value)
import pandas as pd
df=pd.read_csv(FULL460, encoding="utf-8")
# mapear por (campo,nro) -> (text, db_value)
full_map={}
for _,r in df.iterrows():
    full_map[(r["campo"], str(r["nro_licitacion"]))] = (r["content_text"], r["db_value"])

# construir muestras: 10 por campo (5 de GT5 si existe, resto de full460)
from collections import defaultdict
samples=[]
for field in hybrid12:
    # tomar de GT5 primero
    cur=[(f,n,gt,texts.get(str(int(float(n))) if '.' in n else n,"")) for f,n,gt in gt5_rows if f==field]
    # completar hasta 10 con full460
    need=10-len(cur)
    if need>0:
        for (f,n), (txt, dbv) in full_map.items():
            if f!=field: continue
            if any(x[1]==n for x in cur): continue
            try: gt=float(dbv) if str(dbv) not in ("nan","NULL","") else None
            except: gt=None
            cur.append((f,n,gt,txt))
            if len(cur)>=10: break
    samples.extend(cur[:10])

print(f"Total {len(samples)} muestras (10 por hybrid)")

# procesar por batch de 10 (1 campo) con reinicio entre batches
results=[]
raw_ok=0; pp_ok=0
for idx, (field,nro,gt,text) in enumerate(samples):
    if idx%10==0 and idx>0:
        restart_server()
    if not text:
        print(f"skip {field} {nro} no text")
        continue
    msgs=build(field,text)
    out=call_llm(msgs)
    pp=parse_response(out, field=field, text=text, strict=True)
    raw=raw_parse(out)
    ok_r = (raw is None and gt is None) or (raw is not None and gt is not None and abs(raw-gt)<1e-9)
    ok_p = (pp is None and gt is None) or (pp is not None and gt is not None and abs(pp-gt)<1e-9)
    if ok_r: raw_ok+=1
    if ok_p: pp_ok+=1
    print(f"{idx+1:3d} {field:30s} {nro} gt={str(gt):6s} raw={str(raw):6s} pp={str(pp):6s} {'R' if ok_r else '.'}{'P' if ok_p else '.'} {repr(out[:30])}")
    results.append({"field":field,"nro":nro,"gt":gt,"raw":raw,"pp":pp,"out":out[:60],"ok_raw":ok_r,"ok_pp":ok_p})
    time.sleep(0.15)

print(f"\nRAW: {raw_ok}/{len(samples)}={raw_ok/len(samples)*100:.1f}%  PP: {pp_ok}/{len(samples)}={pp_ok/len(samples)*100:.1f}%")
# guardar
OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    json.dump({"raw":raw_ok,"pp":pp_ok,"n":len(samples),"results":results}, f, ensure_ascii=False, indent=1)
print(f"Guardado {OUT}")

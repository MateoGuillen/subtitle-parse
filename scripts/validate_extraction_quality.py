"""Fase 1 validation: 3 models × 3 prompts × 300 sections.
Batch prompts per sample within each model load for efficiency."""

import sys, os, json, time, random, re
from collections import defaultdict
from pathlib import Path

# HF_TOKEN se lee del entorno (.env vía config.settings); nunca hardcodear tokens
os.environ["VLLM_USE_V1"] = "0"
os.environ["VLLM_USE_V2_MODEL_RUNNER"] = "0"
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DB_CONFIG
import psycopg2
import psycopg2.extras
import gc

N_SAMPLES = 30
TITLES = [
    "Fraude Y Corrupcion", "Confidencialidad De La Informacion", "Fuerza Mayor",
    "Aclaracion De Las Ofertas", "Interpretacion", "Formato Y Firma De La Oferta",
    "Impuestos Y Derechos", "Derechos Intelectuales",
    "Retiro Sustitucion Y Modificacion De Las Ofertas", "Causales De Terminacion Del Contrato",
]
TITLE_MAP = {t: t.lower() for t in TITLES}

MODELS = [
    ("Mistral-7B-AWQ", "solidrust/Mistral-7B-Instruct-v0.3-AWQ",
     {"quantization": "awq_marlin", "max_num_seqs": 64}),
    ("Llama-3.1-8B-AWQ", "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4",
     {"quantization": "awq_marlin", "max_num_seqs": 64}),
    ("Qwen2.5-14B-AWQ", "Qwen/Qwen2.5-14B-Instruct-AWQ",
     {"quantization": "awq", "dtype": "float16", "max_num_seqs": 32}),
]

SYSTEM_PROMPTS = {
    "base": "Eres un extractor de datos estructurados. Responde UNICAMENTE con JSON valido.",
    "strict": "Eres un extractor de datos estructurados. Responde UNICAMENTE con JSON valido. No omitas NINGUN campo del schema. Si no hay evidencia clara usa 0.",
    "few-shot": "Eres un extractor de datos estructurados. Responde UNICAMENTE con JSON valido.\n\nEJEMPLO:\nTexto: El oferente presentó la documentación en un único sobre.\nSchema: {\"unico_sobre\": {\"type\": \"binary\", \"extraction_hint\": \"Buscar 'único sobre' en el texto\"}}\nSalida: {\"unico_sobre\": 1}",
}
PROMPT_NAMES = ["base", "strict", "few-shot"]

OUT_DIR = ROOT / "data/processed/validation"
OUT_DIR.mkdir(parents=True, exist_ok=True)

master = json.loads((ROOT / "data/processed/section_clustering/master_schemas.json").read_text(encoding="utf-8"))


def extract_keywords(hint):
    words = set()
    for m in re.finditer(r"'([^']+)'", hint):
        words.add(m.group(1).lower())
    for m in re.finditer(r'"([^"]+)"', hint):
        words.add(m.group(1).lower())
    if not words:
        hint_clean = re.sub(r'\b(buscar|la|el|en|del|de|los|las|un|una|patron|frase|exacta|palabra|clave|texto|que|contiene|menciona|presencia|ausencia)\b', '', hint, flags=re.IGNORECASE)
        hint_clean = re.sub(r'[^a-záéíóúñA-ZÁÉÍÓÚÑ0-9\s]', ' ', hint_clean)
        words = {w.strip().lower() for w in hint_clean.split() if len(w.strip()) > 3}
    return words


def parse_json(resp):
    text = resp.strip()
    for m in ["```json", "```"]:
        if m in text:
            parts = text.split(m)
            text = parts[1] if len(parts) > 1 else text
            if "```" in text:
                text = text.split("```")[0]
    try:
        data = json.loads(text)
        return data.get("campos", data)
    except json.JSONDecodeError:
        return None


def check_keyword_evidence(text, field_name, hint, value):
    text_lower = text.lower()
    keywords = extract_keywords(hint)
    if not keywords:
        return None
    found = sum(1 for kw in keywords if kw.lower() in text_lower)
    ratio = found / len(keywords)
    if value == 1 and ratio < 0.3:
        return "LOW_EVIDENCE"
    if value == 0 and ratio > 0.7:
        return "CONTRADICTS"
    if value == 1:
        return "OK"
    return "NO_EVIDENCE"


def main():
    # Step 1: Sample sections
    print("=== Sampling sections ===")
    conn = psycopg2.connect(**DB_CONFIG)
    samples = []
    for title, db_title in TITLE_MAP.items():
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT nro_licitacion, content_text
                FROM dncp.pliegos_secciones TABLESAMPLE SYSTEM(0.5)
                WHERE LOWER(title_normalized) = %s
                  AND content_text IS NOT NULL AND LENGTH(content_text) > 50
                LIMIT %s
            """, (db_title, N_SAMPLES * 2))
            rows = [dict(r) for r in cur.fetchall()]
            random.shuffle(rows)
            for r in rows[:N_SAMPLES]:
                samples.append({
                    "title": title,
                    "nro": str(r["nro_licitacion"]),
                    "text": r["content_text"][:2000],
                })
            print(f"  {title}: {min(len(rows), N_SAMPLES)}")
    conn.close()
    print(f"  Total: {len(samples)} samples")

    # Step 2: Run each model
    all_results = {}

    for name, model_id, extra_cfg in MODELS:
        print(f"\n=== Loading {name} ===")
        t0 = time.time()
        cfg = {"model": model_id, "max_model_len": 4096, "gpu_memory_utilization": 0.90,
               "enforce_eager": True, "kv_cache_dtype": "fp8", "enable_prefix_caching": True}
        cfg.update(extra_cfg)
        from vllm import LLM, SamplingParams
        from transformers import AutoTokenizer
        llm = LLM(**cfg)
        tok = AutoTokenizer.from_pretrained(model_id, trust_remote_code=("Qwen" in model_id))
        print(f"  Loaded in {time.time()-t0:.1f}s")

        sp = SamplingParams(temperature=0, max_tokens=512)
        model_results = {pn: [] for pn in PROMPT_NAMES}

        for i, s in enumerate(samples):
            schema = master.get(s["title"], {}).get("json_schema", {})
            if not schema:
                continue
            schema_str = json.dumps(schema, ensure_ascii=False, indent=2)
            user = (f"TITULO: {s['title']}\nDOCUMENTO: {s['nro']}\n\n"
                    f"SCHEMA:\n{schema_str}\n\nTEXTO:\n{s['text']}\n\n"
                    f"Extrae los campos segun el schema. Devuelve SOLO JSON.")

            prompt_texts = []
            for pn in PROMPT_NAMES:
                sys_prompt = SYSTEM_PROMPTS[pn]
                if name == "Mistral-7B-AWQ":
                    msgs = [{"role": "user", "content": f"{sys_prompt}\n\n{user}"}]
                else:
                    msgs = [{"role": "system", "content": sys_prompt},
                            {"role": "user", "content": user}]
                rendered = tok.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True)
                prompt_texts.append(rendered)

            # Batch all 3 prompts in one call
            t1 = time.time()
            try:
                outputs = llm.generate(prompt_texts, sp)
                for pn, out in zip(PROMPT_NAMES, outputs):
                    resp = out.outputs[0].text
                    values = parse_json(resp)
                    errors = []
                    if values is None:
                        errors.append("PARSE_FAIL")
                    else:
                        for fname, fdef in schema.items():
                            if fname not in values:
                                errors.append(f"MISSING:{fname}")
                            elif fdef["type"] == "binary" and values[fname] not in (0, 1, 0.0, 1.0):
                                errors.append(f"TYPE:{fname}={values[fname]}")
                    model_results[pn].append({
                        "title": s["title"], "nro": s["nro"],
                        "success": len(errors) == 0, "errors": errors,
                        "values": values or {},
                        "text": s["text"],
                    })
            except Exception as e:
                for pn in PROMPT_NAMES:
                    model_results[pn].append({
                        "title": s["title"], "nro": s["nro"],
                        "success": False, "errors": [f"EXCEPTION:{e}"],
                        "values": {}, "text": s["text"],
                    })
            elapsed = time.time() - t1

            if (i + 1) % 10 == 0:
                oks = {pn: sum(1 for r in model_results[pn] if r["success"]) for pn in PROMPT_NAMES}
                ok_str = " ".join(f"{pn}={oks[pn]}/{i+1}" for pn in PROMPT_NAMES)
                print(f"  [{i+1}/{len(samples)}] {name} — {ok_str} — {elapsed:.2f}s")

        for pn in PROMPT_NAMES:
            key = f"{name}|{pn}"
            all_results[key] = model_results[pn]
            n_ok = sum(1 for r in model_results[pn] if r["success"])
            print(f"  {name:20s} {pn:10s}: {n_ok}/{len(model_results[pn])} OK")
            (OUT_DIR / f"raw_{name}_{pn}.json").write_text(
                json.dumps(model_results[pn], ensure_ascii=False, indent=2), encoding="utf-8")

        # Unload model
        del llm, tok
        gc.collect()
        import torch
        torch.cuda.empty_cache()
        print(f"  Unloaded {name}")

    # Step 3: Pseudo-GT (majority vote across all model-prompt combos)
    print("\n=== Pseudo-GT (majority vote) ===")
    groups = defaultdict(list)
    for key, results in all_results.items():
        for r in results:
            if r["success"] and r["values"]:
                groups[(r["title"], r["nro"])].append(r)

    pseudo_gt = {}
    for (title, nro), res_list in groups.items():
        field_vals = defaultdict(list)
        for r in res_list:
            for fname, fval in r["values"].items():
                field_vals[fname].append(fval)
        entry = {}
        for fname, vals in field_vals.items():
            if all(v in (0, 1, 0.0, 1.0) for v in vals):
                int_vals = [int(v) for v in vals]
                entry[fname] = 1 if sum(int_vals) > len(int_vals) / 2 else 0
            else:
                sorted_vals = sorted(vals, key=lambda x: (isinstance(x, (int, float)), x))
                entry[fname] = sorted_vals[len(sorted_vals)//2]
        pseudo_gt[f"{title}||{nro}"] = {"title": title, "nro": nro, "values": entry}
    print(f"  Pseudo-GT: {len(pseudo_gt)} sections")

    # Step 4: Metrics
    print("\n=== Metrics ===")
    metrics = {}
    for key, results in all_results.items():
        name, prompt = key.split("|", 1)
        tp = fp = tn = fn = 0
        mae_sum = mae_cnt = 0
        agree = total = 0
        kw_ok = kw_total = 0
        evidence_issues = defaultdict(int)

        for r in results:
            if not r["success"]:
                continue
            gt = pseudo_gt.get(f"{r['title']}||{r['nro']}")
            if not gt:
                continue
            gv = gt["values"]
            for fname, gt_val in gv.items():
                pred = r["values"].get(fname)
                if pred is None:
                    continue
                total += 1
                if isinstance(gt_val, int) or (isinstance(gt_val, float) and gt_val in (0.0, 1.0)):
                    pred_int = int(pred)
                    gt_int = int(gt_val)
                    if pred_int == gt_int:
                        agree += 1
                        if gt_int == 1: tp += 1
                        else: tn += 1
                    else:
                        if gt_int == 1: fn += 1
                        else: fp += 1
                else:
                    diff = abs(float(pred) - float(gt_val))
                    mae_sum += diff
                    mae_cnt += 1
                    if diff < 0.5:
                        agree += 1
                # keyword evidence
                hint = master.get(r["title"], {}).get("json_schema", {}).get(fname, {}).get("extraction_hint", "")
                if hint and isinstance(pred, (int, float)):
                    kw_total += 1
                    ev = check_keyword_evidence(r.get("text", ""), fname, hint, pred)
                    if ev in ("OK", "NO_EVIDENCE"):
                        kw_ok += 1
                    if ev in ("LOW_EVIDENCE", "CONTRADICTS"):
                        evidence_issues[f"{r['title']}/{fname}/{ev}"] += 1

        acc_bin = (tp + tn) / max(tp + tn + fp + fn, 1)
        prec = tp / max(tp + fp, 1)
        rec = tp / max(tp + fn, 1)
        f1 = 2 * prec * rec / max(prec + rec, 0.001)
        mae = mae_sum / max(mae_cnt, 1)
        agree_rate = agree / max(total, 1)
        kw_rate = kw_ok / max(kw_total, 1)

        metrics[key] = {
            "model": name, "prompt": prompt,
            "ok": sum(1 for r in results if r["success"]),
            "total": len(results),
            "structural_ok_rate": sum(1 for r in results if r["success"]) / max(len(results), 1),
            "f1": round(f1, 4), "precision": round(prec, 4), "recall": round(rec, 4),
            "accuracy_binary": round(acc_bin, 4), "mae": round(mae, 4),
            "agreement_vs_gt": round(agree_rate, 4),
            "keyword_evidence_rate": round(kw_rate, 4),
            "total_total": total,
            "evidence_issues": dict(sorted(evidence_issues.items(), key=lambda x: -x[1])),
        }
        print(f"  {name:20s} {prompt:10s} struct={metrics[key]['structural_ok_rate']:.0%} "
              f"f1={f1:.1%} kw={kw_rate:.0%} agree={agree_rate:.0%} mae={mae:.3f}")

    # Step 5: Per-title
    print("\n=== Per-title ===")
    title_metrics = {}
    for title in TITLES:
        tm = {}
        for key, results in all_results.items():
            name, prompt = key.split("|", 1)
            tr = [r for r in results if r["title"] == title]
            ok = sum(1 for r in tr if r["success"])
            tm[f"{name}|{prompt}"] = {"n": len(tr), "ok": ok, "rate": ok / max(len(tr), 1)}
        title_metrics[title] = tm
        best = max(tm.items(), key=lambda x: x[1]["rate"])
        print(f"  {title[:30]:30s} best={best[0][:35]:35s} rate={best[1]['rate']:.0%}")

    # Step 6: Cross-prompt consistency (Mistral)
    print("\n=== Cross-prompt consistency (Mistral) ===")
    mistral_keys = sorted(k for k in all_results if k.startswith("Mistral"))
    for i in range(len(mistral_keys)):
        for j in range(i+1, len(mistral_keys)):
            k1, k2 = mistral_keys[i], mistral_keys[j]
            p1, p2 = k1.split("|")[1], k2.split("|")[1]
            r1, r2 = all_results[k1], all_results[k2]
            agree = total = 0
            for a, b in zip(r1, r2):
                if not a["success"] or not b["success"]:
                    continue
                for fname in set(list(a["values"].keys()) + list(b["values"].keys())):
                    total += 1
                    if a["values"].get(fname) == b["values"].get(fname):
                        agree += 1
            rate = agree / max(total, 1)
            print(f"  {p1:10s} vs {p2:10s}: agree={rate:.1%} ({agree}/{total})")

    # Step 7: Report
    print("\n=== Report ===")
    import importlib.metadata
    vllm_ver = importlib.metadata.version("vllm")
    lines = []
    lines.append("# Validation Report — Fase 1: Multi-Model Extraction Quality")
    lines.append(f"\n- Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Samples: {len(samples)} sections across {len(TITLES)} titles")
    lines.append(f"- Models: {', '.join(m[0] for m in MODELS)}")
    lines.append(f"- Prompts: {', '.join(PROMPT_NAMES)}")
    lines.append(f"- vLLM: {vllm_ver}")

    lines.append("\n## Overall\n")
    lines.append("| Model | Prompt | Structural OK | F1 vs GT | Precision | Recall | MAE | Agreement | Keyword Ev. |")
    lines.append("|-------|--------|:------------:|:--------:|:---------:|:-----:|:---:|:---------:|:-----------:|")
    sorted_m = sorted(metrics.items(), key=lambda x: x[1]["f1"], reverse=True)
    for key, m in sorted_m:
        lines.append(f"| {m['model']:15s} | {m['prompt']:10s} | {m['structural_ok_rate']:.0%} ({m['ok']}/{m['total']}) | "
                     f"{m['f1']:.1%} | {m['precision']:.1%} | {m['recall']:.1%} | {m['mae']:.3f} | "
                     f"{m['agreement_vs_gt']:.1%} | {m['keyword_evidence_rate']:.0%} |")

    best_key = max(metrics.keys(), key=lambda k: metrics[k]["f1"])
    bm = metrics[best_key]
    lines.append(f"\n**Best**: {bm['model']} / '{bm['prompt']}' — F1={bm['f1']:.1%}, KW ev={bm['keyword_evidence_rate']:.0%}")

    lines.append("\n## Per Title (structural OK rate)\n")
    lines.append("| Title | Best | Structural OK |")
    lines.append("|-------|------|:------------:|")
    for title in TITLES:
        tm = title_metrics[title]
        if not tm:
            continue
        best = max(tm.items(), key=lambda x: x[1]["rate"])
        lines.append(f"| {title[:40]:40s} | {best[0][:35]:35s} | {best[1]['rate']:.0%} ({best[1]['ok']}/{best[1]['n']}) |")

    lines.append("\n## Cross-Prompt Consistency (Mistral 7B)\n")
    lines.append("| Prompt A | Prompt B | Agreement |")
    lines.append("|----------|----------|:---------:|")
    for i in range(len(mistral_keys)):
        for j in range(i+1, len(mistral_keys)):
            k1, k2 = mistral_keys[i], mistral_keys[j]
            p1, p2 = k1.split("|")[1], k2.split("|")[1]
            r1, r2 = all_results[k1], all_results[k2]
            agree = total = 0
            for a, b in zip(r1, r2):
                if not a["success"] or not b["success"]:
                    continue
                for fname in set(list(a["values"].keys()) + list(b["values"].keys())):
                    total += 1
                    if a["values"].get(fname) == b["values"].get(fname):
                        agree += 1
            lines.append(f"| {p1:10s} | {p2:10s} | {agree/max(total,1):.1%} ({agree}/{total}) |")

    lines.append("\n## Keyword Evidence Issues\n")
    all_issues = {}
    for key, m in sorted_m:
        for issue, count in m["evidence_issues"].items():
            all_issues[issue] = all_issues.get(issue, 0) + count
    if all_issues:
        lines.append(f"Top issues:\n")
        for issue, count in sorted(all_issues.items(), key=lambda x: -x[1])[:15]:
            lines.append(f"- [{count}] {issue}")
    else:
        lines.append("None found.")

    (OUT_DIR / "validation_report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"\nReport: {OUT_DIR / 'validation_report.md'}")
    (OUT_DIR / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n=== DONE ===")
    for key, m in sorted_m:
        print(f"  {m['model']:20s} {m['prompt']:10s} F1={m['f1']:.1%} struct={m['structural_ok_rate']:.0%} kw={m['keyword_evidence_rate']:.0%}")


if __name__ == "__main__":
    main()

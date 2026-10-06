"""
Few-shot dynamic retrieval benchmark vs human GT.
For each GT sample, retrieves similar corpus sections (same field) with known
values using BGE-M3-Legal-Spanish embeddings, injects them into the prompt,
and runs the LLM with guided decoding.

Usage:
  VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/benchmark_fewshot_retrieval.py [--k 4] [--model 0]
"""
import sys, re, time, json, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np
import pandas as pd
from transformers import AutoTokenizer
from vllm import LLM, SamplingParams
from vllm.sampling_params import StructuredOutputsParams

from scripts.benchmark_multi_vs_gt import (
    SYSTEM_PROMPT, get_hint, postprocess, parse, MODELS,
)

GT = Path("data/processed/llm_benchmark/ground_truth_5_campos.csv")
TEXTS_CACHE = Path("data/processed/llm_benchmark/gt_texts.json")
CORPUS = Path("data/processed/fewshot_retrieval/corpus.json")
EMB_DIR = Path("data/processed/fewshot_retrieval")
OUT = Path("data/processed/fewshot_retrieval/results")

FIELDS = ["dias_pago_anticipo", "dias_solicitud_anticipo"]


def load_corpus():
    with open(CORPUS, encoding="utf-8") as f:
        corpus = json.load(f)
    embs = {}
    for field in FIELDS:
        embs[field] = np.load(EMB_DIR / f"corpus_emb_{field}.npy")
    return corpus, embs


def select_examples(field, query_emb, corpus_rows, field_emb, k=4, value_bias=None):
    """Pick k examples: cover distinct values where possible, else most similar."""
    sims = field_emb @ query_emb
    order = np.argsort(-sims)
    selected = []
    seen_values = set()
    # first pass: one per distinct value (prioritize values present in GT)
    for idx in order:
        row = corpus_rows[int(idx)]
        v = row["value"]
        if v in seen_values:
            continue
        if value_bias and v not in value_bias and len(seen_values) >= k:
            continue
        selected.append((int(idx), float(sims[idx]), row))
        seen_values.add(v)
        if len(selected) >= k:
            break
    # second pass: fill remaining with most similar regardless of value
    for idx in order:
        if len(selected) >= k:
            break
        if int(idx) in [s[0] for s in selected]:
            continue
        selected.append((int(idx), float(sims[idx]), corpus_rows[int(idx)]))
    return sorted(selected, key=lambda s: -s[1])


def build_fewshot_prompt(field, text, examples, merge_system=False):
    fs = get_hint(field)
    hint = fs.get("extraction_hint", "")
    user = [f"Campo a extraer: {fs.get('description', '')}",
            f"Tipo esperado: {fs.get('type', '')}"]
    if hint and hint not in ("", "None"):
        user.append(f"Gu\u00eda de extracci\u00f3n: {hint}")
    if examples:
        user.append("\nEJEMPLOS de extracci\u00f3n correcta en textos similares:")
        for i, (_, _, row) in enumerate(examples, 1):
            ex_text = row["text"][:800].strip()
            ex_val = row["value"]
            ex_val_s = str(int(ex_val)) if ex_val == int(ex_val) else str(ex_val)
            user.append(f"\n--- Ejemplo {i} ---")
            user.append(f"Texto: {ex_text}")
            user.append(f"Valor: {ex_val_s}")
    user.append(f"\nTexto del pliego a extraer:\n{text}")
    user.append("\nResponde SOLO con el n\u00famero o NULL.")
    if merge_system:
        return [{"role": "user", "content": "\n".join([SYSTEM_PROMPT] + user)}]
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": "\n".join(user)}]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--k", type=int, default=4)
    parser.add_argument("--model", type=int, default=0)
    args = parser.parse_args()

    cfg = MODELS[args.model]
    print(f"Model: {cfg['name']} | k={args.k}")

    gt = pd.read_csv(GT, encoding="utf-8-sig")
    with open(TEXTS_CACHE, encoding="utf-8") as f:
        gt_texts = json.load(f)
    corpus, embs = load_corpus()

    gt_keys = np.load(EMB_DIR / "gt_keys.npy", allow_pickle=True)
    gt_emb = np.load(EMB_DIR / "gt_emb.npy")
    key_to_emb = {k: i for i, k in enumerate(gt_keys.tolist())}

    # value bias: values present in GT per field
    gt_values = {}
    for field in FIELDS:
        vals = gt.loc[gt["campo"] == field, "ground_truth"].dropna().unique().tolist()
        gt_values[field] = set(vals)

    hf_tok = AutoTokenizer.from_pretrained(cfg["path"], trust_remote_code=True)
    llm = LLM(model=cfg["path"], max_model_len=8192, max_num_seqs=8,
              gpu_memory_utilization=cfg["gpu_mem"], enforce_eager=True,
              trust_remote_code=True, quantization=cfg["quant"], dtype="float16")
    sp = SamplingParams(max_tokens=64, temperature=0.0,
                        structured_outputs=StructuredOutputsParams(
                            regex=r'^(-?\d+(?:[.,]\d+)?|NULL)$'))

    all_results = []
    t0 = time.time()
    for field in FIELDS:
        sub = gt[gt["campo"] == field]
        rows_field = corpus[field]
        emb_field = embs[field]
        for _, row in sub.iterrows():
            nro = str(int(row["nro_licitacion"]))
            text = gt_texts.get(nro, "")
            q_emb = gt_emb[key_to_emb[nro]]
            ex = select_examples(field, q_emb, rows_field, emb_field,
                                 k=args.k, value_bias=gt_values[field])
            msgs = build_fewshot_prompt(field, text, ex, cfg["merge_system"])
            tmpl_kwargs = {"enable_thinking": False} if cfg["thinking_fix"] else {}
            rendered = hf_tok.apply_chat_template(msgs, tokenize=False,
                                                  add_generation_prompt=True,
                                                  **tmpl_kwargs)
            out = llm.generate([rendered], sp)[0].outputs[0].text.strip()
            parsed = parse(out, field=field, text=text)
            gt_val = None if pd.isna(row["ground_truth"]) else float(row["ground_truth"])
            ok = (parsed == gt_val) or (parsed is None and gt_val is None)
            all_results.append({"nro": nro, "field": field, "gt": row["ground_truth"],
                                "llm_value": parsed, "llm_raw": out[:80], "match": ok,
                                "n_examples": len(ex)})

        ok = sum(1 for x in all_results if x["field"] == field and x["match"])
        print(f"    {field:35s}: {ok}/{len(sub)} = {ok/len(sub)*100:.0f}%")

    elapsed = time.time() - t0
    ok = sum(1 for x in all_results if x["match"])
    print(f"    {'TOTAL':35s}: {ok}/{len(gt)} = {ok/len(gt)*100:.0f}%  ({elapsed:.0f}s)")

    OUT.mkdir(parents=True, exist_ok=True)
    fname = OUT / f"{cfg['name'].replace(' ', '_').replace('.', '')}_k{args.k}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump({"model": cfg["name"], "k": args.k, "score": ok,
                   "elapsed": elapsed, "results": all_results}, f,
                  ensure_ascii=False, indent=1)
    print("saved:", fname)


if __name__ == "__main__":
    main()

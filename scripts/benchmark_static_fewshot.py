"""
Static balanced few-shot benchmark: one example per distinct value per field
(no retrieval). Shows the model the full range of possible answers.
Usage:
  VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/benchmark_static_fewshot.py [--model 0]
"""
import sys, time, json, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from transformers import AutoTokenizer
from vllm import LLM, SamplingParams
from vllm.sampling_params import StructuredOutputsParams

from scripts.benchmark_multi_vs_gt import SYSTEM_PROMPT, get_hint, postprocess, parse, MODELS
from scripts.benchmark_fewshot_retrieval import build_fewshot_prompt

GT = Path("data/processed/llm_benchmark/ground_truth_5_campos.csv")
TEXTS_CACHE = Path("data/processed/llm_benchmark/gt_texts.json")
CORPUS = Path("data/processed/fewshot_retrieval/corpus.json")
OUT = Path("data/processed/fewshot_retrieval/results")

FIELDS = ["dias_pago_anticipo", "dias_solicitud_anticipo"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=int, default=0)
    args = parser.parse_args()
    cfg = MODELS[args.model]

    gt = pd.read_csv(GT, encoding="utf-8-sig")
    with open(TEXTS_CACHE, encoding="utf-8") as f:
        gt_texts = json.load(f)
    with open(CORPUS, encoding="utf-8") as f:
        corpus = json.load(f)

    # one example per distinct value, prefer short texts
    examples = {}
    for field in FIELDS:
        rows = corpus[field]
        by_value = {}
        for r in rows:
            v = r["value"]
            if v not in by_value or len(r["text"]) < len(by_value[v]["text"]):
                by_value[v] = r
        examples[field] = list(by_value.values())
        print(f"{field}: {len(examples[field])} distinct values -> "
              f"{sorted(r['value'] for r in examples[field])}")

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
        ex = [(0, 0.0, r) for r in examples[field]]
        for _, row in sub.iterrows():
            nro = str(int(row["nro_licitacion"]))
            text = gt_texts.get(nro, "")
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
    fname = OUT / f"{cfg['name'].replace(' ', '_').replace('.', '')}_static.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump({"model": cfg["name"], "mode": "static", "score": ok,
                   "elapsed": elapsed, "results": all_results}, f,
                  ensure_ascii=False, indent=1)
    print("saved:", fname)


if __name__ == "__main__":
    main()

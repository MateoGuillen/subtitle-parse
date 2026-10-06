"""A2-relog: Gemma (720 items truncados) y Ministral (9) via llama-server con
raw COMPLETO. Lee Fase5_*.json, re-ejecuta solo los items cuyo llm_raw toco el
tope de 80 chars, guarda raw full + recalcula valid/parse sobre esos items.
Requiere servidor en :1234 ya levantado (ver shell queue)."""
import sys
import json
import time
import argparse
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
F5 = ROOT / 'data/processed/llm_benchmark/multimodel_results'
OUTD = ROOT / 'data/processed/llm_benchmark'


def _load_mod(path, name):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--model', required=True)
    a = ap.parse_args()
    gh = _load_mod('scripts/benchmark_gguf_http.py', 'ggufh')
    prev = json.load(open(F5 / f'Fase5_{a.model}.json'))['results']
    todo = [r for r in prev if len(str(r['llm_raw'])) >= 80]
    print(f'{a.model}: {len(todo)} items truncados a re-log', flush=True)
    t0 = time.time()
    pseudo = {str(s['nro']) + s['field']: s['text'] for s in
              json.load(open(ROOT / 'data/processed/llm_benchmark/pseudo900.json'))['samples']}
    res = []
    for i, r in enumerate(todo):
        text = pseudo.get(r['nro'] + r['field'], '')
        try:
            raw = gh.call_llm(gh.build_prompt(r['field'], text), {}, max_tokens=64)
        except Exception as e:
            raw = f'ERROR:{type(e).__name__}'
        parsed = gh.parse_response(raw, field=r['field'], text=text, strict=True)
        pvn = r['pseudo']
        agree = (parsed is not None and abs(parsed - pvn) < 1e-9)
        res.append(dict(nro=r['nro'], field=r['field'], raw_full=raw,
                        value=parsed, agree=agree))
        if (i + 1) % 50 == 0:
            print(f'  {i+1}/{len(todo)} ({time.time()-t0:.0f}s)', flush=True)
    agree_n = sum(x['agree'] for x in res)
    json.dump({'model': a.model, 'n_relog': len(res),
               'agree_rate': round(agree_n / max(1, len(res)), 3),
               'results': res, 'elapsed': time.time() - t0},
              open(OUTD / f'A2_relog_{a.model}.json', 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print(f'{a.model}: agree relog {agree_n}/{len(res)} = '
          f'{agree_n/max(1,len(res))*100:.1f}% ({time.time()-t0:.0f}s)', flush=True)


if __name__ == '__main__':
    main()

"""C4 — brittleness de prompt (plan v4.1 §4): 2 variantes sobre 150 items fijos
(pseudo900, seed 1234, 30/campo) x top-3 modelos, guided decoding, mismo parse.

  P1 sin-hint   : quita la 'Guia de extraccion' del campo (dependencia del hint)
  P2 reescrito  : system prompt reformulado (contenido equivalente, otras palabras)

Reporta agreement-vs-regex por variante contra el mismo subconjunto base (de los
Fase5_*.json guardados) — el delta es la caida por reformulacion, campo a campo.
"""
import sys
import json
import time
import random
import argparse
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
OUTD = ROOT / 'data/processed/llm_benchmark'
SYSTEM_P2 = (
    'Tarea de extraccion de informacion. Se te entrega un fragmento de un pliego de '
    'contratacion publica y debes recuperar UN valor numerico correspondiente al campo '
    'indicado, usando exclusivamente la informacion del fragmento. Si el fragmento no '
    'contiene el valor, responde NULL. Escribe el numero o NULL en la PRIMERA linea de '
    'tu respuesta. Luego puedes explicar en lineas siguientes si es necesario.'
)


def _load_mod(path, name):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def variants(mm, fld, text, merge):
    fs = mm.get_hint(fld)
    hint = fs.get('extraction_hint', '')
    base_user = [f"Campo a extraer: {fs.get('description', '')}",
                 f"Tipo esperado: {fs.get('type', '')}"]
    if hint and hint not in ('', 'None'):
        base_user.append(f'Guía de extracción: {hint}')
    tail = f'\nTexto del pliego:\n{text}'
    p1_user = '\n'.join(base_user + [tail])
    p2_user = '\n'.join(base_user + ([f'Guía de extracción: {hint}']
                                     if hint and hint not in ('', 'None') else []) + [tail])
    def mk(sysc, user, merge):
        if merge:
            return [{'role': 'user', 'content': '\n'.join([sysc, user])}]
        return [{'role': 'system', 'content': sysc},
                {'role': 'user', 'content': user}]
    return {'P1_sin_hint': mk(mm.SYSTEM_PROMPT, p1_user, merge),
            'P2_reescrito': mk(SYSTEM_P2, p2_user, merge)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--idx', type=int, required=True)
    args = ap.parse_args()
    from vllm import LLM, SamplingParams
    from vllm.sampling_params import StructuredOutputsParams
    from transformers import AutoTokenizer
    mm = _load_mod('scripts/benchmark_multi_vs_gt.py', 'mmgt')
    cfg = mm.MODELS[args.idx]
    samples = json.load(open(OUTD / 'pseudo900.json'))['samples']
    by = {}
    for s in samples:
        by.setdefault(s['field'], []).append(s)
    rng = random.Random(1234)
    work = []
    for f in sorted(by):
        pool = by[f][:]
        rng.shuffle(pool)
        work += pool[:30]
    base_agree = {}
    for r in json.load(open(OUTD / 'multimodel_results' /
                            f"Fase5_{cfg['name'].replace(' ', '_').replace('.', '')}.json"))['results']:
        base_agree[(r['nro'], r['field'])] = r['agree']
    tok = AutoTokenizer.from_pretrained(cfg['path'], trust_remote_code=True)
    llm = LLM(model=cfg['path'], max_model_len=cfg['max_len'], max_num_seqs=8,
              gpu_memory_utilization=cfg['gpu_mem'], enforce_eager=True,
              trust_remote_code=True, dtype='bfloat16')
    sp = SamplingParams(max_tokens=64, temperature=0.0,
                        structured_outputs=StructuredOutputsParams(
                            regex=r'^(-?\d+(?:[.,]\d+)?|NULL)$'))
    tmpl = {'enable_thinking': False} if cfg['thinking_fix'] else {}
    out = {'model': cfg['name']}
    for vname in ['P1_sin_hint', 'P2_reescrito']:
        res = []
        t0 = time.time()
        CH = 32
        for i in range(0, len(work), CH):
            chunk = work[i:i + CH]
            rendered = [tok.apply_chat_template(variants(mm, s['field'], s['text'],
                                                         cfg['merge_system'])[vname],
                                                tokenize=False,
                                                add_generation_prompt=True, **tmpl)
                        for s in chunk]
            try:
                outs = llm.generate(rendered, sp)
            except Exception:
                outs = []
                for pr in rendered:
                    try:
                        outs.append(llm.generate([pr], sp)[0])
                    except Exception:
                        outs.append(None)
            for s, o in zip(chunk, outs):
                raw = (o.outputs[0].text.strip() if o else 'ERROR:ctx')
                lv = mm.parse(raw, field=s['field'], text=s['text'])
                ag = lv is not None and abs(lv - s['pseudo']) < 1e-9
                res.append(dict(nro=s['nro'], field=s['field'], agree=ag,
                                raw=raw[:120], value=lv))
        out[vname] = res
        ba = sum(base_agree.get((s['nro'], s['field']), False) for s in work)
        va = sum(r['agree'] for r in res)
        print(f'{cfg["name"]} {vname}: base={ba}/150 variante={va}/150 '
              f'delta={va-ba:+d} ({time.time()-t0:.0f}s)', flush=True)
    json.dump(out, open(OUTD / f"C4_parapfrase_{cfg['name'].replace(' ', '_').replace('.', '')}.json",
                        'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    del llm
    print('OK', flush=True)


if __name__ == '__main__':
    main()

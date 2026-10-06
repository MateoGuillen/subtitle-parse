"""C5 — Replica del prompt-flip con parrases CIEGAS (protocolo pre-registrado):

  - Variantes de REPLICA: P2 (previa), P3/P4 (Mistral-7B ciego desde lista F,
    paraphrases_ciegas.json; auditoria de fuga documentada, P3 re-draw formal).
  - P1 NO entra en el conteo de replicacion (mide remocion de hint, efecto distinto).
  - Corridas: {P2,P3,P4} x top-3 sobre los mismos 150 items de C4 (seed 1234)
    + P2 x top-3 sobre la poblacion HUMANA de 288 (accuracy-humano bajo reescritura).
  - Protocolo de items desiguales: fallback por item, failed_items por modelo x
    variante; el delta se computa sobre interseccion limpia y se declara n efectivo.
"""
import sys
import json
import time
import math
import argparse
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
R = ROOT / 'data/processed/llm_benchmark'
GT_DIR = ROOT / 'data/processed/alolab/gt_por_campo'


def _load_mod(path, name):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def isnull(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


def to_num(v):
    if isnull(v):
        return None
    try:
        return float(str(v).strip().replace(',', '.'))
    except ValueError:
        return None


def eq(a, b, f):
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    if f.startswith('pct_'):
        return abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--idx', type=int, required=True)
    args = ap.parse_args()
    from vllm import LLM, SamplingParams
    from vllm.sampling_params import StructuredOutputsParams
    from transformers import AutoTokenizer
    mm = _load_mod('scripts/benchmark_multi_vs_gt.py', 'mmgt')
    c4 = _load_mod('scripts/c4_paraphrase.py', 'c4m')
    para = json.load(open(R / 'paraphrases_ciegas.json'))
    systems = {'P2': c4.SYSTEM_P2, 'P3': para['P3'], 'P4': para['P4']}
    cfg = mm.MODELS[args.idx]
    print(f'C5 {cfg["name"]}', flush=True)

    # poblaciones
    samples = json.load(open(R / 'pseudo900.json'))['samples']
    by = {}
    for s in samples:
        by.setdefault(s['field'], []).append(s)
    import random
    rng = random.Random(1234)
    work150 = []
    for f in sorted(by):
        pool = by[f][:]
        rng.shuffle(pool)
        work150 += pool[:30]
    hum = []
    FILE = {f: GT_DIR / f'{f}_GT.csv' for f in
            ['dias_validez_garantia', 'anos_experiencia_minima_tecnica',
             'dias_pago_anticipo', 'dias_solicitud_anticipo',
             'pct_monto_referencia_tecnica']}
    FILE['anos_experiencia_minima_tecnica'] = GT_DIR / 'anos_experiencia_minima_tecnica_GT (1).csv'
    import pandas as pd
    for f, fp in FILE.items():
        g = pd.read_csv(fp).drop_duplicates('nro_licitacion')
        for r in g.itertuples():
            dbn, gtn = to_num(r.db_value), to_num(r.ground_truth)
            if dbn is None or gtn is None:
                continue
            hum.append(dict(nro=str(int(float(r.nro_licitacion))), field=f,
                            db=dbn, gt=gtn, text=str(r.text)))

    tok = AutoTokenizer.from_pretrained(cfg['path'], trust_remote_code=True)
    llm = LLM(model=cfg['path'], max_model_len=cfg['max_len'], max_num_seqs=8,
              gpu_memory_utilization=cfg['gpu_mem'], enforce_eager=True,
              trust_remote_code=True, dtype='bfloat16')
    sp = SamplingParams(max_tokens=64, temperature=0.0,
                        structured_outputs=StructuredOutputsParams(
                            regex=r'^(-?\d+(?:[.,]\d+)?|NULL)$'))
    tmpl = {'enable_thinking': False} if cfg['thinking_fix'] else {}

    def user_block(fld, text, hint_keep=True):
        fs = mm.get_hint(fld)
        hint = fs.get('extraction_hint', '')
        b = [f"Campo a extraer: {fs.get('description', '')}",
             f"Tipo esperado: {fs.get('type', '')}"]
        if hint_keep and hint and hint not in ('', 'None'):
            b.append(f'Guía de extracción: {hint}')
        b.append(f'\nTexto del pliego:\n{text}')
        return '\n'.join(b)

    def run(items, system_text, tag, merge):
        rendered = []
        for s in items:
            ub = user_block(s['field'], s['text'])
            c = ([{'role': 'user', 'content': '\n'.join([system_text, ub])}]
                 if merge else
                 [{'role': 'system', 'content': system_text},
                  {'role': 'user', 'content': ub}])
            rendered.append(tok.apply_chat_template(
                c, tokenize=False, add_generation_prompt=True, **tmpl))
        res, failed = [], []
        CH = 32
        for i in range(0, len(rendered), CH):
            try:
                outs = llm.generate(rendered[i:i + CH], sp)
            except Exception:
                outs = []
                for pr in rendered[i:i + CH]:
                    try:
                        outs.append(llm.generate([pr], sp)[0])
                    except Exception:
                        outs.append(None)
            for s, o in zip(items[i:i + CH], outs):
                if o is None:
                    failed.append([s['nro'], s['field']])
                    continue
                raw = o.outputs[0].text.strip()
                try:
                    lv = mm.parse(raw, field=s['field'], text=s['text'])
                except Exception:
                    lv = None
                lvn = to_num(lv)
                rec = dict(nro=s['nro'], field=s['field'], raw=raw[:120],
                           value=lvn)
                if 'pseudo' in s:
                    rec['agree_pseudo'] = lvn is not None and abs(lvn - s['pseudo']) < 1e-9
                else:
                    rec['m_human'] = eq(lvn, s['gt'], s['field'])
                res.append(rec)
        print(f'  {tag}: n={len(res)} failed={len(failed)}', flush=True)
        return res, failed

    out = {'model': cfg['name'], 'variants': {}}
    base = {}
    for r in json.load(open(R / 'multimodel_results' /
            f"Fase5_{cfg['name'].replace(' ', '_').replace('.', '')}.json"))['results']:
        base[(str(r['nro']), r['field'])] = r['agree']
    for tag, sysx in systems.items():
        res, failed = run(work150, sysx, f'150:{tag}', cfg['merge_system'])
        out['variants'][tag] = {'items': res, 'failed': failed}
    res2, failed2 = run(hum, systems['P2'], '288hum:P2', cfg['merge_system'])
    out['P2_288'] = {'items': res2, 'failed': failed2}
    out['base150_agree'] = {f'{k[0]}|{k[1]}': bool(v) for k, v in base.items()
                            if (k[0], k[1]) in {(s['nro'], s['field']) for s in work150}}
    nm = cfg['name'].replace(' ', '_').replace('.', '')
    json.dump(out, open(R / f'C5_replica_{nm}.json', 'w'), ensure_ascii=False, indent=1)
    del llm
    print('OK', flush=True)


if __name__ == '__main__':
    main()

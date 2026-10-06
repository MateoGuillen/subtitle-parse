"""C3 — re-run top-3 (idx MODELS dado) sobre los 288 humanos (GT∩db) con guided xgrammar
y parse post-AloLab (mm.parse), guardando llm_raw COMPLETO (sin truncar a 80).

Output: data/processed/llm_benchmark/C3_{name}.json por item:
  nro, field, gt, db(regex), llm_raw, llm_value, m_human (llm==gt), m_regex (llm==db),
  a_m (clasificacion caja A/M/O sobre desacuerdos vs regex, como triangulacion).
Sumario impreso: accuracy humana vs fiabilidad regex por campo.
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
GT_DIR = ROOT / 'data/processed/alolab/gt_por_campo'
OUTD = ROOT / 'data/processed/llm_benchmark'
FIELDS = ['dias_validez_garantia', 'anos_experiencia_minima_tecnica',
          'dias_pago_anticipo', 'dias_solicitud_anticipo',
          'pct_monto_referencia_tecnica']
FILE = {f: GT_DIR / f'{f}_GT.csv' for f in FIELDS}
FILE['anos_experiencia_minima_tecnica'] = GT_DIR / 'anos_experiencia_minima_tecnica_GT (1).csv'


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


def eq(a, b, field):
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    if field.startswith('pct_'):
        return abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--idx', type=int, required=True)
    ap.add_argument('--mode', default='llm_only',
                    help='modo de resolve(): llm_only mide el MODELO (default desde '
                         'sesión 23); rule_then_llm reproduce el C3 histórico')
    args = ap.parse_args()
    from vllm import LLM, SamplingParams
    from vllm.sampling_params import StructuredOutputsParams
    from transformers import AutoTokenizer
    mm = _load_mod('scripts/benchmark_multi_vs_gt.py', 'mmgt')
    cfg = mm.MODELS[args.idx]
    print(f'C3: {cfg["name"]}', flush=True)

    samples = []
    for fld in FIELDS:
        g = pd_read = __import__('pandas').read_csv(FILE[fld])
        g = g.drop_duplicates('nro_licitacion')
        for r in g.itertuples():
            dbn, gtn = to_num(r.db_value), to_num(r.ground_truth)
            if dbn is None or gtn is None:
                continue  # poblacion 288: GT∩db ambos con valor
            samples.append(dict(nro=str(int(float(r.nro_licitacion))), field=fld,
                                gt=gtn, db=dbn, text=str(r.text)))
    print(f'  poblacion: {len(samples)} items', flush=True)

    tok = AutoTokenizer.from_pretrained(cfg.get('tokenizer_path', cfg['path']),
                                        trust_remote_code=True)
    llm = LLM(model=cfg['path'], max_model_len=cfg['max_len'],
              max_num_seqs=8, gpu_memory_utilization=cfg['gpu_mem'],
              enforce_eager=True, trust_remote_code=True,
              **({'dtype': 'bfloat16'} if not cfg['bf16'] else {}))
    sp = SamplingParams(max_tokens=64, temperature=0.0,
                        structured_outputs=StructuredOutputsParams(
                            regex=r'^(-?\d+(?:[.,]\d+)?|NULL)$'))
    results = []
    t0 = time.time()
    CH = 32
    for i in range(0, len(samples), CH):
        chunk = samples[i:i + CH]
        rendered = []
        for s in chunk:
            msgs = mm.build_prompt(s['field'], s['text'], cfg['merge_system'])
            tmpl = {'enable_thinking': False} if cfg['thinking_fix'] else {}
            rendered.append(tok.apply_chat_template(
                msgs, tokenize=False, add_generation_prompt=True, **tmpl))
        try:
            outs = llm.generate(rendered, sp)
        except Exception as e:
            print(f'  chunk {i} error ({type(e).__name__}); retry por item', flush=True)
            outs = []
            for pr in rendered:
                try:
                    outs.append(llm.generate([pr], sp)[0])
                except Exception:
                    outs.append(None)
        for s, o in zip(chunk, outs):
            raw = (o.outputs[0].text.strip() if o else 'ERROR:gen')
            try:
                lv = mm.parse(raw, field=s['field'], text=s['text'], mode=args.mode)
                lv_legacy = mm.parse(raw, field=s['field'], text=s['text'],
                                     mode='rule_then_llm')
            except Exception:
                lv = lv_legacy = None
            lvn = to_num(lv) if lv is not None else None
            m_h = eq(lvn, s['gt'], s['field'])
            m_r = eq(lvn, s['db'], s['field'])
            # K=llm coincide con ambos; M=llm==humano!=regex (CORRIGE);
            # X=llm==regex!=humano (copia la referencia rota: ceguera compartida);
            # O=falla a ambos.  (poblacion 288: GT y db siempre con valor)
            a_m = ('K' if m_r and m_h else
                   ('M' if m_h and not m_r else
                    ('O' if not m_r and not m_h else 'X')))
            results.append({**{k: s[k] for k in ('nro', 'field', 'gt', 'db')},
                            'llm_raw': raw, 'llm_value': lv,
                            'llm_value_legacy': lv_legacy, 'mode': args.mode,
                            'm_human': m_h, 'm_regex': m_r, 'a_m': a_m})
        done = i + len(chunk)
        json.dump({'model': cfg['name'], 'results': results,
                   'elapsed': time.time() - t0},
                  open(OUTD / f'C3_{cfg["name"].replace(" ", "_").replace(".", "")}.json',
                       'w', encoding='utf-8'), ensure_ascii=False, indent=1)
        print(f'  {done}/{len(samples)} ({time.time()-t0:.0f}s)', flush=True)
    # resumen
    print('== resumen: accuracy-humana vs fiabilidad-regex (misma poblacion) ==')
    by = {}
    for r in results:
        by.setdefault(r['field'], []).append(r)
    for f, rs in sorted(by.items()):
        acc_llm = sum(r['m_human'] for r in rs) / len(rs)
        acc_re = sum(eq(r['db'], r['gt'], f) for r in rs) / len(rs)
        res_m = sum(r['a_m'] == 'M' for r in rs)
        falla = sum(r['a_m'] == 'O' for r in rs)
        copio = sum(r['a_m'] == 'X' for r in rs)
        print(f'  {f:34s} n={len(rs):3d} LLM={acc_llm:.3f} regex={acc_re:.3f} '
              f'LLMcorrige={res_m:3d} copia-malo={copio:3d} LLMfalla={falla:3d}')
    del llm
    print('OK', flush=True)


if __name__ == '__main__':
    main()

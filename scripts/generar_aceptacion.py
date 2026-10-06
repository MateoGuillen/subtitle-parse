"""Aceptación por muestreo sobre staging (plan §9 + propuesta 27-sep).

Tanda A (prioritaria): exp_tecnica CENSO (82 con valor) + fórmula 100 + pago_maximo 100.
Ciego: nro, campo, valor_llm, raw, texto. Sin etiquetas previas.
El auditor juzga el TEXTO: 'el valor es el plazo/cantidad correcto del campo?' si/no/otro.
"""
import random
from pathlib import Path

import pandas as pd
import psycopg2

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import DB_CONFIG

OUT = Path(__file__).resolve().parents[1] / "data" / "processed" / "llm_benchmark" / "planillas"
RID = 'llmfb-meta-llama-3.1-8b-instruct-fp8-2834432bb7237924'
SEED = 31337


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--tanda", choices=["A", "B", "C"], default="A")
    args = ap.parse_args()
    tandas = {
        "A": ([('anos_experiencia_minima_tecnica', None),
               ('num_componentes_formula', 100),
               ('dias_pago_maximo', 100)], 42),
        "B": ([('dias_notificacion_fuerza_mayor', 80),
               ('dias_aceptacion_rechazo_factura', 80),
               ('num_documentos_pago', 80),
               ('num_pasos_verificacion', 80),
               ('ratio_endeudamiento_maximo', 80)], 43),
        "C": ([('dias_pago_anticipo', 13),
               ('dias_solicitud_anticipo', 13),
               ('dias_validez_garantia', 13),
               ('anos_experiencia_minima_tecnica', 12),
               ('num_componentes_formula', 12),
               ('ratio_liquidez_minimo', 12),
               ('ratio_endeudamiento_maximo', 12),
               ('dias_notificacion_fuerza_mayor', 12),
               ('dias_pago_maximo', 12),
               ('dias_aceptacion_rechazo_factura', 12)], 44),
    }
    spec, seed = tandas[args.tanda]
    tanda = args.tanda
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()
    rng = random.Random(seed)
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        'mmgt', str(Path(__file__).resolve().parents[1]
                    / 'scripts/benchmark_multi_vs_gt.py'))
    _mm = _ilu.module_from_spec(_spec)
    try:
        _spec.loader.exec_module(_mm)
    except SystemExit:
        pass
    TITLE = {f: _mm.TITLE_MAP[f] for f, _ in spec}
    rows = []
    val_cond = "r.value IS NULL" if tanda == "C" else "r.value IS NOT NULL"
    for field, n_take in spec:
        cur.execute(
            f"""SELECT r.nro_licitacion, r.value, r.raw_short, s.content_text,
                      s.title_normalized
               FROM dncp.llm_fallback_runs r
               JOIN dncp.pliegos_secciones s ON s.nro_licitacion = r.nro_licitacion
               WHERE r.run_id = %s AND r.field = %s AND r.status = 'done'
               AND {val_cond} AND s.title_normalized = %s""",
            (RID, field, TITLE[field]))
        recs = cur.fetchall()
        if n_take is not None and len(recs) > n_take:
            recs = rng.sample(recs, n_take)
        for nro, val, raw, txt, title in recs:
            rows.append(dict(
                campo=field, nro_licitacion=str(nro),
                valor_llm=(float(val) if val is not None else "NULL"),
                raw_llm=(raw or "")[:60], titulo_seccion=title,
                texto=(txt or "")[:6000],
                veredicto='', valor_correcto_si_no='', notas=''))
    conn.close()
    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=["campo", "nro_licitacion"], keep="first")
    out = out.sample(frac=1.0, random_state=7).reset_index(drop=True)
    fp = OUT / f"planilla_aceptacion_{tanda}_{'nulls' if tanda == 'C' else 'valores'}.csv"
    out.to_csv(fp, index=False)
    print(f"{len(out)} filas -> {fp}")
    print(out["campo"].value_counts().to_dict())


if __name__ == "__main__":
    main()

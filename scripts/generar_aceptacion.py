"""Planillas de aceptación v2 (plan de corrección F5; auditoría H3.2, H3.3, H3.4, H7.5).

Cambios vs v1 (sesión 22):
  - Unidad de muestreo = TEXTO DISTINTO (plantilla), no fila: en v1 `num_pasos`
    tenía 80 filas con 1 solo texto (n efectivo = 1). Cada ítem lleva cuántas filas
    del universo comparten ese texto (`filas_cubiertas`) para la métrica ponderada.
  - Estratos por campo: valor modal / valor no-modal / sin valor (NULL), y por fuente
    (rule / llm). Cuotas mínimas por estrato (los valores raros, sospechosos, entran).
  - CIEGA al valor (H3.4): el auditor ve el texto y escribe el valor correcto
    (`valor_auditor`) o marca `estado` = no_fija / ambiguo. El valor del sistema va
    en un archivo CLAVE aparte que el auditor no abre.
  - Incluye los 12 campos hybrid (en v1 faltaban 4 con valores) y los NULL.
  - n objetivo por campo = 70 plantillas (con precisión real 0,95 el LCB95 de Wilson
    supera 0,85 con ≥66/70); si hay menos plantillas distintas -> CENSO.

Salidas (data/processed/llm_benchmark/planillas/):
  aceptacion_v2_<run>.csv        planilla ciega (lo que se audita)
  aceptacion_v2_<run>_clave.csv  clave: id -> nro, campo, valor_sistema, source, filas_cubiertas
Uso:
  /root/vllm-env/bin/python scripts/generar_aceptacion.py --run-id <run_id> [--fields a,b] [--n 70]
"""
import argparse
import hashlib
import json
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402

OUT = ROOT / "data/processed/llm_benchmark/planillas"
SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
SEED = 20261006
MIN_STRATUM = 8


def norm_hash(text: str) -> str:
    return hashlib.sha256(re.sub(r"\s+", " ", text or "").strip().lower().encode()).hexdigest()[:16]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--fields", default="")
    ap.add_argument("--n", type=int, default=70)
    a = ap.parse_args()
    schemas = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    fdesc = {f["name"]: f.get("description", "") for k, v in schemas.items() if k != "meta"
             for f in v["fields"]}
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg, options="-c default_transaction_read_only=on")
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT field FROM dncp.llm_fallback_runs_v2 WHERE run_id=%s", (a.run_id,))
    fields = sorted(f for (f,) in cur.fetchall())
    if a.fields:
        fields = [f for f in fields if f in {x.strip() for x in a.fields.split(",")}]
    rng = random.Random(SEED)
    sheet, key, resumen = [], [], []
    for field in fields:
        cur.execute("""
            SELECT r.nro_licitacion, r.value, r.source, s.content_text
            FROM dncp.llm_fallback_runs_v2 r
            JOIN dncp.pliegos_secciones s
              ON s.nro_licitacion = r.nro_licitacion AND s.title = r.section_title
             AND s.line_start = r.section_line_start AND s.year = r.section_year
            WHERE r.run_id = %s AND r.field = %s AND r.status = 'done'""", (a.run_id, field))
        rows = cur.fetchall()
        if not rows:
            continue
        # agrupar por texto normalizado: una plantilla puede cubrir miles de filas
        by_text = defaultdict(list)
        for nro, val, src, text in rows:
            by_text[norm_hash(text)].append((nro, None if val is None else float(val), src, text))
        vals = [float(v) for _, v, _, _ in rows if v is not None]
        modal = max(set(vals), key=vals.count) if vals else None
        strata = defaultdict(list)
        for h, items in by_text.items():
            nro, val, src, text = items[0]
            if val is None:
                st = f"null|{src}"
            elif val == modal:
                st = f"modal|{src}"
            else:
                st = f"no_modal|{src}"
            strata[st].append((h, len(items), nro, val, src, text))
        n_texts = len(by_text)
        censo = n_texts <= a.n
        chosen = []
        if censo:
            for st, lst in strata.items():
                chosen += [(st, x) for x in lst]
        else:
            # mínimo por estrato, resto proporcional al nº de plantillas del estrato
            budget = a.n
            for st, lst in strata.items():
                take = min(len(lst), MIN_STRATUM)
                rng.shuffle(lst)
                chosen += [(st, x) for x in lst[:take]]
                budget -= take
            rest = [(st, x) for st, lst in strata.items() for x in lst[MIN_STRATUM:]]
            rng.shuffle(rest)
            chosen += rest[:max(budget, 0)]
        for st, (h, cubre, nro, val, src, text) in chosen:
            iid = hashlib.sha256(f"{a.run_id}|{field}|{h}".encode()).hexdigest()[:10]
            sheet.append(dict(id=iid, campo=field, descripcion=fdesc.get(field, ""),
                              texto=(text or "")[:6000], valor_auditor="", estado="", notas=""))
            key.append(dict(id=iid, campo=field, nro_licitacion=nro, valor_sistema=val,
                            source=src, estrato=st, filas_cubiertas=cubre,
                            filas_campo=len(rows), censo=censo))
        resumen.append(dict(campo=field, filas=len(rows), textos_distintos=n_texts,
                            muestreados=len(chosen), censo=censo,
                            estratos={k: len(v) for k, v in strata.items()}))
    conn.close()
    sh = pd.DataFrame(sheet).sample(frac=1.0, random_state=SEED).reset_index(drop=True)
    tag = a.run_id.split("-")[-1][:8]
    OUT.mkdir(parents=True, exist_ok=True)
    p1 = OUT / f"aceptacion_v2_{tag}.csv"
    p2 = OUT / f"aceptacion_v2_{tag}_clave.csv"
    sh.to_csv(p1, index=False)
    pd.DataFrame(key).to_csv(p2, index=False)
    print(f"{len(sh)} ítems -> {p1.name} (CIEGA) | clave -> {p2.name}")
    for r in resumen:
        print(f"  {r['campo']:34s} filas={r['filas']:6d} textos={r['textos_distintos']:6d} "
              f"muestra={r['muestreados']:3d}{' CENSO' if r['censo'] else ''} {r['estratos']}")


if __name__ == "__main__":
    main()

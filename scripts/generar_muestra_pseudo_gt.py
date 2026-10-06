"""Muestra para el pseudo-GT anotado por Claude Opus 5.5 (sesión 24, pedido del usuario).

Objetivo: segunda referencia INDEPENDIENTE para medir la varianza del benchmark entre modelos
y validar fuera de muestra las reglas/regex (que se desarrollaron sobre gt_por_campo).
Diseño:
  - 13 campos (12 hybrid + pct_monto_referencia_tecnica).
  - Población = secciones de producción del título de cada campo cuyo documento NO está en
    ningún archivo de gt_por_campo (fuera de muestra).
  - Unidad = texto distinto (normalizado); N por campo (default 30) sorteados con semilla fija.
  - CIEGA: el archivo solo tiene campo, descripción, guía y texto. El anotador no ve reglas,
    regex ni salidas de modelos.
Caveat: un pseudo-GT hecho por un LLM NO es GT humano; sirve como referencia independiente
(otro "anotador") para estimar varianza y robustez del ranking, no como verdad.

Salida: data/processed/llm_benchmark/pseudo_gt_opus/muestra.csv
"""
import argparse
import hashlib
import json
import random
import re
import sys
from pathlib import Path

import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402

SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
OUT = ROOT / "data/processed/llm_benchmark/pseudo_gt_opus"
SEED = 55


def norm(t):
    return re.sub(r"\s+", " ", t or "").strip().lower()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=30)
    a = ap.parse_args()
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    fields = {f["name"]: (k.replace("_", " "), f) for k, v in s.items() if k != "meta"
              for f in v["fields"] if f.get("extraction_method") == "hybrid"
              or f["name"] == "pct_monto_referencia_tecnica"}
    gt_nros = set()
    for p in GT_DIR.glob("*.csv"):
        if not p.name.startswith("errores_"):
            gt_nros |= set(pd.read_csv(p, usecols=["nro_licitacion"]).nro_licitacion.astype(str))
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg, options="-c default_transaction_read_only=on")
    cur = conn.cursor()
    rng = random.Random(SEED)
    rows = []
    for field in sorted(fields):
        title, fs = fields[field]
        cur.execute("""SELECT DISTINCT ON (nro_licitacion) nro_licitacion, content_text
                       FROM dncp.pliegos_secciones WHERE title_normalized = %s
                         AND content_text IS NOT NULL AND length(content_text) > 30
                       ORDER BY nro_licitacion, content_length DESC""", (title,))
        by_text = {}
        for nro, text in cur.fetchall():
            if str(nro) in gt_nros:
                continue
            by_text.setdefault(hashlib.sha256(norm(text).encode()).hexdigest(), (str(nro), text))
        keys = sorted(by_text)
        rng.shuffle(keys)
        for h in keys[:a.n]:
            nro, text = by_text[h]
            rows.append(dict(id=hashlib.sha256(f"{field}|{h}".encode()).hexdigest()[:10],
                             campo=field, tipo=fs["type"], descripcion=fs.get("description", ""),
                             guia=fs.get("extraction_hint", ""), nro_licitacion=nro,
                             texto=text[:6000], valor="", estado="", nota=""))
        print(f"  {field:34s} textos distintos fuera de GT={len(keys):6d} -> muestra {min(a.n, len(keys))}")
    conn.close()
    OUT.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(OUT / "muestra.csv", index=False)
    print(f"{len(df)} ítems -> {(OUT / 'muestra.csv').relative_to(ROOT)}")


if __name__ == "__main__":
    main()

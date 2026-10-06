"""
Fase 3 — Extract raw section samples for the top-26 finalist titles
so we can manually inspect text content and design v2 schemas.

Output: data/processed/schemas_v2/title_samples/
"""
import sys, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import DB_CONFIG
import psycopg2
import pandas as pd
import json

conn = psycopg2.connect(
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port'],
    dbname=DB_CONFIG['database'],
)

FINAL_TITLES = [
    # Top-18 auto por scoring v2.2 (ver docs/title_ranking_v2_final.md)
    'analisis de precios ofertados',
    'porcentaje de multas',
    'fuerza mayor',
    'porcentaje de garantia de fiel cumplimiento de contrato',
    'aclaracion de las ofertas',
    'experiencia requerida',
    'plazo de reposicion de bienes',
    'documentacion requerida para la firma del contrato',
    'causales de terminacion del contrato',
    'confidencialidad de la informacion',
    'impuestos y derechos',
    'suministros y especificaciones tecnicas',
    'derechos intelectuales',
    'copias de la oferta  cps',
    'periodo de validez de la garantia de los bienes',
    'capacidad tecnica',
    'formas y condiciones de pago',
    'plan de entrega de los bienes',
    # Filtro D forced (paramétricos por literatura)
    'capacidad financiera',
    'reajuste',
    'criterios de desempate de ofertas',
]

def slug_by_title(t):
    return t.replace(' ', '_')

SAMPLE_SIZE = 8

out_dir = Path('data/processed/schema_design_v2/samples')
out_dir.mkdir(parents=True, exist_ok=True)

all_samples = {}
for t in FINAL_TITLES:
    print(f"  {t}")
    with conn.cursor() as cur:
        cur.execute(
            """SELECT nro_licitacion, content_text, year, category_id, content_length
               FROM dncp.pliegos_secciones
               WHERE title_normalized = %s
               ORDER BY random()
               LIMIT %s""",
            (t, 200),
        )
        rows = cur.fetchall()
    if len(rows) < SAMPLE_SIZE:
        print(f"    ¡sólo {len(rows)} disponibles!")
        sample = rows
    else:
        # diversify by length: min, Q1, median, Q3, max + randoms
        lens = sorted(set(r[4] for r in rows))
        selected_indices = []
        for p in [0, 0.25, 0.50, 0.75, 1.0]:
            target = int(p * (len(lens)-1)) if len(lens) > 1 else 0
            # nearest row
            target_len = lens[min(target, len(lens)-1)]
            candidates = [i for i, r in enumerate(rows) if r[4] == target_len]
            if candidates:
                selected_indices.append(candidates[0])
        # fill remaining slots with random
        import random
        needed = SAMPLE_SIZE - len(selected_indices)
        remaining = [i for i in range(len(rows)) if i not in selected_indices]
        if needed > len(remaining):
            needed = len(remaining)
        selected_indices.extend(random.sample(remaining, needed))
        sample = [rows[i] for i in selected_indices[:SAMPLE_SIZE]]
    
    # guardar en JSON
    samples = []
    for nro, txt, year, cat, clen in sample:
        samples.append({
            'nro_licitacion': nro,
            'year': year,
            'category_id': cat,
            'length': clen,
            'text': txt[:4000] if txt else '',
        })
    all_samples[slug_by_title(t)] = samples
    
    # también escribir cada muestra individual como TXT para inspección fácil
    slug = slug_by_title(t).replace(' ', '_')
    for idx, s in enumerate(samples):
        safe_name = f"{slug}_{idx+1}.txt"
        path = out_dir / safe_name
        try:
            path.write_text(s['text'], encoding='utf-8', errors='ignore')
        except OSError as e:
            print(f"    WARN: no se pudo escribir {safe_name}: {e}")

out_path = Path('data/processed/schema_design_v2/title_samples.json')
out_path.parent.mkdir(parents=True, exist_ok=True)
with out_path.open('w', encoding='utf-8') as f:
    json.dump(all_samples, f, indent=2, ensure_ascii=False)

print(f"\n[OK] Samples escritos en: {out_path}")
print(f"     Per-title TXT:     {out_dir}/")
conn.close()
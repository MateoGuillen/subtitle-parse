"""
Fase 2.3 — Comparativo ranking v1 (6 estrategias) vs v2.2 (8 estrategias).

Output: docs/ranking_v1_vs_v22.md y salida tabular.
"""
import sys
sys.path.insert(0, '.')
import pandas as pd

v1 = pd.read_csv('data/processed/title_ranking/title_ranking.csv')
v22 = pd.read_csv('data/processed/title_ranking_v2/title_ranking.csv')

v1 = v1[['title_slug', 'score_total', 'rank']].rename(
    columns={'score_total': 'score_v1', 'rank': 'rank_v1'}
)
v22 = v22[['title_slug', 'score_total', 'rank', 'score_boilerplate',
           'score_numeric', 'score_economic', 'descartado_boilerplate']].rename(
    columns={'score_total': 'score_v2', 'rank': 'rank_v2'}
)

m = v1.merge(v22, on='title_slug', how='outer')
m['delta_rank'] = m['rank_v1'] - m['rank_v2']

print(f"v1: {len(v1)} titles, v2.2: {len(v22)} titles, interseccion: {m['title_slug'].notna().sum()}")
print()
print("=== TOP-20 v2.2 ===")
top = m.sort_values('rank_v2').head(20)
print(top.to_string(index=False))

# Ganadores: subieron mas posiciones en v2.2
print()
print("=== Top-15 ganadores (delta_rank mas positivo: subio posiciones de v1 -> v2.2) ===")
g = m.dropna(subset=['delta_rank'])
print(g.sort_values('delta_rank', ascending=False).head(15).to_string(index=False))

print()
print("=== Top-15 perdedores (delta_rank mas negativo: bajaron posiciones) ===")
print(g.sort_values('delta_rank', ascending=True).head(15).to_string(index=False))

# Sanity check: titulos sugeridos por doc
aspirantes = [
    'porcentaje_de_multas', 'solicitud_de_pago_de_anticipo',
    'tasa_de_interes_por_mora', 'capacidad_financiera',
    'capacidad_tecnica', 'experiencia_requerida',
    'formas_y_condiciones_de_pago', 'plazo_de_reposicion_de_bienes',
    'plan_de_entrega_de_los_bienes',
    'periodo_de_validez_de_la_garantia_de_los_bienes',
    'periodo_de_validez_de_las_ofertas', 'criterios_de_adjudicacion',
    'criterios_de_desempate_de_ofertas',
    'porcentaje_de_garantia_de_fiel_cumplimiento_de_con',
]
print()
print("=== Aspirantes doc: posicion v1 vs v2.2 ===")
for a in aspirantes:
    row = m[m['title_slug'] == a]
    if len(row):
        r = row.iloc[0]
        print(f"  {a:<55} v1 #{int(r['rank_v1']):3d}  v2 #{int(r['rank_v2']):3d}  delta {r['delta_rank']:+.0f}")

# Escribir MD
lines = ['# Comparativo Ranking v1 vs v2.2\n']
lines.append('## Resumen\n')
lines.append(f'- v1: {len(v1)} títulos rankeados (6 estrategias, K=10 hardcoded)')
lines.append(f'- v2.2: {len(v22)} títulos rankeados (8 estrategias, K=auto-elegido)')
lines.append('')
lines.append('## TOP-20 v2.2 con comparación a v1\n')
lines.append('| rank v2.2 | title | score v2.2 | rank v1 | score v1 | Δ rank | boilerplate | numeric | economic | descart |')
lines.append('|----------:|-------|------------:|--------:|---------:|-------:|------------:|--------:|---------:|:-------:|')
for _, r in top.iterrows():
    rank_v1 = int(r['rank_v1']) if pd.notna(r['rank_v1']) else 0
    score_v1 = float(r['score_v1']) if pd.notna(r['score_v1']) else 0.0
    delta = int(r['delta_rank']) if pd.notna(r['delta_rank']) else 0
    descart = 'Y' if r.get('descartado_boilerplate', False) else ''
    lines.append(
        f"| {int(r['rank_v2'])} | {r['title_slug']} | {r['score_v2']:.4f} | "
        f"{rank_v1} | {score_v1:.4f} | {delta:+d} | "
        f"{r['score_boilerplate']:.3f} | {r['score_numeric']:.3f} | "
        f"{r['score_economic']:.3f} | {descart} |"
    )

lines.append('')
lines.append('## Ganadores (mayor subida v1 → v2.2)\n')
lines.append('| title | v1 | v2.2 | Δ |')
lines.append('|-------|---:|-----:|--:|')
for _, r in g.sort_values('delta_rank', ascending=False).head(15).iterrows():
    lines.append(
        f"| {r['title_slug']} | #{int(r['rank_v1'])} | #{int(r['rank_v2'])} | {int(r['delta_rank']):+d} |"
    )

lines.append('')
lines.append('## Perdedores (mayor bajada v1 → v2.2)\n')
lines.append('| title | v1 | v2.2 | Δ |')
lines.append('|-------|---:|-----:|--:|')
for _, r in g.sort_values('delta_rank', ascending=True).head(15).iterrows():
    lines.append(
        f"| {r['title_slug']} | #{int(r['rank_v1'])} | #{int(r['rank_v2'])} | {int(r['delta_rank']):+d} |"
    )

import os
os.makedirs('docs', exist_ok=True)
with open('docs/ranking_v1_vs_v22.md', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
print()
print('Saved docs/ranking_v1_vs_v22.md')

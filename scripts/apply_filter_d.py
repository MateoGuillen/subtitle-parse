"""
Fase 2.3 (final) — Filtro A-D aplicado sobre ranking v2.2.

Combina:
    - Filtro C: descartar títulos con descartado_boilerplate=True (ya hecho en pipeline)
    - Filtro D (literatura de corrupción): whitelist de 12 títulos forzados

Output: docs/title_ranking_v2_final.md con la selección final de top-N títulos.
"""
import os
import sys
sys.path.insert(0, '.')

import pandas as pd

v22 = pd.read_csv('data/processed/title_ranking_v2/title_ranking.csv')
diag = pd.read_csv('data/processed/title_diagnostics/title_diagnostics.csv')

# Rename for consistency in downstream code
v22 = v22.rename(columns={'rank': 'rank_v2'})

# Filtro D — Títulos vector documentado de manipulación en contratación pública
# (literatura CGR/investigación). Forzados en top-N final independiente del rank.
forced_whitelist = [
    # Título completo en DB normalizada (no slug truncado)
    'porcentaje de garantia de fiel cumplimiento de contrato',
    'porcentaje de multas',
    'solicitud de pago de anticipo',
    'forma de instrumentacion de garantia de anticipo',
    'plazo de reposicion de bienes',
    'plan de entrega de los bienes',
    'periodo de validez de la garantia de los bienes',
    'capacidad financiera',
    'capacidad tecnica',
    'experiencia requerida',
    'requisitos de calificacion',
    'criterios de adjudicacion',
    'criterios de desempate de ofertas',
    'formas y condiciones de pago',
    'tasa de interes por mora',
    'reajuste',
]
# Convertimos a slug (espacios -> _)
forced_slugs = {t.replace(' ', '_') for t in forced_whitelist}

# Mapeo fuzzy: por cada forced slug, encontrar la mejor coincidencia en v22
# (por si el slug está truncado, ej: fiel_cumplimiento_de_con)
forced_caps_list = sorted(forced_slugs)
matched_forced = []
for s in forced_caps_list:
    s_set = set(v22['title_slug'])
    if s in s_set:
        matched_forced.append(s)
        continue
    # slug truncado — buscar v22 slugs que empiecen con el forced slug como prefix
    candidates = [x for x in v22['title_slug'] if x.startswith(s) or s.startswith(x)]
    if candidates:
        # tomar el mas largo (forced slug original puede estar truncado)
        best = max(candidates, key=len)
        matched_forced.append(best)
    else:
        matched_forced.append(None)

print('=== Forced whitelist matching ===')
for orig, match in zip(forced_caps_list, matched_forced):
    print(f"  {orig:<60} -> {match}")

# Construir top-N final
TOP_N = 18
# 1. Tomamos top-N del ranking v2.2
top_by_score = v22.head(TOP_N)['title_slug'].tolist()
# 2. Agregamos los forced que no hayan entrado ya
final_set = list(top_by_score)
for s in matched_forced:
    if s and s not in final_set:
        final_set.append(s)

# Reordenamos final por rank v2.2
final = v22[v22['title_slug'].isin(final_set)].copy()
# Marcamos forced
final['forced_inclusion'] = final['title_slug'].isin([m for m in matched_forced if m])

# Recover diagnostics for these slugs
def slug_to_diag(s):
    raw = s.replace('_', ' ')
    rows = diag[diag['title_normalized'] == raw]
    if len(rows):
        return rows.iloc[0]
    # fuzzy prefix
    candidates = diag[diag['title_normalized'].str.startswith(raw)]
    return candidates.iloc[0] if len(candidates) else pd.Series({})

diag_for_final = final['title_slug'].apply(slug_to_diag)
final['md5_top1_share'] = diag_for_final['md5_top1_share'].values
final['numeric_pattern_hits'] = diag_for_final['numeric_pattern_hits'].values
final['has_numeric_content'] = diag_for_final['has_numeric_content'].values
final = final.sort_values('rank_v2').reset_index(drop=True)

print()
print(f"=== Top-{len(final)} final (ranking v2.2 + Filtro D whitelist) ===")
print(f"{'rank':<5} {'slug':<55} {'score':<8} {'forced':<7} {'md5':<6} {'num_hits':<9} {'has_num':<8}")
print('-'*100)
for _, r in final.iterrows():
    print(
        f" #{int(r['rank_v2']):<3d}  {r['title_slug']:<55} {r['score_total']:.4f}   "
        f"{'Y' if r['forced_inclusion'] else '':<7} "
        f"{r['md5_top1_share']:.3f}   {r['numeric_pattern_hits']:.2f}     {'Y' if r['has_numeric_content'] else 'N'}"
    )

# Write report
table_rows = []
for _, r in final.iterrows():
    table_rows.append(
        f"| {int(r['rank_v2'])} | {r['title_slug']} | {r['score_total']:.4f} | "
        f"#{int(r['rank_v2'])} | {'Y' if r['forced_inclusion'] else ''} | "
        f"{r['md5_top1_share']:.3f} | {r['numeric_pattern_hits']:.2f} | "
        f"{'Y' if r['has_numeric_content'] else 'N'} |"
    )

lines = [
    '# Title Ranking v2.2 + Filtro D — Selección Final\n',
    f'Fecha: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")}',
    '',
    f'- Ranking inicial: {len(v22)} títulos (8 estrategias v2.2)',
    f'- Filtro C descartados: 86 (boilerplate >90% sin numérico)',
    f'- Filtro D whitelist forzados: {len([m for m in matched_forced if m])}/{len(forced_slugs)}',
    f'- Selección final: **{len(final)} títulos** (top-{TOP_N} por score + Filtro D)',
    '',
    '## Selección final',
    '',
    '| rank | title_slug | score | rank_v2 | forced | md5 | num_hits | has_num |',
    '|-----:|-------------|------:|--------:|:------:|----:|---------:|:-------:|',
]
lines.extend(table_rows)
lines.append('')
lines.append('## Aspirantes doc y su posición final')
lines.append('')
lines.append('| aspirante (slug truncado) | matched_v22 | rank v2.2 | forzado? | score |')
lines.append('|----------------------------|-------------|----------:|:--------:|------:|')
for orig, match in zip(forced_caps_list, matched_forced):
    if match is None:
        lines.append(f"| {orig} | NO MATCH | — | — | — |")
    else:
        row = v22[v22['title_slug'] == match].iloc[0]
        in_topN = int(row['rank_v2']) <= TOP_N
        lines.append(
            f"| {orig} | {match} | #{int(row['rank_v2'])} | "
            f"{'auto' if in_topN else 'Y'} | {row['score_total']:.4f} |"
        )

os.makedirs('docs', exist_ok=True)
with open('docs/title_ranking_v2_final.md', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
print()
print(f"Saved docs/title_ranking_v2_final.md — {len(final)} titles final")

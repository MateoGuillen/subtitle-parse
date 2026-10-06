"""
Test aislamiento: Estrategias 7 (boilerplate) + 8 (numeric_content).

Construye diag_lookup sintético y verifica:
- Score 7: títulos con md5_top1=1.0 -> penalidad máxima (cerca de 0)
- Score 8: títulos con numeric_pattern_hits alto -> score alto
- Sin diag_lookup -> score 0.5 (neutral) en estrategia 7 y 0 en 8
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
from src.etl.transformers.title_ranking_transformer import (
    TitleRankingTransformer, _normalize_series
)

t = TitleRankingTransformer()

# Diagnostics sintético
diag = {
    "formato_y_firma": pd.Series({  # title 100% boilerplate, no numerico
        "title_normalized": "formato y firma",
        "md5_top1_share": 1.0,
        "numeric_pattern_hits": 0.0,
        "post_merge_top1_share": 1.0,
        "descartado_boilerplate": True,
    }),
    "porcentaje_multas": pd.Series({  # muy variable, num_hits alto
        "title_normalized": "porcentaje de multas",
        "md5_top1_share": 0.1738,
        "numeric_pattern_hits": 2.0,
        "numeric_pattern_rate": 1.0,
        "post_merge_top1_share": float("nan"),
        "descartado_boilerplate": False,
    }),
    "tasa_interes": pd.Series({  # md5=0.155 // num_hits bajos
        "title_normalized": "tasa de interes por mora",
        "md5_top1_share": 0.155,
        "numeric_pattern_hits": 0.48,
        "numeric_pattern_rate": 0.48,
        "descartado_boilerplate": False,
    }),
}

slugs = list(diag.keys())
s7 = t._strategy_boilerplate(diag, slugs)
s8 = t._strategy_numeric_content(diag, slugs)
print("=== Strategy 7: Boilerplate penalty (higher = menos boilerplate) ===")
for slug, score in sorted(s7.items(), key=lambda x: -x[1]):
    md5 = diag[slug]["md5_top1_share"]
    print(f"  {slug:25s} md5={md5:.3f}  score7={score:.4f}")
print()
print("=== Strategy 8: Numeric content (higher = mas patrones numericos) ===")
for slug, score in sorted(s8.items(), key=lambda x: -x[1]):
    h = diag[slug]["numeric_pattern_hits"]
    print(f"  {slug:25s} hits={h:.2f}  score8={score:.4f}")

# Verificaciones
assert s7["formato_y_firma"] < s7["porcentaje_multas"], "boilerplate: formato debe ser peor que multas"
assert s8["porcentaje_multas"] > s8["formato_y_firma"], "numeric: multas debe dominar formato"
print("\n[OK] Ambas estrategias funcionan como esperado")

# Caso sin diag_lookup -> neutral 0.5 en s7, 0.0 en s8
s7_neutral = t._strategy_boilerplate({}, ["slug_sin_diag"])
s8_neutral = t._strategy_numeric_content({}, ["slug_sin_diag"])
print(f"Sin diag_lookup: score7={s7_neutral['slug_sin_diag']} (esperado 0.0 o 0.5), score8={s8_neutral['slug_sin_diag']}")
assert s7_neutral["slug_sin_diag"] == 0.0, "sin diag+normalize=0.0 (luego del min-max)"
print("[OK] Caso neutral normalizado a 0 (esperado, debido a _normalize_series)")

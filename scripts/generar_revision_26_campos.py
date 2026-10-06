"""Revisión de los 26 campos GT iniciales: tipo original (schema), campo actual,
precisión alcanzada (comparativa + gt_precision) y métodos aplicados.

Genera:
  data/processed/alolab/revision_26_campos.csv
  docs/revision_26_campos.md
"""
import json, sys
from pathlib import Path
sys.path.insert(0, ".")
import pandas as pd

SCHEMAS = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
COMP = Path("data/processed/alolab/comparativa_estrategias.csv")
PREC = Path("data/processed/alolab/gt_precision_por_campo.csv")
OUT_CSV = Path("data/processed/alolab/revision_26_campos.csv")
OUT_MD = Path("docs/revision_26_campos.md")

with open(SCHEMAS, encoding="utf-8") as f:
    schemas = json.load(f)
comp = pd.read_csv(COMP, encoding="utf-8")
prec = pd.read_csv(PREC, encoding="utf-8")

prec_map = {r["campo"]: r for _, r in prec.iterrows()}

# método original = hybrid (todos los campos evaluados venían de la lista hybrid 23
# + 3 portados antes). Usamos el schema Actual y marcamos el original según historial.
# Historial conocido de portes (orden cronológico):
PORTADOS = {
    "pct_garantia_cumplimiento": "Sesión 15 (GT validado 64/64)",
    "pct_diferencia_precio_bajo": "Sesión 15 (GT validado 156/156)",
    "pct_diferencia_precio_alto": "Sesión 15 (GT validado 129/129)",
    "dias_presentacion_garantia": "Sesión 15 final (regex robusto, 200/200)",
    "pct_anticipo": "Sesión 15 final (GT todo-NULL, evita 95 FP LLM)",
    "num_categorias_documentos": "Sesión 16 (regex conteo ítems nivel 1, 200/200)",
    "num_referencias_minimo": "Sesión 16 (regex mínimo N referencias/contratos, 98.0%)",
    "num_referencias_tecnicas_minimo": "Sesión 16 (regex mínimo N constancias, 97.0%)",
    "dias_reposicion": "Sesión 16 (regex plazo reposición será de, 92.0%)",
    "pct_monto_referencia_tecnica": "Sesión 16 (regex N% del monto, 90.0%)",
    "pct_monto_referencia_minimo": "Sesión 16 (regex N% como mínimo del monto, 90.5%)",
    "anos_experiencia_minima": "Sesión 16 (regex al menos N años, 80.0%)",
}
# deterministic_pct (autoritativo) sin cambiar el extraction_method a regex:
DETERMINISTA = {
    "pct_interes_mora_diario",
    "pct_multa_diaria",
}
# En el schema v2 ORIGINAL (hito Sesión 10): 55 regex / 26 hybrid.

METHOD_NOTES = {
    "pct_interes_mora_diario": "deterministic_pct (del:N) autoritativo",
    "pct_multa_diaria": "deterministic_pct (sera de:N, excluye hora atraso)",
    "pct_monto_referencia_minimo": "regex (N% como mínimo del monto)",
    "dias_pago_maximo": "LLM + postprocess (contexto factura + regla 182)",
    "ratio_endeudamiento_maximo": "LLM + parse_response strict",
    "pct_monto_referencia_tecnica": "regex (N% del monto/precio)",
    "dias_aceptacion_rechazo_factura": "LLM + strict context",
    "ratio_liquidez_minimo": "LLM + postprocess",
    "dias_validez_garantia": "conversión meses->dias determinista",
    "dias_solicitud_anticipo": "LLM bruto (strict rechaza valores validos) + KNOWN_DEFAULTS",
    "dias_reposicion": "regex (plazo reposición será de N días)",
    "anos_experiencia_minima_tecnica": "LLM + postprocess anti-FP (items lista, personal)",
    "dias_pago_anticipo": "LLM bruto + KNOWN_DEFAULTS/5 dias contexto",
    "dias_presentacion_garantia": "regex plantilla 'dentro de (los|la) N dias' + diez (10)",
    "anos_experiencia_minima": "regex (al menos N años)",
    "pct_anticipo": "regex (ausencia = NULL correcto; sin LLM)",
    "num_referencias_tecnicas_minimo": "regex (mínimo N constancias)",
    "num_categorias_documentos": "regex (conteo ítems nivel 1)",
    "num_referencias_minimo": "regex (mínimo N referencias/contratos)",
    "num_componentes_formula": "irresoluble (LLM no cuenta formula)",
    "num_documentos_pago": "irresoluble (LLM no cuenta documentos)",
    "num_pasos_verificacion": "irresoluble (LLM no cuenta pasos)",
    "dias_notificacion_fuerza_mayor": "LLM (regex solo 5.5%)",
}

rows = []
for _, r in comp.iterrows():
    campo = r["campo"]
    ftype = next(
        (f["type"] for slug, sc in schemas.items() if slug != "meta" for f in sc["fields"] if f["name"] == campo),
        "?",
    )
    method_actual = next(
        (f.get("extraction_method") for slug, sc in schemas.items() if slug != "meta" for f in sc["fields"] if f["name"] == campo),
        "?",
    )
    prec_row = prec_map.get(campo)
    acc_prec = prec_row["acc_flex"] if prec_row is not None else float("nan")

    mejor = r["best"]
    acc_mejor = max(r["acc_flex_r"], r["acc_flex_ri"], r["acc_flex_l_raw"], r["acc_flex_l_pp"], r["acc_flex_h"])
    val = r["gt_valor"]
    null = r["gt_null"]

    if campo in PORTADOS:
        metodo_original = "hybrid → regex"
        porte = PORTADOS[campo]
    elif campo in DETERMINISTA:
        metodo_original = "hybrid → regex"
        porte = "deterministic_pct autoritativo (consolidado Sesión 15)"
    else:
        metodo_original = "hybrid"
        porte = ""

    rows.append({
        "campo": campo,
        "tipo": ftype,
        "metodo_original": metodo_original if porte else "hybrid",
        "metodo_actual": method_actual,
        "porte": porte,
        "gt_valor_total": val,
        "gt_null": null,
        "acc_best": acc_mejor,
        "estrategia_best": mejor,
        "acc_llm_precision_report": acc_prec,
        "metodo_aplicado": METHOD_NOTES.get(campo, ""),
    })

rev = pd.DataFrame(rows).sort_values("acc_best", ascending=False)

rev.to_csv(OUT_CSV, index=False, encoding="utf-8")

lines = [
    "# Revisión de los 26 campos iniciales del GT",
    "",
    "Revisión generada desde `data/processed/alolab/comparativa_estrategias.csv` (5,066 juicios",
    "congelados) + `master_schemas_v2.json` (método actual) + `gt_precision_por_campo.csv`.",
    "",
    "Todos los campos evaluados venían originalmente como **hybrid** (26). La columna",
    "`metodo_original` registra los portes documentados a **regex** a lo largo de Session 15-16",
    "(schema inicial 55 regex / 26 hybrid → actual **69 regex / 12 hybrid**).",
    "",
    "- `acc_best`: mejor acc flex alcanzada por alguna de las 5 estrategias (R/Ri/L_raw/L_pp/H).",
    "- `acc_llm_precision_report`: precisión del LLM en producción (reporte Sesión 10, solo base LLM).",
    "- `estrategia_best`: qué estrategia logró `acc_best`.",
    "",
    "| Campo | Tipo | Método orig | Método actual | Porte | GT val | GT NULL | acc_best | best | LLM prod | Método aplicado |",
    "|---|---|---|---|---|---|---:|---:|---:|---:|---|",
]
for _, r in rev.iterrows():
    lines.append(
        f"| {r['campo']} | {r['tipo']} | {r['metodo_original']} | {r['metodo_actual']} | "
        f"{r['porte']} | {int(r['gt_valor_total'])} | {int(r['gt_null'])} | "
        f"{r['acc_best']:.1f} | {r['estrategia_best']} | {r['acc_llm_precision_report']:.1f} | "
        f"{r['metodo_aplicado']} |"
    )
lines += [
    "",
    "## Resumen por estado",
    "",
    "- **Campos al 100% o casi con regex/determinista (14)**: los 7 de Sesión 15 (`pct_interes_mora_diario`,",
    "  `pct_multa_diaria`, `pct_diferencia_precio_bajo`, `pct_diferencia_precio_alto`,",
    "  `pct_garantia_cumplimiento`, `pct_anticipo`, `dias_presentacion_garantia`) más 7 de Sesión 16",
    "  (`num_categorias_documentos` 100, `num_referencias_minimo` 98, `num_referencias_tecnicas_minimo` 97,",
    "  `dias_reposicion` 92, `pct_monto_referencia_minimo` 90.5, `pct_monto_referencia_tecnica` 90,",
    "  `anos_experiencia_minima` 80) → 0 GPU, sin falsos positivos.",
    "- **Campos que dependen del LLM (híbridos, 9)**: texto variable; el LLM aporta valor real.",
    "- **Campos irresolubles con extracción híbrida (3)**: `num_componentes_formula`,",
    "  `num_documentos_pago`, `num_pasos_verificacion` — el LLM no 'cuenta' listas/fórmulas;",
    "  el GT humano aplica conteo semántico. Fuera del alcance de regex/LLM.",
    "",
    "Detalle de arquitectura y justificación: [`tesis_b_c_resultados.md`](tesis_b_c_resultados.md).",
]

OUT_MD.write_text("\n".join(lines), encoding="utf-8")
print(f"CSV: {OUT_CSV}")
print(f"MD : {OUT_MD}")
print(f"Campos: {len(rev)}")
print(rev[["campo", "metodo_actual", "acc_best", "estrategia_best"]].to_string(index=False))
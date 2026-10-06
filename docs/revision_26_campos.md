# Revisión de los 26 campos iniciales del GT

Revisión generada desde `data/processed/alolab/comparativa_estrategias.csv` (5,066 juicios
congelados) + `master_schemas_v2.json` (método actual) + `gt_precision_por_campo.csv`.

Todos los campos evaluados venían originalmente como **hybrid** (26). La columna
`metodo_original` registra los portes documentados a **regex** a lo largo de Session 15-16
(schema inicial 55 regex / 26 hybrid → actual **69 regex / 12 hybrid**).

- `acc_best`: mejor acc flex alcanzada por alguna de las 5 estrategias (R/Ri/L_raw/L_pp/H).
- `acc_llm_precision_report`: precisión del LLM en producción (reporte Sesión 10, solo base LLM).
- `estrategia_best`: qué estrategia logró `acc_best`.

| Campo | Tipo | Método orig | Método actual | Porte | GT val | GT NULL | acc_best | best | LLM prod | Método aplicado |
|---|---|---|---|---|---|---:|---:|---:|---:|---|
| dias_presentacion_garantia | int | hybrid → regex | regex | Sesión 15 final (regex robusto, 200/200) | 200 | 0 | 100.0 | H | 57.5 | regex plantilla 'dentro de (los|la) N dias' + diez (10) |
| pct_diferencia_precio_alto | float | hybrid → regex | regex | Sesión 15 (GT validado 129/129) | 129 | 71 | 100.0 | H | 100.0 |  |
| pct_diferencia_precio_bajo | float | hybrid → regex | regex | Sesión 15 (GT validado 156/156) | 156 | 44 | 100.0 | H | 100.0 |  |
| pct_garantia_cumplimiento | float | hybrid → regex | regex | Sesión 15 (GT validado 64/64) | 64 | 2 | 100.0 | H | 100.0 |  |
| pct_interes_mora_diario | float | hybrid → regex | regex | deterministic_pct autoritativo (consolidado Sesión 15) | 102 | 98 | 100.0 | H | 100.0 | deterministic_pct (del:N) autoritativo |
| pct_multa_diaria | float | hybrid → regex | regex | deterministic_pct autoritativo (consolidado Sesión 15) | 105 | 95 | 100.0 | H | 100.0 | deterministic_pct (sera de:N, excluye hora atraso) |
| pct_anticipo | float | hybrid → regex | regex | Sesión 15 final (GT todo-NULL, evita 95 FP LLM) | 0 | 200 | 100.0 | Ri | 52.5 | regex (ausencia = NULL correcto; sin LLM) |
| num_categorias_documentos | int | hybrid → regex | regex | Sesión 16 (regex conteo ítems nivel 1, 200/200) | 86 | 114 | 100.0 | Ri | 0.0 | regex (conteo ítems nivel 1) |
| num_referencias_minimo | int | hybrid → regex | regex | Sesión 16 (regex mínimo N referencias/contratos, 98.0%) | 24 | 176 | 98.0 | Ri | 4.0 | regex (mínimo N referencias/contratos) |
| num_referencias_tecnicas_minimo | int | hybrid → regex | regex | Sesión 16 (regex mínimo N constancias, 97.0%) | 11 | 189 | 97.0 | Ri | 29.0 | regex (mínimo N constancias) |
| dias_solicitud_anticipo | int | hybrid | hybrid |  | 100 | 100 | 96.5 | L_raw | 85.0 | LLM bruto (strict rechaza valores validos) + KNOWN_DEFAULTS |
| dias_notificacion_fuerza_mayor | int | hybrid | hybrid |  | 189 | 11 | 94.5 | H | 94.5 | LLM (regex solo 5.5%) |
| dias_reposicion | int | hybrid → regex | regex | Sesión 16 (regex plazo reposición será de, 92.0%) | 136 | 64 | 92.0 | Ri | 65.5 | regex (plazo reposición será de N días) |
| pct_monto_referencia_minimo | float | hybrid → regex | regex | Sesión 16 (regex N% como mínimo del monto, 90.5%) | 101 | 99 | 91.0 | H | 70.0 | regex (N% como mínimo del monto) |
| pct_monto_referencia_tecnica | float | hybrid → regex | regex | Sesión 16 (regex N% del monto, 90.0%) | 49 | 151 | 90.0 | H | 86.5 | regex (N% del monto/precio) |
| dias_pago_maximo | int | hybrid | hybrid |  | 200 | 0 | 88.5 | H | 72.5 | LLM + postprocess (contexto factura + regla 182) |
| ratio_endeudamiento_maximo | float | hybrid | hybrid |  | 172 | 28 | 86.5 | L_pp | 86.5 | LLM + parse_response strict |
| dias_pago_anticipo | int | hybrid | hybrid |  | 102 | 98 | 86.5 | L_raw | 83.0 | LLM bruto + KNOWN_DEFAULTS/5 dias contexto |
| dias_aceptacion_rechazo_factura | int | hybrid | hybrid |  | 198 | 2 | 85.0 | L_pp | 85.0 | LLM + strict context |
| ratio_liquidez_minimo | float | hybrid | hybrid |  | 170 | 30 | 82.0 | H | 72.0 | LLM + postprocess |
| dias_validez_garantia | int | hybrid | hybrid |  | 72 | 128 | 82.0 | H | 76.0 | conversión meses->dias determinista |
| anos_experiencia_minima | int | hybrid → regex | regex | Sesión 16 (regex al menos N años, 80.0%) | 108 | 92 | 80.0 | Ri | 40.0 | regex (al menos N años) |
| anos_experiencia_minima_tecnica | int | hybrid | hybrid |  | 40 | 160 | 78.0 | L_pp | 78.0 | LLM + postprocess anti-FP (items lista, personal) |
| num_componentes_formula | int | hybrid | hybrid |  | 195 | 5 | 3.0 | H | 3.0 | irresoluble (LLM no cuenta formula) |
| num_documentos_pago | int | hybrid | hybrid |  | 195 | 5 | 2.5 | Ri | 0.0 | irresoluble (LLM no cuenta documentos) |
| num_pasos_verificacion | int | hybrid | hybrid |  | 197 | 3 | 1.5 | Ri | 0.0 | irresoluble (LLM no cuenta pasos) |

## Resumen por estado

- **Campos al 100% o casi con regex/determinista (14)**: los 7 de Sesión 15 (`pct_interes_mora_diario`,
  `pct_multa_diaria`, `pct_diferencia_precio_bajo`, `pct_diferencia_precio_alto`,
  `pct_garantia_cumplimiento`, `pct_anticipo`, `dias_presentacion_garantia`) más 7 de Sesión 16
  (`num_categorias_documentos` 100, `num_referencias_minimo` 98, `num_referencias_tecnicas_minimo` 97,
  `dias_reposicion` 92, `pct_monto_referencia_minimo` 90.5, `pct_monto_referencia_tecnica` 90,
  `anos_experiencia_minima` 80) → 0 GPU, sin falsos positivos.
- **Campos que dependen del LLM (híbridos, 9)**: texto variable; el LLM aporta valor real.
- **Campos irresolubles con extracción híbrida (3)**: `num_componentes_formula`,
  `num_documentos_pago`, `num_pasos_verificacion` — el LLM no 'cuenta' listas/fórmulas;
  el GT humano aplica conteo semántico. Fuera del alcance de regex/LLM.

Detalle de arquitectura y justificación: [`tesis_b_c_resultados.md`](tesis_b_c_resultados.md).
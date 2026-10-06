# Decisión de campos: qué se extrae con regex y qué con LLM (por qué)

> Documento para tutores: explica el **criterio metodológico** usado para elegir qué variables del
> pliego se extraen con **regex determinista** y cuáles con **LLM (hybrid)**, con la lista exacta de
> los 12 campos hybrid y la evidencia de los portes a regex. Fuente:
> `master_schemas_v2.json`, `docs/revision_26_campos.md`, `comparativa_estrategias.csv`.

## 1. Contexto: de 26 hybrid a 69 regex / 12 hybrid

Todos los 26 campos iniciales del golden set fueron diseñados como **hybrid** (regex como primera
pasada + LLM como fallback). Durante las Sesiones 15-16, **14 campos se portaron a regex puro** porque
la evidencia del golden set (5,066 juicios humanos congelados) mostró que un patrón determinista los
resolvía con ≥90% de acierto y **cero falsos positivos**, sin costo GPU y sin riesgo de alucinación.

Estado final del esquema: **81 campos = 69 regex + 12 hybrid**.

## 2. La regla de decisión aplicada campo por campo

| # | Pregunta para el campo | Decisión |
|---|---|---|
| 1 | ¿El pliego usa un **patrón plantilla estable** (misma redacción en la mayoría de los pliegos)? | Probar **regex**. Si valida **≥90% en GT con 0 FPs** → **regex definitivo** |
| 2 | ¿El valor exige **comprensión semántica**, desambiguación entre cláusulas similares o conversión contextual (meses→días)? | **Hybrid (LLM)** + postproceso determinista |
| 3 | ¿El valor exige **"contar" elementos semánticos** (componentes de fórmula, documentos, pasos)? | El LLM no cuenta listas; el regex solo cuenta ítems numerados nivel-1 → si no aplica, **irresoluble** (se mantiene hybrid, rendimiento bajo documentado) |

Fórmula práctica (alineada con la literatura): patrón plantilla → **regex** (OCP & Development
Gateway 2016: red flags computables con estructura; World Bank 2011: rigged specifications
detectables por patrón). Comprensión semántica → **LLM** (Katona & Fazekas 2024: el texto predice
riesgo; Lima et al. 2020; Peppes et al. 2026). El **postproceso determinista** compartido
(`extraction_postprocess.py`) es lo que lleva al LLM de 51% (crudo) a 63.1% y al híbrido a 65.1%.

## 3. Evidencia que justificó cada porte a regex

Los 14 portes (Sesiones 15-16) con su precisión de regex y la subida vs la mejor estrategia LLM
anterior:

| Campo | acc regex | FP | Subida vs LLM previo | Patrón usado |
|---|---|---|---|---|
| `pct_diferencia_precio_bajo` | 100.0 | 0 | — | `superior al N%` (TP 156/156) |
| `pct_diferencia_precio_alto` | 100.0 | 0 | — | `y (del) N%` (TP 129/129) |
| `pct_garantia_cumplimiento` | 100.0 | 0 | — | `N,N%` literal (TP 64/64) |
| `pct_interes_mora_diario` | 100.0 | 0 | — | `deterministic_pct` autoritativo |
| `pct_multa_diaria` | 100.0 | 0 | — | `deterministic_pct` autoritativo |
| `pct_anticipo` | 100.0 | 0 | +47.5 | ausencia = NULL correcto (evita 95 FP LLM) |
| `dias_presentacion_garantia` | 100.0 | 0 | +42.5 | plantilla "dentro de los N días" (200/200) |
| `num_categorias_documentos` | 100.0 | 0 | **+88** | conteo ítems nivel 1 (200/200) |
| `num_referencias_minimo` | 98.0 | 0 | **+87.5** | "mínimo de N referencias/contratos" |
| `num_referencias_tecnicas_minimo` | 97.0 | 0 | **+66.5** | "mínimo de N constancias" |
| `dias_reposicion` | 92.0 | 0 | **+25** | "reposición… será de N días" |
| `pct_monto_referencia_minimo` | 90.5 | 1 | −0.5 (empate) | "N% como mínimo del monto" |
| `pct_monto_referencia_tecnica` | 90.0 | 1 | +5.5 | "N% del monto/precio" |
| `anos_experiencia_minima` | 80.0 | 6 | **+23** | "al menos N años" / "mínimo de N años" |

Nota: `pct_monto_referencia_minimo` se portó aunque es empate técnico (90.5 vs 91.0) por **ahorro de
GPU sin pérdida práctica**; `anos_experiencia_minima` se portó con 80% (fp6) porque el LLM solo daba
57% y el postprocess elimina los 6 FPs residuales.

## 4. Los 12 campos que se quedaron como hybrid (y por qué NO se portan)

| Campo | Tipo | Razón de permanecer en LLM |
|---|---|---|
| `dias_notificacion_fuerza_mayor` | int | regex lograba solo **5.5%** (wording variable: "notificar fehacientemente dentro de los N días"); LLM **94.5%** |
| `dias_pago_maximo` | int | el número debe asociarse a "después de la presentación de la factura", no a otros plazos del párrafo; LLM 88.5% |
| `dias_aceptacion_rechazo_factura` | int | exige proximidad semántica a "aceptar/rechazar/factura"; LLM 85% |
| `dias_solicitud_anticipo` | int | desambiguación fuerte: vs `dias_pago_anticipo` (abonar vs solicitar) y vs el plazo de reclamo del punto 5; reglas postprocess + KNOWN_DEFAULTS; LLM 96.5% |
| `dias_pago_anticipo` | int | desambiguación: solo "abonar/pagar", ignorar "solicitar"; LLM 86.5% |
| `dias_validez_garantia` | int | conversión meses/años→días con la palabra en paréntesis (doce, seis…); se resuelve con conversión determinista en postprocess; LLM 82% |
| `anos_experiencia_minima_tecnica` | int | desambiguación vs cantidad de personal ("5 (cinco) personal técnico") y antigüedad en meses; reglas anti-FP (ítems de lista, "personal"); LLM 78% |
| `ratio_liquidez_minimo` | float | decimal en prosa sin patrón plantilla ("mayor que N", ">= N", "igual o mayor a N"); LLM 82% |
| `ratio_endeudamiento_maximo` | float | idem; LLM 86.5% |
| `num_documentos_pago` | int | **irresoluble**: el LLM no cuenta documentos de primer nivel (acc 2.5%); se mantiene hybrid sin excluir |
| `num_pasos_verificacion` | int | **irresoluble**: conteo semántico de pasos (acc 1.5%) |
| `num_componentes_formula` | int | **irresoluble**: el LLM no cuenta componentes de fórmula (acc 3%) |

**Patrón claro**: los hybrid son casi todos **campos numéricos que dependen de contexto** (días con
cláusula asociada, ratios en prosa, conversiones) o **conteos semánticos**; los regex son
**binarios de presencia/ausencia** (keyword matching) y **magnitudes con plantilla estable** (pct_*,
días de plantilla, conteo de ítems numerados).

## 5. Distribución por título (para contexto)

Los 81 campos se reparten en 25 títulos. Los títulos con campos hybrid: `fuerza_mayor` (1),
`periodo_de_validez_de_la_garantia_de_los_bienes` (1), `capacidad_tecnica` (1), `formas_y_condiciones_de_pago` (3),
`capacidad_financiera` (2), `reajuste` (1), `requisitos_de_calificacion` (1), `solicitud_de_pago_de_anticipo` (2).
El resto de títulos son 100% regex (binarios + pct uniformes).

## Archivos de referencia

- `data/processed/section_clustering_v2/master_schemas_v2.json` — `extraction_method` por campo + hints.
- `docs/revision_26_campos.md` — acc_best y método aplicado por campo.
- `data/processed/alolab/comparativa_estrategias.csv` — 5,066 juicios GT, acc por estrategia.
- `docs/tesis_b_c_resultados.md` — arquitectura y justificación de la capa de postproceso.

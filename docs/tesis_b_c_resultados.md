# Tesis B+C: Arquitectura Híbrida de Extracción y Evaluación Comparativa

> Proyecto: `D:\projects\subtitle-parse`
> Sesión: 15 — Fases 1-3 del plan B+C (documentado en esta revisión)
> Objetivo: demostrar empíricamente dónde aporta el LLM en la extracción estructurada
> de `document_features_v2` (26 campos híbridos) y materializarlo en una capa de
> postproceso determinista compartida, con una evaluación GPU-free contra el GT humano congelado.

---

## Índice

1. [Motivación](#1-motivación)
2. [Arquitectura de 3 capas (núcleo B)](#2-arquitectura-de-3-capas-núcleo-b)
3. [Patrones deterministas portados a regex (B)](#3-patrones-deterministas-portados-a-regex-b)
4. [Evaluación comparativa (C)](#4-evaluación-comparativa-c)
5. [Dónde aporta el LLM y dónde no](#5-dónde-aporta-el-llm-y-dónde-no)
6. [Implicancias para la arquitectura de producción](#6-implicancias-para-la-arquitectura-de-producción)
7. [Costos y throughput](#7-costos-y-throughput)
8. [Limitaciones y reproducibilidad](#8-limitaciones-y-reproducibilidad)
9. [Referencias](#9-referencias)

---

## 1. Motivación

El pipeline de `document_features_v2` mezcla 81 campos de extracción:

- **69 regex puros** (no requieren LLM — se resuelven con patrones deterministas; Sesión 16 portó 7 híbridos más).
- **12 híbridos** (regex como primera línea + fallback LLM cuando la confianza < 0.8).

Las sesiones 8-14 demostraron que el LLM (vía fallback) puede llegar a **100%** de precisión
sobre 100 muestras de GT humano — pero con un costo de GPU alto (miles de llamadas) y con
dependencia del prompt/hints. Esta tesis establece, con el GT humano congelado de **5,066
juicios sobre 26 campos**, la **mejor estrategia por campo** y cuantifica el **valor real del LLM**
frente a una capa determinista barata.

Preguntas centrales:

1. ¿Cuánto del accuracy del LLM es atribuible a patrones deterministas que no necesitan LLM?
2. ¿En qué campos el LLM aporta valor genuino (texto variable, semántica) y en cuáles sobra?
3. ¿Qué arquitectura de producción minimiza el costo de GPU manteniendo la calidad del GT?

---

## 2. Arquitectura de 3 capas (núcleo B)

Nueva capa compartida: `src/etl/transformers/extraction_postprocess.py`.

```
┌───────────────────────────────────────────────────────────────────────┐
│                 CAPA 1 · DETERMINISTA (autoritativa)                  │
│   deterministic_pct(field, text)  — 7 campos pct uniformes            │
│   patrón literal del pliego → valor. Ausencia → None.                 │
│   NUNCA cae al LLM. conf = 1.0.                                       │
├───────────────────────────────────────────────────────────────────────┤
│                 CAPA 2 · REGEX (CPU, v2 transformer)                  │
│   _extract_field_text() — 69 campos regex puros + 12 híbridos         │
│   Patrones de plantilla validados contra GT (ver §3).                 │
├───────────────────────────────────────────────────────────────────────┤
│                 CAPA 3 · LLM FALLBACK (GPU, solo hybrid)              │
│   document_features_v2_llm_transformer — batch vLLM offline           │
│   Modelo: Llama 3.1 8B-Instruct-AWQ (awq_marlin)                      │
└───────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────────────┐
│      CAPA DE POSTPROCESO (determinista, SIEMPRE aplicada)             │
│   postprocess(field, v, text, strict)                                  │
│   - PCT_INT_SCALE: ×100 (pliego dice "20%" → GT=20.0)                 │
│   - contexto "N%": el número debe aparecer literal con %               │
│   - meses/años → días (dias_validez_garantia)                          │
│   - reglas anti-FP (ítems de lista, personal, "5 días" vs "5")         │
│   parse_response(raw, field, text) = determinista → NULL → postprocess │
└───────────────────────────────────────────────────────────────────────┘
```

### 2.1 Mitos y decisiones de diseño

| Decisión | Razón |
|---|---|
| `deterministic_pct` es **autoritativa** (no fusiona con LLM) | El patrón literal ES la regla del GT humano; el LLM aporta ruido |
| `postprocess(strict=True)` en la ruta LLM | El número debe existir **literalmente** en el texto con la unidad correcta, si no → NULL |
| `postprocess(strict=False)` en la ruta regex | El regex ya garantiza que el número provino del texto |
| `parse_response` con forma estable | Usado idéntico por medición (`benchmark_multi_vs_gt.py`) y por producción (`_parse_response`) |
| `KNOWN_DEFAULTS` + excepciones (182, "5 días" con contexto) | Valores modales frágiles: cualquier hint induce sobre-extracción (lección Sesión 14) |

### 2.2 Fuente de verdad única

`extraction_postprocess.py` se importa desde:

- `src/etl/transformers/document_features_v2_transformer.py` (ruta regex, strict=False)
- `src/etl/transformers/document_features_v2_llm_transformer.py` (`_parse_response`, strict=True)
- `scripts/benchmark_multi_vs_gt.py` (medición, strict=True)
- `scripts/comparativa_estrategias.py` (evaluación C)

La API de `benchmark_multi_vs_gt.py` (`MODELS`, `GT`, `parse`, `postprocess`, `build_prompt`,
`get_hint`, `TITLE_MAP`, ...) se preservó — `gold_standard_26campos.py`, `alolab_collect_errors.py`
y los scripts few-shot/gt_extension siguen importando sin cambios.

---

## 3. Patrones deterministas portados a regex (B)

Los portes de `hybrid` a `regex` real en `master_schemas_v2.json` dejaron los campos de
plantilla 100% deterministas sin GPU. La Sesión 15 cerró en **62 regex / 19 hybrid**; la
Sesión 16 portó 7 más → **69 regex / 12 hybrid** (ver `revision_26_campos.md`):

| Campo | Patrón | TP/GT | NULL | Validación |
|---|---|:---:|:---:|---|
| `pct_diferencia_precio_bajo` | `superior\s+al\s+N%` | 156/156 | 44 | 100%, 0 FP |
| `pct_diferencia_precio_alto` | `y\s+(?:del\s+)?N%` | 129/129 | 71 | 100%, 0 FP (captura "y del 15% ... por encima") |
| `pct_garantia_cumplimiento` | `N,N%` literal | 64/64 | 2 | 100%, 0 FP |

Observación: las 22 filas NULL "extras" de bajo/alto corresponden a texto vacío — el pipeline
las rellena a `0.0` por diseño; la capa determinista devuelve `None` correcto.

`deterministic_pct` cubre ahora **7 campos** (los 3 nuevos + `pct_interes_mora_diario`,
`pct_multa_diaria`):

| Campo | Patrón | Rango válido |
|---|---|---|
| `pct_interes_mora_diario` | `del:? N` | [0.01, 100] |
| `pct_multa_diaria` | `(será/es/es de):? N`, excluye "hora de atraso" | [0.01, 100] |
| `pct_diferencia_precio_bajo` | `superior al N%` | [0.01, 100] |
| `pct_diferencia_precio_alto` | `y (del )?N%` | [0.01, 100] |
| `pct_garantia_cumplimiento` | `N,N%` | [0.01, 100] |

### 3.1 Portes adicionales (revisión Sesión 15 final)

Dos campos más pasaron de `hybrid` → `regex` durante la revisión final (decidido en la
comparativa, no por intuición):

| Campo | Situación en comparativa | Decisión |
|---|---|---|
| `dias_presentacion_garantia` | plantilla uniforme "dentro de los 10 días"; regex viejo solo 50%, LLM alucinaba 85 | patrón robusto (`dentro de (los\|la) N días` + variante "diez (10)") → **200/200** |
| `pct_anticipo` | GT todo-NULL; regex ya daba R=100, pero el fallback LLM alucinaba **95 falsos positivos** | `regex` puro → sin GPU, 0 FPs |

En el caso `pct_anticipo` el regex acertaba en la comparativa (R=Ri=100) pero la
arquitectura H caía al LLM porque `v_re is None`; el fallback inventaba un valor en 95/200
filas donde el pliego simplemente no declara el porcentaje. Al marcarlo `regex`, el
pipeline de producción deja de enviarlo al LLM.

---

## 4. Evaluación comparativa (C)

### 4.1 Metodología

- **GPU-free**: re-usa `llm_raw`/`text` almacenados en las CSVs de GT (no re-corre el LLM).
- **5,066 juicios congelados** en 26 CSVs (`data/processed/alolab/gt_por_campo/`).
- `ground_truth` = anotación humana; `NULL` = celda vacía.
- 5 estrategias por campo:

| Sigla | Estrategia | Definición |
|---|---|---|
| **R** | Regex v2 puro | `_extract_field_text()` (sin postproceso) |
| **Ri** | Regex + postproceso | R + `postprocess(strict=True)` |
| **L_raw** | LLM bruto | primer número de `llm_raw` |
| **L_pp** | LLM + postproceso | `parse_response(llm_raw)` — la capa de producción |
| **H** | Híbrido arquitectural | determinista → regex → LLM fallback |

- `acc_flex`: admite factor ×100 (escala de %). `fp`: falsos positivos (GT NULL → extraído).

### 4.2 Resultado global

| Estrategia | Acc flex media |
|:---:|:---:|
| R (regex puro) | 62.9% |
| Ri (regex+post) | 62.8% |
| L_raw (LLM bruto) | 51.0% |
| L_pp (LLM+post) | 63.1% |
| **H (híbrido)** | **66.8%** |

**Mejor estrategia por campo**: H en 12/26, Ri en 9, L_pp en 3, L_raw en 2.

> Media actualizada tras el porte final de `dias_presentacion_garantia` a regex (R solo:
> 50→100, H: 57.5→100). El resto de estrategias varía solo por esa fila.

### 4.3 Resultado por campo

| Campo | R | Ri | L_raw | L_pp | H | best | fp(H) |
|---|---|--:|--:|--:|--:|:--:|:--:|
| pct_interes_mora_diario | 100.0 | 100.0 | 43.0 | 100.0 | 100.0 | H | 0 |
| pct_diferencia_precio_alto | 100.0 | 100.0 | 64.5 | 100.0 | 100.0 | H | 0 |
| pct_diferencia_precio_bajo | 100.0 | 100.0 | 78.0 | 100.0 | 100.0 | H | 0 |
| pct_garantia_cumplimiento | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | H | 0 |
| pct_multa_diaria | 100.0 | 100.0 | 92.5 | 100.0 | 100.0 | H | 0 |
| dias_notificacion_fuerza_mayor | 5.5 | 5.5 | 94.5 | 94.5 | 94.5 | H | 11 |
| pct_monto_referencia_minimo | 90.5 | 89.5 | 27.5 | 70.0 | 91.0 | H | 4 |
| dias_pago_maximo | 33.0 | 33.0 | 72.5 | 72.5 | 88.5 | H | 0 |
| ratio_endeudamiento_maximo | 44.0 | 44.0 | 86.5 | 86.5 | 86.0 | L_pp | 26 |
| pct_monto_referencia_tecnica | 87.5 | 87.5 | 26.5 | 86.5 | 84.5 | Ri | 23 |
| dias_aceptacion_rechazo_factura | 1.0 | 1.0 | 85.0 | 85.0 | 84.0 | L_pp | 2 |
| ratio_liquidez_minimo | 29.5 | 29.5 | 72.0 | 72.0 | 82.0 | H | 30 |
| dias_validez_garantia | 71.0 | 75.0 | 25.5 | 76.0 | 82.0 | H | 34 |
| dias_solicitud_anticipo | 78.0 | 72.5 | 96.5 | 85.0 | 81.0 | L_raw | 1 |
| dias_reposicion | 78.5 | 78.5 | 65.5 | 65.5 | 67.0 | Ri | 64 |
| anos_experiencia_minima_tecnica | 65.5 | 66.5 | 22.5 | 78.0 | 66.0 | L_pp | 50 |
| dias_pago_anticipo | 60.0 | 59.0 | 86.5 | 83.0 | 63.5 | L_raw | 7 |
| dias_presentacion_garantia | 100.0 | 100.0 | 57.5 | 57.5 | 100.0 | H | 0 |
| anos_experiencia_minima | 77.0 | 77.0 | 40.0 | 40.0 | 57.0 | Ri | 62 |
| pct_anticipo | 100.0 | 100.0 | 52.5 | 52.5 | 52.5 | Ri | 95 |
| num_referencias_tecnicas_minimo | 67.5 | 67.5 | 29.0 | 29.0 | 30.5 | Ri | 136 |
| num_categorias_documentos | 69.0 | 69.0 | 0.0 | 0.0 | 12.0 | Ri | 114 |
| num_referencias_minimo | 71.0 | 71.0 | 4.0 | 4.0 | 10.5 | Ri | 172 |
| num_componentes_formula | 2.5 | 2.5 | 3.0 | 3.0 | 3.0 | H | 5 |
| num_documentos_pago | 2.5 | 2.5 | 0.0 | 0.0 | 0.0 | Ri | 5 |
| num_pasos_verificacion | 1.5 | 1.5 | 0.0 | 0.0 | 0.0 | Ri | 3 |

> CSV completo: `data/processed/alolab/comparativa_estrategias.csv`

---

## 5. Dónde aporta el LLM y dónde no

### 5.1 El LLM aporta valor genuino (campos de texto variable)

| Evidencia | Campo(s) |
|---|---|
| R=1-5.5 → H/L_pp = 94.5 | `dias_notificacion_fuerza_mayor`, `dias_aceptacion_rechazo_factura` |
| R=33 → H=88.5 | `dias_pago_maximo` (requiere contexto + regla 182) |
| R=44 → L_pp=86.5 | `ratio_endeudamiento_maximo` |
| R=29.5/71 → H=82/91 | `ratio_liquidez_minimo`, `pct_monto_referencia_minimo` |

Estos campos tienen redacción variable entre pliegos: el regex no captura la semántica;
el LLM sí. **Sin LLM, estos campos colapsan.**

### 5.2 El LLM sobra o es perjudicial (campos-plantilla / "ausencia como señal")

| Evidencia | Campo(s) |
|---|---|
| R=Ri=100, pero H=52.5 tras el fallback | `pct_anticipo` — GT es todo NULL; el LLM alucina 95 FPs |
| R=Ri=71-90, H cae a 10-30 | `num_referencias_minimo`, `num_referencias_tecnicas_minimo`, `num_categorias_documentos` |
| R=100, el LLM solo añade FP | los 5 campos `pct_*` uniformes (ya en regex) |

**Dato clave**: en `pct_anticipo` la capa determinista responde `None` (100% correcto, el
pliego no declara el %), pero H cae al LLM cuando el regex devuelve `None`, alucinando 95
falsos positivos. **El fallback al LLM debe dispararse solo cuando el regex es *no concluyente*
y no cuando su ausencia es la señal válida.**

### 5.3 El postproceso determinista es el mayor valor agregado

| Comparación | Δ |
|---|---|
| L_raw → L_pp | **+12.1 pp** (51.0 → 63.1) |
| L_pp → H | +2.0 pp (63.1 → 65.1) |
| R → Ri | −0.1 pp (60.9 vs 61.0; el strict de production ayuda a la ruta LLM, no a la regex) |

La regla ×100 + contexto `N%` + conversión meses→días y las reglas anti-FP de `postprocess`
explican casi todo el salto sobre el LLM bruto. El valor incremental del **híbrido** sobre la
capa LLM+post está en campos específicos (`dias_pago_maximo` 72.5→88.5, `dias_validez_garantia`
76→82, `pct_monto_referencia_minimo` 70→91).

### 5.4 Campos persistentemente débiles (ninguna estrategia > 58%)

- `num_pasos_verificacion` (1.5), `num_documentos_pago` (2.5), `num_componentes_formula` (3.0)
  → el LLM no "cuenta" listas; el regex tampoco. Fuera del alcance de extracción híbrida.
- `dias_presentacion_garantia` (57.5) — el texto no declara el plazo explícitamente.
- `pct_anticipo` (52.5) — el campo es todo-NULL; el problema es el fallback, no la extracción.

---

## 6. Implicancias para la arquitectura de producción

El resultado C sugiere **enrutamiento por campo** en lugar de un único híbrido ciego:

```
PARA CADA CAMPO (según su "best" en la comparativa):
  Ri  →  ONLY regex + postprocess        (sin GPU)
  H   →  determinista → regex → LLM      (híbrido)
  L_pp→  LLM + postprocess estricto       (sin primera línea regex)
  L_raw→ LLM bruto + filtro de campo      (caso raro)
```

Reglas derivadas:

1. **5 campos pct uniformes** se resuelven en la Capa 1 (determinista) — 0 GPU.
2. **Campos todo-NULL** (`pct_anticipo`) NO deben caer al LLM cuando el regex/no hay patrón
   en el 100% de la muestra y el GT es ausencia pura → se marca `skips_llm=True` en el schema.
3. En campos donde `best=Ri`, el fallback LLM puede **omitirse** si la cobertura regex ya es
   ≥ umbral (ahorro de GPU sin pérdida).
4. `master_schemas_v2.json` ya distingue `extraction_method: regex/hybrid`; se recomienda
   añadir `resolve` (one of `regex|hybrid|deterministic`) como el enrutamiento del §4.3.

Estimación de ahorro en el corpus de 2.7M secciones: los 3 campos portados + los 5
deterministas + los campos con `best=Ri` cubren una fracción grande de las llamadas LLM
actuales. Detalle de throughput en §7.

---

## 7. Costos y throughput

Benchmark de referencia (Sesión 12, Llama 3.1 8B-AWQ, 460 samples, guided decoding):

| Enfoque | Rate | Tiempo 460 docs |
|---|---|---|
| Sin guided (prompt) | 1.32 docs/s | 347.5s |
| **Con guided decoding** | **8.95 docs/s** | **51.4s** |

Proyección con la reducción actual de carga LLM (315K secciones, 26 campos hybrid):

| Escenario | Estado | Llamadas LLM | Tiempo estimado |
|---|---|---:|---:|
| Antes de la tesis B+C | 26 campos hybrid | completo | ~4-8 h (guided) |
| Tras B (3 a regex + 5 deterministas) | 21 → ~13 con enrutamiento | −60-70% | ~1.5-3 h |
| Con guided decoding + Señal de ausencia | solo H/L_pp | mínimo | <1.5 h |

---

## 8. Limitaciones y reproducibilidad

1. **`acc_flex` perdonó 32 casos "ambos encuentran ≠ valor"** con factor ×100 — consistente
   con la escala %, pero puede enmascarar errores de magnitud real en campos `ratio_*`.
2. **`L_raw`** = "primer número del raw" es una aproximación; `llm_raw` ya incorporaba
   postproceso del benchmark original. Reproducible, no idéntico a cero-postproceso.
3. **GT congelado** = 5.066 juicios de las sesiones 9-14. No se re-corrió el LLM para C.
4. El **fallback LLM con guided decoding** requiere vLLM 0.23.0 con `VLLM_USE_V2_MODEL_RUNNER=0`
   (ver AGENTS.md) y el flujo offline `LLM.generate()`.
5. La comparativa ignora `title_slug` salvo para leer el ftype; el título influye en el
   resultado real del LLM (los GT se muestrearon por campo, no por título-equilibrado).

### Reproducir

```bash
# Recalcular la comparativa (GPU-free, ~segundos):
python scripts/comparativa_estrategias.py

# Recalcular el reporte de precisión por campo (también GPU-free):
python scripts/gt_precision_por_campo.py

# Ruta de producción LLM (con GPU):
VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/run_document_features_v2_llm_pipeline.py
```

---

## 9. Referencias

| Documento | Contenido |
|---|---|
| `docs/phase5_llm_fallback_report.md` | Arquitectura inicial regex→LLM, decisiones de prompt (Sesiones 8-12) |
| `data/processed/alolab/gt_precision_por_campo.md` | Precisión LLM vs GT, 26 campos (Sesión 10) |
| `data/processed/alolab/errors_round{1-4}.json` | Ciclo AloLab manual hasta 100% (Sesión 14) |
| `data/processed/alolab/comparativa_estrategias.csv` | Resultado completo de la comparativa (esta tesis) |
| `AGENTS.md` | Historial completo de sesiones + entorno WSL |
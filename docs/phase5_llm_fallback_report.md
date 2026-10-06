# Phase 5: LLM Fallback — Discovery Report

## Resumen

Se implementó y evaluó un pipeline híbrido regex+LLM para extraer valores numéricos de pliegos de licitación donde el regex (Phase 1-4) falla (confidence < 0.8). Se iteraron prompts, se compararon **4 modelos locales**, y se solucionaron problemas de truncamiento, alucinación y formato de chat.

> **Actualización Sesión 15**: la arquitectura final tiene una **capa de postproceso determinista
> compartida** (`src/etl/transformers/extraction_postprocess.py`) y una **evaluación comparativa**
> sobre GT humano congelado. Ver [`docs/tesis_b_c_resultados.md`](../docs/tesis_b_c_resultados.md)
> y la sección [9](#9-tesis-bc-arquitectura-final-y-evaluación) abajo.

---

## 1. Arquitectura Híbrida

```
Regex (Phase 1-4, CPU, ~70s) → 80% cobertura
LLM Fallback (GPU, ~4h estimado) → 20% restante donde conf < 0.8
```

- 81 campos totales en `document_features_v2`
- **69 regex puros** (revisión Sesión 16: eran 55, luego 62) — no necesitan LLM
- **12 híbridos** (regex + LLM fallback si conf < 0.8; eran 26)
- 12 campos híbridos con datos LLM-pendientes (>0 docs)

---

## 2. Problemas Identificados y Solucionados

### 2.1 Confusión Porcentajes vs Días (SOLUCIONADO)

**Problema**: El LLM extraía `10` cuando el texto decía "10%" para campos tipo `dias_*`.

**Ejemplo**: `dias_presentacion_garantia` tenía solo 1/20 (5%) porque el modelo respondía "10" en vez de NULL cuando el único número era un porcentaje.

**Solución**: Reglas estrictas en system prompt:
```
- No confundas tipos: si el campo pide días, no extraigas porcentajes (10%)
- Los porcentajes tienen el símbolo '%' o texto 'por ciento'
```

**Resultado**: `dias_presentacion_garantia`: 5% → 100%

### 2.2 Reglas Demasiado Agresivas para `num_*` (SOLUCIONADO)

**Problema**: Las reglas anti-alucinación también bloqueaban la extracción de `num_documentos_pago` (0%) porque rechazaban números de listas numeradas.

**Solución**: Hints específicas para prefijo `num_`:
```
- CONTAR los elementos listados
- Si hay una lista (1., 2., 3.), contar cuantos items tiene
- NO extraer el valor unitario, sino el TOTAL
```

**Resultado**: `num_documentos_pago`: 0% → 100%

### 2.3 Truncamiento de Texto a 8000 Chars (SOLUCIONADO)

**Problema**: El transformer truncaba `content_text` a 8000 caracteres.

**Solución**: Se eliminó el truncamiento. Impactó solo a ~200/350K docs (principalmente `capacidad_tecnica` con 204 docs >8K).

**Resultado**: Sin cambio en la tasa de extracción — el truncamiento no era el cuello de botella.

### 2.4 Preview CSV Truncado a 500 Chars (SOLUCIONADO)

**Problema**: El CSV de evaluación (`eval_llm_quality.py`) mostraba solo 500 chars de preview, dando la impresión errónea de que el LLM recibía texto truncado.

**Solución**: Se redujo el preview a 300 chars para evitar confusión (solo afecta display, no al LLM).

### 2.5 System Role No Soportado por Mistral 7B (SOLUCIONADO)

**Problema**: Mistral 7B Instruct no soporta `system` como rol separado en la API de chat. El server vLLM rechazaba la petición con "Conversation roles must alternate".

**Solución**: Se implementó `_build_messages_without_system()` en el transformer, que fusiona el system prompt dentro del mensaje `user`.

### 2.6 Thinking Mode en Qwen3-8B (SOLUCIONADO PARCIALMENTE)

**Problema**: Qwen3-8B genera `<think>...razonamiento...</think>` antes de la respuesta numérica. Esto duplica los tokens de salida (~2600 vs ~100 tok_out por campo).

**Solución**: Se actualizó `_parse_response()` para eliminar `<think>...</think>` antes de extraer el número con regex. No afecta la precisión de extracción.

---

## 3. Iteración de Prompts

### Run 1 — Baseline (sin reglas estrictas)
**Modelo**: Qwen2.5-14B-AWQ
**Resultado**: 168/460 = 36.5%
- `dias_presentacion_garantia`: 5% (confundía % con días)
- `num_documentos_pago`: 60%
- `num_componentes_formula`: 35% (posibles falsos positivos)
- 10 campos en 0%

### Run 2 — Reglas estrictas anti-alucinación
**Modelo**: Qwen2.5-14B-AWQ
**Resultado**: 168/460 = 36.5% (misma tasa, diferente distribución)
- `dias_presentacion_garantia`: 5% → **100%** ✅
- `num_documentos_pago`: 60% → **0%** ❌ (reglas muy agresivas)
- `num_componentes_formula`: 35% → **0%** ❌

### Run 3 — Fix reglas `num_*`
**Modelo**: Qwen2.5-14B-AWQ
**Resultado**: 189/460 = **41.1%**
- `num_documentos_pago`: 0% → **100%** ✅
- `num_referencias_minimo`: se mantuvo en 5%
- 10 campos todavía en 0%

---

## 4. Comparación de Modelos

### 4.1 Modelos Evaluados

| Modelo | Path | Tamaño | VRAM | Kernel | Estado |
|---|---|---|---|---|---|
| 🥇 **Llama 3.1 8B-Instruct-AWQ** | `thesven/Meta-Llama-3.1-8B-Instruct-AWQ` | ~5.4 GiB | 5.39 GiB | awq_marlin | **RECOMENDADO** |
| 🥈 **Qwen3-8B-AWQ** | `Qwen/Qwen3-8B-AWQ` | ~6.5 GiB | ~6.5 GiB | awq | Alternativa (mayor cobertura, menor precisión) |
| Qwen2.5-14B-AWQ | `Qwen/Qwen2.5-14B-Instruct-AWQ` | 8.6 GiB | ~9.3 GiB | awq | ❌ Descartado |
| Mistral 7B-Instruct-v0.3-AWQ | `solidrust/Mistral-7B-Instruct-v0.3-AWQ` | 3.88 GiB | 3.89 GiB | awq_marlin | ❌ Inviable (alucina) |

### 4.2 Resultados Globales

| Métrica | Qwen2.5-14B | Llama 3.1 8B | **Qwen3-8B** | Mistral 7B |
|---|---|---|---|---|
| Extracción global | 189/460 (41.1%) | 290/460 (63.0%) | **367/460 (79.8%)** | 460/460 (100%) \* |
| Campos 0% | 10 | 2 | **2** | 0 |
| Velocidad | ~10 docs/s | ~5 docs/s | ~3 docs/s | ~2.2 docs/s |
| Tokens out/campo | ~20 tok | ~100 tok | ~2600 tok | ~1160 tok |
| Alucinación | Baja | **Muy baja** | Media | **Total** |

\* Mistral 7B: 100% porque alucina `3.0` para absolutamente todo. Inviable.

### 4.3 Comparación Campo por Campo

| Campo | Qwen2.5-14B | Llama 3.1 8B | Qwen3-8B | Mejor |
|---|---|---|---|---|
| `anos_experiencia_minima` | 15% | 20% | 20% | Empate |
| `anos_experiencia_minima_tecnica` | 0% | 5% | **85%** | Qwen3 |
| `dias_aceptacion_rechazo_factura` | 100% | 100% | 100% | Todos |
| `dias_notificacion_fuerza_mayor` | 100% | 95% | 100% | Qwen3 |
| `dias_pago_anticipo` | 15% | 20% | **90%** | Qwen3 |
| `dias_pago_maximo` | 100% | 100% | 100% | Todos |
| `dias_presentacion_garantia` | 100% | 100% | 100% ✅ | Todos |
| `dias_reposicion` | 30% | **95%** | 95% | Empate |
| `dias_solicitud_anticipo` | 10% | 25% | **0%** ❓ | Llama |
| `dias_validez_garantia` | 5% | 5% | **30%** | Qwen3 |
| `num_componentes_formula` | 0% | 70% | **100%** ❓ | Qwen3 |
| `num_documentos_pago` | 100% | 95% | 100% ❓ | Qwen3 |
| `num_pasos_verificacion` | 100% | 100% | 100% ❓ | Todos |
| `num_referencias_minimo` | 5% | 65% | **95%** ❓ | Qwen3 |
| `num_referencias_tecnicas_minimo` | 0% | 15% | **100%** ❓ | Qwen3 |
| `pct_anticipo` | 0% | **0%** | 0% | Sin datos |
| `pct_diferencia_precio_alto` | 100% | 100% | 100% | Todos |
| `pct_diferencia_precio_bajo` | 100% | 100% | 100% | Todos |
| `pct_interes_mora_diario` | 0% | 95% | **100%** | Qwen3 |
| `pct_monto_referencia_minimo` | 0% | 60% | **85%** | Qwen3 |
| `pct_monto_referencia_tecnica` | 0% | 0% | **35%** | Qwen3 |
| `ratio_endeudamiento_maximo` | 0% | 95% | **100%** ❓ | Qwen3 |
| `ratio_liquidez_minimo` | 65% | 90% | **100%** ❓ | Qwen3 |

❓ = valores sospechosos de alucinación (ver sección 4.5)

### 4.4 Velocidad y Throughput

| Modelo | docs/s | tok_out/20docs | tok/s | Tiempo total (460 docs) | VRAM |
|---|---|---|---|---|---|
| Qwen2.5-14B | ~10 | ~20 | ~200 | ~46s | 8.6 GB |
| Llama 3.1 8B | ~5 | ~100 | ~360 | ~92s | 5.4 GB |
| Qwen3-8B | ~3 | ~2600 | ~400 | ~153s | 6.5 GB |
| Mistral 7B | ~2.2 | ~1160 | ~126 | ~209s | 3.9 GB |

### 4.5 Advertencia sobre Qwen3-8B: Posible Alucinación

Qwen3-8B muestra valores sospechosos en varios campos numéricos:

- `num_componentes_formula`: todos **1.0** — improbable. Las fórmulas de reajuste suelen tener 2-5 componentes.
- `num_documentos_pago`: todos **1.0** — sospechoso. La cantidad de documentos varía.
- `num_pasos_verificacion`: todos **1.0** — muy sospechoso.
- `num_referencias_tecnicas_minimo`: todos **1.0** — sospechoso.
- `ratio_endeudamiento_maximo`: 18/20 son **0.01** — valor por defecto.
- `ratio_liquidez_minimo`: todos **0.01** — valor por defecto.

**Diagnóstico**: Qwen3-8B tiende a devolver un valor predeterminado (`1.0`, `0.01`) cuando no encuentra el dato en el texto, en lugar de retornar NULL. Esto infla artificialmente su tasa de extracción.

**En contraste**: Llama 3.1 8B retorna NULL cuando no encuentra el dato, lo que resulta en una tasa más baja (63%) pero más honesta.

**Recomendación**: Para producción, usar **Llama 3.1 8B** si se prioriza precisión, o **Qwen3-8B** si se prioriza cobertura (con post-filtrado de valores predeterminados).

### 4.6 Análisis

**Llama 3.1 8B** es el modelo más equilibrado:
1. **Mejor seguimiento de instrucciones**: respeta las reglas anti-alucinación
2. **Valores variados y realistas**: distribuciones que reflejan la realidad
3. **Baja alucinación**: retorna NULL cuando no está seguro
4. **Menos VRAM**: 5.4 GB vs 8.6 GB de Qwen2.5-14B
5. **awq_marlin kernel**: más rápido y eficiente que awq genérico

**Qwen3-8B** tiene mayor cobertura bruta pero sospecha de inflación por valores predeterminados.

**Mistral 7B** es inviable para esta tarea — alucina sistemáticamente.

**Campos que ningún modelo puede extraer** (datos no presentes en el texto extraído):
- `pct_anticipo` — el % de anticipo no está en la sección "anticipo"
- `dias_solicitud_anticipo` — solo Llama logra 25%, los demás 0-10%

---

## 5. Problemas Técnicos

### 5.1 Modelo AWQ `hugging-quants/Llama-3.1-8B-Instruct-AWQ` no disponible

Los modelos AWQ de Llama 3.1 8B de `hugging-quants` ya no existen en HuggingFace (404). Se usó `thesven/Meta-Llama-3.1-8B-Instruct-AWQ` como alternativa.

### 5.2 Mistral 7B: System Role no soportado

Mistral 7B Instruct requiere `[INST]...[/INST]` template. Envía system como rol separado → error 400. Solución: `_build_messages_without_system()` en el transformer.

### 5.3 Qwen3-8B: Thinking Mode

Qwen3-8B genera `<think>...</think>` antes de la respuesta. Solución: eliminar thinking tokens en `_parse_response()` con regex.

### 5.4 vLLM Server en WSL2

- Modelos en `/root/models/` dentro de WSL2
- Servidor vLLM 0.23.0 (0.25.x no funciona en WSL2 por UVA)
- `VLLM_USE_V2_MODEL_RUNNER=0` requerido (UVA no disponible en WSL2)
- AWQ Marlin kernel: solo soportado para MistralForCausalLM/LlamaForCausalLM
- Qwen cae a `awq` genérico (más lento)
- PostgreSQL en container Podman (puerto 5433) — requiere `podman start dncp` tras reinicio WSL

### 5.5 Tiempos de Ejecución

| Etapa | Tiempo |
|---|---|
| Evaluación parcial (460 samples, ~23 campos) | ~46s Qwen2.5 / ~92s Llama / ~153s Qwen3 / ~209s Mistral |
| Pipeline completo estimado (315K docs) | ~2-4h con continuous batching |
| Cuello de botella | tokens de salida (thinking mode Qwen3 duplica) |

---

## 6. Modelos Descargados

| Modelo | Path WSL | Tamaño | Uso |
|---|---|---|---|
| Qwen2.5-14B-Instruct-AWQ | `/root/models/Qwen2.5-14B-Instruct-AWQ` | 8.6 GB | ❌ Descartado |
| Llama-3.1-8B-Instruct-AWQ | `/root/models/Llama-3.1-8B-Instruct-AWQ` | 5.4 GB | ✅ Recomendado |
| Mistral-7B-Instruct-v0.3-AWQ | `/root/models/Mistral-7B-Instruct-v0.3-AWQ` | 3.9 GB | ❌ Inviable |
| Qwen3-8B-AWQ | `/root/models/Qwen3-8B-AWQ` | 6.5 GB | ⚠️ Alternativa |

---

## 7. Benchmark Definitivo (Session 9-10): Llama 3.1 8B vs Qwen3-8B

### 7.1 Metodología

- **Mismos documentos**: Ambos modelos probados sobre los mismos 460 samples (20 × 23 campos hybrid)
- **Mismo prompt**: `build_messages()` idéntico, con system prompt completo (no truncado)
- **Offline API**: vLLM `LLM.generate()` directo (sin HTTP server)
- **Semilla fija**: random_state=42 para sampling reproducible

### 7.2 Resultados Globales

| Métrica | Llama 3.1 8B-AWQ 🥇 | Qwen3-8B-AWQ |
|---|---|---|
| Extraídos | **274/460 (59.6%)** | 210/460 (45.7%) |
| Velocidad | ~5.5 docs/s | ~7.4 docs/s |
| Valores default (1.0/0.01) | 34/274 (12.4%) | 12/210 (5.7%) |

### 7.3 Análisis de Discrepancias

De 479 pares doc×campo comparados:

| Situación | Cantidad | Interpretación |
|---|---|---|
| Ambos encuentran = mismo valor | **369 (77.0%)** | ✓ Alta consistencia |
| Solo Llama encuentra | **71 (14.8%)** | 🏆 **Llama más sensible** |
| Solo Qwen3 encuentra | 7 (1.5%) | Qwen3 menos sensible |
| Ambos encuentran ≠ valor | 32 (6.7%) | Requiere revisión manual |

**Campos donde Llama domina**:
- `num_componentes_formula`: Llama 12/20 vs Qwen3 **0/20**
- `num_documentos_pago`: Llama 16/20 vs Qwen3 **2/20**
- `pct_interes_mora_diario`: Llama 18/20 vs Qwen3 **0/20**
- `num_pasos_verificacion`: Llama 20/20 vs Qwen3 **12/20**
- `num_referencias_minimo`: Llama 9/20 vs Qwen3 **1/20**

### 7.4 Conclusión Final

**Llama 3.1 8B-AWQ es el ganador definitivo.** Tasas anteriores de Qwen3-8B (79.8%) no se replican bajo condiciones controladas — probablemente infladas por diferencias en sampling, temperatura o API.

Llama gana por:
1. **Mayor extracción real** (59.6% vs 45.7%) — **+14%**
2. **Más campos cubiertos**: domina en 5 campos clave donde Qwen3 falla completamente
3. **Valores más diversos**: consistentemente más valores únicos por campo
4. **awq_marlin**: kernel más eficiente (5.4 GB VRAM vs 6.5 GB Qwen3)
5. **Sin thinking mode**: no necesita post-procesar `<think>` tags

---

## 8. Conclusiones Finales

1. **Modelo definitivo: Llama 3.1 8B-Instruct-AWQ** — mejor equilibrio cobertura (59.6%), precisión, velocidad (5.5 docs/s) y VRAM (5.4 GB).
2. **Descartados**: Qwen2.5-14B (41.1%), Mistral 7B (alucina todo), Qwen3-8B (45.7% bajo condiciones controladas).
3. Los **extraction_hints** con reglas positivas + negativas mejoran significativamente la precisión.
4. **Documentos**: `scripts/benchmark_llm_fallback.py` para reproducir pruebas con nuevos modelos.
5. **Output**: `data/processed/llm_benchmark/` — CSVs detallados + comparison + análisis.

### Próximos Pasos
1. Ejecutar pipeline completo con Llama 3.1 8B-AWQ para los 23 campos (~2-4h estimado).
2. Verificar calidad de extracción con muestra manual de 100 docs.
3. Post-procesar valores predeterminados (1.0, 0.01) si persisten.

---

## 9. Tesis B+C: Arquitectura final y evaluación (Sesión 15)

### 9.1 Capa de postproceso determinista compartida

Nuevo `src/etl/transformers/extraction_postprocess.py` — única fuente de verdad para la capa
determinista de la arquitectura híbrida:

| Componente | Rol |
|---|---|
| `deterministic_pct(field, text)` | **Autoritativa** p/ 7 campos pct uniformes (literal, nunca cae al LLM) |
| `postprocess(field, v, text, strict)` | ×100, contexto `N%`, meses→días (`dias_validez_garantia`), reglas anti-FP |
| `parse_response(raw, field, text)` | determinista → NULL → postprocess (misma forma en medición y producción) |
| `PCT_INT_SCALE`, `KNOWN_DEFAULTS`, `NO_RULE` | constantes de escala y valores modales frágiles |

Usada por: `document_features_v2_transformer.py` (regex, strict=False),
`document_features_v2_llm_transformer.py::_parse_response` (LLM, strict=True),
`scripts/benchmark_multi_vs_gt.py` (medición) y `scripts/comparativa_estrategias.py` (evaluación).
La API de `benchmark_multi_vs_gt.py` se preservó: `gold_standard_26campos`, `alolab_collect_errors`,
few-shot y gt_extension siguen importando sin cambios.

### 9.2 3 campos-plantilla portados a regex real (ahorro GPU)

`pct_garantia_cumplimiento`, `pct_diferencia_precio_bajo`, `pct_diferencia_precio_alto`
pasaron de `hybrid` → `regex` (schema: **62 regex / 19 hybrid**). Validados contra GT: **100%,
0 FP** (bajo `superior al N%` TP 156/156, alto `y del N%` TP 129/129, garantía `N,N%` TP 64/64).
Ya no consumen GPU — el filtro `extraction_method == hybrid` los excluye del fallback LLM.

Portes adicionales de la revisión final (Sesión 15) y Sesión 16 (7 más → **69 regex / 12 hybrid**,
ver `revision_26_campos.md`):
- `dias_presentacion_garantia`: plantilla uniforme "dentro de los 10 días". El regex viejo
  (`N días corridos`) solo acertaba 100/200; el nuevo (`dentro de (los|la) N días` + variante
  "diez (10)") acierta **200/200**. GT=10.0 uniforme.
- `pct_anticipo`: GT todo-NULL. El regex ya daba R=100 pero el fallback LLM alucinaba **95
  falsos positivos** (H=52.5). Al marcarlo `regex` desaparecen sin GPU.

### 9.3 Evaluación comparativa (GPU-free, 5,066 juicios GT)

`scripts/comparativa_estrategias.py` sobre el GT humano congelado (26 campos, estrategias
R/Ri/L_raw/L_pp/H):

| Estrategia | Media acc flex |
|:---:|:---:|
| R (regex puro) | 62.9% |
| Ri (regex+post) | 62.8% |
| L_raw (LLM bruto) | 51.0% |
| L_pp (LLM+post) | 63.1% |
| **H (híbrido)** | **66.8%** |

Mejor por campo: **H en 12/26**, Ri 9, L_pp 3, L_raw 2.

**Conclusiones**:
1. El postproceso determinista es el mayor valor: L_raw→L_pp = **+12.1 pp**; el híbrido añade
   **+2.0 pp** más.
2. El LLM aporta en campos de texto variable (`dias_notificacion_fuerza_mayor` R=5.5→H=94.5,
   `dias_pago_maximo` R=33→H=88.5, ratios) y **sobra/perjudica** en campos plantilla:
   `pct_anticipo` es todo-NULL y el fallback alucina **95 falsos positivos** (H=52.5 vs Ri=100).
3. Campos irresolubles con extracción híbrida (ninguna estrategia > 5%): `num_pasos_verificacion`,
   `num_documentos_pago`, `num_componentes_formula` (el LLM no "cuenta" listas).
4. **Implicancia de producción**: enrutamiento por campo según su mejor estrategia
   (`regex|hybrid|deterministic` en `master_schemas_v2.json`) en vez de híbrido ciego.

Resultado completo por campo: `data/processed/alolab/comparativa_estrategias.csv`.
Documento íntegro con tablas: [`docs/tesis_b_c_resultados.md`](../docs/tesis_b_c_resultados.md).

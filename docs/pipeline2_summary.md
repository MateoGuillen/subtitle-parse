# Pipeline 2 — Resumen de Descubrimientos y Guía de Diseño

> Proyecto: `D:\projects\subtitle-parse`
> Fecha: Julio 2026
> Objetivo: Extraer 6-8 campos binarios/numericos de ~315,000 secciones de pliegos de licitaciones paraguayas via LLM local

---

## Índice

1. [Resumen Ejecutivo](#1-resumen-ejecutivo)
2. [Modelos Evaluados](#2-modelos-evaluados)
3. [Ranking de Calidad (vs Claude 3.5 Sonnet GT)](#3-ranking-de-calidad)
4. [Análisis por Campo](#4-análisis-por-campo)
5. [Anomalías Detectadas en Validación](#5-anomalías-detectadas)
6. [Opciones de Mejora de Precisión](#6-opciones-de-mejora)
7. [Arquitectura Propuesta para Pipeline 2](#7-arquitectura-propuesta)
8. [Estimaciones de Throughput](#8-estimaciones-de-throughput)
9. [Referencias](#9-referencias)

---

## 1. Resumen Ejecutivo

Se evaluaron **14 modelos** en **21 configuraciones** (GGUF + AWQ + 3 variantes de prompt) para extracción estructurada.

### Línea de tiempo de descubrimientos

| Fase | Descubrimiento | Implicancia |
|------|---------------|-------------|
| GGUF benchmark | GGUF es experimental en vLLM, 3× más lento que AWQ | Migrar a AWQ |
| AWQ benchmark | awq_marlin kernel es el diferenciador clave (solo Mistral/Llama) | Qwen y Phi no se benefician de AWQ |
| Pseudo-GT validation | Llama 3.1 8B AWQ few-shot: F1=96.4% (vs majority vote local) | Primer líder de calidad |
| Claude GT validation | **Qwen2.5 14B AWQ base: 94.2% vs Claude real** | pseudo-GT sobreestimó Llama/Mistral. Qwen es el verdadero líder |

### Conclusión principal

**Qwen2.5-14B-AWQ** (base prompt) es el mejor modelo local contra Claude GT con **94.2% accuracy**. Llama 3.1 8B AWQ (91.5%) y Mistral 7B AWQ (87.5%) le siguen. Sin embargo, Qwen es el más lento (2.46s) y el que más VRAM consume (9.38 GiB).

**Tradeoff calidad-throughput**:

| Modelo | Accuracy vs Claude | Tiempo | VRAM | Concurrencia | 315K secciones |
|--------|:------------------:|:------:|:----:|:------------:|:--------------:|
| Qwen2.5 14B AWQ | **94.2%** | 2.46s | 9.38 GiB | 8.18× | ~10.5 horas |
| Llama 3.1 8B AWQ | **91.5%** | 1.93s | 5.39 GiB | 29.21× | ~5.8 horas |
| Mistral 7B AWQ | **87.5%** | **1.61s** | **3.89 GiB** | **35.22×** | **~4 horas** |

---

## 2. Modelos Evaluados

### 2.1 Formato AWQ (nativo, recomendado)

| # | Modelo | AWQ | Tamaño | VRAM | Kernel | Velocidad | Struct OK | F1 vs Claude | Veredicto |
|---|--------|:---:|:------:|:----:|:------:|:---------:|:---------:|:------------:|-----------|
| 1 | **Qwen2.5-14B** | W4A16 | 9.29 GiB | 9.38 GiB | awq | 2.46s | 100% | **94.2%** | 🥇 Calidad |
| 2 | **Llama 3.1 8B** | W4A16 | 5.33 GiB | 5.39 GiB | **awq_marlin** 🚀 | 1.93s | 100% | 91.5% | 🥈 Alternativa |
| 3 | **Mistral 7B** | W4A16 | **3.88 GiB** | **3.89 GiB** | **awq_marlin** 🚀 | **1.61s** | 100% | 87.5% | 🥉 Throughput |
| 4 | Qwen3-8B | W4A16 | 5.68 GiB | 5.71 GiB | awq | 4.80s | 100% | — | ⚠️ Lento |
| 5 | Qwen3-14B | W4A16 | 9.29 GiB | 9.44 GiB | awq | 5.29s | 100% | — | ⚠️ Lento |
| 6 | Phi-4 14B | W4A16 | 8.51 GiB | 8.51 GiB | awq | 4.47s | 90% | — | ❌ dtype issues |

### 2.2 Formato GGUF (experimental, deprecado)

| Modelo | Quant | Tamaño | Velocidad | Veredicto |
|--------|:-----:|:------:|:---------:|-----------|
| Mistral 7B | Q5_K_M | 4.8 GB | 4.90s | ✅ Backup |
| Qwen3-8B | Q5_K_M | 5.4 GB | 4.50s | ✅ Alternativa rápida |
| Qwen3-14B | Q5_K_M | 9.8 GB | 5.10s | ✅ Anterior campeón |
| Phi-4 14B | Q4_K_M | 8.4 GB | 6.09s | ✅ Alternativa robusta |

### 2.3 Modelos descartados

| Modelo | Razón |
|--------|-------|
| Gemma 2 27B | No cabe en 16 GB VRAM |
| Gemma 3 12B | Incompatible bfloat16 vs AWQ float16 |
| Llama 3.1 8B GGUF | Tokenizer gated (requiere HF_TOKEN) |
| Qwen2.5 7B | 0% calidad (omite campos sistematicamente) |
| NuExtract 2.0 | Es modelo VL, no LLM |
| gpt-oss-20b | Formato MXFP4 no compatible vLLM |
| DeepSeek-Coder | Entrenado en codigo, no administrativo |
| Phi-3.5 | Espanol debil |

> Detalle completo en [`docs/benchmark_resultados.md`](../docs/benchmark_resultados.md)

---

## 3. Ranking de Calidad

### 3.1 Global (vs Claude 3.5 Sonnet — 270 samples, 1770 fields)

| Modelo | Prompt | Accuracy | Precision | Recall | KW evidence |
|--------|--------|:--------:|:---------:|:-----:|:-----------:|
| **Qwen2.5-14B-AWQ** | **base** | **94.2%** | **90.8%** | **95.8%** | **93%** |
| Qwen2.5-14B-AWQ | few-shot | 93.9% | 91.4% | 95.3% | 92% |
| Qwen2.5-14B-AWQ | strict | 93.7% | 90.3% | 95.7% | 93% |
| **Llama-3.1-8B-AWQ** | **few-shot** | **91.5%** | 94.2% | **98.8%** | 92% |
| Llama-3.1-8B-AWQ | strict | 90.5% | 93.1% | 98.8% | 91% |
| Llama-3.1-8B-AWQ | base | 89.5% | 91.9% | 99.1% | 91% |
| Mistral-7B-AWQ | base | 87.6% | 87.8% | **99.8%** | 91% |
| Mistral-7B-AWQ | strict | 87.5% | 88.9% | **99.8%** | 91% |
| Mistral-7B-AWQ | few-shot | 86.5% | 87.4% | **99.9%** | 91% |

### 3.2 Por Título (mejor modelo por título)

| Título | Mejor modelo | Accuracy | Campos |
|--------|-------------|:--------:|:------:|
| Aclaracion De Las Ofertas | Llama-3.1-8B-AWQ strict | **100.0%** | 8 |
| Causales De Terminacion Del Contrato | Llama-3.1-8B-AWQ base | **50.0%** | 2 |
| Confidencialidad De La Informacion | Qwen2.5-14B-AWQ base | **97.1%** | 7 |
| Derechos Intelectuales | Qwen2.5-14B-AWQ few-shot | **83.3%** | 6 |
| Formato Y Firma De La Oferta | Llama-3.1-8B-AWQ base | **100.0%** | 8 |
| Fraude Y Corrupcion | Qwen2.5-14B-AWQ few-shot | **99.5%** | 7 |
| Fuerza Mayor | Qwen2.5-14B-AWQ base | **97.9%** | 8 |
| Impuestos Y Derechos | Mistral-7B-AWQ few-shot | **97.6%** | 7 |
| Interpretacion | Qwen2.5-14B-AWQ base | **100.0%** | 6 |

### 3.3 Cross-Prompt Consistency (Mistral 7B)

| Prompt A | Prompt B | Agreement |
|----------|----------|:---------:|
| base | few-shot | 94.5% |
| base | strict | 95.3% |
| few-shot | strict | 94.7% |

> Detalle completo en:
> - [`data/processed/validation/validation_report.md`](../data/processed/validation/validation_report.md) — Fase 1 pseudo-GT
> - [`data/processed/validation/gt_validation_report.md`](../data/processed/validation/gt_validation_report.md) — vs Claude GT
> - [`data/processed/validation/gt_detailed_analysis.md`](../data/processed/validation/gt_detailed_analysis.md) — desglose por campo

---

## 4. Análisis por Campo

### 4.1 Campos donde Qwen2.5 14B domina

| Campo | Qwen2.5 14B | Llama 3.1 8B | Mistral 7B | Tipo de campo |
|-------|:-----------:|:------------:|:----------:|---------------|
| `menciona_contrato_con_mayuscula` | **100%** | 33-63% | 33-37% | Deteccion de mayusculas |
| `num_acciones_especificadas` | **100%** | 100% | 17-30% | Conteo numerico |
| `menciona_ley_7021` | **100%** | 90-97% | 27-33% | Legislacion paraguaya |
| `contiene_instruccion_reserva_aclaraciones` | **100%** | **20%** | **20%** | Deteccion semantica |
| `num_eventos_ejemplo` | **87-90%** | 3% | 0% | Conteo de elementos |

### 4.2 Campos donde Qwen2.5 14B es debil

| Campo | Qwen2.5 14B | Mejor alternativo | Mejor accuracy |
|-------|:-----------:|:-----------------:|:--------------:|
| `num_tipos_impuestos_listados` | **13%** | Llama/Mistral | **90%** |
| `num_clausulas_principales` | 90% | Llama/Mistral | **90%** (empate) |
| `acciones_con_conector_y_o` (base) | 87% | Qwen few-shot | **100%** |

### 4.3 Campos donde todos fallan

| Campo | Todos los modelos | Causa |
|-------|:-----------------:|-------|
| `longitud_seccion_palabras` | **0%** | Campo calculado (word count), no extraible via LLM |
| `longitud_texto_Causales_De_Terminacion` | **0%** | Idem — Claude GT tampoco lo extrae |

### 4.4 Campos con impacto en deteccion de anomalias

Los campos mas relevantes para el pipeline de deteccion de anomalias (ranking) y su fiabilidad:

| Campo | Fiabilidad | Modelo recomendado | Nota |
|-------|:----------:|-------------------|------|
| `contiene_denuncia_penal` | 100% | Cualquiera | Deteccion perfecta |
| `menciona_dncp` | 100% | Cualquiera | Deteccion perfecta |
| `contiene_clausula_garantia` | 100% | Cualquiera | Deteccion perfecta |
| `menciona_ley_5282/7021` | 100% (Qwen) / 27-33% (Mistral) | **Qwen** | Mistral no reconoce leyes |
| `acciones_con_conector_y_o` | 100% (Qwen few-shot) / 83-87% (otros) | **Qwen few-shot** | Conector legal critico |
| `contiene_exclusion_previsibilidad` | 73-97% (variable segun prompt) | **Qwen base** | Mas fiable con prompt sin few-shot |
| `texto_vacio_o_muy_reducido` | 100% | Cualquiera | Deteccion perfecta |

> Detalle completo en [`data/processed/validation/gt_detailed_analysis.md`](../data/processed/validation/gt_detailed_analysis.md)

---

## 5. Anomalías Detectadas

### 5.1 pseudo-GT engañoso

La validacion inicial usaba **majority vote entre 9 configuraciones locales** como pseudo-ground truth. Esto **sobreestimo** a Llama 3.1 (+4.9pp) y Mistral (+6.5pp) porque los modelos chicos tendian a concordar en errores.

| Modelo | pseudo-GT F1 | Claude GT accuracy | Diferencia |
|--------|:-----------:|:------------------:|:----------:|
| Llama 3.1 8B few-shot | 96.4% | **91.5%** | **−4.9pp** |
| Mistral 7B strict | 94.0% | **87.5%** | **−6.5pp** |
| Qwen2.5 14B base | 93.3% | **94.2%** | **+0.9pp** |

**Leccion**: pseudo-GT por majority vote entre modelos 7-14B es poco fiable. Para ground truth real se necesita un modelo significativamente mas capaz (Claude 3.5 Sonnet, GPT-4) o anotacion humana.

### 5.2 Campos de word count

`longitud_seccion_palabras` y `longitud_texto_Causales_De_Terminacion` dan 0% en **todos los modelos** porque son campos calculados (word count = `len(text.split())`). Ni siquiera Claude los extrae. **Solucion**: calcular con Python post-extraccion, no via LLM.

### 5.3 Causales de Terminacion del Contrato

Solo 50% de accuracy (mejor modelo). Este titulo tiene solo 2 campos definidos en el schema, y Claude pudo ser inconsistente. Vale la pena revisar el schema de este titulo.

### 5.4 Keyword Evidence Rate

La tasa de evidencia por keywords es ~91-93% para todos los modelos. Los 270 CONTRADICTS en `texto_vacio_o_muy_reducido` son falsos positivos del validador — este campo requiere juicio semantico, no keyword matching.

### 5.5 Mistral 7B few-shot

Solo 86.5% accuracy con structural OK de 88% (vs 100% de los otros prompts). El few-shot en Mistral 7B **empeora** la calidad en lugar de mejorarla — el modelo se confunde con los ejemplos.

> Detalle: [`data/processed/validation/validation_report.md`](../data/processed/validation/validation_report.md)

---

## 6. Opciones de Mejora

### 6.1 Guided decoding (xgrammar) — +2-3pp estimado

Forzar gramatica JSON en el decode para eliminar parse errors y campos faltantes.
- Implementacion: vLLM server con `--guided-decoding-backend xgrammar`
- Impacto: garantiza JSON valido + todos los campos presentes
- Esfuerzo: bajo (dia)

### 6.2 Post-procesamiento inteligente — +1-2pp

Calcular word count con `len(text.split())` en vez de pedir al modelo (0% actual). Regex para campos como `acciones_con_conector_y_o`.

### 6.3 Ensemble por campo — +0.5-1pp

Usar Qwen 14B para 95% de campos + Llama/Mistral solo para `num_tipos_impuestos_listados` (donde Qwen da solo 13% vs 90%). Cuesta 1.93s extra solo cuando aplica.

### 6.4 Prompt engineering — +1-2pp

- **Few-shot dinamico**: incluir 1-2 ejemplos reales del GT de Claude en el prompt, especificos del titulo actual
- **Instrucciones por tipo**: separar "campos binarios: busca frase exacta" de "campos numericos: cuenta ocurrencias"
- **Template de respuesta**: dar el JSON exacto esperado (es lo que hace guided decoding, pero via prompt)

### 6.5 Fine-tuning LoRA — +1-3pp (potencial)

Usar los 270 samples de Claude como dataset (~1800 pares instruccion-respuesta). Riesgo de overfitting por dataset chico.

### Matriz de impacto

| Opcion | Esfuerzo | Impacto | Acumulado |
|--------|:--------:|:-------:|:---------:|
| Guided decoding | Bajo | +2-3pp | → 96-97% |
| Post-procesamiento | Bajo | +1-2pp | → 97-98% |
| Ensemble campos debiles | Medio | +0.5-1pp | → 98-99% |
| Prompt engineering | Medio | +1-2pp | → 95-96% |
| Fine-tuning LoRA | Alto | +1-3pp | incierto |
| Auto-consistencia N=3 | Medio | +0.5-1pp | → 94-95% |

---

## 7. Arquitectura Propuesta para Pipeline 2

### 7.1 Opcion A: Calidad maxima (Qwen2.5 14B AWQ)

```
┌──────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ 315K secciones│───▶│ Qwen2.5 14B AWQ │───▶│ Post-procesamiento│
│ (pliegos_sec.)│    │ max_num_seqs=32  │    │ (word count,     │
└──────────────┘    │ gpu_mem=0.90      │    │  regex, tipos)   │
                    │ fp8 kv cache      │    └────────┬────────┘
                    │ 2.46s / seccion   │             │
                    │ 8.18× concurrencia│             ▼
                    └──────────────────┘    ┌─────────────────┐
                                            │ Guided decoding │
                                            │ (xgrammar)      │
                                            └────────┬────────┘
                                                     ▼
                                            ┌─────────────────┐
                                            │ section_cluster │
                                            │ tabla DB        │
                                            └─────────────────┘
```

**Throughput**: ~10.5 horas para 315K secciones.  
**Calidad esperada**: ~96-97% (base 94.2% + guided decoding + post-procesamiento).

### 7.2 Opcion B: Throughput balanceado (Llama 3.1 8B AWQ)

```
┌──────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ 315K secciones│───▶│ Llama 3.1 8B AWQ│───▶│ Post-procesamiento│
│ (pliegos_sec.)│    │ awq_marlin       │    └────────┬────────┘
└──────────────┘    │ max_num_seqs=64   │             │
                    │ gpu_mem=0.90      │             ▼
                    │ fp8 kv cache      │    ┌─────────────────┐
                    │ 1.93s / seccion   │    │ Ensemble parcial│
                    │ 29.21× concurrencia│    │ (Mistral 7B     │
                    └──────────────────┘    │  para num_tipos │
                                            │  _impuestos)     │
                                            └────────┬────────┘
                                                     ▼
                                            ┌─────────────────┐
                                            │ section_cluster │
                                            │ tabla DB        │
                                            └─────────────────┘
```

**Throughput**: ~5.8 horas para 315K secciones.  
**Calidad esperada**: ~93-95% (base 91.5% + ensemble + post-procesamiento).

### 7.3 Opcion C: Rapida (Mistral 7B AWQ)

```
┌──────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ 315K secciones│───▶│ Mistral 7B AWQ  │───▶│ Post-procesamiento│
│ (pliegos_sec.)│    │ awq_marlin      │    └────────┬────────┘
└──────────────┘    │ max_num_seqs=64  │             │
                    │ gpu_mem=0.90     │             ▼
                    │ fp8 kv cache     │    ┌─────────────────┐
                    │ 1.61s / seccion  │    │ Guided decoding │
                    │ 35.22× concurren.│    │ (xgrammar)      │
                    └──────────────────┘    └────────┬────────┘
                                                     ▼
                                            ┌─────────────────┐
                                            │ section_cluster │
                                            │ tabla DB        │
                                            └─────────────────┘
```

**Throughput**: ~4 horas para 315K secciones.  
**Calidad esperada**: ~90-92% (base 87.5% + guided decoding + post-procesamiento).

### 7.4 Capa de postproceso determinista compartida (actualización Sesión 15)

Independiente del modelo elegido, toda extracción (regex o LLM) pasa por una **capa de
postproceso determinista compartida** — `src/etl/transformers/extraction_postprocess.py`:

```
Capa 1 · DETERMINISTA (autoritativa)  deterministic_pct()  — 7 campos pct uniformes
Capa 2 · REGEX (CPU)                  _extract_field_text() — 60 campos (v2 transformer)
Capa 3 · LLM fallback (GPU)           hybrid fields conf < 0.8
      ↓ (siempre)
Capa de POSTPROCESO  postprocess(strict) — ×100, contexto N%, meses→días, reglas anti-FP
```

Decisiones clave (detalle en [`docs/tesis_b_c_resultados.md`](../docs/tesis_b_c_resultados.md)):
- `deterministic_pct` es **autoritativa**: ausencia del patrón → None, nunca cae al LLM.
- `postprocess(strict=True)` en ruta LLM: el número debe aparecer literal en el texto con
  la unidad correcta; si no → NULL.
- Evaluación comparativa sobre GT humano congelado (5,066 juicios, 26 campos): media acc flex
  **H (híbrido)=65.1%** > L_pp=63.1 > R=61.0 > Ri=60.9 > L_raw=51.0. LLM aporta valor real en
  campos de texto variable (`dias_notificacion`, `dias_pago_maximo`, ratios) y sobra en campos
  plantilla (pct uniformes, `pct_anticipo` todo-NULL: 95 falsos positivos del fallback).

---

## 8. Estimaciones de Throughput

### 8.1 Tiempos por configuracion

| Configuracion | s/sample | 315K secuencial | 315K con batching |
|---------------|:--------:|:---------------:|:-----------------:|
| HTTP server baseline | 9.3s | 33.9 dias | — |
| Qwen3-14B GGUF | 5.1s | 18.6 dias | ~1.8 dias |
| Mistral 7B GGUF | 4.9s | 17.8 dias | ~1 dia |
| **Llama 3.1 8B AWQ** | **1.93s** | **6.9 dias** | **~5.8 horas** |
| **Mistral 7B AWQ** | **1.61s** | **5.9 dias** | **~4 horas** |
| **Qwen2.5 14B AWQ** | **2.46s** | **9.0 dias** | **~10.5 horas** |
| + guided JSON | ~0.03s | — | ~2-3 horas |

### 8.2 Memoria requerida

| Modelo | VRAM modelo | KV cache disp. | Concurrencia |
|--------|:-----------:|:--------------:|:------------:|
| Mistral 7B AWQ | 3.89 GiB | 8.81 GiB | 35.22× |
| Llama 3.1 8B AWQ | 5.39 GiB | 7.30 GiB | 29.21× |
| Qwen2.5 14B AWQ | 9.38 GiB | 3.07 GiB | 8.18× |

---

## 9. Referencias

### Documentos del proyecto

| Documento | Contenido | Ruta |
|-----------|-----------|------|
| Benchmark completo | Todos los modelos, ranking, throughput | `docs/benchmark_resultados.md` |
| Validacion Fase 1 | pseudo-GT, keyword evidence, cross-prompt | `data/processed/validation/validation_report.md` |
| Validacion Claude GT | Accuracy real vs Claude 3.5 Sonnet | `data/processed/validation/gt_validation_report.md` |
| Analisis detallado | Por campo, por modelo, campos problematicos | `data/processed/validation/gt_detailed_analysis.md` |
| Ground Truth (JSON) | 270 samples etiquetados por Claude | `data/processed/validation/gt_labels_web.json` |
| Session context | Historial completo de sesiones | `AGENTS.md` |
| Plan feature improvements | Fases 1-6 del proyecto | `docs/plan_feature_improvements.md` |

### Codigo fuente relevante

| Script | Proposito | Ruta |
|--------|-----------|------|
| Validacion multi-modelo | 3 modelos × 3 prompts, 270 samples | `scripts/validate_extraction_quality.py` |
| Generar prompts GT | Crear archivos .txt para Claude web | `scripts/generate_gt_prompts.py` |
| Importar respuestas GT | Compilar respuestas Claude en GT | `scripts/import_gt_responses.py` |
| Analisis detallado GT | Per-field, per-model breakdown | `scripts/_analyze_gt.py` |

### Datasets generados

| Dataset | Formato | Contenido | Ruta |
|---------|---------|-----------|------|
| Ground Truth Claude | JSON | 270 sections × 1770 fields etiquetados | `data/processed/validation/gt_labels_web.json` |
| Predicciones locales | JSON (×9) | 3 modelos × 3 prompts × 270 samples | `data/processed/validation/raw_*.json` |
| Metricas Fase 1 | JSON | F1, precision, recall, KW evidence | `data/processed/validation/metrics.json` |
| Prompts para Claude | TXT (×10) | System prompt + 9 titulos | `data/processed/validation/gt_prompts/` |

---

## Pendientes / Proximos Pasos

1. 🚀 **Implementar Pipeline 2** — decidir modelo (Qwen2.5 14B para calidad, Mistral 7B para throughput)
2. 🎯 **Guided decoding** — probar xgrammar para eliminar errores estructurales
3. 🧪 **Re-correr validacion con guided decoding** — medir impacto real
4. 📐 **Post-procesamiento** — word count via Python, regex para conectores
5. 🔄 **Ensemble opcional** — Mistral 7B para `num_tipos_impuestos_listados`
6. 📝 **Schema review** — revisar `Causales de Terminacion` (50% accuracy) y word count fields
7. 🧹 **Commit** — todos los cambios de sesiones 1-7 estan sin commitear

---

*Documento generado: Julio 2026*
*Fuente: [`scripts/generate_gt_prompts.py`](../scripts/generate_gt_prompts.py), [`scripts/import_gt_responses.py`](../scripts/import_gt_responses.py), [`scripts/validate_extraction_quality.py`](../scripts/validate_extraction_quality.py)*

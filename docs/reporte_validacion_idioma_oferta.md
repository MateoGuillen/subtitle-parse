# Schema Validation Report — "idioma de la oferta"

## Resumen Ejecutivo

Schema DeepSeek v4 para **"idioma de la oferta"** validado con **94.4% accuracy** (Qwen 2.5 14B Q5_K_M). Apto para Pipeline 2.

---

## 1. Modelos Evaluados

### Hardware

| Componente | Especificación |
|------------|----------------|
| GPU | RTX 4070 Ti SUPER (16 GB VRAM) |
| RAM | ~32 GB |
| CPU | Intel 16 cores |
| Software | LM Studio con backend llama.cpp |

### Modelos probados

| Modelo | Cuantización | VRAM | Velocidad* | Accuracy | ¿Recomendado? |
|--------|:-----------:|:----:|:----------:|:--------:|:-------------:|
| Qwen 2.5 7B | Q4_K_M | ~4.5 GB | 1.1s | **70%** | ❌ |
| Qwen 2.5 14B | Q4_K_M | ~8.5 GB | **1.9s** | 87.5% | ✅ |
| **Qwen 2.5 14B** | **Q5_K_M** | ~11 GB | **2.6s** | **94.4%** | **✅ RECOMENDADO** |
| Qwen 3 8B | ? | ~6 GB | 7.4s | No testeado | ❌ Lento |
| Gemma 3 12B | ? | ~8 GB | 11.1s | No testeado | ❌ Lento |

*\* Tiempo efectivo por sample con batch 4x concurrente en LM Studio*

### Decisión final: Qwen 2.5 14B Q5_K_M

| Criterio | Veredicto |
|----------|-----------|
| Accuracy | **94.4%** — supera el umbral de 85% |
| Velocidad | **2.6s/sample efectivo** con batch 4x = ~27h para 10 títulos |
| VRAM | ~11 GB — cabe en 16 GB con margen para batch 4x |
| Disponibilidad | Cargado y estable en LM Studio |

---

## 2. Benchmark de Velocidad

### Metodología

- 1264 tokens de prompt + 114 tokens de output por sample
- 4 requests concurrentes (LM Studio / llama.cpp continuous batching)
- Modelos bartowski/qwen2.5-14b-instruct (Q4) y lmstudio-community/qwen2.5-14b-instruct (Q5)

### Resultados

| Modelo | 1 request | 4 concurrentes | Speedup | Efectivo/sample |
|--------|:---------:|:--------------:|:-------:|:---------------:|
| 7B Q4 | 4.1s | 4.4s | 3.7x | **1.1s** |
| 14B Q4 | 8.4s | 7.7s | **4.4x** | **1.9s** |
| 14B Q5 | 16.5s | ~10.4s* | ~4x* | **2.6s*** |

*\* Estimado para Q5 — el benchmark Q4 mostró 4.4x speedup, extrapolando a Q5*

### Throughput proyectado

| Modelo | 5K secciones (1 título) | 10 títulos |
|--------|:-----------------------:|:----------:|
| 7B Q4 | 1.5h | 15h |
| 14B Q4 | 2.7h | 27h |
| 14B Q5 | 3.6h | 36h |

### Conclusiones de velocidad

1. **LM Studio batch 4x** da 3.7-4.4x speedup — la clave del throughput
2. 14B Q5 es solo 1.4x más lento que 14B Q4, pero da 7 ppt más de accuracy
3. Con batch 4x, 10 títulos tardan **~36h** en 14B Q5 — factible para ejecución overnight

---

## 3. Accuracy por Campo

### Resultado final: 94.4%

| Campo | Type | Method | Accuracy | TP | TN | FP | FN | Importancia |
|-------|:----:|:------:|:--------:|:--:|:--:|:--:|:--:|:-----------:|
| texto_vacio | binary | structural | **100%** | 3 | 17 | 0 | 0 | 7 |
| contiene_clausula_traduccion_oferta | binary | literal | **100%** | 16 | 4 | 0 | 0 | 9 |
| contiene_contenido_adicional | binary | literal | **100%** | 1 | 19 | 0 | 0 | 8 |
| menciona_traduccion_oficial_o_matriculado | binary | literal | **95%** | 16 | 3 | 1 | 0 | 8 |
| num_palabras_seccion | numeric | structural | **95%** | 16 | 3 | 0 | 1 | 5 |
| respuesta_permiso_presente | binary | semantic | **90%** | 15 | 3 | 1 | 1 | 9 |
| pregunta_permiso_es_sin_traduccion | binary | literal | **90%** | 14 | 4 | 0 | 2 | 6 |
| permite_documentos_sin_traduccion | binary | semantic | **85%** | 0 | 17 | 2 | 1 | 9 |
| **PROMEDIO** | | | **94.4%** | | | | | |

### Campos con menor accuracy

| Campo | % | Error dominante | Causa raíz |
|-------|:-:|:---------------:|------------|
| permite_documentos_sin_traduccion | 85% | 2 FP, 1 FN | Depende de `respuesta_permiso_presente`, el modelo a veces interpreta "No Aplica" como permiso |
| respuesta_permiso_presente | 90% | 1 FP, 1 FN | Textos donde la respuesta está implícita o truncada |
| pregunta_permiso_es_sin_traduccion | 90% | 2 FN | El modelo encuentra "sin traducción" en el texto pero no lo asocia a la línea de permiso |

### Límites conocidos del modelo

1. **Qwen 7B** falla en distinguir `pregunta_permiso_es_sin_traduccion` de `permite_documentos_sin_traduccion` (0%) porque ambos campos se refieren a "sin traducción" pero con distinto significado (pregunta vs respuesta). **14B es el mínimo viable** para este schema.
2. **Qwen 14B Q4** pierde capacidad semántica vs Q5 — `pregunta_permiso_es_sin_traduccion` baja de 90% a 30% con Q4.
3. **Campos numéricos**: el LLM cuenta palabras distinto a `cluster_values`. Se usa tolerancia ±15.

---

## 4. Prompt Engineering — Lecciones Aprendidas

### Lo que funciona

| Técnica | Ejemplo | Efecto |
|---------|---------|--------|
| **Lenguaje imperativo fuerte** | "IMPORTANTE", "ESTRICTAMENTE LITERAL", "NO inferir, NO interpretar", "ABSOLUTAMENTE cualquier otro caso" | 30% → **100%** en `contiene_contenido_adicional` |
| **Método literal para campos de presencia textual** | Buscar frase exacta en el texto | 100% si existe patrón textual único |
| **Método semantic para campos de interpretación** | Analizar significado de la pregunta | 85-90%, dependiente del modelo |
| **Ejemplos negativos explícitos** | "Incluso si contiene 'No Aplica'..." | Reduce FP al excluir casos frontera |

### Lo que no funciona

| Técnica | Problema |
|---------|----------|
| **Descripciones estructurales vagas** | "Identificar el bloque de texto tras la línea..." → el modelo interpreta cualquier texto como "adicional" |
| **Hint con dependencia entre campos** | "Si `respuesta_permiso_presente` es 1..." → el modelo no siempre calcula los campos en orden |
| **Cambiar extraction_method sin re-testear** | Cambiar `literal` → `semantic` para `pregunta_permiso` mejoró 7B pero empeoró 14B de 90%→40% |
| **Hint largo con múltiples condiciones** | >3 condiciones → el modelo ignora las últimas |

### Regla general

```
Para campos binarios con patrón textual conocido:
  extraction_method = literal
  hint = "IMPORTANTE... [condición exacta]. 
         Retornar 1 SOLO si... 
         Retornar 0 en ABSOLUTAMENTE cualquier otro caso."

Para campos que requieren interpretación:
  extraction_method = semantic
  hint = "Descripción clara del concepto en 1-2 oraciones.
         [Ejemplo positivo]. [Ejemplo negativo]."
```

---

## 5. Evolución de la Validación

### Iteraciones

| Iteración | Modelo | Accuracy | Cambio |
|:---------:|:------:|:--------:|--------|
| 1 (baseline) | 7B Q4 | 70% | 6 campos 100%, 2 campos 0% |
| 2 | 14B Q5 | 86.2% | `contiene_contenido_adicional` al 30% |
| 3 | 14B Q5 + hint fix | 94.4% | `contiene_contenido_adicional` 30%→100% |
| 4 (regresión) | 14B Q4 + hint fix | 87.5% | `pregunta_permiso` 90%→30% por Q4 |
| 5 (final) | 14B Q5 + ambos fixes | **94.4%** | Todos los campos ≥85%, 4 campos 100% |

### Comparación antes/después del hint fix

| Campo | Antes | Después | Delta |
|-------|:-----:|:-------:|:-----:|
| contiene_contenido_adicional | 30% | **100%** | **+70 ppt** |
| contiene_clausula_traduccion_oferta | 100% | 100% | 0 |
| menciona_traduccion_oficial_o_matriculado | 95% | 95% | 0 |
| num_palabras_seccion | 100% | 95% | -5 ppt |
| permite_documentos_sin_traduccion | 85% | 85% | 0 |
| pregunta_permiso_es_sin_traduccion | 90% | 90% | 0 |
| respuesta_permiso_presente | 90% | 90% | 0 |
| texto_vacio | 100% | 100% | 0 |
| **PROMEDIO** | **86.2%** | **94.4%** | **+8.2 ppt** |

---

## 6. Limitaciones y Riesgos

### Conocidas

1. **Muestra de validación**: 20 samples (1 por cluster). No cubre toda la variabilidad del dataset de 5000 secciones.
2. **cluster_values de DeepSeek**: Los valores esperados son predicciones de otro LLM, no ground truth. Especialmente frágil para campos numéricos como `num_palabras_seccion`.
3. **contiene_contenido_adicional**: El hint literal solo busca 2 frases específicas. Si aparecen nuevas formas de contenido adicional en producción, este campo fallará silenciosamente.

### Mitigaciones

| Riesgo | Mitigación |
|--------|------------|
| Nuevas formas de contenido adicional | Monitorear en producción, expandir hint periódicamente |
| Degradación del LLM local | Tener pipeline con fallback a OpenRouter |
| Variabilidad entre ejecuciones | Usar temperature=0.0, documentar modelo exacto usado |
| Cluster values no representan ground truth | Validación manual periódica de una muestra estadística |

---

## 7. Recomendaciones para Pipeline 2

### Configuración recomendada

```yaml
model: lmstudio-community/qwen2.5-14b-instruct  # Q5_K_M
batch_size: 4                                      # Concurrent requests
temperature: 0.0
timeout: 180s
retries: 3
```

### Orden de implementación sugerido

1. Implementar extraction runner con 14B Q5 (schema ya validado)
2. Validar en 1000 secciones vs ground truth manual
3. Si accuracy ≥ 90%, expandir a 5000 secciones
4. Repetir validación para los otros 9 títulos

### Tiempo estimado

| Tarea | Tiempo |
|-------|:------:|
| Validar 1 título | ~4h (20 samples con Q5) |
| Pipeline 1 título (5K secciones) | ~3.6h (batch 4x) |
| Pipeline 10 títulos (50K secciones) | ~36h |
| Validación manual de 100 muestras | ~1 día hábil |

---

## 8. Schema Final (8 campos)

```json
{
  "texto_vacio": {"type": "binary", "method": "structural", "importance": 7},
  "contiene_clausula_traduccion_oferta": {"type": "binary", "method": "literal", "importance": 9},
  "menciona_traduccion_oficial_o_matriculado": {"type": "binary", "method": "literal", "importance": 8},
  "respuesta_permiso_presente": {"type": "binary", "method": "semantic", "importance": 9},
  "permite_documentos_sin_traduccion": {"type": "binary", "method": "semantic", "importance": 9},
  "pregunta_permiso_es_sin_traduccion": {"type": "binary", "method": "literal", "importance": 6},
  "contiene_contenido_adicional": {"type": "binary", "method": "literal", "importance": 8},
  "num_palabras_seccion": {"type": "numeric", "method": "structural", "importance": 5}
}
```

---

## Anexo: Comandos de Reproducción

```bash
# Validación completa (20 samples, Q5)
python scripts/validate_extraction_prompts.py \
  --title "idioma de la oferta" \
  --samples 20 \
  --mode auto \
  --model "lmstudio-community/qwen2.5-14b-instruct"

# Validación rápida (5 samples, Q4)
python scripts/validate_extraction_prompts.py \
  --title "idioma de la oferta" \
  --samples 5 \
  --mode auto \
  --model "bartowski/qwen2.5-14b-instruct"
```

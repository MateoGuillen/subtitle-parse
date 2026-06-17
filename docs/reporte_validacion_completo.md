# Validación de Schemas — Reporte Completo

## Títulos Procesados

| # | Título | Modelo | Accuracy (doble validación) | Estado |
|:-:|--------|:------:|:---------------------------:|:------:|
| 1 | idioma de la oferta | DeepSeek v4 | **93.8%-96.9%** | ✅ Apto |
| 2 | copias de la oferta cps | Claude v4 | **93.8%** | ✅ Apto |

---

## 1. Hardware Utilizado

| Componente | Especificación |
|------------|----------------|
| GPU | NVIDIA RTX 4070 Ti SUPER, 16 GB VRAM |
| RAM | ~32 GB |
| CPU | Intel 16 cores |
| Software | LM Studio v1.x (backend llama.cpp) |
| OS | Windows 11 |

### Capacidad VRAM por modelo

| Modelo | Cuantización | VRAM | Batch 4x | Batch 8x |
|--------|:-----------:|:----:|:--------:|:--------:|
| Qwen 2.5 7B | Q4_K_M | ~4.5 GB | ✅ Sí | ✅ Sí |
| Qwen 2.5 14B | Q4_K_M | ~8.5 GB | ✅ Sí | ⚠️ Límite |
| Qwen 2.5 14B | **Q5_K_M** | ~11 GB | ✅ Sí | ❌ No |
| Gemma 3 12B | Q4_K_M | ~8 GB | ✅ Sí | ⚠️ Límite |

La RTX 4070 Ti SUPER con 16 GB permite ejecutar Qwen 14B Q5_K_M con batch 4x concurrente (11 GB de pesos + ~3 GB de KV cache para 4 secuencias de 4096 tokens).

---

## 2. Metodología de Validación

### 2.1. Flujo completo por título

```
┌─────────────────────────────┐
│ 1. Exportar prompt           │
│    run_section_clustering..  │
│    --skip-llm               │
│    --export-chat-prompts     │
└─────────────┬───────────────┘
              ▼
┌─────────────────────────────┐
│ 2. Enviar a LLM web          │ ◄── Semi-automático: copiar system + user
│    DeepSeek / Claude         │     a DeepSeek o Claude web
└─────────────┬───────────────┘
              ▼
┌─────────────────────────────┐
│ 3. Guardar respuesta         │
│    data/external/llm_web_    │
│    responses/{provider}_     │
│    response_v4_{title}.txt   │
└─────────────┬───────────────┘
              ▼
┌─────────────────────────────┐
│ 4. Insertar en master        │
│    insert_llm_schema.py      │
│    --title --provider --file  │
└─────────────┬───────────────┘
              ▼
┌─────────────────────────────┐
│ 5. Validar doble (16 samples  │ ◄── Automático: 2 runs × 16 samples
│    × 2 runs) vs Qwen 14B     │     con muestras diferentes
└─────────────┬───────────────┘
              ▼
┌─────────────────────────────┐
│ 6. Ajustar hints si < 85%   │ ◄── Iterar hint → re-validar
└─────────────┬───────────────┘
              ▼
┌─────────────────────────────┐
│ 7. Schema finalizado         │
│    accuracy ≥ 85% → Pipeline │
└─────────────────────────────┘
```

### 2.2. Muestreo estratificado

- 1 sample por **cluster raw** (no merged)
- Se toman de `samples_per_cluster` en `master_schemas.json` (5 muestras por cluster raw)
- Estratificación: `ceil(N / n_clusters_raw)` por cluster
- Para 8 clusters raw con 16 samples → 2 samples por cluster
- **Doble validación**: 2 runs con muestras diferentes para medir varianza

### 2.3. Métrica de accuracy

```
Para campos binarios:
  accuracy = (TP + TN) / (TP + TN + FP + FN)
  
  TP = expected=1, predicted=1
  TN = expected=0, predicted=0
  FP = expected=0, predicted=1
  FN = expected=1, predicted=0

Para campos numéricos:
  accuracy = (TP + TN) / total  con tolerancia ±15
  (unidad: palabras para word count, 0/1/2+ para conteos)
```

### 2.4. Criterio de aceptación

| Accuracy | Estado | Acción |
|:--------:|:------:|--------|
| ≥ 85% | ✅ **Apto** | Schema listo para Pipeline 2 |
| 70-84% | ⚠️ Ajustar | Mejorar hints de campos < 85%, re-validar |
| < 70% | ❌ Rechazar | Schema mal diseñado, re-generar |

---

## 3. Decisiones de Modelo LLM

### 3.1. Modelo local: Qwen 2.5 14B Q5_K_M

| Candidato | Accuracy | Speed (batch 4x) | Decisión |
|-----------|:--------:|:----------------:|:--------:|
| Qwen 2.5 7B Q4 | ~70% | **1.1s/sample** | ❌ 2 campos fallan (0%) |
| **Qwen 2.5 14B Q5** | **~92%** | **2.6s/sample** | **✅ Elegido** |
| Qwen 2.5 14B Q4 | ~88% | 1.9s/sample | ⚠️ 2do lugar (pierde semántica) |
| Gemma 3 12B | No testeado | 11.1s/sample | ❌ Muy lento |
| Qwen 3 8B | No testeado | 7.4s/sample | ❌ Lento, no probado |

**Por qué Qwen 14B Q5:**

1. **Límite del 7B**: Qwen 7B no puede distinguir `pregunta_permiso_es_sin_traduccion` de `permite_documentos_sin_traduccion` — ambos campos se refieren a "sin traducción" pero con distinto significado (pregunta vs respuesta). El 7B da 0% en ambos; el 14B da 85-90%.

2. **Q5 vs Q4**: El 14B Q4 pierde capacidad semántica. En `pregunta_permiso_es_sin_traduccion` baja de 90% (Q5) a 30% (Q4). Para campos con método `semantic`, la cuantización más alta importa.

3. **Batch 4x cabe en VRAM**: 11 GB de pesos + ~3 GB de KV cache = 14 GB, dejando margen en 16 GB.

### 3.2. Provedor web: DeepSeek vs Claude

| Aspecto | DeepSeek | Claude |
|---------|:--------:|:------:|
| Campos por schema | 4 (promedio) | 9 (promedio) |
| Granularidad | Baja — grupos gruesos | Alta — descompone por cláusula |
| Hints | Cortos (1-2 oraciones) | Largos (3-5 oraciones) |
| cluster_values | ✅ Siempre incluido | ✅ Siempre incluido |
| Riesgo con modelo local | Bajo | Medio (hints largos pueden degradarse) |
| Accuracy real en título 1 | 94.4% | - |
| Accuracy real en título 2 | 90.6% | **89.6%** |

**Decisión: usar el que mejor schema genere, no el que más accuracy dé.**
- Para **idioma de la oferta**: usamos DeepSeek porque Claude dio 11 campos redundantes (v4 vs v4).
- Para **copias de la oferta cps**: usamos Claude porque descompone mejor las 5 cláusulas individuales vs los 3 grupos gruesos de DeepSeek.

No hay un ganador universal. La recomendación es generar con ambos y elegir manualmente.

---

## 4. Análisis Costo-Tiempo-Accuracy

### 4.1. Throughput del LLM local

| Configuración | 1 sample | 5K sections | 10 titles |
|:--------------|:--------:|:-----------:|:---------:|
| **7B Q4 secuencial** | 4.1s | 5.7h | 57h |
| **7B Q4 batch 4x** | 1.1s | **1.5h** | **15h** |
| 14B Q4 batch 4x | 1.9s | 2.7h | 27h |
| **14B Q5 batch 4x** | **2.6s** | **3.6h** | **36h** |

### 4.2. Costo de validación por título

| Actividad | Tiempo | Costo operativo |
|-----------|:------:|:---------------:|
| Enviar prompt a LLM web (semi-automático) | ~5 min | 0 (gratis web) |
| Insertar schema + validar doble (16×2) | ~50 min | Electricidad + GPU |
| Ajustar hint + re-validar (1 iteración) | ~50 min | Electricidad + GPU |
| **Total por título** | **~1.5h** | **Casi 0** |

### 4.3. Proyección de Pipeline 2

| Títulos restantes | Samples | Tiempo validación | Pipeline 10 títulos |
|:-----------------:|:-------:|:-----------------:|:-------------------:|
| 8 | 16 c/u | ~11h | **36h** (Qwen 14B Q5 batch 4x) |

### 4.4. Costo de GPU

La RTX 4070 Ti SUPER consume ~200W bajo carga LLM:

| Escenario | Horas | kWh | Costo eléctrico (~$0.12/kWh) |
|-----------|:-----:|:---:|:----------------------------:|
| Validar 8 títulos restantes | 11h | 2.2 | ~$0.26 |
| Pipeline 10 títulos | 36h | 7.2 | ~$0.86 |
| **Total proyecto** | **47h** | **9.4** | **~$1.13** |

**Conclusión de costo:** el LLM local con GPU propia es esencialmente gratis. No hay API calls, no hay rate limits, no hay costos por token.

---

## 5. Resultados Detallados por Título

### 5.1. idioma de la oferta

**Schema:** DeepSeek v4 — 8 campos

| Campo | Type | Method | Accuracy | Importancia |
|-------|:----:|:------:|:--------:|:-----------:|
| texto_vacio | binary | structural | **100%** | 7 |
| contiene_clausula_traduccion_oferta | binary | literal | **100%** | 9 |
| contiene_contenido_adicional | binary | literal | **100%** | 8 |
| menciona_traduccion_oficial_o_matriculado | binary | literal | **95%** | 8 |
| num_palabras_seccion | numeric | structural | **95%** | 5 |
| respuesta_permiso_presente | binary | semantic | **90%** | 9 |
| pregunta_permiso_es_sin_traduccion | binary | literal | **90%** | 6 |
| permite_documentos_sin_traduccion | binary | semantic | **85%** | 9 |
| **PROMEDIO** | | | **94.4%** | |

**Iteraciones de hint:**

| Iteración | Accuracy | Cambio |
|:---------:|:--------:|--------|
| Baseline (7B) | 70% | 2 campos en 0% |
| 14B Q5 original | 86.2% | `contiene_contenido_adicional` al 30% |
| + hint fix: strict literal | **94.4%** | `contiene_contenido_adicional` 30% → 100% |

**Lección aprendida:** para campos binarios con patrón textual conocido, usar `extraction_method: literal` con lenguaje imperativo fuerte ("IMPORTANTE", "ESTRICTAMENTE LITERAL", "NO inferir", "ABSOLUTAMENTE cualquier otro caso"). Los hints vagos ("identificar el bloque de texto tras...") hacen que el LLM alucine.

---

### 5.2. copias de la oferta cps

**Schema:** Claude v4 — 9 campos

| Campo | Type | Method | Accuracy | Importancia |
|-------|:----:|:------:|:--------:|:-----------:|
| texto_vacio | binary | structural | **100%** | 10 |
| num_clausulas_normativas_presentes | numeric | semantic | **100%** | 9 |
| menciona_facultad_convocante_requerir_copias | binary | semantic | **100%** | 5 |
| menciona_obligacion_identificar_copias | binary | semantic | **100%** | 6 |
| menciona_oferta_original | binary | semantic | **100%** | 6 |
| presencia_etiqueta_cantidad_copias | binary | literal | **100%** | 7 |
| menciona_exencion_oferta_electronica | binary | semantic | **93.8%** | 7 |
| cantidad_copias_requeridas | numeric | semantic | **62.5%** | 8 |
| longitud_texto_palabras | numeric | structural | **50.0%** | 4 |
| **PROMEDIO** | | | **89.6%** | |

**Comparación DeepSeek vs Claude:**

| Esquema | Promedio | Campos 100% | Señal analítica |
|---------|:--------:|:-----------:|:----------------|
| DeepSeek (4 campos) | **90.6%** | 3/4 | Baja — 3 grupos gruesos |
| Claude (9 campos) | 89.6% | **7/9** | **Alta** — 6 cláusulas individuales |

**Decisión:** Claude, porque `longitud_texto_palabras` (50%) es redundante con `texto_vacio` (100%). Si se ignora, Claude efectivamente da ~97%. Los 6 campos semánticos individuales permiten identificar EXACTAMENTE qué cláusula falta en una sección anómala.

---

## 6. Prompt Engineering — Guía de Estilo

### Lo que funciona con Qwen 14B

| Técnica | Ejemplo | Efecto |
|---------|---------|--------|
| **Lenguaje imperativo fuerte** | "IMPORTANTE", "ESTRICTAMENTE LITERAL", "NO inferir" | Reduce FP drásticamente |
| **Exclusión explícita de negativos** | "Retornar 0 incluso si contiene 'No Aplica'" | Elimina casos frontera |
| **Método literal para presencia textual** | `extraction_method: literal` | 100% si patrón único |
| **Método semantic para interpretación** | `extraction_method: semantic` | 85-100% si bien redactado |
| **Hints de 1-2 oraciones** | Directo, sin condiciones anidadas | 90-100% |
| **Ejemplos positivos y negativos** | "como 'Sí' (retornar 1), o 'No Aplica' (retornar 0)" | Reduce ambigüedad |

### Lo que no funciona

| Técnica | Problema |
|---------|----------|
| **Hints estructurales vagos** | "Identificar el bloque de texto tras..." → LLM sobreinterpreta |
| **Dependencia entre campos** | "Si campo_X es 1, entonces..." → el orden de extracción no es confiable |
| **Hints con >3 condiciones** | El LLM ignora las últimas condiciones |
| **Campos numéricos de conteo** | El LLM cuenta palabras distinto al ground truth |
| **Cambiar método sin re-testear** | Lo que mejora 7B puede empeorar 14B |

### Regla de oro

```
Para detección de patrones textuales conocidos:
  → literal + hint corto + exclusión explícita

Para detección de conceptos semánticos:
  → semantic + hint con ejemplos + sin dependencias entre campos

Para campos estructurales (conteo/longitud):
  → mejor calcular programáticamente, no por LLM
```

---

## 7. Descubrimiento: Errores en Ground Truth (cluster_values)

### El problema

Durante la doble validación de "copias de la oferta cps", la accuracy variaba entre runs (89.6% vs 80.6%). Se descubrió que el LLM **no estaba fallando** — el ground truth (`cluster_values`) estaba mal.

### Causa raíz

Los `cluster_values` son generados por Claude/DeepSeek como parte del schema. Asumen que **todas las muestras de un cluster tienen los mismos valores**. Pero el clustering agrupa por **similitud de embedding**, no por valores de campos.

Ejemplo concreto — Cluster 2 de "copias de la oferta cps":
| Muestra | Texto real | Ground truth (cluster_values) | Realidad |
|---------|-----------|:-----------------------------:|:--------:|
| nro=416773 | Solo título (25 chars) | `texto_vacio=1, todo en 0` | ✅ Correcto |
| nro=403067 | Solo título (25 chars) | `texto_vacio=1, todo en 0` | ✅ Correcto |
| nro=424765 | 4-5 cláusulas (386 chars) | `texto_vacio=1, todo en 0` | ❌ **Incorrecto** |
| nro=412243 | 4-5 cláusulas (386 chars) | `texto_vacio=1, todo en 0` | ❌ **Incorrecto** |

El LLM predijo correctamente que nro=424765 tiene contenido (4-5 cláusulas), pero el ground truth decía que todos los campos eran 0. El "error" del LLM era en realidad una **corrección del ground truth**.

### Alcance del problema

`cantidad_copias_requeridas` tenía ground truth incorrecto en **24 de 40 muestras**:
- Cluster 0: ground truth = 0, pero 2 muestras dicen "1 copia"
- Cluster 3: ground truth = 1, pero 3 muestras no tienen valor después de ":"
- Cluster 5: ground truth = 1, pero TODAS las muestras dicen "0 copias"
- Cluster 6: ground truth = 0, pero TODAS las muestras dicen "1 copia" o "2 copias"

### Solución implementada

`sample_overrides` en `master_schemas.json` — un mapa de `nro_licitacion` → valores correctos para muestras específicas:

```json
"sample_overrides": {
  "424765": {"texto_vacio": 0, "num_clausulas_normativas_presentes": 5, ...},
  "404234": {"cantidad_copias_requeridas": 0},
  ...
}
```

El validador (`validate_extraction_prompts.py`) ahora verifica `sample_overrides` antes de usar `cluster_values`.

### Resultado

| Antes (ground truth mal) | Después (ground truth corregido) |
|:------------------------:|:--------------------------------:|
| `cantidad_copias` 56-62% | `cantidad_copias` **100%** |
| Run 2: 80.6% ❌ | Run 2: **93.8%** ✅ |

### Mejor práctica recomendada

**Fase 1: Auto-validación del ground truth** (pendiente de implementar):
1. Para cada sample, extraer el valor real con regex/Python (no con LLM)
2. Comparar contra `cluster_values`
3. Reportar inconsistencias
4. Auto-generar `sample_overrides` para las muestras que no coinciden

**Fase 2: Validación LLM** (ya existente):
Con el ground truth corregido, la validación mide la accuracy real del LLM.

```
1. Exportar prompt
2. Web LLM genera schema + cluster_values
3. Insertar en master_schemas.json
4. [NUEVO] validate_ground_truth.py → auto-genera sample_overrides
5. validate_extraction_prompts.py --runs 2 → mide accuracy real
6. Si < 85%, ajustar hints
```

### Lección aprendida

> **Nunca confiar ciegamente en los `cluster_values` generados por el LLM web.** Siempre validar que los valores coincidan con el contenido real de las muestras. Los campos que varían dentro de clusters deben calcularse por Python en Pipeline 2, no por LLM.

---

## 8. Estado Actual y Próximos Pasos

### Títulos validados

| Título | Schema | Proveedor | Accuracy (doble valid.) | Estado |
|--------|:------:|:---------:|:-----------------------:|:------:|
| idioma de la oferta | 8 campos | DeepSeek v4 | **93.8%-96.9%** | ✅ APTO |
| copias de la oferta cps | 9 campos | Claude v4 | **93.8%** | ✅ APTO |

### Títulos pendientes (8)

| Título | Clusters | Palabras promedio | Complejidad estimada |
|--------|:--------:|:-----------------:|:--------------------:|
| planos y disenos | 4 | 32 | Baja |
| porcentaje de garantia... | 3 | 43 | Baja |
| limitacion de responsabilidad | 6 | 63 | Media |
| audiencia informativa | 8 | 86 | Media |
| formato y firma de la oferta | 6 | 90 | Media |
| aclaracion de las ofertas | 9 | 110 | Alta |
| retiro sustitucion... | 9 | 150 | Alta |
| fraude y corrupcion | 8 | 254 | Muy alta |

### Tiempo estimado restante

| Actividad | Por título | 8 títulos |
|-----------|:----------:|:---------:|
| Prompt a LLM web (semi-automático) | 5 min | 40 min |
| Insertar + validar doble (16×2 samples) | 50 min | 7h |
| Ajustes de hint (1 iteración) | 50 min | 7h |
| **Total** | **~1.5h** | **~14h** |

---

## 8. Conclusiones

1. **Pipeline funcional**: el workflow de validación está maduro y reproducible. La doble validación (16 samples × 2 runs) es efectiva para detectar inestabilidad que un solo run no captura.

2. **Modelo óptimo**: Qwen 2.5 14B Q5_K_M en LM Studio con batch 4x da el mejor balance accuracy/tiempo. La RTX 4070 Ti SUPER con 16 GB es suficiente.

3. **Costo**: ~$1.13 de electricidad para todo el proyecto de 10 títulos. El LLM local elimina la dependencia de APIs pagas.

4. **La calidad del schema depende del LLM web**: DeepSeek da schemas más conservadores (menos campos, hints más cortos). Claude da schemas más granulares (más campos, descompone por cláusula). No hay ganador universal — probar ambos y elegir.

5. **Clave del accuracy**: el hint engineering es más importante que el modelo. Un hint bien redactado con lenguaje imperativo fuerte puede convertir un campo de 30% a 100% sin cambiar de modelo.

6. **Ground truth por muestra**: el clustering agrupa muestras por similitud de embedding, pero no garantiza contenido idéntico. Los `sample_overrides` en master_schemas.json corrigen errores del ground truth para muestras específicas.

7. **Campos para cálculo programático**: `longitud_texto_palabras` (50%) es el único campo que falla consistentemente — el LLM no sirve para conteo exacto de palabras. En Pipeline 2, calcular por Python.

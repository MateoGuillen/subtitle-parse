# Informe de Selección de Features para Isolation Forest — `dncp.pliegos_secciones`

## Resumen Ejecutivo

Análisis exhaustivo de 17 columnas de la tabla `dncp.pliegos_secciones` (2,779,386 registros) para identificar las mejores características para detección de anomalías con Isolation Forest. **La característica más valiosa es `content_length`** (longitud del contenido de cada sección en líneas), que combina el mayor poder de separación univariado (7% de outliers por Tukey), la máxima importancia en Random Forest (36.9% de ganancia) y una alta relevancia de dominio: secciones anómalamente largas o cortas son un indicador directo de irregularidades estructurales en pliegos de licitación.

---

## Metodología

Se aplicaron **7 estrategias complementarias** de selección de características sobre una muestra aleatoria de 50,000 registros (1.8% del total):

| # | Estrategia | Fundamento |
|---|-----------|-----------|
| 2.1 | **Potencial univariado de outlier** | Asimetría, curtosis y % fuera del rango Tukey [Q1-1.5×IQR, Q3+1.5×IQR]. Variables con colas pesadas contienen señales de anomalía. |
| 2.2 | **Reducción de multicolinealidad** | Matriz de Pearson + VIF. Variables con |r|>0.85 se consideran redundantes; se retiene la de mayor varianza o interpretabilidad. |
| 2.3 | **Calidad de datos y cardinalidad** | Se descartan columnas constantes (depth, word_count), IDs únicos (document_id, nro_licitacion) y texto libre muy largo (>200 chars avg). |
| 2.4 | **Relevancia de dominio** | Puntuación cualitativa (0-1) basada en el valor explicativo de cada columna para detectar fraudes/irregularidades en pliegos de licitación. |
| 2.5 | **Importancia Random Forest (proxy)** | Clasificación supervisada artificial: original vs. 5% ruido sintético. Importancia de Gini mide qué variables separan mejor el espacio. |
| 2.6 | **Importancia Isolation Forest** | Permutation importance sobre IF entrenado (contamination=0.05, n_estimators=200). Métrica directa de contribución al score de anomalía. |
| 2.7 | **Anomalías contextuales** | Distribuciones numéricas dentro de grupos (por título y categoría). Detecta outliers locales que son normales a nivel global. |

### Fórmula de puntuación global

```
final_score = 0.25 × outlier_score + 0.30 × domain_score + 0.25 × rf_importance + 0.10 × if_importance
              - 0.05 × vif_penalty - 0.05 × null_penalty
```

Penalización VIF aplica cuando VIF > 10. Penalización por nulos cuando > 0%.

---

## Resultados Intermedios

### 1. Metadatos y columnas descartadas

| Columna | Tipo | % Nulos | Cardinalidad | Decisión |
|---------|------|---------|-------------|----------|
| depth | smallint | 0% | 1 (constante=2) | **DESCARTADA** |
| word_count | integer | 0% | 1 (constante=0) | **DESCARTADA** |
| document_id | text | 0% | 32,182 | **DESCARTADA** (ID único) |
| nro_licitacion | text | 0% | 31,321 | **DESCARTADA** (FK) |
| content_text | text | 0% | - | **DESCARTADA** (texto libre, ~1150 chars avg) |
| content_clean | ARRAY | 0% | - | **DESCARTADA** (array de texto) |

### 2. Columnas numéricas — descriptivos

| Feature | mean | std | min | 25% | 50% | 75% | max | skewness | % outliers |
|---------|------|-----|-----|-----|-----|-----|-----|----------|-----------|
| content_length | 17.2 | 173.3 | 1 | 4 | 7 | 14 | 24,446 | 80.2 | **7.05%** |
| size_bytes | 1,491 | 11,892 | 57 | 326 | 665 | 1,397 | 1,585,556 | 82.2 | **7.33%** |
| estimated_tokens | 283 | 1,688 | 2 | 62 | 145 | 304 | 195,158 | 65.2 | **6.35%** |
| page | 25.7 | 23.6 | 3 | 10.8 | 21 | 35 | 758 | 5.9 | **2.58%** |
| content_density | - | - | - | - | - | - | - | 3.6 | **10.77%** |
| line_end_missing | - | - | 0 | 0 | 0 | 0 | 1 | 0.45 | 0% (binaria) |

### 3. Top correlaciones altas

| Feature 1 | Feature 2 | Pearson r |
|-----------|-----------|-----------|
| content_length | content_length_clean | **1.000** |
| estimated_tokens | size_bytes | **0.963** |
| content_length | estimated_tokens | 0.822 |
| line_start | line_end | 0.667 |
| bytes_per_token | content_density | 0.662 |

✅ **Decisión**: Se descartan `content_length_clean` (idéntica a `content_length`) y `estimated_tokens` (redundante con `size_bytes`, que tiene mayor interpretabilidad).

### 4. Importancia Random Forest (proxy)

```
content_length      ████████████████████████████████████████████████████████████████████ 36.9%
content_length_clean████████████████████████████████████████████████                     24.2%
size_bytes          ████████████████████████████████                                     14.3%
page                ███████████████████████                                              10.0%
estimated_tokens    █████████████                                                       5.7%
content_density     ███████████                                                          5.4%
```

### 5. Importancia Isolation Forest (permutation)

```
title_encoded       ████████████████████████████████████████████████████████████████
page                ██████████████████████████████████████████████████████████████
line_start          █████████████████████████████████████████████████████████████
year                ████████████████████████████████████████████████████████████
category_encoded    ███████████████████████████████████████████████████████████
line_end            ██████████████████████████████████████████████████████
lines_per_page      ████████████████████████████████████████████████████
content_density     ████████████████████████████████████████████████
```

### 6. Anomalías contextuales (% fuera de rango dentro de grupos)

| Feature | Agrupado por | % outliers contextuales |
|---------|-------------|------------------------|
| estimated_tokens | title_encoded | **1.57%** |
| size_bytes | title_encoded | **1.56%** |
| content_length | title_encoded | **1.28%** |
| line_span | category_encoded | **1.04%** |
| size_bytes | category_encoded | **0.92%** |

---

## Ranking Final de Features

### Top 12 (tras eliminar columnas redundantes)

| Rank | Feature | Puntaje | Outlier | Dominio | RF | IF |
|------|---------|---------|---------|---------|----|-----|
| 1 | **content_length** | **0.556** | 0.235 | 0.7 | 1.000 | 0.419 |
| 2 | **page** | **0.358** | 0.086 | 0.6 | 0.270 | 0.885 |
| 3 | **line_end_missing** | **0.335** | 0.000 | 0.8 | 0.000 | 1.000 |
| 4 | **size_bytes** | **0.333** | 0.244 | 0.5 | 0.389 | 0.281 |
| 5 | **title_encoded** | **0.302** | 0.000 | 0.7 | 0.003 | 0.913 |
| 6 | **content_density** | **0.276** | 0.359 | 0.3 | 0.146 | 0.600 |
| 7 | **lines_per_page** | **0.247** | 0.245 | 0.4 | 0.014 | 0.670 |
| 8 | **line_start** | **0.220** | 0.051 | 0.4 | 0.026 | 0.858 |
| 9 | **line_end** | **0.220** | 0.041 | 0.5 | 0.000 | 0.692 |
| 10 | **bytes_per_token** | **0.214** | 0.294 | 0.3 | 0.011 | 0.478 |
| 11 | **year** | **0.214** | 0.000 | 0.4 | 0.037 | 0.845 |
| 12 | **category_encoded** | **0.203** | 0.000 | 0.4 | 0.000 | 0.826 |

---

### Descripción detallada del Top 10

#### 1. `content_length` — Longitud de la sección (en líneas)

**Score: 0.556**

Es la característica dominante. Su distribución es extremadamente asímetrica (skewness=80.2): el 50% de las secciones tienen ≤7 líneas, pero existen valores de hasta 24,446 líneas. Un 7% de observaciones caen fuera del rango Tukey. Esto es altamente relevante en el contexto de pliegos de licitación: secciones anómalamente extensas pueden indicar inserción de cláusulas abusivas, condiciones ocultas, o contenido copiado. Por el contrario, secciones sospechosamente cortas pueden reflejar omisión de información crítica. RF le otorga la máxima importancia (36.9% de ganancia total). Complementa a `page` y `size_bytes` porque captura un aspecto estructural distinto (densidad de contenido por sección, no volumen total por página o peso en bytes).

#### 2. `page` — Número de página

**Score: 0.358**

Distribución con cola larga a la derecha (skewness=5.9, curtosis=83.9): la mayoría de secciones están entre páginas 3-35, pero existen hasta página 758. Un 2.6% son outliers Tukey. Isolation Forest le asigna una importancia de permutación muy alta (0.885), indicando que el page number por sí solo es informativo para aislar anomalías. En términos de dominio, páginas extremas pueden reflejar documentos inusualmente largos (posible manipulación), secciones que aparecen en páginas inesperadas (desorden estructural), o documentos con paginación alterada. Su complementariedad con `content_density` permite distinguir entre "sección larga en página normal" vs "sección normal en página extrema".

#### 3. `line_end_missing` — Indicador de línea final faltante (-1)

**Score: 0.335**

Variable binaria que indica si `line_end` tiene valor -1 (39% de los casos). Aunque tiene skewness 0 y 0% outliers según Tukey, su importancia en IF es la más alta de todas (1.0 normalizada). En el dominio de pliegos, una línea final faltante sugiere que la sección está truncada o que su metadata no se extrajo correctamente. Esto puede indicar documentos con estructura atípica, secciones incompletas, o problemas en el proceso de escaneo/OCR que frecuentemente acompañan a irregularidades documentales. Su codificación binaria la convierte en un complemento ortogonal a las variables continuas como `content_length` o `size_bytes`.

#### 4. `size_bytes` — Tamaño en bytes

**Score: 0.333**

Altamente correlacionado con `estimated_tokens` (r=0.96), pero con mejor interpretabilidad de negocio. Skewness=82.2, con 7.3% de outliers. Documentos con tamaño inusualmente grande pueden contener imágenes, tablas extensas o contenido embebido que no es texto plano, lo cual puede ser señal de manipulación. Documentos demasiado pequeños para su tipo de sección también son sospechosos. Su segunda posición en importancia RF (14.3%) confirma su utilidad. Es complementaria a `content_length` porque el tamaño en bytes puede reflejar contenido no textual (imágenes, tablas) que `content_length` en líneas no captura.

#### 5. `title_encoded` — Título de la sección (codificado)

**Score: 0.302**

Variable categórica con 284 valores únicos. Tiene la segunda importancia más alta en IF (0.913), lo que revela que el tipo de sección es un factor clave que Isolation Forest utiliza para aislar observaciones. Ciertos títulos como "Fraude y Corrupción", "Subcontratación" o "Capacidad Financiera" aparecen con frecuencias muy específicas. Una sección con un título inusual o raro dentro de un documento es una señal de alerta. Su valor de dominio es alto (0.7) porque los pliegos tienen una estructura estandarizada; cualquier desviación en la nomenclatura de secciones puede indicar un documento generado fuera de los estándares DNCP. Complementa a `category_encoded` porque el título es más granular que la categoría.

#### 6. `content_density` — Ratio contenido/tokens

**Score: 0.276**

Feature derivada = content_length / estimated_tokens. Tiene el porcentaje más alto de outliers Tukey (10.77%) con skewness=3.6. Mide qué tan denso es el contenido de una sección en relación a sus tokens. Una densidad anómala (muy alta o muy baja) puede indicar contenido críptico, tablas sin formato, o secciones con terminología excesivamente técnica. IF le asigna un peso moderado-alto (0.600). Es un complemento ideal para `size_bytes` y `content_length` porque captura la **relación** entre métricas, no solo sus valores absolutos.

#### 7. `lines_per_page` — Líneas por página

**Score: 0.247**

Derivada = line_span / page. Mide la compacidad de las secciones por página. Skewness=3.8, 7.3% outliers. Alta densidad de líneas por página puede reflejar documentos mal formateados o con contenido apretado intencionalmente (para ocultar cláusulas en espacio reducido). Baja densidad puede indicar páginas con contenido disperso o interlineado excesivo. IF le asigna 0.670 de importancia. Complementa a `page` y `line_span` al normalizar por página.

#### 8. `line_start` — Línea de inicio

**Score: 0.220**

Skewness=1.6, 1.5% outliers. La línea donde comienza una sección dentro de una página. Isolation Forest le da una importancia alta (0.858), indicando que la posición vertical de inicio de sección es informativa. En pliegos estandarizados, cada tipo de sección tiende a comenzar en posiciones predecibles. Desviaciones pueden reflejar maquetación irregular o inserción de contenido no estándar. Complementario a `line_end` para caracterizar completamente la posición vertical de la sección.

#### 9. `line_end` — Línea final

**Score: 0.220**

Skewness=1.4, 1.24% outliers. La línea donde termina la sección. Aunque el 39% de los valores son -1 (manejados por `line_end_missing`), cuando existe aporta información sobre la extensión vertical de la sección. IF le asigna 0.692 de importancia. En conjunto con `line_start` permite calcular `line_span`, pero retener ambas por separado da al Isolation Forest más flexibilidad para descubrir interacciones no lineales.

#### 10. `bytes_per_token` — Bytes por token

**Score: 0.214**

Derivada = size_bytes / estimated_tokens. Skewness=4.2, 8.8% outliers. Mide la eficiencia de codificación del contenido. Un ratio alto sugiere presencia de caracteres multibyte, imágenes o datos embebidos. Un ratio bajo sugiere texto limpio y homogéneo. IF le asigna 0.478 de importancia. Es la métrica más útil para detectar secciones que contienen contenido no textual (tablas, imágenes escaneadas) que podrían pasar desapercibidas para `content_length` o `size_bytes` por separado.

---

## Recomendaciones

### Para el modelo Isolation Forest definitivo:

1. **Conjunto final recomendado de 10 features:**
   - `content_length` (escalado con log-transform)
   - `page` (escalado con RobustScaler)
   - `line_end_missing` (binario, sin escalar)
   - `size_bytes` (log-transform)
   - `title_encoded` (Label Encoding, o considerar target encoding)
   - `content_density` (RobustScaler)
   - `lines_per_page` (RobustScaler)
   - `line_start` (min-max scaling)
   - `line_end` (min-max scaling, con imputación de -1)
   - `bytes_per_token` (RobustScaler)

2. **Manejo de nulos:**
   - `line_end` con -1 → imputar con NaN y agregar `line_end_missing` como flag. Para IF, imputar `line_end` con la mediana del grupo (por título).

3. **Transformaciones recomendadas:**
   - Aplicar **log(1+x)** a `content_length` y `size_bytes` (skewness > 80). Esto reduce el dominio extremo y permite a IF aislar mejor en el espacio transformado.
   - **RobustScaler** para las features derivadas (`content_density`, `lines_per_page`, `bytes_per_token`) que tienen outliers pero skewness moderado (3-5).

4. **Contamination estimada:**
   - Basado en el % de outliers Tukey (2-10% según feature) y el contexto de pliegos públicos (donde se estima 1-3% de irregularidades), se recomienda `contamination=0.03` para el IF final.

5. **Features adicionales a explorar:**
   - Incorporar features de `dncp.licitaciones` mediante JOIN por `nro_licitacion`: monto_estimado, cantidad_oferentes, duración_contrato_dias, etc. Las anomalías estructurales en secciones se potencian al cruzarlas con variables económicas.
   - Análisis de texto del `content_text` usando TF-IDF sobre los títulos de sección para detectar cláusulas inusuales.

6. **Monitoreo temporal:**
   - `year` no entró en el Top 10 por sí sola (score=0.214), pero debe incluirse como variable de estratificación o como feature si el modelo se entrena con datos históricos, para evitar que cambios legítimos en la estructura de pliegos a lo largo del tiempo sean marcados como anomalías.

7. **Validación con expertos de dominio:**
   - Las secciones "Fraude y Corrupción" y "Subcontratación" aparecen entre los títulos más frecuentes. Un análisis cualitativo con especialistas en contratación pública ayudaría a calibrar pesos específicos para estas categorías.

---

## Código completo de reproducibilidad

Todo el código Python utilizado está disponible en:
- `analysis_step2.py` — Script principal con las 7 estrategias
- `step1_metadata_full.py` — Exploración inicial y muestreo
- `step2_describe.py` — Estadísticos descriptivos

Los datos muestrales y resultados intermedios se guardaron en `C:\Users\Giovanni\AppData\Local\Temp\opencode\`.

---

*Informe generado el 2026-05-19. Análisis sobre muestra de 50,000 registros de `dncp.pliegos_secciones` (total: 2,779,386).*

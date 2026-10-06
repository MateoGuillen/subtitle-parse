# Informe de Selección de Features para Isolation Forest — Document-level (pivote por `title_normalized`)

## Resumen Ejecutivo

Análisis de 254 características a nivel documento, generadas pivotando `title_normalized` (323 valores únicos) de `dncp.pliegos_secciones` sobre 5,000 documentos de licitación. **La característica más valiosa es `has_interpretacion`** (score 0.585) — un indicador binario de si el documento contiene la sección "Interpretación", que resultó ser la variable con mayor poder de separación en Random Forest (16.7% de importancia) y con alta relevancia de dominio, ya que documentos donde esta sección está ausente o presente de forma inusual son candidatos naturales a anomalía documental.

---

## Metodología

Se transformó la tabla seccional (`pliegos_secciones`, ~2.7M filas) en una matriz documento × features mediante pivote de `title_normalized`:

1. **Agregados por documento**: total_sections, unique_titles, avg/max/std/sum de content_length, estimated_tokens, size_bytes
2. **Indicadores binarios** (`has_*`): presencia de cada título específico (80 títulos más frecuentes)
3. **Suma de content_length** por título (`len_*`): qué tan extensa es cada sección en el documento
4. **Suma de estimated_tokens** por título (`tok_*`): volumen textual de cada sección

Se aplicaron las 7 estrategias de selección sobre las 234 features no constantes:

| Estrategia | Peso en scoring |
|-----------|:---:|
| Outlier univariado (Tukey) | 0.25 |
| Relevancia de dominio | 0.30 |
| Importancia Random Forest (proxy con ruido sintético 5%) | 0.25 |
| Importancia Isolation Forest (permutation, n=3,000) | 0.10 |
| Penalización VIF (>10 → −0.1) | −0.10 |

---

## Resultados Clave Intermedios

### 1. Feature categories engineered

| Categoría | Cantidad | Ejemplo |
|-----------|:--------:|---------|
| has_* (presencia binaria de título) | 60 | `has_interpretacion`, `has_fraude_y_corrupcion` |
| len_* (suma content_length por título) | 80 | `len_inspecciones_y_pruebas`, `len_transporte` |
| tok_* (suma estimated_tokens por título) | 80 | `tok_suministros_y_especificaciones_tecnicas` |
| Agregados documento | 14 | `total_sections`, `std_content_length` |
| **Total features** | **234** | |

### 2. Top features por outlier univariado (Tukey)

| Feature | % Outliers | Skewness | Interpretación |
|---------|:---------:|:--------:|----------------|
| `tok_inspecciones_y_pruebas` | **39.7%** | 2.9 | Token count de "Inspecciones y Pruebas" |
| `len_inspecciones_y_pruebas` | **36.6%** | 4.3 | Longitud de sección "Inspecciones y Pruebas" |
| `tok_retiro_sustitucion_y_modificacion...` | **34.1%** | −0.2 | Tokens de "Retiro, sustitución y modificación de ofertas" |
| `tok_disconformidad_errores_y_omisiones` | **32.0%** | −0.6 | Tokens de "Disconformidad, errores y omisiones" |
| `len_periodo_de_validez_garantia_mantenimiento` | **30.9%** | 0.1 | Longitud de "Periodo de validez de garantía de mantenimiento" |

### 3. Top por Importancia Random Forest

| Feature | RF Importance | Tipo |
|---------|:------------:|------|
| `has_interpretacion` | **0.167** | Binaria |
| `has_retiro_sustitucion_y_modificacion_de_las_ofer` | **0.132** | Binaria |
| `has_formas_y_condiciones_de_pago` | **0.129** | Binaria |
| `has_garantias:_instrumentacion_plazos_y_ejecucion` | **0.119** | Binaria |
| `has_oferentes_en_consorcio` | **0.104** | Binaria |

→ Las **variables binarias de presencia/ausencia** dominan completamente la importancia RF, lo que indica que la *estructura de secciones* (qué títulos están presentes) es más informativa que la *extensión* de las mismas.

### 4. Multicolinealidad

Se detectaron **450 pares** con correlación |r| > 0.85, principalmente entre:
- **has_* (binarias) con correlación perfecta (r=1.0)**: títulos que siempre aparecen juntos (ej. `has_confidencialidad` ↔ `has_fuerza_mayor`, `has_transporte` ↔ `has_incoterms`). Esto es esperable: ciertos conjuntos de secciones son obligatorios en tipos específicos de contratación.
- **Agregados documento** con VIF extremo (std_size_bytes VIF=8,892; sum_tokens VIF=1,599) — son combinaciones lineales entre sí.

✅ Se recomienda seleccionar **un representante por clúster de correlación** para el modelo final.

### 5. Importancia Isolation Forest (permutation)

Las features con mayor impacto en la puntuación de anomalía del IF fueron:
- `has_identificacion_de_la_unidad_solicitante_y_jus`
- `has_contratacion_publica_sostenibles___cps`
- `has_requisitos_documentales_para_la_evaluacion_de`
- `has_formas_y_condiciones_de_pago`
- `len_moneda_de_la_oferta_y_pago`

→ El IF confirma que las **variables binarias de presencia** y algunas **variables de longitud** son las más influyentes.

---

## Ranking Final de Features (Top 20)

| Rank | Feature | Score | Outlier | Dominio | RF | IF | Explicación |
|:----:|---------|:-----:|:-------:|:-------:|:--:|:--:|-------------|
| **1** | **has_interpretacion** | **0.585** | 0.00 | 0.8 | **1.000** | 0.953 | Presencia de la sección "Interpretación". La más discriminante en RF. |
| **2** | **len_periodo_de_validez_garantia_mantenimiento** | **0.555** | **1.00** | 0.7 | 0.00 | 0.945 | Extensión de la sección de garantía de oferta. 30.9% outliers. |
| **3** | **len_inspecciones_y_pruebas** | **0.553** | **1.00** | 0.7 | 0.00 | 0.929 | 36.6% outliers. Secciones anómalamente largas/cortas. |
| **4** | **len_idioma_de_la_oferta** | **0.540** | 0.96 | 0.7 | 0.00 | 0.902 | Longitud de "Idioma de la oferta" — 28.7% outliers. |
| **5** | **has_retiro_sustitucion_modificacion_ofertas** | **0.536** | 0.00 | 0.8 | **0.790** | 0.984 | Presencia de sección de modificación de ofertas. |
| **6** | **has_formas_y_condiciones_de_pago** | **0.532** | 0.00 | 0.8 | 0.769 | **0.994** | Presencia de formas de pago. Alta en RF e IF. |
| **7** | **tok_inspecciones_y_pruebas** | **0.530** | **1.00** | 0.6 | 0.01 | 0.970 | Token count de inspecciones. 39.7% outliers. |
| **8** | **len_transporte** | **0.520** | 0.92 | 0.7 | 0.00 | 0.793 | Longitud de "Transporte". 27.6% outliers. |
| **9** | **tok_disconformidad_errores_omisiones** | **0.519** | **1.00** | 0.6 | 0.00 | 0.888 | 32% outliers. Sección de disconformidades. |
| **10** | **has_garantias_instrumentacion_plazos** | **0.517** | 0.00 | 0.8 | 0.712 | 0.987 | Presencia de garantías. Tercera más importante en RF. |
| 11 | len_suministros_especificaciones_tecnicas | 0.516 | 0.83 | 0.7 | 0.02 | 0.934 | 24.9% outliers |
| 12 | tok_retiro_sustitucion_modificacion_ofertas | 0.512 | **1.00** | 0.6 | 0.01 | 0.792 | 34.1% outliers |
| 13 | tok_transporte | 0.510 | **1.00** | 0.6 | 0.00 | 0.804 | 30.1% outliers |
| 14 | tok_convenios_modificatorios | 0.500 | 0.92 | 0.6 | 0.00 | 0.897 | 27.7% outliers |
| 15 | tok_idioma_de_la_oferta | 0.498 | 0.93 | 0.6 | 0.00 | 0.870 | 27.8% outliers |
| 16 | has_oferentes_en_consorcio | 0.494 | 0.00 | 0.8 | 0.624 | 0.980 | Presencia de oferentes en consorcio |
| 17 | tok_suministros_especificaciones_tecnicas | 0.485 | 0.83 | 0.6 | 0.04 | 0.866 | 25% outliers |
| 18 | len_solicitud_pago_anticipo | 0.483 | 0.70 | 0.7 | 0.00 | 0.983 | Anticipo MIPYMES |
| 19 | tok_aclaracion_de_las_ofertas | 0.478 | 0.81 | 0.6 | 0.00 | 0.959 | 24.2% outliers |
| 20 | tok_solicitud_pago_anticipo | 0.477 | 0.77 | 0.6 | 0.02 | 0.984 | Tokens de anticipo |

---

## Recomendaciones

### Para el modelo Isolation Forest definitivo:

**Conjunto final recomendado (Top 10 — balance entre correlación y diversidad):**

| # | Feature | Razón para incluir |
|:-:|---------|-------------------|
| 1 | `has_interpretacion` | Máxima importancia RF; captura variación estructural |
| 2 | `len_periodo_de_validez_garantia_mantenimiento` | 30.9% outliers univariados; extremos en garantías = riesgo |
| 3 | `has_retiro_sustitucion_modificacion_ofertas` | Alta en RF e IF; modificación de ofertas es señal de fraude |
| 4 | `has_formas_y_condiciones_de_pago` | Alta en IF; condiciones de pago inusuales |
| 5 | `len_inspecciones_y_pruebas` | 36.6% outliers; inspecciones anómalas |
| 6 | `has_garantias_instrumentacion_plazos` | Alta en RF; garantías indican tipo de contratación |
| 7 | `has_oferentes_en_consorcio` | Consorcios pueden ocultar colusión |
| 8 | `len_transporte` | 27.6% outliers; logística inusual |
| 9 | `tok_disconformidad_errores_omisiones` | 32% outliers; sección de quejas/reclamos |
| 10 | `total_sections` o `unique_titles` | Métrica global de complejidad documental |

**Top 10 reducido + agregados**: Para evitar la multicolinealidad, se sugiere este subconjunto de 12-15 features que cubren:

- **Estructurales** (has_*): 1, 3, 4, 6, 7
- **De extensión** (len_*): 2, 5, 8
- **De contenido** (tok_*): 9
- **Globales**: 10

### Transformaciones

- **len_*** y **tok_*** tienen distribuciones muy asimétricas → aplicar **log(1+x)**
- **has_*** son binarias → no requieren escalado
- Usar **RobustScaler** para features continuas (menos sensible a outliers que StandardScaler)

### Manejo de nulos

- Las features derivadas no tienen nulos (fillna=0 en pivot)
- Para producción, verificar que los documentos nuevos tengan todas las secciones esperadas

### Contamination

- Basado en % de outliers Tukey (10-40% en top features), pero en contexto real de licitaciones se estima 1-3% de irregularidades. Recomendación: `contamination=0.03`

### Próximos pasos

1. **Integrar con `dncp.licitaciones`**: cruzar por `nro_licitacion` para añadir features económicas (monto estimado, cantidad de oferentes, tipo de procedimiento)
2. **Validar con casos conocidos**: contrastar las anomalías detectadas con bases de datos de contratos adjudicados o sanciones
3. **Análisis de texto**: extraer features de `content_text` usando TF-IDF para detectar cláusulas inusuales en secciones críticas como "Fraude y Corrupción" o "Subcontratación"

---

*Informe generado el 2026-05-19. 5,000 documentos muestreados de 31,321 totales en `dncp.pliegos_secciones`. 234 features analizadas tras descartar 20 constantes.*

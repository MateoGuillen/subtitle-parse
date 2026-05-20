# README — Análisis de Features para Detección de Anomalías en `dncp.pliegos_secciones`

## Objetivo

Identificar las mejores características (features) de la tabla `dncp.pliegos_secciones` para entrenar un modelo **Isolation Forest** que detecte documentos de licitación con estructuras anómalas o irregulares.

---

## 1. Contexto: Esquema `dncp`, Tabla `pliegos_secciones`

### 1.1. ¿Qué contiene esta tabla?

Cada fila representa una **sección** de un **pliego de bases y condiciones** (documento de licitación pública de Paraguay). Un documento (`nro_licitacion`) tiene múltiples secciones (~80 en promedio). Cada sección tiene un título (ej. "Subcontratación", "Fraude y Corrupción") y contenido textual.

### 1.2. DDL completo

```sql
CREATE TABLE dncp.pliegos_secciones (
    document_id              text          NOT NULL,   -- ID único de la sección
    nro_licitacion           text          NOT NULL,   -- ID del documento de licitación (FK lógica → dncp.licitaciones)
    category_id              text          NULL,       -- Categoría de contratación (82 valores distintos)
    year                     smallint      NOT NULL,   -- Año de la licitación (2021-2025)
    title                    text          NULL,       -- Título original de la sección (284 valores)
    title_normalized         text          NULL,       -- Título normalizado (323 valores, lowercase sin acentos)
    page                     smallint      NULL,       -- Número de página donde comienza la sección
    line_start               integer       NULL,       -- Línea de inicio dentro de la página
    line_end                 integer       NULL,       -- Línea final (valor -1 = sección truncada/sin final)
    depth                    smallint      NULL,       -- Profundidad en la jerarquía del documento (SIEMPRE = 2)
    content_length           integer       NULL,       -- Cantidad de líneas del contenido de la sección
    estimated_tokens         integer       NULL,       -- Cantidad estimada de tokens (palabras)
    word_count               integer       NULL,       -- Conteo de palabras (SIEMPRE = 0, columna no implementada)
    size_bytes               integer       NULL,       -- Tamaño en bytes del contenido textual
    content_text             text          NULL,       -- Texto completo de la sección (~1150 chars promedio)
    content_clean            ARRAY         NULL,       -- Mismo contenido que content_text pero como array de líneas (tipo _text)
    content_length_clean     integer       NULL,       -- Largo del array content_clean (IDÉNTICO a content_length)
);

-- Índices existentes:
CREATE INDEX idx_ps_document_id ON dncp.pliegos_secciones USING btree (document_id);
CREATE INDEX idx_ps_nro_licitacion ON dncp.pliegos_secciones USING btree (nro_licitacion);
CREATE INDEX idx_ps_category_id ON dncp.pliegos_secciones USING btree (category_id);
CREATE INDEX idx_ps_title_normalized ON dncp.pliegos_secciones USING btree (title_normalized);
CREATE INDEX idx_ps_estimated_tokens ON dncp.pliegos_secciones USING btree (estimated_tokens);
CREATE INDEX idx_ps_title_norm_licit ON dncp.pliegos_secciones USING btree (title_normalized, nro_licitacion);
CREATE INDEX idx_ps_page_brin ON dncp.pliegos_secciones USING brin (page);
CREATE INDEX idx_ps_title_normalized_trgm ON dncp.pliegos_secciones USING gin (title_normalized gin_trgm_ops);
CREATE UNIQUE INDEX idx_pliegos_secciones_unique ON dncp.pliegos_secciones USING btree (nro_licitacion, title, line_start, year);
```

### 1.3. Estadísticas generales

| Métrica | Valor |
|---------|-------|
| Filas totales | **2,779,386** |
| Documentos distintos (`nro_licitacion`) | **31,321** |
| Secciones promedio por documento | **~89** (mín 64, máx 220) |
| Títulos normalizados distintos | **323** |
| Categorías distintas | **82** |
| Años cubiertos | **2021–2025** |

### 1.4. Descripción detallada de cada columna

| Columna | Tipo | Nulos | Cardinalidad | Rango/Valores | Significado |
|---------|------|:----:|:------------:|---------------|-------------|
| `document_id` | text | 0 | 32,182 | IDs únicos | Identificador único de cada sección (formato: `año_categoría_nro`) |
| `nro_licitacion` | text | 0 | 31,321 | números | ID del documento de licitación al que pertenece la sección. Se relaciona con `dncp.licitaciones.nro_licitacion` |
| `category_id` | text | 0 | 82 | "2","7","18","72000000"... | Código de categoría de la contratación (ej. 72000000 = servicios de TI) |
| `year` | smallint | 0 | 5 | 2021–2025 | Año de publicación de la licitación |
| `title` | text | 0 | ~284 | "Subcontratación", "Fraude..." | Título original de la sección (con acentos y mayúsculas) |
| `title_normalized` | text | 0 | **323** | "subcontratacion", "fraude y corrupcion"... | Título normalizado (lowercase, sin acentos, sin caracteres especiales) |
| `page` | smallint | 0 | 638 | 1–1,302 | Número de página donde comienza la sección |
| `line_start` | integer | 0 | 207 | 1–365 | Línea de inicio dentro de la página |
| `line_end` | integer | 0 | 172 | −1 a 249 | Línea final. **-1 = sección truncada o sin final definido (~39% de los casos)** |
| `depth` | smallint | 0 | **1** | siempre 2 | **CONSTANTE** — no aporta información |
| `content_length` | integer | 0 | 3,080 | 1–61,209 (avg=17) | Cantidad de líneas de la sección **MUY ASIMÉTRICA** (skewness=80) |
| `estimated_tokens` | integer | 0 | 11,049 | 1–785,292 (avg=284) | Tokens estimados del contenido |
| `word_count` | integer | 0 | **1** | siempre 0 | **CONSTANTE** — no implementada |
| `size_bytes` | integer | 0 | 22,570 | 55–6,341,746 (avg=1,509) | Tamaño en bytes del texto |
| `content_text` | text | 0 | — | ~1,150 chars promedio | Contenido textual completo de la sección |
| `content_clean` | `text[]` | 0 | — | array de líneas | Mismo contenido que `content_text` pero como array PostgreSQL (cada elemento = una línea) |
| `content_length_clean` | integer | 0 | 3,080 | **IDÉNTICO a content_length** | Largo del array (redundante con content_length) |

### 1.5. Relaciones con otras tablas (sin FK formal)

| Tabla relacionada | Columna join | Cardinalidad |
|-------------------|-------------|:------------:|
| `dncp.licitaciones` | `nro_licitacion` | 1 doc → N secciones |
| `dncp.adjudicaciones` | `nro_licitacion` | 1 doc → N adjudicaciones |
| `dncp.oferentes` | `nro_licitacion` | 1 doc → N oferentes |
| `dncp.proveedores_notificados` | `nro_licitacion` | 1 doc → N proveedores |
| `dncp.contratos` | `nro_licitacion` | 1 doc → N contratos |

### 1.6. Columnas descartables (sin valor para detección de anomalías)

| Columna | Motivo |
|---------|--------|
| `document_id` | ID único por fila (32,182 valores distintos en 27K filas) |
| `depth` | Constante (siempre 2) |
| `word_count` | Constante (siempre 0) |
| `content_length_clean` | **Idéntica** a `content_length` (r=1.0) |

---

## 2. Feature Engineering ya realizado

Se transformaron los datos de formato **largo** (1 fila = 1 sección) a formato **ancho** (1 fila = 1 documento) pivotando `title_normalized`:

### 2.1. Tipos de features creados

#### a) `has_*` — Indicadores binarios de presencia de sección

Para cada título de sección (top 80 más frecuentes): **1** si el documento contiene esa sección, **0** si no.

```
has_interpretacion       → 1 si el documento tiene sección "Interpretación"
has_fraude_y_corrupcion  → 1 si tiene sección "Fraude y Corrupción"
has_subcontratacion      → 1 si tiene sección "Subcontratación"
... (60 features)
```

#### b) `len_*` — Suma de content_length por título

Para cada título: suma total de líneas de todas las secciones con ese título en el documento.

```
len_transporte           → líneas totales de secciones "Transporte"
len_inspecciones_y_pruebas → líneas totales de "Inspecciones y pruebas"
... (80 features)
```

#### c) `tok_*` — Suma de estimated_tokens por título

Similar a `len_*` pero con la columna `estimated_tokens`.

```
tok_suministros_y_especificaciones_tecnicas → tokens de "Suministros y especificaciones técnicas"
tok_solicitud_de_pago_de_anticipo → tokens de "Solicitud de pago de anticipo"
... (80 features)
```

#### d) Agregados documento (14 features)

| Feature | Cálculo | Qué mide |
|---------|---------|----------|
| `total_sections` | COUNT(*) por doc | Complejidad del documento |
| `unique_titles` | COUNT(DISTINCT title) | Variedad de secciones |
| `avg_content_length` | AVG(content_length) | Tamaño promedio de sección |
| `std_content_length` | STDDEV(content_length) | Heterogeneidad de tamaños |
| `max_content_length` | MAX(content_length) | Sección más larga |
| `sum_content_length` | SUM(content_length) | Volumen total de contenido |
| `avg_tokens` | AVG(estimated_tokens) | Promedio de palabras |
| `std_tokens` | STDDEV(estimated_tokens) | Variabilidad de palabras |
| `max_tokens` | MAX(estimated_tokens) | Máximo de palabras en una sección |
| `sum_tokens` | SUM(estimated_tokens) | Total de palabras del documento |
| `avg_size_bytes` | AVG(size_bytes) | Tamaño promedio en bytes |
| `std_size_bytes` | STDDEV(size_bytes) | Variabilidad de tamaño |
| `max_size_bytes` | MAX(size_bytes) | Sección más pesada |
| `sum_size_bytes` | SUM(size_bytes) | Peso total del documento |

### 2.2. Total de features generadas

| Categoría | Cantidad |
|-----------|:--------:|
| `has_*` | 60 (no constantes) |
| `len_*` | 80 |
| `tok_*` | 80 |
| Agregados | 14 |
| **Total features** | **234** |
| Constantes descartadas | 20 |
| Documentos muestreados | 5,000 |

---

## 3. Estrategias de selección de features ya aplicadas

### 3.1. Potencial univariado de outlier (peso 0.25)

Para cada feature numérica se calculó:
- **Asimetría (skewness)**: distribución sesgada → cola larga → outliers naturales
- **Curtosis**: picos agudos → valores extremos
- **% outliers Tukey**: porcentaje de valores fuera de [Q1−1.5×IQR, Q3+1.5×IQR]

**Hallazgo**: Features `len_*` y `tok_*` tienen 20–40% de outliers. Ej: `tok_inspecciones_y_pruebas` tiene **39.7%** de valores extremos.

### 3.2. Reducción de multicolinealidad

- Matriz de correlación de Pearson
- VIF (Variance Inflation Factor)
- Se identificaron **450 pares** con |r| > 0.85
- Muchas `has_*` tienen correlación perfecta (r=1.0): ciertos grupos de secciones siempre aparecen juntos porque son obligatorios en tipos específicos de contratación
- Features agregadas (`std_content_length`, `sum_tokens`, etc.) tienen VIF extremo (>1,000) porque son combinaciones lineales entre sí

### 3.3. Calidad de datos

- 0% de nulos en todas las features (el pivot con fillna(0) eliminó nulos)
- 20 features constantes descartadas
- Columnas ID descartadas (document_id, nro_licitacion como feature)

### 3.4. Relevancia de dominio

Puntuación manual (0–1) basada en criterio de negocio:

| Tipo | Score | Justificación |
|------|:----:|---------------|
| `has_*` | 0.8 | Presencia/ausencia de secciones críticas es altamente informativa |
| `len_*` | 0.7 | Tamaño anómalo de secciones específicas |
| `tok_*` | 0.6 | Volumen textual anómalo |
| Agregados std | 0.6 | Variabilidad estructural del documento |
| Agregados avg | 0.5 | Promedios |
| Agregados max/sum | 0.4 | Totales |

### 3.5. Importancia Random Forest (proxy supervisado)

**Procedimiento**:
1. Se tomaron los 5,000 documentos originales (etiqueta = 1)
2. Se añadió **5% de ruido sintético** (etiqueta = 0): se perturban las features con ruido gaussiano (σ × 0.5)
3. Se entrenó un Random Forest Regressor (100 árboles, profundidad 10)
4. Se extrajo la importancia de Gini

**Hallazgo**: Las **`has_*`** dominan completamente. `has_interpretacion` tiene 16.7% de importancia, seguida de `has_retiro_sustitucion_modificacion_ofertas` (13.2%) y `has_formas_y_condiciones_de_pago` (12.9%).

### 3.6. Importancia Isolation Forest (permutation)

**Procedimiento**:
1. Isolation Forest entrenado (contamination=0.05, n_estimators=200) sobre 3,000 documentos
2. Permutation importance: se permuta cada feature y se mide el impacto en el anomaly score
3. 3 repeticiones por feature

**Hallazgo**: Las `has_*` también dominan, con `has_identificacion_de_la_unidad_solicitante_y_jus`, `has_contratacion_publica_sostenibles___cps` y `has_formas_y_condiciones_de_pago` como las más influyentes.

### 3.7. Anomalías contextuales

Se analizaron distribuciones de features numéricas dentro de subgrupos (por título y categoría) para detectar outliers contextuales (normales a nivel global pero extremos dentro de su grupo).

### 3.8. Fórmula de scoring final

```
final_score = 0.25 × outlier_score + 0.30 × domain_score + 0.25 × rf_importance 
              + 0.10 × if_importance - 0.10 × vif_penalty
```

---

## 4. Resultados: Top 20 Features

| Rank | Feature | Score | Tipo | Explicación |
|:----:|---------|:-----:|:----:|-------------|
| 1 | `has_interpretacion` | 0.585 | binaria | Presencia de sección "Interpretación". Máxima importancia RF. |
| 2 | `len_periodo_de_validez_garantia_mantenimiento` | 0.555 | numérica | 30.9% outliers. Longitud de garantía de oferta. |
| 3 | `len_inspecciones_y_pruebas` | 0.553 | numérica | 36.6% outliers. |
| 4 | `len_idioma_de_la_oferta` | 0.540 | numérica | 28.7% outliers. |
| 5 | `has_retiro_sustitucion_modificacion_ofertas` | 0.536 | binaria | 79% importancia RF. |
| 6 | `has_formas_y_condiciones_de_pago` | 0.532 | binaria | Alta en RF e IF. |
| 7 | `tok_inspecciones_y_pruebas` | 0.530 | numérica | 39.7% outliers. |
| 8 | `len_transporte` | 0.520 | numérica | 27.6% outliers. |
| 9 | `tok_disconformidad_errores_omisiones` | 0.519 | numérica | 32% outliers. |
| 10 | `has_garantias_instrumentacion_plazos` | 0.517 | binaria | 71% importancia RF. |
| 11 | `len_suministros_especificaciones_tecnicas` | 0.516 | numérica | 24.9% outliers. |
| 12 | `tok_retiro_sustitucion_modificacion_ofertas` | 0.512 | numérica | 34.1% outliers. |
| 13 | `tok_transporte` | 0.510 | numérica | 30.1% outliers. |
| 14 | `tok_convenios_modificatorios` | 0.500 | numérica | 27.7% outliers. |
| 15 | `tok_idioma_de_la_oferta` | 0.498 | numérica | 27.8% outliers. |
| 16 | `has_oferentes_en_consorcio` | 0.494 | binaria | 62% importancia RF. |
| 17 | `tok_suministros_especificaciones_tecnicas` | 0.485 | numérica | 25% outliers. |
| 18 | `len_solicitud_pago_anticipo` | 0.483 | numérica | Anticipo MIPYMES. |
| 19 | `tok_aclaracion_de_las_ofertas` | 0.478 | numérica | 24.2% outliers. |
| 20 | `tok_solicitud_pago_anticipo` | 0.477 | numérica | Tokens de anticipo. |

---

## 5. Estrategias solicitadas para complementar (para que otro LLM proponga más)

A continuación, una lista de **posibles estrategias adicionales** que otro LLM puede desarrollar o proponer variantes.

### 5.1. Estrategias básicas (tradicionales)

1. **Filtrado por varianza**: descartar features con varianza cercana a cero (ya hecho parcialmente)
2. **SelectKBest (ANOVA F-test)**: test estadístico para ranking univariado
3. **Mutual Information**: medir dependencia no lineal entre features y variable objetivo
4. **Chi-cuadrado**: para features categóricas vs. variable objetivo binaria
5. **Lasso (L1 regularization)**: regresión con penalización L1 para selección automática
6. **RFE (Recursive Feature Elimination)**: eliminar recursivamente las features menos importantes
7. **ExtraTreesClassifier**: feature importance de árboles extra-aleatorios

### 5.2. Estrategias avanzadas (model-based)

8. **SHAP values**: valores Shapley para interpretación de modelo (mejor que permutation importance)
9. **Feature agglomeration**: clustering de features correlacionadas en grupos
10. **PCA + interpretación de componentes**: análisis de componentes principales + mapa de carga
11. **Autoencoder reconstruction error**: usar error de reconstrucción de autoencoder como métrica de importancia
12. **Gradient Boosting (XGBoost/LightGBM)**: importancias con modelos más robustos que RF
13. **Permutation importance con múltiples modelos**: promediar importancia a través de distintos algoritmos

### 5.3. Estrategias específicas para detección de anomalías

14. **One-Class SVM weights**: usar coeficientes del modelo como proxy de importancia
15. **Local Outlier Factor (LOF) feature contribution**: medir qué features contribuyen más al score LOF
16. **Feature bagging en Isolation Forest**: entrenar múltiples IF con subconjuntos de features y ver frecuencia de selección
17. **Extended Isolation Forest**: variante que considera features categóricas
18. **Isolation Forest con importancia por profundidad**: features que aparecen más temprano en los árboles son más importantes

### 5.4. Estrategias basadas en texto (NLP)

19. **TF-IDF sobre content_text**: extraer términos inusuales por documento
20. **Topic Modeling (LDA)**: detectar temas latentes en las secciones
21. **Sentence embeddings (SBERT)**: similitud semántica entre secciones del mismo tipo
22. **Readability scores**: medir complejidad del texto (Flesch, etc.)
23. **NER (Named Entity Recognition)**: extraer entidades (montos, fechas, personas) inusuales
24. **Longitud de párrafos**: distribución de largos de párrafos dentro de cada sección

### 5.5. Estrategias temporales

25. **Temporal features**: día de la semana, mes de publicación, días desde fin de año fiscal
26. **Cambio de estructura año a año**: detectar documentos que no siguen la estructura típica de su año
27. **Ventanas deslizantes**: features de tendencia (promedio móvil de ciertas métricas)

### 5.6. Estrategias de integración con otras tablas

28. **Join con `dncp.licitaciones`**: incorporar monto_estimado, presupuesto_monto, cantidad_oferentes, tipo_procedimiento
29. **Join con `dncp.adjudicaciones`**: número de oferentes, montos adjudicados, diferencias con presupuesto
30. **Join con `dncp.oferentes`**: cantidad de oferentes por licitación, concentración
31. **Join con `dncp.contratos`**: duración del contrato, modificaciones
32. **Join con `dncp.proveedores`**: historial de proveedores, cantidad de contratos previos

### 5.7. Estrategias de ingeniería de features avanzada

33. **Ratio features**: `len_x / total_sections`, `tok_x / sum_tokens` (proporciones normalizadas)
34. **Entropía de títulos**: diversidad de secciones como medida de complejidad
35. **Rareza de título**: TF-IDF aplicado a títulos de sección (qué tan inusual es tener cierto título)
36. **Co-ocurrencia de pares de títulos**: reglas de asociación — ciertos pares de títulos que no deberían co-ocurrir
37. **Missing section combinations**: combinaciones esperadas de secciones que faltan
38. **Page density**: secciones por página (qué tan compacto es el documento)
39. **Anomaly score por título**: entrenar IF pequeño por cada título y usar su score como feature

### 5.8. Estrategias de validación

40. **Cross-validation de selección**: evaluar estabilidad de la selección en distintos folds
41. **Correlación con outcomes conocidos**: si existen datos de contratos cuestionados, validar contra ellos
42. **Análisis de sensibilidad**: perturbar features y ver cuánto cambia el ranking final

---

## 6. Recomendaciones iniciales para el modelo final

1. **Top 10 features recomendadas**: combinar `has_*` (estructurales) + `len_*`/`tok_*` (de extensión) + `total_sections`
2. **Transformaciones**: log(1+x) para `len_*` y `tok_*` (skewness extremo); RobustScaler para el resto
3. **Contamination**: 0.03 (estimación conservadora basada en % de irregularidades esperadas)
4. **Eliminar redundantes**: de cada grupo de `has_*` correlacionadas (r=1.0), dejar solo una

---

## 7. Conexión a base de datos

```python
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://postgres:Temporal123@172.31.233.136:5433/dncp')
```

## 8. Scripts generados

| Script | Propósito |
|--------|-----------|
| `step4_engineer_title_features_v2.py` | Pivot de `title_normalized` a features documento × título |
| `step6_feature_selection_full.py` | 7 estrategias de selección + ranking final |
| `informe_features_v2.md` | Informe completo del análisis |

---

*Documento generado el 2026-05-19 como handoff para continuar análisis de features para detección de anomalías con Isolation Forest.*

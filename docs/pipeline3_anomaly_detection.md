# Pipeline 3: Detección de Anomalías No Supervisada

## Contexto del Dataset

### Datos disponibles en `dncp`

| Tabla | Registros | Descripción |
|:------|:---------:|:------------|
| `licitaciones` | 29,811 | Procesos de compra |
| `adjudicaciones` | 39,463 | Decisiones de adjudicación |
| `contratos` | 39,691 | Contratos firmados |
| `oferentes` | 78,695 | Participantes |
| `items_licitacion` | 1.8M | Items detallados |
| `pliegos_secciones` | 2.7M | Textos de pliegos |
| **Con ambos datos** | **29,784** | OCDS + Pliegos |

### Tablas pre-computadas

| Tabla | Filas | Columnas | Pipeline fuente |
|:------|:-----:|:--------:|:---------------|
| `document_features` | 31,321 | 571 | `run_document_features_pipeline.py` |
| `document_economic_features` | 29,811 | 46 | `run_document_economic_features_pipeline.py` |
| `title_ranking` (output CSV) | 80 | 8 | `TitleRankingPipeline` |

---

## Title Ranking Pipeline

Pipeline 3 se alimenta del **ranking de títulos de pliegos** generado por `TitleRankingPipeline`. Este ranking determina qué títulos de pliego son más relevantes para detectar anomalías en procesos de contratación.

### Metodología: 6 Estrategias

| # | Estrategia | Peso | Descripción |
|:-:|:-----------|:----:|:------------|
| 1 | **Outlier Frequency** | 0.20 | Mide qué tan frecuentemente un título presenta valores extremos en su `len_*`, `tok_*`, `word_*` a nivel documento |
| 2 | **Random Forest Importance** | 0.25 | Importancia del título como predictor de anomalía documental (modelo RF con 243 features) |
| 3 | **Contextual Effect** | 0.15 | Variación del título entre categorías de contratación — valores atípicos dentro de su grupo señalan anomalías |
| 4 | **Section Isolation Forest** | 0.15 | Frecuencia con que las secciones de este título son marcadas como anómalas por Isolation Forest |
| 5 | **Document Correlation** | 0.10 | Correlación entre presencia del título y score global de anomalía del documento |
| 6 | **Economic Risk** | 0.15 | Correlación entre presencia del título e indicadores de riesgo económico (oferta única, sobrecosto, colusión, protestas) |

**Pesos finales:** `total = 0.20·S₁ + 0.25·S₂ + 0.15·S₃ + 0.15·S₄ + 0.10·S₅ + 0.15·S₆`

### Features utilizadas por el ranking

El ranking carga solo las columnas necesarias de `document_features` para evitar problemas de memoria:

| Fuente | Columnas cargadas |
|:-------|:------------------|
| `document_features` | 245 (de 571): `has_*`, `len_*`, `tok_*` por título + `nro_licitacion`, `category_id` |
| `document_economic_features` | 46: todas las columnas económicas + colusión |

*Las columnas `word_*`, `count_*`, `span_*`, `page_range_*` y agregados (`gini`, `entropy`, etc.) no se usan en el ranking. Se cargan directamente en Pipeline 3 para la detección de anomalías.*

### Implementación

```python
from src.pipelines.title_ranking_pipeline import TitleRankingPipeline
from config.settings import DB_CONFIG

config = {
    'db_params': DB_CONFIG,
    'top_n': 80,                          # Rankear 80 títulos
    'section_sample': 30000,              # Muestreo de secciones
    'output_dir': 'data/processed/title_ranking',
    'auto_k': False,                      # K fijo (top_k)
    'top_k': 10,                          # Top-K para clustering
}
pipeline = TitleRankingPipeline(config)
pipeline.run()
```

### Resultado: Top-20 Títulos

Ranking generado con las 6 estrategias (ejecución: ~12-17s, 31,321 documentos).

| Rank | Título | Score Total | Outlier | RF | Context | Sec.IF | Corr | Econ. |
|:----:|:-------|:-----------:|:-------:|:--:|:-------:|:------:|:----:|:-----:|
| 1 | Fraude Y Corrupcion | **0.5432** | 0.9261 | 0.5954 | 0.5131 | 0.2437 | 0.8910 | 0.0434 |
| 2 | Confidencialidad De La Informacion | **0.5250** | 0.0181 | 0.4950 | 0.4559 | 1.0000 | 0.9819 | 0.5404 |
| 3 | Fuerza Mayor | **0.5238** | 0.7728 | 0.1211 | 0.8736 | 0.1914 | 0.9819 | 0.5404 |
| 4 | Aclaracion De Las Ofertas | **0.4884** | 0.4892 | 0.8862 | 0.4891 | 0.0000 | 0.8910 | 0.0434 |
| 5 | Interpretacion | **0.4875** | 0.3898 | 0.6084 | 0.5218 | 0.0064 | 0.9858 | 0.5310 |
| 6 | Formato Y Firma De La Oferta | **0.4677** | 0.7476 | 0.5595 | 0.5512 | 0.0000 | 0.8910 | 0.0434 |
| 7 | Impuestos Y Derechos | **0.4539** | 0.7087 | 0.1600 | 0.6443 | 0.0718 | 0.5081 | 0.7594 |
| 8 | Derechos Intelectuales | **0.4467** | 1.0000 | 0.0790 | 0.3584 | 0.0645 | 0.6126 | 0.6814 |
| 9 | Retiro Sustitucion Y Modificacion De Las Ofertas | **0.4425** | 0.4461 | 0.6106 | 0.5016 | 0.0000 | 0.8570 | 0.2643 |
| 10 | Causales De Terminacion Del Contrato | **0.4367** | 0.3326 | 0.3627 | 0.5920 | 0.0831 | 0.9858 | 0.5310 |
| 11 | Embalajes Y Documentos | **0.4267** | 0.8203 | 0.0535 | 0.4047 | 0.1854 | 0.6082 | 0.6661 |
| 12 | Copias De La Oferta  Cps | **0.4252** | 0.2738 | 0.3427 | 0.8535 | 0.0000 | 0.9604 | 0.4049 |
| 13 | Audiencia Informativa | **0.4146** | 0.1311 | 0.7319 | 0.6029 | 0.1291 | 0.8910 | 0.0434 |
| 14 | Planos Y Disenos | **0.4114** | 0.7241 | 0.2234 | 0.2364 | 0.1611 | 0.9320 | 0.3862 |
| 15 | Inspecciones Y Pruebas | **0.4114** | 0.4838 | 0.1865 | 0.3241 | 0.3204 | 0.5057 | 0.8050 |
| 16 | Idioma De La Oferta | **0.4098** | 0.3812 | 0.4614 | 0.5528 | 0.0000 | 0.9006 | 0.3014 |
| 17 | Otras Causales De Terminacion Del Contrato | **0.4060** | 0.4798 | 0.3410 | 0.3007 | 0.0094 | 0.9858 | 0.5310 |
| 18 | Tiempo De Funcionamiento De Los Bienes | **0.4040** | 0.9175 | 0.0701 | 0.2647 | 0.0000 | 0.6139 | 0.6792 |
| 19 | Periodo De Validez De La Garantia De Los Bienes | **0.4031** | 0.9773 | 0.0380 | 0.2326 | 0.0000 | 0.6139 | 0.6792 |
| 20 | Porcentaje De Multas | **0.3982** | 0.5215 | 0.1139 | 0.6302 | 0.0000 | 0.4067 | 0.8681 |

### Decisiones de diseño del ranking

| Decisión | Rationale |
|:---------|:----------|
| **6 estrategias** | Las 5 originales miden anomalía estructural/documental, pero ninguna incorpora señales de corrupción económica. La 6ta estrategia (`economic_risk`) cierra esa brecha correlacionando presencia/ausencia de títulos con oferta única, sobrecostos, colusión y protestas. |
| **Pesos suman 1.0** | `rf=0.25` es el predictor más robusto de anomalía; `outlier=0.20` captura variabilidad estructural; `contextual=0.15` + `section_if=0.15` equilibran anomalías locales; `corr=0.10` es la más débil; `economic=0.15` da peso a corrupción sin dominar. |
| **Carga selectiva de 245 columns** | `SELECT *` desde `document_features` (587 cols) crasheaba el ranking por agotamiento de memoria en Windows. Se consultan `information_schema.columns` y se filtran `has_*/len_*/tok_*/nro_licitacion/category_id`, reduciendo 58% la memoria. |
| **Tablas pre-computadas** | Evita recomputar JOINs complejos de OCDS (9 tablas) y pivot de 2.7M secciones cada vez que se re-ejecuta Pipeline 3. Garantiza reproducibilidad y velocidad. |

### Salida del ranking

El pipeline produce 3 archivos en `data/processed/title_ranking/`:

| Archivo | Contenido |
|:--------|:----------|
| `title_ranking.csv` | 80 títulos × 8 columnas (6 scores + total + rank) |
| `title_ranking_report.md` | Top-20 con justificación por título |
| `pipeline_summary.json` | Metadatos de ejecución (tiempo, top-1, K) |

#### Schema de `title_ranking.csv`

| Columna | Tipo | Descripción |
|:--------|:----:|:------------|
| `title_slug` | string | Slug normalizado del título (e.g. `fraude_y_corrupcion`) |
| `display_name` | string | Nombre legible del título |
| `score_outlier` | float | Estrategia 1: Outlier Frequency (0-1) |
| `score_rf` | float | Estrategia 2: Random Forest Importance (0-1) |
| `score_contextual` | float | Estrategia 3: Contextual Effect (0-1) |
| `score_section_if` | float | Estrategia 4: Section Isolation Forest (0-1) |
| `score_corr` | float | Estrategia 5: Document IF Correlation (0-1) |
| `score_economic` | float | Estrategia 6: Economic Risk Correlation (0-1) |
| `score_total` | float | Score ponderado total (suma pesada de las 6 estrategias) |
| `rank` | int | Posición en el ranking (1 = más relevante) |

### Integración con Pipeline 3

El top-K del ranking alimenta dos pipelines aguas abajo:

1. **Pipeline 1 (Clustering + LLM Extraction):** Los K títulos con mayor score se procesan para clustering de secciones y extracción de schemas vía LLM
2. **Pipeline 3 (Anomaly Detection):** Las features estructurales `(has_*, len_*, tok_*, word_*)` y económicas de esos títulos se usan como variables de entrada junto con datos OCDS

### Variables disponibles por tipo

**Numéricas (OCDS + pre-computadas):**
- `monto_estimado`, `presupuesto_monto`, `monto_adjudicado`, `monto_contrato`
- `cantidad_oferentes`, `cantidad_items`, `cantidad_lotes`
- `duracion_consultas_dias`, `duracion_oferta_dias`, `duracion_contrato_dias`
- `garantia_porcentaje`, `costo_pliego`
- `cantidad_enmiendas`, `monto_total_enmiendas`
- `cantidad_pagos`, `monto_total_pagado`
- **Derivadas (económicas):** `contract_value_ratio`, `precio_vs_estimado`, `overbudget_ratio`, `enmienda_ratio`, `pago_vs_contrato_ratio`, `bidding_urgency`, `oferentes_por_item`
- **Colusión:** `winner_category_frequency`, `winner_total_contracts`, `is_repeat_winner`, `bidder_diversity`

**Categóricas (OCDS + flags derivados):**
- `metodo_contratacion`, `metodo_detalle`
- `categoria_principal`, `categoria_detalle`
- `criterio_adjudicacion`, `criterio_detalle`
- `estado`, `estado_detalle`
- **Flags:** `es_unico_oferente`, `is_high_value_single_bidder`, `has_multas`, `has_protesta`

**Temporales (OCDS):**
- `fecha_publicacion`, `fecha_apertura`, `fecha_adjudicacion`, `fecha_firma`

**Estructurales (571 columnas desde `document_features`):**
- **Por título:** `has_<title>`, `len_<title>`, `tok_<title>`, `word_<title>`, `count_<title>`, `span_<title>`, `page_range_<title>`
- **Agregados por documento:** `gini_content_length`, `title_entropy`, `avg_page`, `max_page`, `sum_word_count`, `avg_word_count`, `sum_content_length`, `avg_content_length`

**Texto (Pliegos - Pipeline 2):**
- `cluster_id`, `texto_vacio`, `contiene_contenido_adicional`
- `permiso_es_sin_traduccion`, `cantidad_copias_requeridas`
- `num_clausulas_normativas_presentes`, `longitud_texto_palabras`

---

## Feature Engineering

Todas las features están precomputadas en `document_features` (571 columnas estructurales) y `document_economic_features` (46 columnas económicas + colusión). No es necesario recalcularlas on-the-fly.

### Features estructurales (`document_features`)

Por cada título de pliego, `document_features` contiene:
- `has_<slug>`: booleano de presencia del título
- `len_<slug>`: largo total de secciones de ese título
- `tok_<slug>`: tokens estimados
- `word_<slug>`: conteo de palabras real (corregido)
- `span_<slug>`: rango de líneas ocupado por el título
- `page_range_<slug>`: rango de páginas
- `count_<slug>`: número de secciones de ese título

Además incluye agregados por documento: `gini_content_length` (desigualdad estructural), `title_entropy` (complejidad de composición), y promedios (`avg_page`, `max_page`, `avg_word_count`, `sum_word_count`).

### Features económicas (`document_economic_features`)

Ya derivadas en la tabla precomputada:

```python
cols_economicas = [
    # Ratios financieros
    'precio_vs_estimado', 'contract_value_ratio', 'overbudget_ratio',
    # Concentración de oferentes
    'cantidad_oferentes', 'es_unico_oferente', 'n_oferentes_distintos',
    'oferentes_por_item', 'is_high_value_single_bidder',
    # Colusión
    'winner_category_frequency', 'winner_total_contracts',
    'is_repeat_winner', 'bidder_diversity',
    # Enmiendas
    'n_enmiendas', 'total_monto_enmiendas', 'enmienda_ratio',
    # Pagos
    'n_pagos', 'total_pagado', 'pago_vs_contrato_ratio',
    'total_multas', 'has_multas',
    # Protestas
    'has_protesta', 'n_protestas',
    # Urgencia
    'bidding_urgency'
]
```

**Variables estructurales** pre-computadas en `dncp.document_features` (571 columnas, 31,321 documentos).

**Solo las features de Pipeline 2 (LLM) se computan durante la ejecución de Pipeline 3.**

### Justificación de features con literatura de corrupción

Cada variable fue seleccionada por su relación documentada con señales de riesgo en contratación pública:

| Feature | Señal de corrupción | Referencia |
|:--------|:--------------------|:-----------|
| `es_unico_oferente` | Ausencia de competencia (mapeo de mercados) — indicador clásico de manipulación | Auriol (2006); OECD Public Procurement Reviews |
| `overbudget_ratio` / `contract_value_ratio` | Sobrecostos y renegociaciones favorables (medición de precio vs. estimado) | Zamboni & Litschig (2018) |
| `enmienda_ratio` | Cambios post-adjudicación — canal para redistribuir rentas | Coviello & Gagliarducci (2017); Colombia NCPP |
| `n_protestas` / `has_protesta` | Inconformidad de competidores con procedimiento | Filipino Procurement Monitoring Project |
| `winner_category_frequency` | Concentración de adjudicaciones en un proveedor en la misma categoría | Lewis-Faupel et al. (2014) |
| `is_repeat_winner` | Favoritismo histórico del convocante hacia un proveedor | Miralles (2012) en corrupción local |
| `is_high_value_single_bidder` | Alto valor + oferta única = riesgo de colusión/cartel | Estache & Iimi (2011); SNAI strategy |
| `bidding_urgency` | Pliegos urgentes sobre montos elevados — flexibilidad que reduce escrutinio | Bandiera et al. (2009) |
| `has_multas` | Incumplimiento recurrente del adjudicatario | Métrica de riesgo de contratista |
| `gini_content_length` | Desigualdad extrema en la estructura del pliego — posible manipulación de contenido | Métrica novedosa propia |
| `title_entropy` | Baja/elevada diversidad de títulos — inconsistencia documental | Métrica novedosa propia |

### Variables categóricas a codificar

```python
# One-Hot Encoding para variables categóricas
df_encoded = pd.get_dummies(df, columns=[
    'metodo_contratacion',
    'categoria_principal',
    'criterio_adjudicacion',
    'estado'
])

# Label Encoding para ordinales
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
df['metodo_encoded'] = le.fit_transform(df['metodo_contratacion'])
```

### Variables de Pipeline 2 (Pliegos)

```python
# Features de texto extraídas por LLM
df['tiene_contenido_adicional'] = df['contiene_contenido_adicional']
df['permiso_sin_traduccion'] = df['permiso_es_sin_traduccion']
df['copias_requeridas'] = df['cantidad_copias_requeridas']
df['clausulas_normativas'] = df['num_clausulas_normativas_presentes']
df['longitud_texto'] = df['longitud_texto_palabras']
df['cluster_id'] = df['cluster_id']
```

---

## Preprocessing Pipeline

### Carga de tablas pre-computadas

```python
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://...")

# Cargar tablas pre-computadas
with engine.connect() as conn:
    df_features = pd.read_sql(text("SELECT * FROM dncp.document_features"), conn)
    df_econ = pd.read_sql(text("SELECT * FROM dncp.document_economic_features"), conn)

# Merge con licitaciones si es necesario
df = licitaciones.merge(df_features, on='nro_licitacion', how='left')
df = df.merge(df_econ, on='nro_licitacion', how='left')

# Las features de Pipeline 2 (LLM) se agregan aquí
```

```python
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer

# Features numéricas de 3 fuentes
numeric_features = [
    # OCDS sin procesar (licitaciones)
    'monto_estimado', 'presupuesto_monto', 'monto_adjudicado', 'monto_contrato',
    'cantidad_oferentes', 'cantidad_items', 'duracion_oferta_dias',
    'duracion_contrato_dias',
    # Económicas derivadas (document_economic_features)
    'precio_vs_estimado', 'overbudget_ratio', 'contract_value_ratio',
    'enmienda_ratio', 'pago_vs_contrato_ratio', 'bidding_urgency',
    'oferentes_por_item', 'bidder_diversity',
    'winner_category_frequency', 'winner_total_contracts',
    # Estructurales agregadas (document_features)
    'gini_content_length', 'title_entropy', 'avg_page', 'max_page',
    'sum_word_count', 'avg_word_count',
    # Pipeline 2 (LLM)
    'longitud_texto', 'clausulas_normativas', 'copias_requeridas'
]

categorical_features = [
    'metodo_contratacion', 'categoria_principal', 'criterio_adjudicacion'
]

# Pipeline de preprocessing
preprocessor = ColumnTransformer(
    transformers=[
        ('num', Pipeline([
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ]), numeric_features),
        ('cat', Pipeline([
            ('imputer', SimpleImputer(strategy='constant', fill_value='unknown')),
            ('encoder', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
        ]), categorical_features)
    ]
)

# Aplicar
X_processed = preprocessor.fit_transform(df)
```

---

## Métodos de Detección de Anomalías

### Método 1: Isolation Forest (Principal)

**Fundamento teórico:**
El Isolation Forest (Liu et al., 2008) se basa en la premisa de que las anomalías son **pocas y diferentes**. Construye un bosque de árboles de aislamiento (iTrees) donde cada nodo divide los datos aleatoriamente. Las anomalías se aíslan más rápido (camino más corto en el árbol) porque requieren menos divisiones para ser separadas del resto.

**Fórmula del score de anomalía:**
```
s(x, n) = 2^(-E(h(x))/c(n))

Donde:
- h(x) = longitud del camino para aislar x
- E(h(x)) = promedio de longitudes en todos los árboles
- c(n) = factor de normalización (longitud promedio de búsqueda fallida)
- n = número de muestras
```

**Implementación:**
```python
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Preprocessing
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_numeric)

# Model
clf = IsolationForest(
    contamination=0.05,  # 5% estimado de fraude
    n_estimators=200,
    max_samples='auto',
    random_state=42,
    n_jobs=-1
)

# Training
clf.fit(X_scaled)

# Predictions
anomaly_labels = clf.predict(X_scaled)  # -1 = anomaly, 1 = normal
anomaly_scores = clf.decision_function(X_scaled)  # lower = more anomalous

# Top anomalies
top_anomalies = np.argsort(anomaly_scores)[:100]  # 100 más sospechosos
```

**Parámetros clave:**
| Parámetro | Valor recomendado | Justificación |
|:----------|:-----------------:|:--------------|
| `contamination` | 0.01 - 0.05 | Tasa de fraude estimada en contrataciones |
| `n_estimators` | 200 | Balance entre precisión y tiempo |
| `max_samples` | 256 | Submuestreo para eficiencia |

**Qué detecta:**
- Montos atípicamente altos o bajos para la categoría
- Cantidades de oferentes anormales
- Duraciones de proceso inusuales
- Combinaciones de variables numéricas inusuales

**Ventajas:**
- Rápido: O(n log n)
- Eficiente en memoria
- Maneja datos de alta dimensionalidad
- No necesita métricas de distancia
- Implementado en scikit-learn

**Desventajas:**
- Falla en anomalías locales (no detecta variaciones de densidad local)
- Parámetro `contamination` debe ser estimado

---

### Método 2: Local Outlier Factor (LOF)

**Fundamento teórico:**
LOF (Breunig et al., 2000) detecta anomalías **locales** comparando la densidad local de un punto con la densidad de sus k vecinos más cercanos. Un punto es anomalía si su densidad local es significativamente menor que la de sus vecinos.

**Fórmula:**
```
LOF_k(p) = (Σ_{o ∈ N_k(p)} lrd_k(o) / lrd_k(p)) / |N_k(p)|

Donde:
- lrd_k(p) = densidad local reachability de p
- N_k(p) = k vecinos más cercanos de p
```

**Implementación:**
```python
from sklearn.neighbors import LocalOutlierFactor

clf = LocalOutlierFactor(
    n_neighbors=20,
    contamination=0.05,
    metric='minkowski'
)

anomaly_labels = clf.fit_predict(X_scaled)
lof_scores = clf.negative_outlier_factor_  # lower = more anomalous
```

**Qué detecta:**
- Proveedores con patrones anormales dentro de su categoría
- Licitaciones sospechosas comparadas con similares
- Anomalías que Isolation Forest puede pasar por alto

**Ventajas:**
- Detecta anomalías locales
- Se adapta a diferentes densidades

**Desventajas:**
- O(n²) en el peor caso, lento con >30K registros
- Sensible al parámetro k

---

### Método 3: DBSCAN (Clustering + Anomalías)

**Fundamento teórico:**
DBSCAN (Ester et al., 1996) agrupa puntos densamente conectados y marca como **ruido** (anomalías) los puntos en regiones de baja densidad.

**Parámetros:**
- `eps`: radio de la vecindad (determinado por k-distancia)
- `min_samples`: mínimo de puntos para formar un cluster

**Implementación:**
```python
from sklearn.cluster import DBSCAN
from sklearn.neighbors import NearestNeighbors

# Determinar eps óptimo con k-distancia
nn = NearestNeighbors(n_neighbors=5)
nn.fit(X_scaled)
distances, indices = nn.kneighbors(X_scaled)
k_distances = np.sort(distances[:, -1])

# Usar elbow point como eps
eps_optimal = k_distances[int(len(k_distances) * 0.9)]  # percentil 90

# DBSCAN
clustering = DBSCAN(eps=eps_optimal, min_samples=5)
labels = clustering.fit_predict(X_scaled)

# Anomalías = puntos con label -1
anomaly_mask = labels == -1
anomalies = X[anomaly_mask]
```

**Qué detecta:**
- Documentos que no pertenecen a ningún grupo conocido
- Proveedores atípicos en el espacio de features
- Tipos de contratación poco comunes

**Ventajas:**
- Da clusters interpretables + anomalías
- No necesita especificar número de clusters

**Desventajas:**
- Sensible a eps y min_samples
- No está diseñado específicamente para detección de anomalías

---

### Método 4: One-Class SVM (Novelty Detection)

**Fundamento teórico:**
One-Class SVM (Schölkopf et al., 2001) aprende una frontera que envuelve los datos "normales". Puntos fuera de la frontera son anomalías.

**Implementación:**
```python
from sklearn.svm import OneClassSVM

clf = OneClassSVM(
    kernel='rbf',
    gamma='scale',
    nu=0.05  # fracción estimada de anomalías
)

anomaly_labels = clf.fit_predict(X_scaled)
decision_scores = clf.decision_function(X_scaled)
```

**Ventajas:**
- Poderoso en espacios de alta dimensionalidad
- Bien para novelty detection

**Desventajas:**
- O(n²) a O(n³), no escala bien con >30K registros
- Selección de kernel y parámetro nu es crítica

---

### Método 5: Autoencoder (Deep Learning)

**Fundamento teórico:**
Un autoencoder (Rumelhart et al., 1986) aprende una representación comprimida de los datos normales. Las anomalías tienen **alto error de reconstrucción** porque el modelo no puede reconstruir patrones que nunca vio.

**Arquitectura:**
```
Input (N features) → Encoder → Latent Space (M features) → Decoder → Output (N features)

Loss = MSE(Input, Output)

Anomaly Score = MSE(Input, Output) por muestra
```

**Implementación:**
```python
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

class ProcurementAutoencoder(nn.Module):
    def __init__(self, input_dim, latent_dim=16):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, latent_dim)
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, input_dim)
        )
    
    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z)

# Training
model = ProcurementAutoencoder(input_dim=X_scaled.shape[1])
optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
criterion = nn.MSELoss()

# Entrenar solo con datos "normales" (o todos si no hay etiquetas)
dataset = TensorDataset(torch.FloatTensor(X_scaled))
loader = DataLoader(dataset, batch_size=256, shuffle=True)

for epoch in range(100):
    for batch in loader:
        x = batch[0]
        x_hat = model(x)
        loss = criterion(x_hat, x)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

# Anomaly scores
with torch.no_grad():
    X_tensor = torch.FloatTensor(X_scaled)
    X_hat = model(X_tensor)
    reconstruction_errors = torch.mean((X_tensor - X_hat)**2, dim=1).numpy()

# Top anomalies
top_anomalies = np.argsort(reconstruction_errors)[-100:]
```

**Qué detecta:**
- Patrones no lineales complejos
- Relaciones entre features que otros métodos no capturan
- Anomalías en datos de texto + numéricos combinados

**Ventajas:**
- Captura patrones complejos
- Maneja datos mixtos

**Desventajas:**
- Requiere más datos y tiempo de entrenamiento
- Arquitectura y hiperparámetros son complejos
- Menos interpretable

---

### Método 6: Ensemble (Recomendado para la tesis)

**Fundamento:**
Combinar múltiples métodos mejora la precisión (Arroyo-Castro, 2025). Un documento es anomalía si **múltiples métodos lo detectan**.

**Implementación:**
```python
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import DBSCAN

# Entrenar múltiples modelos
models = {
    'IF': IsolationForest(contamination=0.05, random_state=42),
    'LOF': LocalOutlierFactor(n_neighbors=20, contamination=0.05),
}

# Obtener scores de cada modelo
all_scores = {}
for name, model in models.items():
    model.fit(X_scaled)
    if hasattr(model, 'decision_function'):
        scores = model.decision_function(X_scaled)
    else:
        scores = model.negative_outlier_factor_
    all_scores[name] = scores

# Normalizar scores a [0, 1]
from sklearn.preprocessing import MinMaxScaler
scaler_scores = MinMaxScaler()
normalized_scores = {}
for name, scores in all_scores.items():
    normalized_scores[name] = scaler_scores.fit_transform(scores.reshape(-1, 1)).flatten()

# Promedio de scores (ensemble)
ensemble_score = np.mean(list(normalized_scores.values()), axis=0)

# Confidence = número de métodos que lo detectan como anomalía
confidence = np.zeros(len(X_scaled))
for name, scores in normalized_scores.items():
    # Umbral: percentil 5 (más bajos = más anómalos)
    threshold = np.percentile(scores, 5)
    confidence[scores <= threshold] += 1

# Confidence score: 0-3 (0 = normal, 3 = muy sospechoso)
```

**Scoring final:**
```
confidence_score = (IF_detecta ? 1 : 0) + (LOF_detecta ? 1 : 0) + (DBSCAN_ruido ? 1 : 0)
```

| Confidence | Interpretación |
|:----------:|:---------------|
| 0 | Normal |
| 1 | Posiblemente anómalo |
| 2 | Sospechoso |
| 3 | Muy sospechoso (revisar manualmente) |

---

## Evaluación (Sin Ground Truth)

### Estrategia 1: Inyección de Anomalías Sintéticas

```python
# Crear anomalías conocidas
n_synthetic = 100
synthetic_indices = np.random.choice(len(X_scaled), n_synthetic, replace=False)

# Modificar features para crear anomalías
X_synthetic = X_scaled.copy()
X_synthetic[synthetic_indices, 0] *= 3  # Monto 3x mayor
X_synthetic[synthetic_indices, 1] *= 0.1  # Cantidad de oferentes muy baja

# Evaluar si el modelo las detecta
scores_synthetic = clf.decision_function(X_synthetic)
detection_rate = np.sum(scores_synthetic[synthetic_indices] < threshold) / n_synthetic
print(f"Tasa de detección de anomalías sintéticas: {detection_rate:.1%}")
```

### Estrategia 2: Validación Cruzada entre Métodos

```python
# Acuerdo entre métodos
methods = ['IF', 'LOF', 'DBSCAN']
agreement_matrix = np.zeros((len(X_scaled), len(methods)))

for i, method in enumerate(methods):
    agreement_matrix[:, i] = (anomaly_labels[method] == -1).astype(int)

# Índice de Jaccard por par
from itertools import combinations
for m1, m2 in combinations(methods, 2):
    intersection = np.sum((agreement_matrix[:, methods.index(m1)] == 1) & 
                         (agreement_matrix[:, methods.index(m2)] == 1))
    union = np.sum((agreement_matrix[:, methods.index(m1)] == 1) | 
                   (agreement_matrix[:, methods.index(m2)] == 1))
    jaccard = intersection / union if union > 0 else 0
    print(f"Jaccard {m1}-{m2}: {jaccard:.3f}")
```

### Estrategia 3: Revisión Experta

```python
# Exportar top-N para revisión manual
top_n = 50
top_indices = np.argsort(ensemble_score)[-top_n:]

# Generar reporte
report = df.iloc[top_indices][[
    'nro_licitacion', 'titulo', 'monto_estimado', 'monto_adjudicado',
    'cantidad_oferentes', 'metodo_contratacion', 'confidence_score'
]]

report.to_csv('anomalies_top50_review.csv', index=False)
```

### Estrategia 4: Métricas Internas

```python
from sklearn.metrics import silhouette_score, davies_bouldin_score

# Para clustering-based (DBSCAN)
if len(set(labels)) > 1:
    sil = silhouette_score(X_scaled[labels != -1], labels[labels != -1])
    db = davies_bouldin_score(X_scaled[labels != -1], labels[labels != -1])
    print(f"Silhouette: {sil:.3f}, Davies-Bouldin: {db:.3f}")
```

---

## Comparación de Métodos

| Método | Tipo de anomalía detectada | Complejidad | Escalabilidad | Interpretabilidad |
|:-------|:--------------------------:|:-----------:|:-------------:|:-----------------:|
| **Isolation Forest** | Outliers multivariados | Baja | Alta | Media |
| **LOF** | Anomalías locales | Media | Media | Media |
| **DBSCAN** | Ruido / documentos atípicos | Media | Media | Alta |
| **One-Class SVM** | Novedades fuera de frontera | Alta | Baja | Baja |
| **Autoencoder** | Patrones no lineales | Alta | Media | Baja |
| **Ensemble** | Combinación de los anteriores | Media | Alta | Media |

### Recomendación por escenario

| Escenario | Método recomendado | Por qué |
|:----------|:------------------:|:--------|
| **Baseline rápido** | Isolation Forest | Rápido, robusto, sin tuning |
| **Anomalías locales** | LOF | Captura densidad local |
| **Patrones complejos** | Autoencoder | Relaciones no lineales |
| **Producción** | Ensemble (IF + LOF) | Mejor precisión |

---

## Arquitectura Recomendada para la Tesis

```
┌──────────────────────────────────────────────────────────────────┐
│                       FASE 3A: FEATURE ENGINEERING                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────┐   ┌─────────────────┐   ┌───────────────┐  │
│  │ pliegos_secciones│   │ OCDS Data       │   │ Adjudicaciones│  │
│  │ (2.7M rows)     │   │ (29K rows)      │   │ Contratos     │  │
│  └────────┬────────┘   │                 │   │ Oferentes     │  │
│           │            └────────┬────────┘   │ Pagos         │  │
│           ▼                     │            └───────┬───────┘  │
│  ┌────────────────┐            │                    │          │
│  │ document_features│           │                    │          │
│  │ 571 cols        │           │                    │          │
│  │ has_*, len_*,   │           └────────┬───────────┘          │
│  │ tok_*, word_*   │                    ▼                      │
│  │ count_*, span_* │        ┌─────────────────────┐            │
│  │ page_range_*    │        │ document_economic_  │            │
│  │ + agregados     │        │ features (46 cols)  │            │
│  └────────┬────────┘        └──────────┬──────────┘            │
│           │                            │                       │
│           └────────────┬───────────────┘                       │
│                        ▼                                       │
│  ┌──────────────────────────────────────────┐                  │
│  │        Title Ranking Pipeline             │                  │
│  │  6 estrategias → Top-K títulos           │                  │
│  │  outlier(0.20) + rf(0.25) + context(0.15)│                  │
│  │  section_if(0.15) + corr(0.10) + econ(0.15)│                │
│  └───────────────────┬──────────────────────┘                  │
│                      │                                         │
│                      ▼                                         │
│  ┌──────────────────────────────────────────┐                  │
│  │  Pipeline 3: UNSUPERVISED ANOMALY DETECTION                │
│  ├──────────────────────────────────────────┤                  │
│  │  ┌────────────────────────────────────┐  │                  │
│  │  │  Preprocessing                     │  │                  │
│  │  │  Scaling + Encoding + Merge tables │  │                  │
│  │  └──────────────┬─────────────────────┘  │                  │
│  │                 ▼                        │                  │
│  │  ┌────────────────────────────────────┐  │                  │
│  │  │  Ensemble Detection                │  │                  │
│  │  │  ┌──────┐ ┌──────┐ ┌──────────┐   │  │                  │
│  │  │  │  IF  │ │ LOF  │ │ DBSCAN   │   │  │                  │
│  │  │  │(0.05)│ │(0.05)│ │ (ruido)  │   │  │                  │
│  │  │  └──┬───┘ └──┬───┘ └────┬─────┘   │  │                  │
│  │  │     └─────────┼──────────┘         │  │                  │
│  │  │              ▼                     │  │                  │
│  │  │      ┌──────────────┐             │  │                  │
│  │  │      │ Aggregation  │             │  │                  │
│  │  │      │(Voting+Score)│             │  │                  │
│  │  │      └──────┬───────┘             │  │                  │
│  │  └─────────────┼─────────────────────┘  │                  │
│  │                ▼                        │                  │
│  │  ┌────────────────────────────────────┐  │                  │
│  │  │  Output                            │  │                  │
│  │  │  • Anomaly Score (0-1)             │  │                  │
│  │  │  • Confidence (0-3)                │  │                  │
│  │  │  • Top-N anomalies for review      │  │                  │
│  │  └────────────────────────────────────┘  │                  │
│  └──────────────────────────────────────────┘                  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Novedades de la tesis (vs. tesis anterior)

| Aspecto | Tesis anterior | Tu tesis |
|:--------|:--------------:|:--------:|
| Datos | Solo OCDS | OCDS + **Pliegos** |
| Features | Numéricos/categóricos | + **Texto estructurado** + **Económicos + Colusión** |
| Método | Isolation Forest | **Ensemble** (IF + LOF + DBSCAN) |
| Análisis | Superficial | **Ranking de títulos** + **Caracterización de riesgos** |
| Tablas pre-computadas | No | `document_features` (571 cols), `document_economic_features` (46 cols) |
| Estrategias de ranking | 0 | **6 estrategias** con selección automática de top-K |

---

## Referencias

### Métodos de detección de anomalías

1. Liu, F.T., Ting, K.M., & Zhou, Z.H. (2008). Isolation Forest. *ICDM*.
2. Breunig, M.M., et al. (2000). LOF: Identifying Density-Based Local Outliers. *SIGMOD*.
3. Ester, M., et al. (1996). A Density-Based Algorithm for Discovering Clusters. *KDD*.
4. Schölkopf, B., et al. (2001). Estimating the Support of a High-Dimensional Distribution. *Neural Computation*.
5. Arroyo-Castro, J.P. & Chou-Chen, S.W. (2025). Unsupervised Detection of Anomaly in Public Procurement Processes. *Springer*.

### Corrupción en contratación pública

6. Auriol, E. (2006). Corruption in procurement and public purchase. *International Journal of Industrial Organization*.
7. Bandiera, O., Prat, A., & Valletti, T. (2009). Active and Passive Waste in Government Spending: Evidence from a Policy Experiment. *American Economic Review*.
8. Coviello, D., & Gagliarducci, S. (2017). Tenure in office and public procurement. *American Economic Journal: Economic Policy*.
9. Estache, A., & Iimi, A. (2011). (Un)Bundling, Procurement, and Structural Reforms in Network Industries. *The Review of Network Economics*.
10. Lewis-Faupel, S., Neggers, Y., Olken, B., & Pande, R. (2014). Can Electronic Procurement Improve Infrastructure Provision? Evidence from Public Works in India and Indonesia. *American Economic Journal: Economic Policy*.
11. Miralles, C. (2012). Winner’s curse in public procurement: Evidence from public works. (Tesis/Documento de trabajo).
12. OECD (2016). **Preventing Corruption in Public Procurement**. OECD Publishing.
13. Zamboni, S., & Litschig, S. (2018). Audit risk and rent extraction: Evidence from a randomized evaluation in Brazil. *Journal of Development Economics*.

---

## Reproducibilidad

Para regenerar todas las tablas y el ranking desde cero:

```bash
# 1. Reconstruir features estructurales (Phase 1)
python scripts/run_document_features_pipeline.py

# 2. Reconstruir features económicas (Phases 2-3)
python scripts/run_document_economic_features_pipeline.py

# 3. Re-ejecutar ranking con 6 estrategias (Phases 4-5)
python -c "
import sys; sys.path.insert(0, '.')
from src.pipelines.title_ranking_pipeline import TitleRankingPipeline
from config.settings import DB_CONFIG
TitleRankingPipeline({
    'db_params': DB_CONFIG,
    'top_n': 80,
    'section_sample': 30000,
    'output_dir': 'data/processed/title_ranking',
    'auto_k': False,
    'top_k': 10
}).run()
"

# 4. Probar 6ta estrategia en aislamiento
python scripts/test_economic_strategy.py
```

**Outputs esperados:**

| Paso | Output | Tiempo |
|:----:|:-------|:------:|
| 1 | `dncp.document_features` (31,321 × 571) | ~171s |
| 2 | `dncp.document_economic_features` (29,811 × 46) | ~3s |
| 3 | `data/processed/title_ranking/title_ranking.csv` + `.md` + `.json` | ~11.6s |
| 4 | Validación de estrategia económica | ~1s |

---

## Limitaciones y notas técnicas

| Limitación | Impacto | Mitigación |
|:-----------|:--------|:-----------|
| Memory crash con `SELECT *` (587 cols) | Ranking no ejecutable en Windows | Cargar solo 245 cols via `information_schema.columns` ✅ |
| `bidder_diversity` = 0.0 para todos los docs | Indicador de colusión sin variación | `cantidad_items` = 0 en licitaciones — issue de datos fuente |
| `has_otros` varianza cero | Estrategia económica retorna 0 para este título | Comportamiento esperado, no bug |
| Validación de schemas requiere LLM | Clustering bloqueado sin endpoint local o OpenRouter | Usar `--skip-llm` para clustering no-LLM o configurar `OPENROUTER_API_KEY` |
| Pipeline 2 (LLM extraction) | Schema validation 2/10 | Necesita continuar validación manual/LLM para 9 títulos |

---

## Próximos pasos

| Paso | Descripción | Estado | Tiempo est. |
|:----:|:------------|:-----:|:-----------:|
| 0 | Pre-computar `document_features` (571 cols) | ✅ Listo | — |
| 1 | Pre-computar `document_economic_features` (46 cols) | ✅ Listo | — |
| 2 | Ejecutar ranking de títulos (6 estrategias → top-K) | ✅ Listo | — |
| 3 | Pipeline 2: Clustering + extracción LLM por título | ❌ Pendiente | 5-7 días |
| 4 | Feature engineering (cargar tablas pre-computadas + Pipeline 2) | 🟡 Tablas listas | 1 día |
| 5 | Preprocessing pipeline (ColumnTransformer) | 🟡 Código listo | 1 día |
| 6 | Isolation Forest (baseline) | ⏳ | 1 día |
| 7 | LOF + DBSCAN | ⏳ | 1-2 días |
| 8 | Ensemble + scoring | ⏳ | 2-3 días |
| 9 | Evaluación (sintética + cruzada) | ⏳ | 2-3 días |
| 10 | Validación experta (top-50) | ⏳ | 1-2 días |
| 11 | Dashboard Streamlit | ⏳ | 3-4 días |
| 12 | Documentación + hallazgos | ⏳ | 3-4 días |
| | **Total pendiente** | | **15-20 días** |

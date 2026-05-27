# Clustering Pipeline — Documentación Completa para Continuidad

## Índice

1. [Visión General](#1-visión-general)
2. [Arquitectura](#2-arquitectura)
3. [Componentes en Detalle](#3-componentes-en-detalle)
   - 3.1 Extractor
   - 3.2 Transformer
   - 3.3 Loader
   - 3.4 Pipeline
   - 3.5 Entry Point (script)
4. [Mejoras Implementadas](#4-mejoras-implementadas)
   - 4.1 Validación de Schemas post-LLM
   - 4.2 Combined Optimal K (3 métricas)
   - 4.3 HDBSCAN como alternativa
   - 4.4 Comparación de Embeddings
   - 4.5 Grid de UMAP
   - 4.6 Reporte de Comparación
5. [Decisiones Arquitectónicas](#5-decisiones-arquitectónicas)
6. [Formato de Output](#6-formato-de-output)
7. [Cómo Extender](#7-cómo-extender)
8. [Tareas Pendientes (Próximos Pasos)](#8-tareas-pendientes)
9. [Problemas Conocidos](#9-problemas-conocidos)
10. [Referencia Rápida de Archivos](#10-referencia-rápida-de-archivos)

---

## 1. Visión General

Pipeline ETL que procesa ~30,000 secciones de pliegos de licitaciones públicas paraguayas por título (10 títulos principales). Por cada título:

1. Extrae todas las secciones desde PostgreSQL (`dncp.pliegos_secciones`)
2. Agrupa textos similares mediante clustering no supervisado
3. Selecciona muestras representativas y extremas por cluster
4. Usa un LLM (OpenRouter) para diseñar un esquema JSON de extracción
5. Genera prompts de extracción para downstream LLM
6. Compara múltiples enfoques de clustering y elige el mejor

### Objetivo final

Alimentar un sistema de detección de anomalías: los schemas binarios/numeric capturan diferencias de redacción entre variantes de cláusulas legales, y los prompts permiten extraer features estructuradas de todas las secciones para downstream anomaly detection.

---

## 2. Arquitectura

```
SectionClusteringPipeline (pipeline orchestrator)
│
├── SectionClusteringExtractor (src/etl/extractors/)
│   └── Lee DB → DataFrame con nro_licitacion, content_text, year, category_id
│
├── SectionClusteringTransformer (src/etl/transformers/)
│   ├── Sentence-Transformers (embeddings multilingües)
│   ├── UMAP / PCA (reducción de dimensionalidad)
│   ├── K-Means con combined metrics + HDBSCAN como alternativa
│   ├── Sampling: 3 representativos + 2 extremos por cluster
│   ├── LLM → schema validation con retry
│   └── Extraction prompt builder
│
└── SectionClusteringLoader (src/etl/loaders/)
    ├── master_schemas.json
    ├── clustering_comparison.json
    └── 10 reports Markdown individuales
```

### Flujo por título

```
textos → embeddings → UMAP → K-Means(k_opt) → HDBSCAN → best → 
  → sample → LLM → validate → schema → extraction_prompt → output
```

---

## 3. Componentes en Detalle

### 3.1 Extractor (`section_clustering_extractor.py`)

**Propósito**: Conectar a PostgreSQL y extraer secciones.

```python
class SectionClusteringExtractor:
    SECTIONS_BY_TITLE_QUERY = """
        SELECT nro_licitacion, content_text, year, category_id
        FROM dncp.pliegos_secciones
        WHERE LOWER(title_normalized) = :title
          AND content_text IS NOT NULL
    """
```

**Puntos clave**:
- Usa SQLAlchemy + psycopg2 para DB connection
- TITLE_MAPPING: 3 títulos tienen nombre distinto en BD vs display (ver `TITLE_MAPPING` en el código)
- Filtra `content_text` NULL o vacío
- `TOP_10_TITLES` = claves de `TITLE_MAPPING`

**Configuración DB**: parámetros desde `config/settings.py` → variables de entorno:
- DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

### 3.2 Transformer (`section_clustering_transformer.py`)

**Propósito**: Corazón del pipeline. Embeddings → clustering → LLM → schema.

#### Constructor (`__init__`)

| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `llm_api_key` | (requerido) | OpenRouter API key |
| `llm_model` | `openai/gpt-oss-20b:free` | Modelo LLM para schema design |
| `embedding_model` | `paraphrase-multilingual-MiniLM-L12-v2` | Embedding primario |
| `random_state` | `42` | Seed global para reproducibilidad |
| `k_range` | `(2, 15)` | Rango de búsqueda para k óptimo |
| `use_hdbscan` | `True` | Activar HDBSCAN como alternativa a K-Means |
| `validate_schema` | `True` | Validar schema post-LLM |
| `schema_validation_retries` | `2` | Número de retries si falla validación |
| `compare_embeddings` | `False` | Modo comparación multi-embedding |
| `embedding_models_to_test` | `[MiniLM, mpnet, distiluse]` | Modelos para comparación |
| `umap_params_grid` | `[{n_comp:10, n_neigh:15}, ...]` | Configs UMAP para comparación |

#### Método principal: `process_title(title, df, max_samples=5000)`

Flujo de ejecución:

1. **Subsample**: si `len(df) > max_samples`, samplea aleatoriamente con `random_state=42`
2. **Clustering path**:
   - Si `compare_embeddings=True`: llama a `_run_grid_comparison()` que prueba M modelos × N UMAP configs × {K-Means, HDBSCAN} y elige el mejor
   - Si `compare_embeddings=False` (default):
     a. Embeddings con modelo primario
     b. UMAP con DEFAULT_UMAP_PARAMS (10 comp, 15 neigh)
     c. `_find_optimal_k_combined()` para k óptimo
     d. K-Means con k óptimo
     e. Si `use_hdbscan=True`: también HDBSCAN, comparar, elegir el mejor
3. **Apply labels** al DataFrame
4. **Sample**: 3 representativos (cercanos al centroide) + 2 extremos (lejanos)
5. **LLM**: genera schema JSON con validación y retry
6. **Build extraction prompt** con template + schema
7. **Return** dict con todo

#### Métodos internos

| Método | Propósito |
|--------|-----------|
| `_get_embedder(model_name)` | Carga SentenceTransformer (caché por nombre) |
| `_get_embeddings(texts, model_name)` | Genera embeddings batch_size=32 |
| `_reduce_dimensions(embeddings, n_components, n_neighbors)` | UMAP con fallback PCA |
| `_evaluate_clustering(X, labels, ignore_noise)` | Calcula silhouette + Davies-Bouldin + Calinski-Harabasz |
| `_find_optimal_k_combined(X)` | Busca k óptimo con 3 métricas normalizadas |
| `_cluster(X, k)` | K-Means con n_init=10 |
| `_cluster_hdbscan(X, min_cluster_size, min_samples)` | HDBSCAN con manejo de ruido |
| `_compute_centroids(X, labels)` | Centroides para HDBSCAN (media por cluster) |
| `_make_comparison_entry(...)` | Serializa un resultado de comparación |
| `_run_grid_comparison(title, texts, models, configs)` | Grid search completo |
| `_sample_clusters(...)` | Sampling con manejo de labels no contiguos |
| `_validate_schema(result)` | Validación estricta post-LLM |
| `_generate_schema_via_llm(title, samples)` | LLM + validación + retry |
| `_default_schema(title)` | Fallback de 2 campos genéricos |
| `_call_llm(messages)` | HTTP call a OpenRouter con 3 retries |
| `_build_extraction_prompt(...)` | Template engine para extraction prompt |
| `_build_cluster_report(df, samples)` | Texto plano con distribución de clusters |
| `_build_fallback(title, df)` | Cuando hay < 3 secciones |

### 3.3 Loader (`section_clustering_loader.py`)

**Propósito**: Persistir resultados en 3 formatos.

| Método | Output | Propósito |
|--------|--------|-----------|
| `save_master_schema()` | `master_schemas.json` | Archivo maestro con schemas, prompts, metadata |
| `save_title_report()` | `report_{titulo}.md` | Reporte legible por título |
| `save_clustering_comparison()` | `clustering_comparison.json` | Solo cuando hay datos de comparación |

### 3.4 Pipeline (`section_clustering_pipeline.py`)

**Propósito**: Orquestar extractor → transformer → loader.

Flujo `run()`:
1. Extrae todos los títulos
2. Por cada título: `transformer.process_title(title, df)`
3. Guarda master_schemas.json (wrap en try/except)
4. Guarda reports individuales (wrap en try/except)
5. Guarda clustering_comparison.json (wrap en try/except)
6. Log summary con método y modelo seleccionado

### 3.5 Entry Point (`scripts/run_section_clustering_pipeline.py`)

```powershell
python scripts/run_section_clustering_pipeline.py [--compare] [--no-hdbscan] 
    [--k-min N] [--k-max N] [--max-samples N] [--skip-titles T1 T2]
```

Parsea argumentos CLI, construye config dict, instancia y ejecuta pipeline.

Cuando `--compare` está activo, agrega `embedding_models_to_test` y `umap_params_grid` al config.

---

## 4. Mejoras Implementadas

### 4.1 Validación de Schemas post-LLM

**Archivo**: `section_clustering_transformer.py:810-842`

**Problema original**: El LLM podía devolver JSON sintácticamente válido pero semánticamente inválido (tipos incorrectos, objetos anidados, demasiados campos, etc.) sin que el pipeline lo detectara.

**Solución**: Método `_validate_schema(result)` ejecutado después de cada `json.loads()`.

**Reglas de validación**:
1. Claves raíz `"schema"` y `"justification"` obligatorias
2. `"schema"` debe ser dict no vacío
3. Máximo 12 campos (regla de negocio)
4. Cada campo debe tener `type ∈ {"binary", "numeric"}`
5. Cada campo debe tener `description` no vacía
6. Sin claves extra además de `type` y `description` (prohíbe objetos anidados)
7. `"justification"` no vacía

**Retry con feedback**: Si la validación falla, se reenvía al LLM con un prompt modificado (`SCHEMA_VALIDATION_SYSTEM_PROMPT`) que incluye el error específico (`{razon_rechazo}`). Configurable via `schema_validation_retries` (default: 2).

**Si todos los intentos fallan**: cae en `_default_schema()` que genera 2 campos genéricos.

**System prompts**: 2 constantes en el módulo:
- `SCHEMA_DESIGN_SYSTEM_PROMPT`: prompt original (primer intento)
- `SCHEMA_VALIDATION_SYSTEM_PROMPT`: prompt con feedback de error (retries)

### 4.2 Combined Optimal K (3 métricas)

**Archivo**: `section_clustering_transformer.py:416-513`

**Problema original**: Usaba solo silhouette score para elegir k, y el rango era [3,7]. Todos los títulos convergían a k=7 (el límite superior), sugiriendo que el resultado era un artefacto del rango acotado.

**Solución**: Tres métricas combinadas con normalización min-max:

```python
metrics = {
    "silhouette": silhouette_score(X, labels),           # mayor = mejor
    "davies_bouldin": davies_bouldin_score(X, labels),   # menor = mejor
    "calinski_harabasz": calinski_harabasz_score(X, labels),  # mayor = mejor
}
# Normalización inversa para DB (menor = mejor)
sil_norm = (sil - sil_min) / (sil_max - sil_min)
db_norm = 1.0 - (db - db_min) / (db_max - db_min)
ch_norm = (ch - ch_min) / (ch_max - ch_min)
combined = sil_norm + db_norm + ch_norm  # igual peso
```

Rango expandido a (2, 15) por defecto, configurable via `k_range`.

### 4.3 HDBSCAN como alternativa

**Archivo**: `section_clustering_transformer.py:527-552`

**Problema original**: K-Means asume clusters esféricos de tamaño similar. Los textos legales tienen clusters de densidad muy variable (1-2 clusters dominantes con 60-80% + varios minoritarios).

**Solución**: `_cluster_hdbscan()` ejecuta HDBSCAN después de K-Means y compara usando combined score (silhouette como fallback en el modo simple, combined en el modo grid).

**Ventajas de HDBSCAN**:
- No requiere predefinir k
- Etiqueta outliers como ruido (label = -1)
- Encuentra clusters de densidad variable
- Ideal para variantes raras/minoritarias

**Manejo de ruido**: 
- `_evaluate_clustering(ignore_noise=True)` filtra puntos con label = -1 antes de calcular métricas
- `_compute_centroids()` calcula centroides como media por cluster (para sampling de HDBSCAN)
- `_sample_clusters()` salta labels = -1

**Selección**: En modo normal, compara HDBSCAN vs K-Means por silhouette. En modo grid, el combined score decide.

### 4.4 Comparación de Embeddings

**Archivo**: `section_clustering_transformer.py:588-738`

**Problema original**: Solo se probaba un modelo de embedding (`MiniLM-L12-v2`, 384-dim).

**Solución**: `_run_grid_comparison(title, texts, models_to_try, configs_to_try)` itera sobre modelos × configs UMAP × métodos de clustering.

**Modelos disponibles** (ya descargados en el entorno):

| Modelo | Dims | Tamaño | Calidad relativa |
|--------|:----:|:------:|:----------------:|
| `paraphrase-multilingual-MiniLM-L12-v2` | 384 | ~470 MB | Baseline |
| `paraphrase-multilingual-mpnet-base-v2` | 768 | ~1.1 GB | Mejor (MPNet > Transformer) |
| `distiluse-base-multilingual-cased-v2` | 512 | ~520 MB | Alternativa (DistilBERT) |

**Mecanismo**: Cada modelo se carga bajo demanda via `_get_embedder(model_name)`. Si el modelo pedido no es el primario, se carga temporalmente para ese título (no se cachea como el primario).

### 4.5 Grid de UMAP

**Archivo**: `section_clustering_transformer.py:607-614`

**Problema original**: UMAP usaba parámetros fijos `n_components=10, n_neighbors=15`.

**Solución**: `_reduce_dimensions()` acepta `n_components` y `n_neighbors` como parámetros. El grid por defecto prueba:

```python
umap_params_grid = [
    {"n_components": 10, "n_neighbors": 15},  # default original
    {"n_components": 10, "n_neighbors": 30},  # más vecinos = más global
    {"n_components": 20, "n_neighbors": 15},  # más componentes = menos compresión
    {"n_components": 20, "n_neighbors": 30},  # combinado
]
```

**Fallback a PCA**: Si `umap-learn` no está instalado, usa PCA con los mismos `n_components`.

### 4.6 Reporte de Comparación

**Output**: `clustering_comparison.json` + tabla en reports Markdown.

Cada entrada de comparación incluye:

```json
{
    "embedding_model": "paraphrase-multilingual-mpnet-base-v2",
    "clustering_method": "hdbscan",
    "umap_n_components": 10,
    "umap_n_neighbors": 30,
    "n_clusters": 8,
    "n_noise": 120,
    "silhouette": 0.32,
    "davies_bouldin": 1.15,
    "calinski_harabasz": 4520.0,
    "combined_score": 0.85
}
```

El summary del pipeline muestra por título: `clusters=N  method=hdbscan  model=mpnet-base-v2`.

---

## 5. Decisiones Arquitectónicas

### 5.1 Por qué no se usa `jsonschema` library

Aunque el reporte original menciona "Usar jsonschema para validar", se optó por validación manual porque:
- Las reglas son específicas del dominio (≤12 campos, solo binary/numeric, no objetos anidados)
- `jsonschema` validaría estructura JSON genérica pero no reglas de negocio
- La validación manual da mensajes de error más precisos para el retry con feedback

### 5.2 Por qué el embedding model cachea solo el primario

`_get_embedder()` cachea el modelo primario en `self._embedder` para reusarlo entre títulos. Los modelos secundarios (para comparación) se cargan y descartan. Esto evita consumir VRAM con 3 modelos simultáneamente.

### 5.3 Por qué HDBSCAN no reemplaza a K-Means

Ambos se ejecutan y se compara el resultado. K-Means es mejor cuando los clusters son esféricos y balanceados. HDBSCAN es mejor para densidad variable y outliers. Usar ambos y elegir da lo mejor de ambos mundos.

### 5.4 Por qué el combined score usa igual peso

Silhouette, DB y CH miden aspectos diferentes de la calidad del clustering. Sin una razón a priori para ponderar una más que otra, se usa peso igual. Si los resultados muestran que una métrica es más informativa para este dominio, se puede ajustar.

### 5.5 Determinismo

Todos los componentes usan `random_state=42`: subsample, embedding (no aplica), UMAP, K-Means, train/test split. Esto garantiza reproducibilidad.

---

## 6. Formato de Output

### `master_schemas.json`

```json
{
  "fraude y corrupcion": {
    "clusters": 7,
    "total_sections": 5000,
    "silhouette_scores": {
      "2": {"silhouette": 0.1, "davies_bouldin": 1.2, "calinski_harabasz": 300, "combined": 0.5, ...},
      ...
    },
    "cluster_counts": {"0": 676, ...},
    "samples_per_cluster": {
      "0": [{"nro_licitacion": "...", "year": 2023, "text": "...", "is_extreme": false}, ...]
    },
    "json_schema": {"campo": {"type": "binary", "description": "..."}},
    "schema_justification": "...",
    "extraction_prompt": "Eres un extractor...",
    "cluster_report": "Total sections: ...",
    "selected_method": "hdbscan",
    "selected_embedding_model": "paraphrase-multilingual-mpnet-base-v2",
    "selected_umap_params": {"n_components": 10, "n_neighbors": 30},
    "clustering_comparison": [
      {"embedding_model": "...", "clustering_method": "kmeans", ...}
    ]
  }
}
```

### `clustering_comparison.json`

```json
{
  "fraude y corrupcion": {
    "selected_method": "hdbscan",
    "selected_embedding_model": "paraphrase-multilingual-mpnet-base-v2",
    "selected_umap_params": {"n_components": 10, "n_neighbors": 30},
    "n_clusters": 8,
    "total_sections": 5000,
    "all_results": [
      {"embedding_model": "...", "clustering_method": "kmeans", "umap_n_components": 10, ...}
    ]
  }
}
```

---

## 7. Cómo Extender

### 7.1 Agregar un nuevo embedding model

1. Asegurarse de que esté disponible vía Sentence-Transformers
2. Agregarlo a `embedding_models_to_test` en el config (o en el runner `--compare`)

El modelo se cargará automáticamente bajo demanda.

### 7.2 Agregar una nueva configuración UMAP

Agregar un dict al `umap_params_grid`. Los parámetros se pasan como `**kwargs` a `_reduce_dimensions()`.

### 7.3 Agregar un nuevo método de clustering

1. Crear método `_cluster_{nombre}(self, X, ...)` que retorne `(labels, n_clusters, n_noise, extra_info)`
2. Agregar la evaluación en `_run_grid_comparison()` (para modo comparación) y en el bloque `else` de `process_title()` (para modo normal)
3. Si el nuevo método no produce centroides (como HDBSCAN), usar `_compute_centroids()`
4. Agregar entrada a `_make_comparison_entry()`

### 7.4 Agregar una nueva métrica de clustering

1. Importar de sklearn (o implementar)
2. Agregar en `_evaluate_clustering()` 
3. Agregar en `_find_optimal_k_combined()` en la normalización
4. Agregar en `_make_comparison_entry()` en la serialización

### 7.5 Cambiar el LLM provider

1. Modificar `_call_llm()` para usar el nuevo endpoint
2. O modificar `llm_model` en el config
3. El formato de mensajes (system/user) es OpenAI-compatible

### 7.6 Agregar más títulos

1. Agregar entrada en `TITLE_MAPPING` en `section_clustering_extractor.py`
2. La query `SECTIONS_BY_TITLE_QUERY` es genérica por `title_normalized`
3. Si hay otro título que requiera mapeo, agregarlo al dict

---

## 8. Tareas Pendientes (Próximos Pasos)

### 8.1 Extracción Masiva (pendiente)

**Archivos**: Nuevo script + posible nuevo pipeline

Conectar `master_schemas.json` → extraer features de TODAS las secciones (~300,000 totales). Reutilizar `LLMClient` y `LLMResultsLoader` existentes.

**Diseño propuesto**:

```
SectionExtractionRunner
├── Read master_schemas.json (schema + extraction_prompt per title)
├── For each title:
│   ├── Query ALL sections from DB (no limit)
│   ├── Filter already-processed (checkpoint via DB)
│   ├── Batch: for each N sections:
│   │   ├── Build extraction prompt with section text
│   │   ├── Call LLM (reuse LLMClient)
│   │   ├── Validate output vs json_schema
│   │   └── Save to dncp.section_extracted_features
│   └── Save checkpoint
└── Summary report
```

**Tabla DB propuesta**:
```sql
CREATE TABLE dncp.section_extracted_features (
    nro_licitacion VARCHAR(20) NOT NULL,
    title_normalized VARCHAR(200) NOT NULL,
    features JSONB NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    extraido_en TIMESTAMP DEFAULT NOW(),
    UNIQUE (nro_licitacion, title_normalized, modelo)
);
```

**Consideraciones**:
- ~300,000 LLM calls totales
- Necesita rate limiting + checkpointing
- Costo estimado: $30-$300 (dependiendo del modelo)
- Dry-run mode para estimar costo antes de ejecutar

### 8.2 Mejorar LLM model (pendiente)

El modelo actual `openai/gpt-oss-20b:free` es mediocre. Opciones:
- `deepseek/deepseek-v4-flash` (pago)
- `qwen/qwen3-coder` (pago)
- `meta-llama/llama-3.3-70b-instruct` (free pero rate-limited)

### 8.3 Anomaly Detection (pendiente)

Una vez que `section_extracted_features` tenga datos:
- Isolation Forest sobre vectores de features
- LOF (Local Outlier Factor)
- DBSCAN
- Reportar secciones anómalas por título

### 8.4 Tests (pendiente)

No existe directorio `tests/`. Prioridad:
1. Test de `_validate_schema()` con casos válidos e inválidos
2. Test de `_evaluate_clustering()` con datos sintéticos
3. Test de `_find_optimal_k_combined()` con clusters conocidos
4. Test de `_sample_clusters()` con HDBSCAN labels no contiguos
5. Test de integración: process_title con datos mock

### 8.5 Aumentar `max_samples_per_title`

De 5,000 a 10,000+ — ahora con GPU el embedding es barato.

### 8.6 Parametrizar conexión DB

La configuración DB en `settings.py` ya lee de `.env`, pero el host `172.31.233.136` está implícito en las variables de entorno. Documentar en `.env.example`.

---

## 9. Problemas Conocidos

### 9.1 hdbscan binary wheel

`hdbscan==0.8.42` requiere C++ build tools en Windows. Usar `hdbscan==0.8.40` que tiene wheel pre-compilado para `cp39-win_amd64`.

Instalación:
```powershell
pip install hdbscan==0.8.40 --only-binary :all:
```

### 9.2 labels no contiguos en sampling

HDBSCAN puede producir labels no secuenciales (ej: 0, 2, 5, -1). `_sample_clusters()` ahora usa un mapping explícito (`centroid_map`) para traducir label → índice en el array de centroides. También salta labels = -1 (ruido).

### 9.3 Ruido en cluster_report

Cuando HDBSCAN gana y hay ruido (-1), `_build_cluster_report()` muestra "Number of clusters" excluyendo -1, y agrega "Noise points: N (X.X%)".

### 9.4 Silhouette scores vacío en modo comparación

En `process_title()` modo comparación, `combined_scores` del grid no se pasa al return. `silhouette_scores` queda `{}`. Toda la información está en `clustering_comparison`.

### 9.5 PyTorch CUDA en venv

El `.venv` local no tiene torch CUDA directamente. Se usa un archivo `global.pth` en `site-packages` que expone los packages globales (donde torch CUDA está instalado). Alternativa: instalar torch CUDA en el venv (~3.5 GB).

### 9.6 batch_size de embeddings

`batch_size=32` es conservador para 16 GB VRAM. Puede aumentarse a 64-128 para acelerar embeddings si VRAM lo permite.

---

## 10. Referencia Rápida de Archivos

| Archivo | Líneas | Rol |
|---------|:------:|-----|
| `scripts/run_section_clustering_pipeline.py` | 103 | Entry point CLI |
| `src/pipelines/section_clustering_pipeline.py` | 187 | Orchestrador ETL |
| `src/etl/extractors/section_clustering_extractor.py` | 84 | Lector DB |
| `src/etl/transformers/section_clustering_transformer.py` | 1106 | **Core: clustering + LLM** |
| `src/etl/loaders/section_clustering_loader.py` | 248 | Persistencia JSON + MD |
| `config/settings.py` | 42 | Variables de entorno |
| `REPORTE_CLUSTERING_Y_ESQUEMAS.md` | 461 | Reporte histórico v1 |
| `requirements.txt` | 25 | Dependencias |

### Dependencias clave

```
sentence-transformers  → embeddings
umap-learn            → dimensionality reduction
scikit-learn          → K-Means, metrics
hdbscan               → HDBSCAN clustering
torch                 → GPU acceleration (CUDA 12.8)
psycopg2-binary       → PostgreSQL
requests              → LLM API calls
```

### Para empezar a trabajar

```powershell
# 1. Activar entorno
.\.venv\Scripts\Activate.ps1

# 2. Ejecutar (modo normal, ~10-12 min en GPU)
python scripts/run_section_clustering_pipeline.py

# 3. Ejecutar (modo comparación, ~20-30 min)
python scripts/run_section_clustering_pipeline.py --compare

# 4. Ver outputs
ls data/processed/section_clustering/
```

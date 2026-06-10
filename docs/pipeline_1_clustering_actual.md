# Pipeline 1: Section Clustering (Actual)

## Propósito
Agrupa secciones de pliegos de licitación en clusters semánticos, diseña un JSON schema por título para detectar anomalías, y genera prompts de extracción. Es el pipeline **actual**, completamente implementado.

---

## Arquitectura

```
DB (dncp.pliegos_secciones)
  -> SectionClusteringExtractor
    -> SectionClusteringTransformer
      -> embeddings (Sentence-Transformer)
      -> UMAP reduction
      -> K-Means (k óptimo)
      -> muestras representativas + extremas por cluster
      -> LLM genera JSON schema
    -> SectionClusteringLoader
      -> master_schemas.json
      -> reportes .md por título
      -> clustering_comparison.json
      -> llm_chat_prompts.json (opcional)
```

---

## Componentes

### `scripts/run_section_clustering_pipeline.py`
Entry point CLI. Parsea argumentos, ensambla configuración, ejecuta el pipeline.

| Flag | Default | Descripción |
|------|---------|-------------|
| `--compare` | False | Prueba múltiples embeddings + UMAP |
| `--skip-titles` | [] | Títulos a saltar |
| `--max-samples` | 5000 | Máx secciones por título |
| `--k-min` / `--k-max` | 2 / 20 | Rango de K para K-Means |
| `--llm-provider` | `local` | `local` o `openrouter` |
| `--export-chat-prompts` | False | Exporta prompts para LLM web |
| `--skip-llm` | False | Usa schema default (sin LLM) |

### `src/pipelines/section_clustering_pipeline.py`
Orquestador principal. Inicializa extractor, transformer, loader, y LLM provider. Ejecuta 7 pasos: extraer títulos → procesar cada título (cluster + LLM) → guardar master schema → guardar reportes → guardar samples → guardar comparación → exportar chat prompts.

### `src/etl/transformers/section_clustering_transformer.py`
Núcleo del pipeline (~1139 líneas).

| Método | Propósito |
|--------|-----------|
| `process_title()` | Workflow completo por título |
| `_get_embeddings()` | Codifica textos con Sentence-Transformer |
| `_reduce_dimensions()` | UMAP (fallback PCA) |
| `_find_optimal_k_combined()` | K óptimo: silhouette + Davies-Bouldin + Calinski-Harabasz |
| `_cluster()` | K-Means con n_init=10 |
| `_sample_clusters()` | 3 representativos + 2 extremos por cluster |
| `_generate_schema_via_llm()` | Envía samples al LLM, valida, reintenta, fallback |
| `_validate_schema()` | Máx 12 campos, solo binary/numeric, extraction_methods válidos |
| `build_section_extraction_prompt()` | Construye prompt para Pipeline 2 (filtra por importance≥threshold) |

### `src/etl/transformers/llm_provider.py`
Abstracción para llamadas LLM.

| Proveedor | Endpoint | Timeout | Reintentos |
|-----------|----------|---------|------------|
| `LocalLLMProvider` | `localhost:1234/v1` | 180s | 3 (backoff) |
| `OpenRouterProvider` | `openrouter.ai/api/v1` | 120s | 3 (backoff) |

### `src/etl/loaders/section_clustering_loader.py`
Persiste resultados a disco.

| Método | Archivo |
|--------|---------|
| `save_master_schema()` | `master_schemas.json` |
| `save_title_report()` | `report_{title}.md` |
| `save_clustering_comparison()` | `clustering_comparison.json` |
| `save_chat_prompts()` | `llm_chat_prompts.json` |
| `save_cluster_samples()` | `all_cluster_samples_raw/merged.json` |

### `src/etl/extractors/section_clustering_extractor.py`
Extrae secciones de la DB: `SELECT ... FROM dncp.pliegos_secciones WHERE LOWER(title_normalized) IN (...)`.

---

## Datos

| Métrica | Valor |
|---------|-------|
| Documentos | ~31,000 |
| Secciones totales | ~2,700,000 |
| Títulos procesados | 10 |
| Tamaño cluster promedio | 31,500 secciones |
| Muestras por título (clustering) | 5,000 (subsample) |
| Campos típicos por schema | 2-12 |

## Archivos de salida

```
data/processed/section_clustering/
  master_schemas.json            ← schemas + prompts por título
  all_cluster_samples_raw.json   ← samples crudos por cluster
  all_cluster_samples_merged.json ← clusters similares fusionados
  clustering_comparison.json     ← comparación de métodos
  llm_chat_prompts.json          ← prompts para LLM web
  report_{title}.md              ← reportes individuales (10)
```

## Prompts compartidos (`src/utils/prompt_utils.py`)

| Constante | Uso |
|-----------|-----|
| `SCHEMA_DESIGN_SYSTEM_PROMPT` | Diseño de schema vía clustering |
| `SCHEMA_VALIDATION_SYSTEM_PROMPT` | Reintento tras schema inválido |
| `EXTRACTION_SYSTEM_PROMPT` | Extracción de campos de secciones individuales |
| `build_extraction_prompt_text()` | Genera template legible |

## DB (lectura)
- `dncp.pliegos_secciones` — tabla fuente con `nro_licitacion`, `content_text`, `title_normalized`, `category_id`, `year`

---

## Mejoras de Claude (análisis externo)

Claude revisó la arquitectura de ambos pipelines identificando problemas concretos. Acciones viables para Pipeline 1:

| # | Hallazgo | Prioridad | Estado |
|---|----------|:---------:|:------:|
| 1 | **Merge por embedding similarity como segundo pass**: después del merge por difflib (>0.95), agregar un segundo merge por cosine similarity de centroides (>0.92). Los embeddings ya están calculados en `_sample_clusters()`, el costo es marginal. | Alta | ❌ Pendiente |
| 2 | **Stratified sampling**: el random sample de 5,000 sobre 31,000 puede perder clusters minoritarios (<2%). No implementar sin antes medir cuántos clusters se pierden actualmente. | Baja | ❌ En evaluación |
| 3 | **Truncado inteligente para Pipeline 2**: en `build_section_extraction_prompt()`, si el texto excede el límite, incluir siempre los últimos 300 chars además de los primeros N. Aplica al método compartido, el cambio está acá. | Alta | ❌ Pendiente |

### Detalle técnico — Merge por embedding similarity

```python
# Después del merge por difflib en process_title()
from sklearn.metrics.pairwise import cosine_similarity

def _merge_by_embedding_similarity(self, cluster_labels, embeddings, threshold=0.92):
    """Merge clusters whose centroids have cosine similarity > threshold."""
    unique_labels = sorted(set(cluster_labels) - {-1})
    centroids = {}
    for cid in unique_labels:
        mask = cluster_labels == cid
        centroids[cid] = embeddings[mask].mean(axis=0)
    # mergear pares con similitud > threshold
    ...
```

### Detalle técnico — Truncado con cola preservada

```python
# En build_section_extraction_prompt(), cuando se recibe el texto
MAX_CHARS = 2000
if len(texto) > MAX_CHARS:
    tail_chars = 300
    texto = texto[:MAX_CHARS - tail_chars] + "\n...[truncado]...\n" + texto[-tail_chars:]
```

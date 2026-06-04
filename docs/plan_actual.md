# Plan Actual — Pipeline de Clustering y Detección de Anomalías

---

## Metadata del Proyecto

| Campo                   | Valor                                                                                                                                                                                                                                                                                                                                                                       |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Branch activo**       | `feature/ocds-csv-pipeline`                                                                                                                                                                                                                                                                                                                                                 |
| **HEAD commit**         | `9d31adc` feat: add section clustering and title ranking pipelines                                                                                                                                                                                                                                                                                                          |
| **Branches existentes** | `develop`, `feature/llm-extraction-pipeline`, `feature/llm-titles-clasification`, `feature/masive-async-downloader`, `feature/ocds-csv-pipeline` (\*), `feature/outline_content_merged`, `feature/pdf-to-parquet-file-split-lines`, `main`                                                                                                                                  |
| **Remotes**             | `origin/HEAD -> origin/main`, `origin/develop`, `origin/feature/llm-extraction-pipeline`, `origin/feature/llm-titles-clasification`, `origin/feature/masive-async-downloader`, `origin/feature/ocds-csv-pipeline`, `origin/feature/ocds-pipeline-extract-pbc-url`, `origin/feature/outline_content_merged`, `origin/feature/pdf-to-parquet-file-split-lines`, `origin/main` |
| **Staged (sin commit)** | `config/settings.py`, `docs/readme_pipeline_extraction_runner.md`, `reporte_clustering_decisiones.md`, `scripts/run_section_clustering_pipeline.py`, `src/etl/loaders/section_clustering_loader.py`, `src/etl/transformers/llm_provider.py` (nuevo), `src/etl/transformers/section_clustering_transformer.py`, `src/pipelines/section_clustering_pipeline.py`               |

---

## 1. Conexión a Base de Datos

### Config (`config/settings.py`)

Lee de `.env` mediante `python-dotenv`:

```python
DB_CONFIG = {
    "user": os.getenv("DB_USER"),       # postgres
    "password": os.getenv("DB_PASSWORD"), # Temporal123
    "host": os.getenv("DB_HOST"),        # 172.31.233.136
    "port": int(os.getenv("DB_PORT")),   # 5433
    "database": os.getenv("DB_NAME"),    # dncp
}
```

### `.env` activo (NO subir a git)

```
DB_NAME=dncp
DB_USER=postgres
DB_PASSWORD=Temporal123
DB_HOST=172.31.233.136
DB_PORT=5433
LOGS_FILE=logs/dncp.log
BASE_LOGS_DIR=logs
DNCP_BASE_URL=https://www.contrataciones.gov.py
BASE_OUTPUT_RAW_DIR=./data/raw
BASE_OUTPUT_PROCESSED_DIR=./data/processed
BASE_INPUT_EXTERNAL_DATA_DIR=./data/external
LLM_USERNAME=mateoggv94
LLM_PASSWORD=pliegos-pbc-123
DEFAULT_BATCH_SIZE=10
DEFAULT_MAX_RETRIES=3
GROQ_API_KEY=tu_clave_aqui
OPEN_ROUTER_API_KEY=
```

### Credenciales alternativas (comentadas)

```python
# DB_NAME=pbc
# DB_USER=mateoggv94
# DB_PASSWORD=pliegos-dncp/.
# DB_HOST=144.33.29.61
# DB_PORT=5432
```

---

## 2. Tabla de Datos: `dncp.pliegos_secciones`

### Columnas relevantes

| Columna            | Tipo     | Descripción                                               |
| ------------------ | -------- | --------------------------------------------------------- |
| `nro_licitacion`   | text     | FK a licitación                                           |
| `title_normalized` | text     | Título normalizado de la sección (~323 valores distintos) |
| `content_text`     | text     | Contenido textual de la sección                           |
| `year`             | smallint | Año de la licitación (2021-2025)                          |
| `category_id`      | integer  | FK a categoría                                            |
| `content_length`   | integer  | Longitud en líneas                                        |
| `estimated_tokens` | integer  | Tokens estimados                                          |

### Estadísticas

- **Total registros**: 2,779,386
- **Documentos únicos**: 31,321
- **Secciones por documento**: ~89 promedio
- **Títulos normalizados**: 323
- **Categorías**: 82
- **Años**: 2021-2025

---

## 3. Feature Selection (Análisis Completado)

### Archivos

| Archivo                                                 | Descripción                         |
| ------------------------------------------------------- | ----------------------------------- |
| `informe_features.md` (240 lines)                       | Reporte de selección de features v1 |
| `informe_features_v2.md` (161 lines)                    | Reporte de selección de features v2 |
| `readme_estrategias_analisis_feature_db.md` (369 lines) | Estrategias de feature engineering  |
| `step4_engineer_title_features_v2.py`                   | Script de ingeniería de features    |
| `step5_clean_features.py`                               | Limpieza de features                |
| `step6_feature_selection_full.py`                       | Selección de features               |

### 234 Features a nivel documento

- `has_*` (60): presencia binaria de sección por título
- `len_*` (80): sum(content_length) por título
- `tok_*` (80): sum(estimated_tokens) por título
- 14 agregadas: total_sections, unique_titles, avg/max/std/sum de length/tokens/bytes

### Top 10 Features

| Feature                                             | Score | Tipo     |
| --------------------------------------------------- | :---: | -------- |
| `has_interpretacion`                                | 0.585 | binaria  |
| `has_retiro_sustitucion_y_modificacion_de_las_ofer` | 0.558 | binaria  |
| `has_formas_y_condiciones_de_pago`                  | 0.552 | binaria  |
| `has_garantias:_instrumentacion_plazos_y_ejecucion` | 0.546 | binaria  |
| `has_oferentes_en_consorcio`                        | 0.536 | binaria  |
| `len_disconformidad_errores_y_omisiones`            | 0.530 | longitud |
| `has_identificacion_de_la_unidad_solicitante_y_jus` | 0.523 | binaria  |
| `len_periodo_de_validez_garantia_mantenimiento`     | 0.518 | longitud |
| `has_planos_y_disenos`                              | 0.512 | binaria  |
| `tok_disconformidad_errores_y_omisiones`            | 0.510 | tokens   |

### Scoring Formula

```
final_score = 0.25 * outlier_score + 0.30 * domain_score + 0.25 * rf_importance
              + 0.10 * if_importance - 0.05 * vif_penalty - 0.05 * null_penalty
```

---

## 4. Pipeline de Clustering (SectionClusteringPipeline)

### Archivos del Pipeline

| Archivo                                                  | Líneas | Propósito                                                      |
| -------------------------------------------------------- | :----: | -------------------------------------------------------------- |
| `src/etl/extractors/section_clustering_extractor.py`     |   84   | Query secciones por título desde DB                            |
| `src/etl/transformers/section_clustering_transformer.py` |  1117  | Clustering, sampling, LLM schema, merging, chat prompts        |
| `src/etl/transformers/llm_provider.py`                   |  147   | Provider abstracto + Local + OpenRouter                        |
| `src/etl/loaders/section_clustering_loader.py`           |  261   | Guardar master_schemas.json, reports, chat prompts             |
| `src/pipelines/section_clustering_pipeline.py`           |  217   | Orquestación completa                                          |
| `scripts/run_section_clustering_pipeline.py`             |  132   | CLI entry point                                                |
| `scripts/update_schemas.py`                              |  450   | Script para actualizar schemas manuales en master_schemas.json |
| `docs/readme_pipeline_extraction_runner.md`              |  218   | Plan del extraction runner (próximo pipeline)                  |

### SectionClusteringExtractor (`section_clustering_extractor.py`)

**Clase**: `SectionClusteringExtractor`

**Query SQL**:

```sql
SELECT nro_licitacion, content_text, year, category_id
FROM dncp.pliegos_secciones
WHERE LOWER(title_normalized) = :title
  AND content_text IS NOT NULL
```

**Title Mapping** (nombres display → nombres reales en BD):

```python
TITLE_MAPPING = {
    "fraude y corrupcion": "fraude y corrupcion",
    "formato y firma de la oferta": "formato y firma de la oferta",
    "copias de la oferta cps": "copias de la oferta - cps",
    "limitacion de responsabilidad": "limitacion de responsabilidad",
    "planos y disenos": "planos y disenos",
    "porcentaje de garantia de fiel cumplimiento de con":
        "porcentaje de garantia de fiel cumplimiento de contrato",
    "idioma de la oferta": "idioma de la oferta",
    "aclaracion de las ofertas": "aclaracion de las ofertas",
    "retiro sustitucion y modificacion de las ofertas":
        "retiro, sustitucion y modificacion de las ofertas",
    "audiencia informativa": "audiencia informativa",
}
TOP_10_TITLES = list(TITLE_MAPPING.keys())
DB_TITLE_MAP = {v: k for k, v in TITLE_MAPPING.items()}
```

**Método principal**:

```python
def extract_all_titles(self) -> Dict[str, pd.DataFrame]:
```

Retorna `{"titulo_display": DataFrame}` con columnas `nro_licitacion`, `content_text`, `year`, `category_id`.
Filtra `content_text` no nulos y no vacíos.

### SectionClusteringTransformer (`section_clustering_transformer.py`)

**Clase**: `SectionClusteringTransformer`

**Constructor**:

```python
def __init__(
    self,
    llm_provider: LLMProvider,
    embedding_model: str = "paraphrase-multilingual-MiniLM-L12-v2",
    random_state: int = 42,
    k_range: Tuple[int, int] = (2, 20),
    validate_schema: bool = True,
    schema_validation_retries: int = 2,
    compare_embeddings: bool = False,
    embedding_models_to_test: Optional[List[str]] = None,
    umap_params_grid: Optional[List[Dict[str, Any]]] = None,
    skip_llm: bool = False,
)
```

**Método principal**:

```python
def process_title(
    self, title: str, df: pd.DataFrame, max_samples: int = 5000
) -> Dict[str, Any]:
```

**Pipeline interno** (single-model path, cuando `compare_embeddings=False`):

1. **Subsample**: `df.sample(n=max_samples, random_state=self.random_state)` si `len(df) > max_samples`
2. **Embeddings**: `self._get_embeddings(texts)` → SentenceTransformer encode con batch_size=32
3. **UMAP**: `self._reduce_dimensions(embeddings, n_components=10, n_neighbors=15)` → 384d → 10d
   - Fallback a PCA si `umap-learn` no está instalado
4. **Optimal k**: `self._find_optimal_k_combined(reduced)` → grid search k∈[2,20]
   - Métricas: silhouette + Davies-Bouldin + Calinski-Harabasz
   - Combina: `sil_norm + (1-db_norm) + ch_norm`
   - Selecciona k con combined score máximo
5. **K-Means**: `self._cluster(reduced, optimal_k)` → labels + centroids
6. **Sampling**: `self._sample_clusters(df, reduced, kmeans=kmeans_obj)`
   - `n_near=3` (representativos, más cercanos al centroide)
   - `n_far=2` (extremos, más lejanos del centroide)
   - Textos truncados a 2000 chars en samples
   - Distancia: `np.linalg.norm(cluster_emb - centroid, axis=1)` en espacio reducido
7. **Schema via LLM**: `self._generate_schema_via_llm(title, samples)` (si `skip_llm=False`)
   - Si `skip_llm=True`: usa `self._default_schema(title)` — esquema de 2 campos genérico
8. **Extraction prompt**: `self._build_extraction_prompt(title, schema)`
9. **Report**: `self._build_cluster_report(df, samples)`

### LLM Provider (`llm_provider.py`)

**Clase abstracta**:

```python
class LLMProvider(ABC):
    @abstractmethod
    def generate(self, messages, response_format=None, max_tokens=4096, temperature=0.0) -> str: ...
    @abstractmethod
    def name(self) -> str: ...
```

**LocalLLMProvider**:

- Endpoint: `http://localhost:1234/v1` (configurable via `--llm-base-url`)
- POST `/chat/completions` con payload OpenAI-compatible
- `response_format` soportado (ej: `{"type": "json_object"}`)
- Timeout: 180s
- Retries: 3 con backoff exponencial (1s, 2s)
- Fallback: `{"schema": {}, "justification": "LLM call failed after 3 retries."}`

**OpenRouterProvider**:

- URL: `https://openrouter.ai/api/v1`
- Auth: `Authorization: Bearer {api_key}`
- Modelo default: `openai/gpt-oss-20b:free`
- Timeout: 120s
- Retries: 3 con backoff exponencial (1s, 2s)
- API Key en `.env`: `OPEN_ROUTER_API_KEY`

### Schema Generation via LLM

**Prompt de diseño** (`SCHEMA_DESIGN_SYSTEM_PROMPT`):

- System: experto en detección de anomalías en pliegos paraguayos
- User: clusters con muestras + instrucción de diseñar esquema
- Restricciones: solo binary/numeric, ≤12 campos, al mismo nivel
- Formato respuesta: JSON con `schema` + `justification`
- `response_format: {"type": "json_object"}` enviado al provider

**Validación** (`_validate_schema`):

```python
def _validate_schema(self, result: dict) -> Tuple[bool, str]:
    # Verifica: schema existe, es dict, 1-12 campos
    # Cada campo: type ∈ {binary, numeric}, description no vacía
    # Sin claves extra (solo type, description)
    # justification no vacía
```

**Retry**: hasta `1 + schema_validation_retries` (default 3) intentos

- Attempt 0: `SCHEMA_DESIGN_SYSTEM_PROMPT`
- Attempt 1-2: `SCHEMA_VALIDATION_SYSTEM_PROMPT` con `{razon_rechazo}`
- Fallback: `_default_schema(title)` → `tiene_{slug}` (binary) + `longitud_texto_{slug}` (numeric)

### Optimizaciones de Chat Prompts

**Cluster merging** (`_merge_similar_clusters` @staticmethod):

- Threshold: `difflib.SequenceMatcher` ratio ≥ 0.95
- Compara textos representativos (primer no-extremo)
- IDs fusionados: `"0_3_7_9"` indicando qué clusters originales se combinaron
- Dedup: `s["text"][:300]` como key para evitar textos duplicados
- `cluster_counts` se suman para clusters fusionados

**Build chat prompt** (`_build_chat_prompt`):

```python
def _build_chat_prompt(
    self,
    title: str,
    samples: Dict[str, List[Dict[str, Any]]],
    total_sections: int,
    cluster_counts: Dict[str, int],
    samples_per_cluster: int = 2,
    truncation_chars: Optional[int] = None,
    merge_similar_clusters: bool = True,
    merge_similarity_threshold: float = 0.95,
) -> Dict[str, Any]:
```

Retorna:

````json
{
  "title": "...",
  "total_sections": 32180,
  "n_clusters": 10,
  "cluster_counts": {"0_3": 15000, "5": 5000, ...},
  "truncation_chars": 2000,
  "samples_per_cluster": 2,
  "prompt": {"system": "...", "user": "..."},
  "expected_output": "```json\\n{\\n  \\"schema\\": {...}"
}
````

**Extraccion prompt** (`_build_extraction_prompt`):

- Template: `EXTRACTION_PROMPT_TEMPLATE` (constante global, line 98)
- Placeholders: `{titulo}`, `{titulo_descripcion}`, `{esquema_str}`, `{texto}`
- `{texto}` queda como literal `{texto}` para reemplazo posterior
- Descripciones de título hardcodeadas en `titulo_descripcion` dict (line 1028-1039)

### SectionClusteringLoader (`section_clustering_loader.py`)

**Métodos**:

```python
save_master_schema(results, filename="master_schemas.json") -> str  # path
save_title_report(title, data, filename=None) -> str
save_clustering_comparison(results, filename="clustering_comparison.json") -> str
save_chat_prompts(prompts, filename="llm_chat_prompts.json") -> str
```

**Output dir default**: `data/processed/section_clustering/`

### SectionClusteringPipeline (`section_clustering_pipeline.py`)

**Método `run()`**:

1. Extract all titles
2. For each title: `transformer.process_title(title, df, max_samples)`
3. Save master schema → `master_schemas.json`
4. Save per-title reports → `report_{title}.md`
5. Save clustering comparison → `clustering_comparison.json`
6. If `export_chat_prompts`: build + save → `llm_chat_prompts.json`
7. Log summary table

---

## 5. CLI: `scripts/run_section_clustering_pipeline.py`

### Flags

```
python scripts/run_section_clustering_pipeline.py                     # Local LLM, default
python scripts/run_section_clustering_pipeline.py --compare           # Grid de embeddings+UMAP
python scripts/run_section_clustering_pipeline.py --skip-titles fraude  # Saltar títulos
python scripts/run_section_clustering_pipeline.py --max-samples 1000  # Subsample más chico
python scripts/run_section_clustering_pipeline.py --k-min 3 --k-max 15
python scripts/run_section_clustering_pipeline.py --llm-provider openrouter  # OpenRouter
python scripts/run_section_clustering_pipeline.py --llm-provider openrouter --llm-model "openai/gpt-4o-mini"
python scripts/run_section_clustering_pipeline.py --llm-base-url http://localhost:8080/v1
python scripts/run_section_clustering_pipeline.py --export-chat-prompts  # + chat prompts JSON
python scripts/run_section_clustering_pipeline.py --skip-llm           # Sin LLM, esquema default
```

### Config pasada al pipeline

```python
config = {
    "db_params": DB_CONFIG,              # de config/settings.py
    "llm_provider_type": "local",        # o "openrouter"
    "llm_base_url": "http://localhost:1234/v1",
    "llm_api_key": OPENROUTER_API_KEY,
    "llm_model": "openai/gpt-oss-20b:free",
    "embedding_model": "paraphrase-multilingual-MiniLM-L12-v2",
    "output_dir": "./data/processed/section_clustering",
    "base_output_dir": "./data/processed",
    "skip_titles": [],
    "max_samples_per_title": 5000,
    "k_range": (2, 20),
    "validate_schema": True,
    "schema_validation_retries": 2,
    "compare_embeddings": False,          # True si --compare
    "export_chat_prompts": False,
    "skip_llm": False,
}
```

---

## 6. Script de Actualización Manual: `scripts/update_schemas.py`

### Propósito

Actualizar `master_schemas.json` con schemas diseñados manualmente (no por LLM).

### Schemas Actuales (hardcodeados en el script)

| Título                                             | Campos |  Tipo predominante  |
| -------------------------------------------------- | :----: | :-----------------: |
| fraude y corrupcion                                |   9    | 8 binary, 1 numeric |
| formato y firma de la oferta                       |   8    | 7 binary, 1 numeric |
| copias de la oferta cps                            |   7    | 6 binary, 1 numeric |
| limitacion de responsabilidad                      |   8    | 8 binary, 0 numeric |
| planos y disenos                                   |   8    | 7 binary, 1 numeric |
| porcentaje de garantia de fiel cumplimiento de con |   7    | 5 binary, 2 numeric |
| idioma de la oferta                                |   6    | 6 binary, 0 numeric |
| aclaracion de las ofertas                          |   8    | 7 binary, 1 numeric |
| retiro sustitucion y modificacion de las ofertas   |   8    | 8 binary, 0 numeric |
| audiencia informativa                              |   8    | 7 binary, 1 numeric |

### Uso

```bash
python scripts/update_schemas.py
# Lee ./data/processed/section_clustering/master_schemas.json
# Actualiza json_schema + schema_justification + extraction_prompt
# Guarda de vuelta
```

### Template de extraction prompt (duplicado en `update_schemas.py`)

```python
EXTRACTION_PROMPT_TEMPLATE = """Eres un extractor de datos estructurados...
...
RESPUESTA (solo JSON):"""
```

**NOTA**: La función `build_extraction_prompt` y `TITULO_DESCRIPCION` están duplicadas entre `section_clustering_transformer.py` y `update_schemas.py`. Cualquier cambio en una debe reflejarse en la otra.

---

## 7. Hardware

| Componente               | Especificación                                           |
| ------------------------ | -------------------------------------------------------- |
| **GPU**                  | RTX 4070 Ti SUPER (16 GB VRAM)                           |
| **RAM**                  | 16 GB                                                    |
| **Local LLM**            | Qwen 2.5 14B Q5_K_L (Bartowski)                          |
| **Tamaño modelo**        | ~11 GB (12.5 GB en disco)                                |
| **KV Cache**             | Q8 (`-ctk q8_0 -ctv q8_0`)                               |
| **VRAM total necesaria** | ~14.2 GB (modelo 12.5 GB + KV cache 2.9 GB para 36K ctx) |
| **Margen**               | ~0.6 GB libre                                            |
| **Endpoint local**       | `http://localhost:1234/v1`                               |
| **Servidor**             | LM Studio o llama-server                                 |
| **7B alternativo**       | Qwen 2.5 7B Q4_K_M (~4.5 GB, 85 tok/s)                   |
| **7B VRAM**              | ~6.2 GB (modelo 4.5 GB + KV cache 1.7 GB)                |

---

## 8. Archivos Generados

| Archivo                    | Ruta                                                           |    Tamaño    |
| -------------------------- | -------------------------------------------------------------- | :----------: |
| master_schemas.json        | `data/processed/section_clustering/master_schemas.json`        |    888 KB    |
| llm_chat_prompts.json      | `data/processed/section_clustering/llm_chat_prompts.json`      |    115 KB    |
| clustering_comparison.json | `data/processed/section_clustering/clustering_comparison.json` |      —       |
| report\_\*.md              | `data/processed/section_clustering/report_*.md`                | ~10 archivos |

### Contenido de master_schemas.json

```json
{
  "fraude y corrupcion": {
    "clusters": 10,
    "total_sections": 32180,
    "silhouette_scores": {"3": 0.45, "4": 0.52, ..., "20": 0.38},
    "cluster_counts": {"0_3_7_9_18_19": 15200, "5": 8100, ...},
    "samples_per_cluster": {
      "0_3_7_9_18_19": [
        {"nro_licitacion": "...", "year": 2023, "category_id": 5,
         "text": "...", "is_extreme": false}
      ]
    },
    "json_schema": {"campo": {"type": "binary", "description": "..."}},
    "schema_justification": "...",
    "extraction_prompt": "Eres un extractor...",
    "cluster_report": "Total sections: 5000\n...",
    "selected_method": "kmeans",
    "selected_embedding_model": "paraphrase-multilingual-MiniLM-L12-v2",
    "selected_umap_params": {"n_components": 10, "n_neighbors": 15},
    "clustering_comparison": [...]
  },
  ...
}
```

---

## 9. Dependencias (requirements.txt)

```
pandas, requests, rarfile, PyPDF2, pdfplumber, asyncio, aiohttp, aiofiles,
tqdm, resource, pyarrow, sqlalchemy, psycopg2-binary, python-dotenv, tika,
cryptography==39.0.1, scikit-learn, sentence-transformers, umap-learn, nltk,
matplotlib, openpyxl, lxml, hdbscan
```

**NOTA**: `hdbscan` está en requirements pero ya no se usa en el código (fue eliminado). `psycopg2-binary` aparece duplicado.

---

## 10. Resultados de Clustering (post-optimización)

| Título                     | Secciones | Clusters raw | Post-merge | Trunc (chars) |
| -------------------------- | :-------: | :----------: | :--------: | :-----------: |
| fraude y corrupcion        |  32,180   |      20      |     10     |     2000      |
| formato y firma            |  32,180   |      19      |     6      |      870      |
| copias de la oferta        |  31,298   |      10      |     3      |      589      |
| limitacion responsabilidad |  30,320   |      9       |     5      |      757      |
| planos y disenos           |  28,924   |      20      |   **3**    |      500      |
| porcentaje garantia        |  32,180   |      —       |     —      |      564      |
| idioma de la oferta        |  31,653   |      12      |     4      |      516      |
| aclaracion ofertas         |  32,180   |      —       |     —      |     1182      |
| retiro sustitucion         |  31,900   |      19      |     11     |     1759      |
| audiencia informativa      |  32,180   |      20      |     10     |      634      |

---

## 11. Logging

**Config**: `src/utils/logging_utils.py`

- Logger por módulo
- Stream handler (stdout) + File handler (`logs/{module}.log`)
- Formato: `[%H:%M:%S] [LEVEL] message` (stdout) / `[%Y-%m-%d %H:%M:%S] [LEVEL] message` (file)
- Encoding: UTF-8

**Error handler**: `src/utils/error_handler.py`

- Decorador `@error_handling(default_return=...)`
- Captura excepciones, loggea traceback, retorna default

---

## 12. Decisiones Técnicas Documentadas

| Decisión            | Detalle                                              |
| ------------------- | ---------------------------------------------------- |
| HDBSCAN eliminado   | Siempre producía 45-58 micro-clusters, inconsistente |
| K-Means + UMAP      | En lugar de clustering en espacio original           |
| k-max=20            | 6/10 títulos se benefician de k>15                   |
| Combined metric     | silhouette + Davies-Bouldin + CH normalizado         |
| 2 samples/cluster   | 1 rep + 1 extreme (antes: 3 rep + 2 extreme)         |
| Truncado dinámico   | median×1.5 clamped [500, 2000]                       |
| Cluster merging     | difflib ≥ 0.95                                       |
| Export chat prompts | Puente entre pipeline y web LLM                      |
| Skip LLM            | `--skip-llm` regenera export sin LLM (~7 min)        |
| Provider abstracto  | ABC con Local + OpenRouter                           |
| Fallback schema     | 2 campos genéricos cuando LLM falla                  |

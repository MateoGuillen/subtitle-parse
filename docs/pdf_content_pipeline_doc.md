# PDF Content Pipeline — Documentación para LLM

## 1. Estilo de Programación del Proyecto

### Arquitectura General: ETL por Pipeline

Cada pipeline sigue el patrón **Script → Pipeline → Extractor → Transformer → Loader**:

```
scripts/run_*.py        ← Entry point (configura paths, args, instancia Pipeline)
src/pipelines/*.py      ← Orquestador (coordinación ETL, logging, batch control)
src/etl/extractors/*.py ← Lectura de datos (parquet, CSV, DB, API)
src/etl/transformers/*.py ← Lógica de negocio (match, clean, cluster, etc.)
src/etl/loaders/*.py    ← Escritura de resultados (parquet, CSV, PostgreSQL)
src/core/entities/*.py  ← Data classes @dataclass con to_dict() y get_schema() (PyArrow)
src/core/exceptions/*.py ← PipelineError → ExtractionError, TransformationError, LoadError
src/utils/*.py          ← logging_utils, error_handler (decorator), file_utility, etc.
config/settings.py      ← Variables de entorno (.env)
```

### Reglas del Patrón

1. **Cada pipeline es una clase** con constructor `__init__(self, config: dict)` y método `run()`.
2. **Cada componente ETL** tiene su propio logger: `self.logger = setup_logger(__name__)`.
3. **Decorador `@error_handling(default_return=...)`** envuelve métodos que pueden fallar. Captura excepciones, logea traceback, retorna `default_return`.
4. **Logging dual**: consola (formato `[HH:MM:SS] [LEVEL] msg`) + archivo en `{BASE_LOGS_DIR}/{module}.log`.
5. **Outputs intermedios en Parquet** (particionado por año). Usan `pyarrow.parquet` con compression snappy.
6. **Los scripts entry-point** hacen `sys.path.insert(0, ...)` para resolver imports.
7. **Config desde `.env`**: rutas BASE_*, credenciales DB, endpoints LLM.
8. **Data directories**: `data/raw/`, `data/processed/`, `data/external/`.
9. **Idempotencia**: checkpoints en disco (archivos `.done_*`) para reanudar pipelines.
10. **Batch processing**: los pipelines grandes iteran en batches para limitar RAM.

### Excepciones Personalizadas

| Excepción | Cuándo se usa |
|-----------|--------------|
| `ExtractionError(source, msg)` | Falla al leer datos |
| `TransformationError(etapa, msg)` | Falla al transformar |
| `LoadError(destino, msg)` | Falla al escribir |
| `PipelineError(msg)` | Base class |

### Data Models (`src/core/entities/`)

- `@dataclass` con tipo de cada campo.
- Método `get_schema() -> pa.Schema` para esquema PyArrow.
- Método `to_dict() -> Dict` para convertir a diccionario plano.
- Métodos helper: `get_content_text()`, `estimate_tokens()`, `get_word_count()`, etc.

---

## 2. Pipeline: PDF Content Extraction

### Posición en el Flujo General

```
Pipeline anterior                          Este pipeline                          Pipeline siguiente
┌──────────────────────┐                  ┌──────────────────────┐                ┌──────────────────────────┐
│ run_pdf_parquet      │  ──► parquet ──► │ run_pdf_content      │  ──► parquet ──►│ run_content_cleaning     │
│ (PDF→líneas Tika)    │                  │ (outlines + líneas)  │                │ (limpia texto secciones) │
└──────────────────────┘                  └──────────────────────┘                └──────────────────────────┘
```

**Inputs que consume:**
1. `data/processed/parquet/pdf-to-parquet/combined_documents_all_years.parquet` — todas las líneas de texto de los PDFs (de Tika)
2. `data/processed/parquet/merged_outlines.parquet` — outlines (TOC) de todos los PDFs

**Outputs que produce:**
1. `data/processed/outlines/merged_outlines_with_position_in_content.parquet` — outlines con línea de inicio resuelta
2. `data/processed/sections/year={year}/part-*.parquet` — secciones particionadas por año

### Entry Point: `scripts/run_pdf_content_pipeline.py`

```python
# 1. Construye paths absolutos desde BASE_OUTPUT_PROCESSED_DIR
# 2. Crea directorios de output
# 3. Arma config dict con 5 keys:
#    - pdf_lines_path
#    - outlines_path
#    - outlines_with_position_in_content_path
#    - content_sections_path
#    - content_sections_dir
# 4. Instancia PdfContentPipeline(config)
# 5. Ejecuta pipeline.run()
```

### Pipeline: `src/pipelines/pdf_content_pipeline.py` (class `PdfContentPipeline`)

**Atributos:**
- `self.config` — dict de paths
- `self.extractor` → `PdfContentExtractor`
- `self.transformer` → `PdfContentTransformer`
- `self.loader` → `PdfContentLoader`
- `self.logger` → `setup_logger(__name__)`

**Método `run()` — Algoritmo:**

```
1. Cargar outlines_df desde merged_outlines.parquet
2. Extraer lista unique de document_ids
3. Iterar pdf_lines.parquet en row_batches de 200K filas
4. Para cada batch:
   a. Filtrar líneas que pertenecen a document_ids de interés
   b. Acumular en line_buffers (dict[doc_id → DataFrame])
   c. Cuando hay ≥1000 doc_ids en buffer, procesar batch:
      → _process_batch()
5. Procesar buffer remanente
```

**Método `_process_batch()` — Algoritmo:**

```
1. Concatenar line_buffers → pdf_lines_chunk
2. Filtrar outlines_chunk para esos doc_ids
3. transformer.match_titles_with_lines(outlines_chunk, pdf_lines_chunk)
   → asigna line_number a cada outline
4. Escribir outlines_with_position a parquet (append)
5. transformer.preprocess_dataframes()
   → pdf_lines_dict (dict anidado: doc_id → page → list(lines))
   → sorted_outlines_df
6. transformer.extract_content_sections(pdf_lines_dict, sorted_outlines_df)
   → List[ContentSection]
7. transformer.prepare_sections_dataframe(sections) → DataFrame
8. loader.save_content_sections_partitioned(df, content_sections_dir)
9. Liberar line_buffers
```

### Extractor: `src/etl/extractors/pdf_content_extractor.py` (class `PdfContentExtractor`)

| Método | Input | Output | Propósito |
|--------|-------|--------|-----------|
| `load_single_dataframe(path)` | ruta parquet | `DataFrame` o `None` | Carga cualquier parquet |
| `load_dataframes(pdf_lines_path, outlines_path)` | 2 rutas | `(DataFrame, DataFrame)` | Carga ambos inputs |
| `get_document_ids(pdf_lines_path)` | ruta parquet | `list[int]` | Obtiene doc_ids únicos sin cargar todo |
| `load_lines_for_documents(pdf_lines_path, doc_ids)` | ruta + ids | `DataFrame` | Carga líneas filtradas por doc_id |
| `convert_df_to_pdflines(pdf_lines_df)` | DataFrame | `List[PDFLine]` | Convierte a objetos |
| `convert_df_to_pdfoutlines(outlines_df)` | DataFrame | `List[PDFOutline]` | Convierte a objetos |
| `save_as_parquet(df, schema, output_path)` | DF + schema + ruta | `None` | Guarda con schema |

### Transformer: `src/etl/transformers/pdf_content_transformer.py` (class `PdfContentTransformer`)

| Método | Input | Output | Propósito |
|--------|-------|--------|-----------|
| `clean_text(texto)` | str | str | Normaliza espacios |
| `_normalize_exact(text)` | str | str | Lowercase + sin acentos + sin puntuación |
| `match_titles_with_lines(outlines_df, pdf_lines_df)` | 2 DataFrames | DataFrame | Cruza outlines con líneas por título=página. Fallback con búsqueda normalizada ±5 páginas |
| `preprocess_dataframes(pdf_lines_df, outlines_df)` | 2 DataFrames | `(dict, DataFrame)` | Crea dict anidado doc→page→lines + ordena outlines |
| `get_section_content(pdf_lines_dict, doc_id, start_page, end_page, start_line, end_line)` | dict + params | `List[str]` | Extrae líneas entre 2 puntos |
| `extract_content_sections(pdf_lines_dict, outlines_df)` | dict + DF | `List[ContentSection]` | Itera outlines de cada doc. Determina end usando 5 casos. Genera synthetic `__END_OF_DOCUMENT__` marker |
| `prepare_sections_dataframe(sections)` | `List[ContentSection]` | DataFrame | Convierte a DF plano con schema fields |

**Casos de determinación de fin de sección** (dentro de `extract_content_sections`):

| Caso | Condición | Comportamiento |
|------|-----------|----------------|
| 1 | Siguiente outline: misma página + line_number | Corta en esa línea |
| 2 | Siguiente outline: otra página + line_number | Corta al final de la página anterior |
| 3 | Siguiente outline: otra página + SIN line_number | Corta al final de la página anterior |
| 4 | Siguiente outline: misma página + SIN line_number | Busca el próximo outline con line_number |
| 5 | No hay más outlines con line_number | Corta al inicio de página siguiente + 1 |

**Depth 1**: Los outlines de profundidad 1 son títulos generales (boundaries virtuales). No generan sección pero sirven como límite.

### Loader: `src/etl/loaders/pdf_content_loader.py` (class `PdfContentLoader`)

| Método | Input | Output | Propósito |
|--------|-------|--------|-----------|
| `save_outlines_with_lines(merged_df, output_path)` | DF + ruta | DataFrame | Guarda outlines con posición |
| `save_content_sections(sections_df, output_path, include_metrics)` | DF + ruta + bool | DataFrame | Guarda secciones en parquet (chunked writer) |
| `save_content_sections_partitioned(sections_df, output_dir)` | DF + directorio | `None` | Guarda particionado por `year=` usando `pq.write_to_dataset` |

### Data Models Relevantes: `src/core/entities/pdf_models.py`

**`ContentSection`** — campos:
- `document_id`, `nro_licitacion`, `category_id`, `year` (identificación)
- `title`, `page`, `line_start`, `line_end`, `depth` (posición)
- `content` (lista de strings — raw lines)
- Campos computados en `to_dict()`: `content_length`, `estimated_tokens`, `word_count`, `character_count`, `size_bytes`

**`PDFOutline`** — campos: `document_id`, `title`, `page`, `depth`, `nro_licitacion`, `category_id`, `year`, `line_number`

**`PDFLine`** — campos: `document_id`, `page_number`, `line_number`, `line_text`, `processed_date`

---

## 3. Mapa de Archivos del Pipeline

```
scripts/run_pdf_content_pipeline.py           ← Entry point (configura paths, ejecuta)
├── src/pipelines/pdf_content_pipeline.py     ← Orquestador (PdfContentPipeline class)
│   ├── src/etl/extractors/pdf_content_extractor.py  ← Lee parquet (líneas + outlines)
│   ├── src/etl/transformers/pdf_content_transformer.py ← Match + extracción de secciones
│   └── src/etl/loaders/pdf_content_loader.py    ← Escribe parquet (particionado)
├── src/core/entities/pdf_models.py           ← ContentSection, PDFOutline, PDFLine
├── src/utils/logging_utils.py               ← setup_logger()
├── src/utils/error_handler.py               ← @error_handling decorator
├── src/core/exceptions/exceptions.py        ← PipelineError, ExtractionError, etc.
└── config/settings.py                       ← BASE_OUTPUT_PROCESSED_DIR, etc.
```

### Pipelines Vecinos (mismo patrón ETL)

| Pipeline | Script | Clase Pipeline | Extractor | Transformer | Loader |
|----------|--------|---------------|-----------|-------------|--------|
| Outline Extraction | `run_pdf_outline_pipeline.py` | `PDFOutlinePipeline` | `PDFOutlineExtractor` | `OutlineProcessor`, `OutlineMerger` | `CSVOutlineLoader` |
| Content Cleaning | `run_content_cleaning_pipeline.py` | `ContentCleaningPipeline` | `ContentCleaningExtractor` | `ContentCleaningTransformer` | `ContentCleaningLoader` |
| Sections → DB | `run_sections_to_db_pipeline.py` | `SectionsToDbPipeline` | `SectionsDbExtractor` | `SectionsDbTransformer` | `SectionsDbLoader` |
| Section Clustering | `run_section_clustering_pipeline.py` | `SectionClusteringPipeline` | `SectionClusteringExtractor` | `SectionClusteringTransformer` | `SectionClusteringLoader` |
| OCDS CSV | `run_ocds_csv_pipeline.py` | `OcdsCsvPipeline` | `OcdsCsvExtractor` | `OcdsTenderTransformer`, etc. | `OcdsLoader` |

### Directorios de Datos

| Directorio | Contenido | Pipeline que lo produce |
|-----------|-----------|------------------------|
| `data/raw/pdf/{year}/` | PDFs descargados | `run_async_download_pipeline` |
| `data/raw/csv/` | CSVs OCDS crudos | `run_ocds_pipeline` |
| `data/processed/parquet/merged_outlines.parquet` | Outlines consolidados | `run_pdf_outline_pipeline` |
| `data/processed/parquet/pdf-to-parquet/combined_documents_all_years.parquet` | Líneas de todos los PDFs | `run_pdf_parquet_pipeline` |
| `data/processed/outlines/merged_outlines_with_position_in_content.parquet` | Outlines con línea de inicio | **este pipeline** |
| `data/processed/sections/year={year}/part-*.parquet` | Secciones extraídas | **este pipeline** |
| `data/processed/sections_clean/year={year}/part-*.parquet` | Secciones limpias | `run_content_cleaning_pipeline` |
| `logs/` | Archivos `.log` por módulo | todos |

### Flujo de Datos (Diagrama)

```
merged_outlines.parquet
  │
  ▼
[PdfContentPipeline.run()]
  │
  ├── 1. Cargar outlines_df (Extractor.load_single_dataframe)
  │
  ├── 2. Iterar pdf_lines.parquet en batches de 200K rows (PyArrow iter_batches)
  │      │
  │      ▼
  │   Acumular en line_buffers[doc_id]
  │      │
  │      ▼ (cuando 1000 doc_ids acumulados)
  │   _process_batch()
  │      │
  │      ├── 3. match_titles_with_lines() → asigna line_number a cada outline
  │      ├── 4. Guarda outlines_with_position (ParquetWriter append)
  │      ├── 5. preprocess_dataframes() → pdf_lines_dict anidado
  │      ├── 6. extract_content_sections() → List[ContentSection]
  │      ├── 7. prepare_sections_dataframe() → DataFrame
  │      └── 8. save_content_sections_partitioned() → year=*/part-*.parquet
  │
  └── 9. Procesar buffer remanente
```

### Columnas del Output (Secciones)

| Columna | Tipo PyArrow | Origen |
|---------|-------------|--------|
| `document_id` | `string` | Del outline |
| `nro_licitacion` | `string` | Del outline |
| `category_id` | `string` | Del outline |
| `year` | `string` | Del outline |
| `title` | `string` | Del outline |
| `content` | `string` | Join de líneas extraídas |
| `page` | `int32` | Página de inicio |
| `line_start` | `int32` | Línea de inicio |
| `line_end` | `int32` | Línea de fin (-1 si no aplica) |
| `depth` | `int32` | Nivel jerárquico del outline |
| `content_length` | `int32` | Número de líneas |
| `estimated_tokens` | `int32` | `content_length / 4` |
| `word_count` | `int32` | Conteo de palabras |
| `size_bytes` | `int32` | Tamaño en bytes |

### Notas para LLM al Programar en Este Proyecto

1. **Siempre usar `@error_handling(default_return=...)`** en métodos ETL. No try/except manual.
2. **Siempre usar `setup_logger(__name__)`** para cada clase. Loggear inicio/fin de cada operación.
3. **Los paths vienen del config dict**, nunca hardcodear rutas dentro de pipelines.
4. **Usar `os.makedirs(..., exist_ok=True)`** antes de escribir archivos.
5. **Parquet con `compression="snappy"`** y `row_group_size=10000` para escritura.
6. **Particionar por `year`** con `pq.write_to_dataset(partition_cols=["year"])`.
7. **Para batches grandes**: usar `ParquetFile.iter_batches()` en lugar de cargar todo.
8. **Data classes** con `@dataclass` + `get_schema()` + `to_dict()`.
9. **Los scripts entry-point** deben insertar `sys.path` para resolver imports desde raíz.
10. **`.env`** con `config/settings.py` para todas las variables de entorno.

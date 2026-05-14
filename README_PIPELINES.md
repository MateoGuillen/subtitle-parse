# Pipeline ETL Completo — DNCP Paraguay

## Visión General

Siete pipelines ETL que toman datos crudos del DNCP y los transforman hasta tener una base PostgreSQL poblada con ~100+ features por licitación, combinando datos estructurados OCDS + contenido textual de Pliegos de Bases y Condiciones (PDF).

```
FUENTES
  │
  ├─ CSVs OCDS (masivo.zip)
  │    ↓
  │  ocds_csv_pipeline    → 13 tablas estructuradas
  │
  └─ PDFs de Pliegos
       ↓
     run_ocds_pipeline        ← genera CSV con URLs de PDFs
     run_async_download       ← descarga PDFs
     run_pdf_outline          ← extrae outlines (TOC) de cada PDF
     run_pdf_parquet          ← extrae todo el texto línea por línea
     run_pdf_content          ← cruza outlines + líneas → secciones
     run_content_cleaning     ← limpia el texto de cada sección
     run_sections_to_db       ← inserta todo en PostgreSQL
```

---

## Orden de Ejecución

Cada pipeline consume la salida del anterior. Deben ejecutarse en este orden exacto:

```
1. python -m scripts.run_ocds_pipeline
2. python -m scripts.run_async_download_pipeline
3. python -m scripts.run_pdf_outline_pipeline
4. python -m scripts.run_pdf_parquet_pipeline
5. python -m scripts.run_pdf_content_pipeline
6. python -m scripts.run_content_cleaning_pipeline
7. python -m scripts.run_sections_to_db_pipeline
```

---

## Pipeline 0: `run_ocds_pipeline` — Dataset de URLs de Pliegos

**Script**: `scripts/run_ocds_pipeline.py`
**Pipeline**: `src/pipelines/ocds_pipeline.py` (clase `OCDSPipeline`)

### Propósito

Genera un CSV con las URLs de todos los pliegos PDF asociados a cada licitación. Este CSV es el insumo para el pipeline de descarga.

### Proceso paso a paso

1. **Extrae** datos OCDS de la API del DNCP para cada año (2021-2026)
2. **Transforma** los datos crudos en dos DataFrames:
   - `pdf_df`: filas con metadatos + URLs de pliegos PDF (`ten_documents_pliego_pdf_*`)
   - `json_df`: filas con metadatos de pliegos JSON
3. **Mergea** todos los años en un solo archivo: `ten_documents_pliego_pdf_every_year.csv`
4. **Filtra** licitaciones únicas: `ten_documents_pliego_pdf_every_year_filtered.csv`

### Output

| Archivo | Descripción |
|---------|-------------|
| `{BASE_OUTPUT_RAW_DIR}/csv/datasets/merged_tender_data_pdf_{year}.csv` | URLs de PDFs por año |
| `{BASE_OUTPUT_PROCESSED_DIR}/csv/ten_documents_pliego_pdf_every_year.csv` | URLs de PDFs, todos los años |
| `{BASE_OUTPUT_PROCESSED_DIR}/csv/ten_documents_pliego_pdf_every_year_filtered.csv` | Licitaciones únicas con URLs |

### Columnas clave del output

- `nro_licitacion` — FK a la tabla `licitaciones`
- `document_id` — ID único del documento pliego
- `url_documento` — URL para descargar el PDF
- `year` — Año de la licitación

---

## Pipeline 1: `run_async_download_pipeline` — Descarga Masiva de PDFs

**Script**: `scripts/run_async_download_pipeline.py`
**Pipeline**: `src/pipelines/async_download_pipeline.py`

### Propósito

Descarga los archivos PDF de pliegos desde las URLs generadas por el pipeline anterior, de forma asincrónica.

### Proceso paso a paso

1. **Prepara datos**: Filtra el CSV de URLs por año (usa `filter_column=year`)
2. **Descarga**: Procesa los PDFs en batches de 1000 archivos con pausa de 10s entre batches
3. **Organiza**: `organize_files_by_category()` — organiza archivos descargados
4. **Resumen**: `create_download_summary()` — genera CSV con resumen de descargas
5. **Limpieza**: `clean_temporary_files()` — elimina archivos temporales

### Output

| Directorio | Contenido |
|------------|-----------|
| `{BASE_OUTPUT_RAW_DIR}/pdf/{year}/` | PDFs descargados organizados por año |
| `{BASE_OUTPUT_RAW_DIR}/pdf/{year}/{category}/` | PDFs organizados por categoría |
| `{BASE_OUTPUT_RAW_DIR}/download_summary_pdf_{year}.csv` | Resumen de descargas |

---

## Pipeline 2: `run_pdf_outline_pipeline` — Extracción de Outlines (TOC)

**Script**: `scripts/run_pdf_outline_pipeline.py`
**Pipeline**: `src/pipelines/pdf_outline_pipeline.py`

### Propósito

Extrae la tabla de contenidos (outlines/bookmarks) de cada PDF de pliego. Los outlines definen la estructura jerárquica del documento: títulos de secciones, subsecciones y sus números de página.

### Proceso paso a paso

1. **Escanea** `{BASE_OUTPUT_RAW_DIR}/pdf/{year}/` en busca de archivos PDF
2. **Procesa** en batches de 10000 archivos con `ThreadPoolExecutor`
3. **Extrae** outlines de cada PDF usando `pdf_outline_extractor.extract_outlines_batch()`
4. **Transforma** con `outline_processor.process_outlines()` — limpia y estructura los outlines
5. **Guarda** por año en `{BASE_OUTPUT_PROCESSED_DIR}/outlines/{year}/outlines_{year}.csv`
6. **Mergea** todos los años → `merged_outlines.csv` + `merged_outlines.parquet`

### Output

| Archivo | Formato | Descripción |
|---------|---------|-------------|
| `{output_dir}/outlines/{year}/outlines_{year}.csv` | CSV | Outlines por año |
| `{output_dir}/csv/merged_outlines.csv` | CSV | Outlines de todos los años |
| `{output_dir}/parquet/merged_outlines.parquet` | Parquet | Outlines de todos los años |

### Columnas del output

| Columna | Descripción |
|---------|-------------|
| `document_id` | ID del documento PDF |
| `title` | Título de la sección (ej: "2.1. Objeto de la contratación") |
| `page` | Número de página donde comienza (extraído o estimado) |
| `depth` | Nivel jerárquico (0=capítulo, 1=sección, 2=subsección) |
| `line_start` | Número de línea de inicio (rellenable post-content) |
| `line_end` | Número de línea de fin (rellenable post-content) |
| `year` | Año de la licitación |

---

## Pipeline 3: `run_pdf_parquet_pipeline` — PDF a Parquet (Líneas)

**Script**: `scripts/run_pdf_parquet_pipeline.py`
**Pipeline**: `src/pipelines/pdf_parquet_pipeline.py`

### Propósito

Extrae el texto completo de cada PDF línea por línea usando Apache Tika, y lo guarda en formato Parquet. Esto produce el dataset de "líneas" que luego se cruza con los outlines.

### Proceso paso a paso

1. **Extrae** lista de archivos PDF del directorio `{BASE_OUTPUT_RAW_DIR}/pdf/{year}/`
2. **Procesa** cada PDF con Apache Tika (vía contenedores Podman paralelos, batch de 100 archivos)
3. **Transforma** cada PDF en un DataFrame de líneas (página, línea, texto, coordenadas)
4. **Carga** a Parquet por año: `{output_dir}/pdf_text_{year}.parquet`
5. **Mergea** todos los años → `combined_documents_all_years.parquet`

### Output

| Archivo | Descripción |
|---------|-------------|
| `{output_dir}/pdf_text_{year}.parquet` | Líneas de PDF por año |
| `{output_dir}/combined_documents_all_years.parquet` | ** Todas las líneas de todos los años ** (~2GB) |

### Columnas del output

| Columna | Descripción |
|---------|-------------|
| `document_id` | ID del documento PDF |
| `page_number` | Número de página (1-indexed) |
| `line_number` | Número de línea dentro del documento |
| `content` | Texto de la línea |
| `x`, `y`, `width`, `height` | Coordenadas espaciales (opcional) |
| `year` | Año de la licitación |

---

## Pipeline 4: `run_pdf_content_pipeline` — Cruce Outlines + Líneas → Secciones

**Script**: `scripts/run_pdf_content_pipeline.py`
**Pipeline**: `src/pipelines/pdf_content_pipeline.py`

### Propósito

Cruza los outlines (estructura) con las líneas de texto (contenido) para extraer el contenido textual de cada sección del pliego. Cada sección es un fragmento del documento con: título, página de inicio/fin, líneas de inicio/fin, y el texto completo.

### Proceso paso a paso

1. **Carga** `merged_outlines.parquet` (outlines) y `combined_documents_all_years.parquet` (líneas)
2. **Pre-agrupa** las líneas por `document_id` en un diccionario para acceso rápido
3. **Procesa** en batches de 1000 documentos:
   - **Match**: `match_titles_with_lines()` — asigna a cada outline la línea donde aparece su título
   - **Preprocesa**: `preprocess_dataframes()` — prepara diccionarios para búsqueda
   - **Extrae secciones**: `extract_content_sections()` — extrae el texto entre un outline y el siguiente
   - **Prepara DataFrame**: `prepare_sections_dataframe()` — estructura columnas
   - **Guarda**: particionado por año en `{output_dir}/sections/year={year}/part-N.parquet`
4. Escribe outlines con posición en `merged_outlines_with_position_in_content.parquet`

### Algoritmo de matching de outlines

Para cada outline (título + página estimada), busca en las líneas del mismo `document_id` una línea cuyo texto coincida con el título. Si encuentra match, registra `line_start` y `page_number`. Si no, interpola entre outlines cercanos.

### Output

| Archivo | Descripción |
|---------|-------------|
| `{output_dir}/outlines/merged_outlines_with_position_in_content.parquet` | Outlines con línea de inicio resuelta |
| `{output_dir}/sections/year={year}/part-*.parquet` | Secciones particionadas por año |

### Columnas de secciones

| Columna | Descripción |
|---------|-------------|
| `document_id` | ID del documento |
| `nro_licitacion` | FK a licitaciones |
| `category_id` | Categoría de la licitación |
| `year` | Año |
| `title` | Título de la sección (ej: "Garantía de Oferta") |
| `title_normalized` | Título normalizado (minúsculas, sin acentos) |
| `page` | Página donde comienza |
| `line_start` | Línea de inicio en el documento |
| `line_end` | Línea de fin |
| `depth` | Nivel jerárquico |
| `content_length` | Longitud del texto crudo |
| `estimated_tokens` | Tokens estimados (content_length / 4) |
| `word_count` | Cantidad de palabras |
| `size_bytes` | Tamaño en bytes |
| `content_text` | Texto completo de la sección |
| `content` | Lista de strings (líneas raw, usado como paso intermedio) |

---

## Pipeline 5: `run_content_cleaning_pipeline` — Limpieza de Texto

**Script**: `scripts/run_content_cleaning_pipeline.py`
**Pipeline**: `src/pipelines/content_cleaning_pipeline.py`

### Propósito

Limpia el `content` (lista de líneas raw) de cada sección aplicando una secuencia de transformaciones de texto. El output reemplaza `content` por:

- `content_clean[]` (TEXT[] — array de líneas limpias)
- `content_text` (TEXT — texto completo plano, para búsqueda textual)
- `content_length_clean` (INT — caracteres del texto limpio)

### Proceso paso a paso

1. **Itera** por año: lee las particiones `{input_dir}/year={year}/part-*.parquet`
2. **Procesa** en batches de 200,000 filas cada uno
3. **Limpieza secuencial** (`clean_content_field`):
   - Elimina `\r` (retornos de carro)
   - Elimina guiones de fin de línea (word hyphenation): `"docu-\nmento"` → `"documento"`
   - Normaliza espacios múltiples → espacio simple
   - Elimina líneas vacías y líneas de solo números de página
   - Reemplaza caracteres Unicode problemáticos
   - Aplica stemming básico en español
   - Calcula `content_length_clean` (caracteres del texto limpio)
4. **Descarta** secciones vacías (`content_length_clean == 0`)
5. **Guarda** resultado en `{output_dir}/year={year}/part-*.parquet`

### Output

| Directorio | Contenido |
|------------|-----------|
| `{BASE_OUTPUT_PROCESSED_DIR}/sections_clean/year={year}/` | Secciones limpias particionadas por año |

### Columnas nuevas/adicionadas

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `content_clean` | `TEXT[]` | Array de líneas de texto limpias (para PostgreSQL) |
| `content_text` | `TEXT` | Texto completo concatenado (para búsqueda full-text) |
| `content_length_clean` | `INT` | Caracteres del texto limpio |

---

## Pipeline 6: `run_sections_to_db_pipeline` — Carga a PostgreSQL

**Script**: `scripts/run_sections_to_db_pipeline.py`
**Pipeline**: `src/pipelines/sections_to_db_pipeline.py`

### Propósito

Lee las secciones limpias desde Parquet y las inserta en PostgreSQL usando COPY para máxima performance.

### Proceso paso a paso

1. **Conecta** a PostgreSQL usando el DSN de configuración
2. **Itera** por año:
   - `SectionsDbExtractor.iter_years()` — lee partición `{sections_dir}/year={year}/*.parquet`
3. **Upsert categorías**: extrae `category_id` únicos → `INSERT INTO dncp.categorias ... ON CONFLICT DO NOTHING`
4. **Upsert licitaciones**: extrae `(nro_licitacion, category_id, year)` únicos → `INSERT INTO dncp.licitaciones ... ON CONFLICT DO NOTHING`
5. **Prepara filas**: convierte tipos numpy a Python nativos (int, str, list)
6. **COPY sections**: `INSERT INTO dncp.pliegos_secciones ...` en batches de 10,000 filas con `execute_values`
7. **ANALYZE**: actualiza estadísticas del planificador de consultas en la partición cargada

### Output en PostgreSQL

| Tabla | Columnas pobladas |
|-------|-------------------|
| `dncp.categorias` | `category_id` |
| `dncp.licitaciones` | `nro_licitacion`, `category_id`, `year` |
| `dncp.pliegos_secciones` | 17 columnas (ver DDL) |

---

## Modelo de Datos Final en PostgreSQL

La tabla `dncp.pliegos_secciones` está **particionada por año** y contiene ~2.8M filas (una por sección de pliego). Cada licitación tiene **~50-150 secciones** = features textuales adicionales para el algoritmo de Isolation Forest.

### Relaciones entre tablas

```
licitaciones (nro_licitacion PK)
  ├── adjudicaciones (nro_licitacion FK)
  ├── contratos (nro_licitacion FK)
  ├── items_licitacion (nro_licitacion FK)
  ├── oferentes (nro_licitacion FK)
  ├── proveedores_notificados (nro_licitacion FK)
  ├── consultas_llamado (nro_licitacion FK)
  ├── criterios_llamado (nro_licitacion FK)
  ├── protestas (nro_licitacion FK)
  └── pliegos_secciones (nro_licitacion FK) ← ~100 features textuales
```

### Volúmenes estimados

| Pipeline | Tabla/Dataset | Filas |
|----------|---------------|-------|
| OCDS CSV | licitaciones | ~61K |
| OCDS CSV | proveedores | ~55K |
| OCDS CSV | adjudicaciones | ~89K |
| OCDS CSV | contratos | ~85K |
| OCDS CSV | pagos | ~684K |
| OCDS CSV | items | ~2.5M |
| **PDF Sections** | **pliegos_secciones** | **~2.8M** (~100 por licitación) |
| PDF Sections | categorias | ~500 |
| PDF Lines | combined_documents_all_years.parquet | ~50M líneas |

---

## Diagrama de Flujo Completo

```
OCDS API (JSON)
    │
    ▼
run_ocds_pipeline.py
    │
    ├─► CSV con URLs de PDFs ───────────────────────────────────┐
    │                                                            │
    ▼                                                            ▼
run_async_download_pipeline.py                         ocds_csv_pipeline.py
    │                                                            │
    ▼                                                            ▼
PDFs en disco ({year}/pdf/{documento}.pdf)          13 tablas PostgreSQL
    │
    ▼
run_pdf_outline_pipeline.py
    │
    ├─► merged_outlines.parquet (estructura)
    │
    ▼
run_pdf_parquet_pipeline.py
    │
    ├─► combined_documents_all_years.parquet (líneas)
    │
    ▼
run_pdf_content_pipeline.py
    │
    ├─► merged_outlines_with_position.parquet
    ├─► sections/year={year}/*.parquet
    │
    ▼
run_content_cleaning_pipeline.py
    │
    ├─► sections_clean/year={year}/*.parquet
    │
    ▼
run_sections_to_db_pipeline.py
    │
    ├─► dncp.pliegos_secciones (PostgreSQL) ← ~2.8M secciones
    ├─► dncp.categorias (upsert)
    └─► dncp.licitaciones (upsert)
```

---

## Columnas Calculadas en `pliegos_secciones`

El pipeline de limpieza (`content_cleaning_pipeline.py`) calcula:

| Columna | Cálculo | Propósito |
|---------|---------|-----------|
| `content_clean` | Limpieza secuencial de cada línea | Array de strings limpio para PostgreSQL TEXT[] |
| `content_text` | `'\n'.join(content_clean)` | Texto plano para búsqueda full-text |
| `content_length_clean` | `len(content_text)` | Feature numérica: qué tan detallada es cada sección |
| `word_count` | `len(content_text.split())` | Feature: cantidad de palabras |
| `estimated_tokens` | `content_length / 4` | Estimación de tokens LLM |

En el transformer de secciones a DB (`sections_db_transformer.py`), los tipos numpy se convierten a tipos Python nativos para compatibilidad con psycopg2:

| Tipo Parquet → | Tipo Python → | Tipo PostgreSQL |
|----------------|---------------|-----------------|
| `int32/int16` | `int` o `None` | `INTEGER`/`SMALLINT` |
| `float64` | `int` o `None` | `INTEGER` |
| `str` | `str` o `None` | `TEXT` |
| `list[str]` | `list[str]` | `TEXT[]` |
| `NaN` | `None` | `NULL` |
| `NaT` | `None` | `NULL` |

---

## Decisiones Técnicas

### Particionamiento por año

`pliegos_secciones` está particionada por `LIST (year)` con particiones `pliegos_secciones_2021` a `2025`. Esto permite:
- ANALYZE individual por partición
- Drop/reload de un año sin afectar otros
- Consultas más rápidas cuando se filtra por año

### COPY vs INSERT

`SectionsDbLoader` usa `psycopg2.extras.execute_values()` que utiliza `COPY` internamente. Para 2.8M filas, COPY es 10-50x más rápido que INSERT individual.

### Batch commits

Cada 10,000 filas se commitean independientemente. Esto evita OOM en buffers WAL y permite recuperación parcial si el pipeline falla.

### Idempotencia

Todos los upserts usan `ON CONFLICT DO NOTHING`. El pipeline puede re-ejecutarse sin duplicar datos. Las secciones se insertan sin ON CONFLICT porque cada sección es única por diseño (mismo documento + título + línea de inicio = misma sección).

### Normalización de títulos

El pipeline de contenido normaliza los títulos de secciones (minúsculas, sin acentos, sin espacios extra) para facilitar matching textual y agrupación por tipo de sección.

---

## Estados Intermedios (Idempotencia)

Cada pipeline verifica si su output ya existe antes de ejecutarse. Esto permite:

- Re-ejecutar desde cualquier punto si un pipeline falla
- Saltar años ya procesados
- Modificar un pipeline específico sin re-ejecutar todo

```
Checkpoints por pipeline:
  run_ocds_pipeline          → archivos CSV en output_processed_dir
  run_async_download         → archivos PDF existentes
  run_pdf_outline            → outlines_{year}.csv por año
  run_pdf_parquet            → pdf_text_{year}.parquet por año
  run_pdf_content            → sections/year={year}/*.parquet
  run_content_cleaning       → sections_clean/year={year}/*.parquet
  run_sections_to_db         → año en checkpoint parquet (via extractor)
```

---

## Feature Engineering para Isolation Forest

La tesis usa Isolation Forest sobre una feature matrix híbrida:

**Features estructuradas** (del pipeline OCDS CSV):
- Montos: monto_estimado, monto_adjudicado, monto_contrato
- Tiempos: duración del proceso, plazos
- Cantidades: cantidad_oferentes, cantidad_items, cantidad_enmiendas
- Categóricas: método de contratación, estado, tipo de entidad
- Relacionales: mismo proveedor en múltiples licitaciones

**Features textuales** (del pipeline PDF Sections):
- Por sección del pliego (~50-150 por licitación)
- content_length_clean, word_count
- Presencia/ausencia de ciertas secciones
- TF-IDF sobre titles normalizados
- Contenido semántico vía embeddings

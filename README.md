# Subtitle Parse

ETL pipeline para el procesamiento y análisis de documentos de licitaciones públicas (DNCP - Contrataciones Públicas de Paraguay), con detección de anomalías mediante extracción de contenido y análisis con LLM.

## Estructura del Proyecto

```
subtitle-parse/
├── config/               # Configuración (DB, paths, LLM, API keys)
├── data/
│   ├── raw/              # Datos crudos descargados
│   ├── processed/        # Datos procesados
│   └── external/         # Datos externos
├── docker/
│   └── Dockerfile
├── scripts/              # Scripts ejecutables de cada pipeline
├── src/
│   ├── core/             # Entidades y excepciones
│   ├── etl/              # Extractores y procesadores
│   ├── pipelines/        # Implementaciones de pipelines
│   ├── tasks/            # Scripts individuales (algunos legacy)
│   └── utils/            # Utilidades compartidas
└── requirements.txt
```

## Pipeline por Pipeline

### 1. OCDS Pipeline (`scripts/run_ocds_pipeline.py`)

**Clase:** `OCDSPipeline`

**Propósito:** Descarga y procesa metadatos de licitaciones desde la API DNCP (Open Contracting Data Standard).

**Proceso:**
- Consulta la API de `contrataciones.gov.py` por años (2021-2026)
- Extrae información de contratos, licitaciones y pliegos
- Guarda los datos en formato estructurado en `data/processed/`

**Dependencias:** `requests`, `pandas`, `aiohttp`, `aiofiles`

---

### 2. PDF Outline Pipeline (`scripts/run_pdf_outline_pipeline.py`)

**Clase:** `PDFOutlinePipeline`

**Propósito:** Extrae el esquema (outline/índice) de archivos PDF de pliegos de licitación.

**Proceso:**
- Descarga asíncrona de PDFs desde URLs de licitaciones
- Extrae el outline (estructura de capítulos/secciones) con posiciones
- Procesa en paralelo con multi-core
- Fusiona resultados en CSV/Parquet

**Dependencias:** `aiohttp`, `aiofiles`, `PyPDF2`, `pdfplumber`, `tqdm`

---

### 3. PDF Parquet Pipeline (`scripts/run_pdf_parquet_pipeline.py`)

**Clase:** `PDFParquetPipeline`

**Propósito:** Convierte archivos PDF a formato Parquet extrayendo contenido completo.

**Proceso:**
- Utiliza Apache Tika para extracción de contenido de PDFs
- Procesa PDFs en pods de Tika
- Genera archivos Parquet con el contenido extraído

**Dependencias:** `tika`, `pyarrow`, `pandas`

---

### 4. Content Cleaning Pipeline (`scripts/run_content_cleaning_pipeline.py`)

**Clase:** `ContentCleaningPipeline`

**Propósito:** Limpia y fusiona las secciones extraídas de los PDFs por año.

**Proceso:**
- Limpia texto extraído (remueve artefactos, normaliza)
- Fusiona secciones relacionadas por licitación
- Organiza resultados por año en `data/processed/`

**Dependencias:** `pandas`, `text_cleaner`, `nltk`

---

### 5. LLM Extraction Pipeline (`scripts/run_llm_extraction_pipeline.py`)

**Clase:** `LLMExtractionPipeline`

**Propósito:** Ejecuta extracción de información clave usando LLM sobre secciones limpias.

**Proceso:**
- Envía contenido limpio a endpoint de LLM configurado
- Extrae campos estructurados (montos, fechas, proveedores, etc.)
- Guarda resultados en formato estructurado

**Dependencias:** `llm_client`, `requests`, `cryptography`

---

### 6. Sections to DB Pipeline (`scripts/run_sections_to_db_pipeline.py`)

**Clase:** `SectionsToDbPipeline`

**Propósito:** Carga los datos procesados en PostgreSQL.

**Proceso:**
- Conecta a PostgreSQL usando `DB_CONFIG`
- Hace upsert en las siguientes tablas:
  - `dncp.categorias` - Categorías de licitación
  - `dncp.licitaciones` - Datos de licitaciones
  - `dncp.pliegos_secciones` - Secciones de pliegos procesadas
- Usa `sqlalchemy` + `psycopg2` para la conexión

**Dependencias:** `sqlalchemy`, `psycopg2-binary`, `pandas`

---

### 7. Async Download Pipeline (`scripts/run_async_download_pipeline.py`)

**Propósito:** Descarga asíncrona de archivos desde URLs.

**Proceso:**
- Descarga masiva asíncrona con `aiohttp`
- Manejo de errores y reintentos
- Guardado en `data/raw/`

**Dependencias:** `aiohttp`, `aiofiles`, `tqdm`

---

## Flujo de Datos (Pipeline Chain)

```
[1] OCDS Pipeline       → Descarga metadatos de licitaciones (API DNCP)
            ↓
[2] PDF Outline Pipeline → Extrae estructura de PDFs de pliegos
            ↓
[3] PDF Parquet Pipeline → Convierte PDFs a Parquet (contenido completo)
            ↓
[4] Content Cleaning    → Limpia y fusiona secciones extraídas
            ↓
[5] LLM Extraction      → Extrae información clave con LLM
            ↓
[6] Sections to DB      → Carga resultados en PostgreSQL
```

## Configuración

Copiar `example.env` a `.env` y configurar:

```env
DB_NAME=pbc_dncp
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_HOST=localhost
DB_PORT=5432
DNCP_BASE_URL=https://www.contrataciones.gov.py
LLM_ENDPOINT=https://ruta.llm.local
LLM_USERNAME=tu_usuario_ll
LLM_PASSWORD=tu_password_llm
BASE_OUTPUT_RAW_DIR=./data/raw
BASE_OUTPUT_PROCESSED_DIR=./data/processed
```

## Ejecución

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar cada pipeline en orden
python scripts/run_ocds_pipeline.py
python scripts/run_pdf_outline_pipeline.py
python scripts/run_pdf_parquet_pipeline.py
python scripts/run_content_cleaning_pipeline.py
python scripts/run_llm_extraction_pipeline.py
python scripts/run_sections_to_db_pipeline.py
```

## Dependencias Principales

| Categoría | Paquetes |
|-----------|----------|
| Datos | `pandas`, `pyarrow`, `openpyxl`, `lxml` |
| PDF | `PyPDF2`, `pdfplumber`, `tika` |
| Red | `requests`, `aiohttp`, `aiofiles` |
| DB | `sqlalchemy`, `psycopg2-binary` |
| LLM/ML | `scikit-learn`, `nltk`, `matplotlib` |
| Utilidades | `rarfile`, `cryptography`, `python-dotenv`, `tqdm` |

## Propósito Final

Este repositorio implementa un sistema ETL completo para:

1. **Ingesta** de datos de licitaciones públicas de Paraguay (DNCP)
2. **Procesamiento** de documentos PDF (pliegos de condiciones)
3. **Extracción inteligente** con LLM de información clave
4. **Carga** a base de datos PostgreSQL para análisis posterior
5. **Detección de anomalías** en procesos de contratación pública

El flujo transforma datos crudos de licitaciones en información estructurada y analizable, permitiendo identificar patrones, inconsistencias o anomalías en los procesos de contratación pública.

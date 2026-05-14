# Subtitle Parse - Pipeline de Procesamiento de Documentos de Licitaciones

## 🎯 Propósito del Proyecto

**Subtitle Parse** es un sistema ETL (Extract, Transform, Load) completo diseñado para el procesamiento y análisis automatizado de documentos de **licitaciones públicas del Paraguay (DNCP - Dirección Nacional de Contrataciones Públicas)**. 

El sistema permite la extracción, transformación y carga masiva de pliegos de condiciones (PDF y JSON), con detección de anomalías mediante Inteligencia Artificial y LLM, facilitando el análisis de transparencia y eficiencia en procesos de contratación pública.

---

## 📂 Estructura del Proyecto

```
subtitle-parse/
├── config/                    # Configuración de base de datos, LLM y API keys
├── data/
│   ├── raw/                   # Datos crudos desde DNCP (API)
│   ├── processed/             # Datos transformados y limpios
│   └── external/              # Tablas de categorías y metadatos externos
├── docker/                    # Configuración contenerizada
├── scripts/                   # Scripts ejecutables independientes
├── src/
│   ├── core/                  # Entidades, modelos y excepciones
│   │   ├── entities/         # Modelos de datos (PDF, documentos, DB)
│   │   └── exceptions/       # Manejo de excepciones custom
│   ├── etl/                   # Componentes ETL principales
│   │   ├── loaders/          # Lectores de archivos y bases de datos
│   │   ├── transformers/     # Transformaciones y enriquecimiento
│   │   └── extractors/       # Componentes de extracción
│   ├── pipelines/             # Implementaciones de pipelines maestros
│   ├── tasks/                 # Scripts individualizados (legacy & utilities)
│   └── utils/                 # Herramientas compartidas
├── ddl.sql                    # Script SQL para creación de esquema
├── requirements.txt           # Dependencias Python
└── ejemplo.env                # Plantilla de variables de entorno
```

---

## 🏗️ Arquitectura del Pipeline

### Flujo ETL Completo (7 Fases)

```
[1] OCDS Pipeline → Descarga API DNCP (metadata)
         ↓
[2] PDF Outline Pipeline → Extrae índice/estructura de PDFs
         ↓
[3] PDF Parquet Pipeline → Convierte PDF a formato Parquet + texto
         ↓
[4] Content Cleaning Pipeline → Limpieza y fusión de contenido
         ↓
[5] LLM Extraction Pipeline → Extracción estructurada con IA (LLM)
         ↓
[6] Sections to DB Pipeline → Carga en PostgreSQL con upserts
```

### Componentes ETL por Fase

#### Phase 1: Extraer - OCDS Transformer
- **Fuente:** API `https://contrataciones.gov.py` de la DNCP Paraguaya
- **Datos:** Metadata de contratos, licitaciones y pliegos (2021-2026)
- **Formato:** JSON → CSV con filtrado por tipo de documento

#### Phase 2: Transformar - PDF Outline & Parquet Transformers
- **Extracción PDF:** Usa `PyPDF2`, `pdfplumber` para obtener outline/índice
- **Conversión:** Apache Tika para texto completo desde PDF
- **Procesamiento masivo:** Descargas asíncronas con `aiohttp` y `aiofiles`

#### Phase 3: Enriquecer - Data Enricher & Feature Parser
- **Extracción inteligente:** LLM extrae campos clave (montos, fechas, proveedores)
- **Categorización:** Clustering de documentos por temática/proveedor
- **Validación:** Detección de duplicados y anomalías

#### Phase 4: Cargar - Loaders Multi-formato
```python
CSVLoader      → Archivos CSV con dataframes
ParquetLoader  → Formato Parquet (columnar, eficiente)
SectionsDBLoader → Bulk COPY a PostgreSQL
```

---

## 🛠️ Tecnologías Utilizadas

### Python Libraries Core
| Categoria | Paquetes Clave |
|-----------|-----------------|
| **Análisis de Datos** | `pandas`, `pyarrow`, `scikit-learn`, `numpy` |
| **Procesamiento PDF** | `PyPDF2`, `pdfplumber`, `tika` (Apache Tika wrapper) |
| **Descargas Asíncronas** | `aiohttp`, `aiofiles`, `asyncio`, `tqdm` |
| **Base de Datos** | `sqlalchemy`, `psycopg2-binary` |
| **API/LLM** | `requests`, `llm_client`, `cryptography` |
| **Text Cleaning** | `nltk`, `text_cleaner`, `lxml` |
| **Configuración** | `python-dotenv`, `dotenv` |

### Dependencias Comunes
```bash
pip install -r requirements.txt
# pandas, requests, rarfile, PyPDF2, pdfplumber, aiohttp, 
# pyarrow, sqlalchemy, psycopg2-binary, python-dotenv, tika, 
# scikit-learn, nltk, matplotlib, openpyxl, lxml, cryptography
```

---

## 📊 Base de Datos - Esquema SQL

El sistema crea automáticamente el esquema `dncp` en PostgreSQL con:

### Tablas Principales

#### 1. `dncp.categorias`
Almacena categorías de licitación (abiertas, cerradas, etc.)

```sql
CREATE TABLE dncp.categorias (
    category_id TEXT PRIMARY KEY,
    descripcion TEXT
);
```

#### 2. `dncp.licitaciones`
Metadatos principales de cada licitación:

```sql
CREATE TABLE dncp.licitaciones (
    nro_licitacion TEXT PRIMARY KEY,           -- Identificador único
    category_id TEXT REFERENCES dncp.categorias(category_id),
    year TEXT                                   -- Año del proceso
);
```

#### 3. `dncp.pliegos`
Contenido completo de pliegos con particionamiento por año:

```sql
CREATE TABLE dncp.pliegos (
    document_id TEXT,              -- Identificador documento
    nro_licitacion REFERENCES dncp.licitaciones(nro_licitacion),
    title TEXT,                    -- Título/encabezado
    content TEXT,                  -- Contenido textual completo
    page TEXT,                     -- Página de origen
    line_start TEXT,               -- Línea inicial
    line_end TEXT,                 -- Línea final
    depth TEXT,                    -- Level del outline
    content_length TEXT,           -- Longitud del contenido
    year TEXT                      -- Particionamiento por año
) PARTITION BY LIST (year);
```

**Particiones automáticas:** `2021`, `2022`, `2023`, `2024`, `2025`, `2026`

---

## 🚀 Ejecución del Sistema

### Instalación Inicial

```bash
# 1. Clonar y entrar
git clone <repo>
cd subtitle-parse

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Cargar datos de NLTK (si aplica)
python download_nltk_data.py

# 4. Crear copias de ejemplo.env y configurar
cp ejemplo.env .env
# Editar con tus claves: DB_USER, DB_PASSWORD, LLM_ENDPOINT, etc.
```

### Variables de Entorno (`~/.env` o `/src/../.env`)

```bash
DB_NAME=pbc_dncp              # Nombre BD PostgreSQL
DB_USER=tu_usuario            # Usuario PostgreSQL
DB_PASSWORD=tu_password       # Contraseña PostgreSQL  
DB_HOST=localhost             # Host DB (localhost o IP servidor)
DB_PORT=5432                  # Puerto PostgreSQL

DNCP_BASE_URL=https://contrataciones.gov.py  # API Licitaciones DNCP
LLM_ENDPOINT=https://tu.llm.local/          # Endpoint LLM (Opcional)
LLM_USERNAME=tu_usuario_llm
LLM_PASSWORD=tu_password_llm

BASE_OUTPUT_RAW_DIR=./data/raw              # Carpeta datos crudos
BASE_OUTPUT_PROCESSED_DIR=./data/processed  # Carpeta datos procesados
DEFAULT_BATCH_SIZE=100                      # Batch size para operaciones
DEFAULT_MAX_RETRIES=3                       # Reintentos máximo
```

### Ejecución de Pipelines (Secuencial)

Ejecutar **en orden** para mantener consistencia entre tablas:

```bash
# FASE 1: Extraer datos desde API DNCP
python scripts/run_ocds_pipeline.py

# FASE 2: Extraer outline/estructura de PDFs
python scripts/run_pdf_outline_pipeline.py

# FASE 3: Convertir PDF a Parquet con texto completo
python scripts/run_pdf_parquet_pipeline.py

# FASE 4: Limpiar y fusionar contenido
python scripts/run_content_cleaning_pipeline.py

# FASE 5: Extraer información estructurada con LLM (opcional)
python scripts/run_llm_extraction_pipeline.py

# FASE 6: Cargar todo en PostgreSQL
python scripts/run_sections_to_db_pipeline.py
```

### Ejecución Individual de Tasks (Legacy/Utilities)

Los archivos en `src/tasks/` son componentes individuales o pipelines alternativos:

```bash
# Extracción masiva PDF a Parquet con GPU (si aplica)
python src/tasks/masive_async_convert_pdf_parquet_gpu.py

# Migración Parquet → PostgreSQL  
python src/tasks/migrate_parquet_to_postgres.py

# Ejecución de tasks individuales
python src/tasks/extract_tender_documents.py
python src/tasks/calculate_clusters.py
```

---

## 📋 Tipos de Documentos Procesados

El sistema clasifica y procesa automáticamente según `tender_documents_format`:

### **PDF Documents** (application/pdf)
- Pliegos Electrónicos de Bases y Condiciones en PDF
- Contenido escaneado o digitalizado
- Requiere OCR/extractión de texto desde formato binario

### **JSON Documents** (application/json)  
- Pliegos electrónicos digitales
- Estructura nativa JSON/XML
- Extracción directa del contenido textual

---

## 🔍 Detección de Anomalías

El sistema implementa análisis automático:

- **Duplicados:** `filter_duplicate_content()` detecta pliegos repetidos
- **Clustering Analítico:** Agrupamiento por temas/proveedores
  - Uso de K-Means para segmentación automática
  - Identificación de patrones en procesos de contratación
- **Comparación LLM:** Análisis comparativo entre pliegos similares
- **Validación de Consistencia:** Cross-checking de campos críticos

---

## 📈 Métricas de Procesamiento

El sistema genera logs con:

- **Progress tracking** con `tqdm` (barras de progreso)
- **Logging estructurado** en `src/utils/logging_utils.py`
- **Counters de éxito/errores** por pipeline y tarea
- **Tiempo de ejecución** por fase del ETL

---

## 🔄 Migraciones y Versiones

### Historial de Migraciones (Archivo `src/tasks/migrate_*`)

| Versión | Descripción |
|---------|-------------|
| v1 | Migración inicial Parquet → PostgreSQL |
| v2 | Optimización con bulk COPY operations |
| v3 | Mejora de particionamiento por año |

---

## 🐍 Clases y Patrones de Diseño

### Pattern Factory ETL
```python
# Loader Factory Pattern
CSVLoader(output_dir, processed_dir)
ParquetLoader(...)  
SectionsDBLoader(engine, batch_size=DEFAULT_BATCH_SIZE)

# Transformer Pattern  
OCDSTransformer(csv_path, transform_func)
PDFTransformer(extraction_method='tika')
OutlineMerger(merge_strategy="concatenation")

# Pipeline Strategy Pattern
AbstractPipeline.define_pipeline_steps()
```

---

## 🔒 Consideraciones de Seguridad

- **API Keys:** Guardadas en `.env`, NO en código fuente
- **DB Credentials:** Nunca hardcodear, usar `os.getenv()`  
- **LLM Endpoints:** Encriptados con base64/pkcs12 si necesario
- **Logging sensible:** Usar `logging_utils.mask_sensitive()`

---

## 📝 Uso Típico en Producción

### Sencillo: Ejecutar todo el ETL con 1 comando por pipeline
```bash
python scripts/run_ocds_pipeline.py              # Descargar metadata DNCP
python scripts/run_pdf_outline_pipeline.py       # Extraer outline PDFs
python scripts/run_pdf_parquet_pipeline.py       # Convertir a formato Parquet
python scripts/run_sections_to_db_pipeline.py    # Cargar en PostgreSQL
```

### Avanzado: Ejecución granular con control de tasks individuales
```bash
# Solo descargar de API
python src/tasks/masive_async_json_downloader.py -year 2024

# Solo extraer PDFs específicos (filtros)  
python src/tasks/extract_tender_documents.py --filter "abierta"

# Procesar batch específico
python src/tasks/masive_async_pdf_outline_extractor.py -batch_size 50
```

---

## 🛠️ Herramientas de Desarrollo

### Logs y Debug
```bash
tail -f logs/pipeline.log              # Ver logs en tiempo real
python src/verify_exact_pages.py       # Validación de páginas PDF
python src/list_duplicate_content.py   # Detectar duplicados
```

### Inspección de Datos
```bash
# Explorar base de datos (pgcli o psql)
psql -d pbc_dncp -U tu_usuario
-- SELECT * FROM dncp.licitaciones LIMIT 10;
-- SELECT COUNT(*) FROM dncp.pliegos WHERE year='2024';
```

---

## 📦 Exportación de Datos Procesados

El sistema exportaría a múltiples formatos según necesidad:

**Formatos soportados:** CSV, Parquet, Parquet con GPU, JSON

**Endpoints de exportación recomendados:**
- `data/processed/*_pdf_{year}.csv` → Datos limpios por año  
- `data/processed/*_json_{year}.csv` → Datos JSON limpios
- Tablas PostgreSQL: `dncp.licitaciones*, dncp.pliegos*`

---

## 🔄 Actualización incremental

Para re-ejecutar solo un año específico:

```bash
# Borrar resultados anteriores de ese año (opcional)
rm data/processed/*2024*.csv  
rm data/raw/*2024*.*

# Re-ejecutar pipelines completos
python scripts/run_ocds_pipeline.py  # Se re-escribe solo 2024
```

---

## 🎓 Casos de Uso Principal

### 1. Auditoría de Transparencia Pública
Analizar todos los pliegos de contratación gubernamental para identificar:
- Proveedores recurrentes
- Patrones de precios anómalos  
- Procesos repetitivos sin cambios sustanciales

### 2. Análisis de Eficiencia Contractual
Cross-analyze montos, plazos y condiciones entre licitaciones similares.

### 3. Minería de Datos Pública
Tasaar toda la information estructurada de la contratación estatal para investigación académica, periodismo de datos o inteligencia empresarial.

---

## 📢 Recursos Externos

- **DNCP Paraguay:** https://contrataciones.gov.py - Portal oficial
- **OCDS Especificación:** http://ocds.worldbank.org/ - Estándar Open Contracting Data Standard  
- **PostgreSQL:** https://www.postgresql.org/about/feature-comparison/
- **Tika Apache:** https://tika.apache.org/ - Extractor de texto multi-formato

---

## 🛡️ Licencia y Copyright

Este proyecto implementa soluciones ETL para transparencia en contratación pública del Paraguay.

**Para uso gubernamental, académico o empresarial.** Contiene código de utilidad pública bajo licencia compatible con la iniciativa de datos abiertos del gobierno paraguayo.

---

## 📞 Soporte y Contribución

Para reportar issues, solicitar features o contribuir al desarrollo del sistema:
- Revisar commits en `src/tasks/*` para legacy code (documentado pero descontinuado)
- Ejecutar pipelines desde `scripts/run_*_pipeline.py` para uso productivo actual

**Versione recomendada de Python:** 3.10+  
**Python:** Sí, el sistema corre nativamente en entornos Python con dependencias listadas en `requirements.txt`.


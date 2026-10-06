# Preguntas típicas de la reunión con tutores — respuestas verificadas

> Respuestas concretas, en 3–6 líneas cada, con herramienta/script y cifra. Todo verificado contra el
> repo (HOJA DE HECHOS). Si algo tiene duda, se marca `[NO CONFIRMADO]`.

## 1. "¿Cómo extrajiste los datos de los pliegos?"

Los pliegos son **PDF** descargados del portal DNCP. Cada PDF se parseó con **Apache Tika**
(contenedores Podman en paralelo) obteniendo el texto **línea a línea** con su página, guardado en
Parquet (~50M líneas). Además de ese texto plano, de cada PDF se leyó su **índice/bookmarks
(outlines)** con PyPDF2.

## 2. "¿Cómo los metiste a la base de datos?"

Con el pipeline `sections_to_db_pipeline` (script `scripts/run_sections_to_db_pipeline.py`): lee las
secciones limpias desde Parquet y las inserta en PostgreSQL con **COPY** (`execute_values`) en
batches de 10,000 filas, commit por batch, `ON CONFLICT DO NOTHING`. La tabla `pliegos_secciones` está
**particionada por año** y es **idempotente** (re-ejecutable sin duplicar).

## 3. "¿Cuál fue la estrategia (outlines dentro del PDF)?"

Se usó la **estructura del propio PDF**: sus **bookmarks (outlines)** definen los títulos de
secciones y su página. El pipeline cruza cada título del outline contra la línea del cuerpo en que
aparece (normalización NFKD sin acentos) y extrae el texto "desde este título hasta el siguiente".
Se resuelven **5 casos de límites** (entre páginas, mismo-página, fin de documento) para no perder
ni duplicar contenido. → docs: `README_FIX_SECTION_EXTRACT.md`, `pdf_content_pipeline.py`.

## 4. "Muéstrame un ejemplo de cómo extrajiste las variables y las metiste."

Contexto general: por sección se genera `title_normalized` (título limpio), `content_text`, fechas no.
Para las **variables** (`document_features_v2`) el pipeline aplica **regex** sobre el texto de la
sección para campos con patrón (p.ej. `dias_pago_maximo`, `pct_garantia_cumplimiento`) y **LLM
fallback** para lo semántico.campo `pct_garantia_cumplimiento` se extrae con un patrón determinista y
se valida contra el golden set (100% en campos de plantilla). Ejemplo real de transformación:
`content_clean[]` (líneas) → `content_text` → field calificado con `_source: regex/absent/llm` y `_conf`.

## 5. "¿Qué limpiezas y filtros aplicaste?"

**Filtros**: solo **pliegos electrónicos** (`application/pdf` tipo "Pliego Electrónico de Bases y
Condiciones") → **~31,387 licitaciones**; el resto se descarta antes de descargar. **Limpieza**:
unión de guiones de fin de línea (`docu-\nmento` → `documento`), eliminación de líneas vacías y de
solo números de página, normalización Unicode/espacios, stemming básico, y descarte de secciones con
longitud 0.

## 6. "¿Solo pliegos electrónicos? ¿Qué pasa con el resto?"

Sí, el pipeline es explícito: conserva `tender_documents_format == application/pdf` **y** tipo
"Pliego Electrónico de Bases y Condiciones". Los pliegos escaneados (imagen/OTROS) no pasan a la
whitelist → es un filtro de **calidad de texto** (el PDF electrónico es el único donde el texto es
editable/extraíble). Log en la clase `OcdsCsvPipeline`: "Filtradas %d licitaciones sin pliego
electrónico".

## 7. "¿Los ZIP bajados? ¿Cómo transferiste OCDS a tu base?"

Se descargó el **bulk `masivo_{year}.zip`** del portal (`data/raw/csv/datasets/`, 73–462 MB por
año). El `ocds_csv_pipeline` lo lee y carga **17 tablas OCDS estructuradas** (licitaciones,
oferentes, adjudicaciones, contratos, pagos, items…). No se usó la API en vivo del DNCP — se eligió
el **bulk CSV** por reproducibilidad y completitud; el motivo "rate limit" exacto no aparece en docs
`[NO CONFIRMADO — solo confirmada la elección de bulk]`.

## 8. "¿No usaste la API de la DNCP por rate limit?"

Correcto, no se usó la API en vivo como fuente primaria: el pipeline opera sobre el **bulk CSV
(`masivo.zip`)** del portal, lo que evita límites de tasa y permite reconstruir el dataset completo y
repetible. *Matiz académico: la motivación exacta de rate limit está documentada solo implícitamente
(no como cita en el repo); si el tutor lo pregunta, se afirma la elección de bulk y se puede citar la
política de la API del portal.*

## 9. "¿Cómo descargaste los PDFs de forma óptima?"

Descargador **asíncrono concurrente** (`async_download_pipeline.py`): batches de **1,000** PDFs con
pausa de **300 s** entre batch. Maneja `.zip/.rar` embebidos (descomprime y extrae el PDF),
renombra `{year}_{categoria_id}_{nro_licitacion}.pdf` y organiza por año/categoría. Resultado:
**32,785 PDFs**.

## 10. "¿Cómo transformaste con Tika?"

Tika corre en contenedores **Podman** paralelos (puertos 9998/9999, `tika_init.py`). El
transformador `pdf_transformer.py` parsea el XHTML devuelto por Tika, omite líneas de número de
página (`^\d{1,3}/\d{1,3}$`) y emite por página un bloque de líneas con `Semaphore(64)` para el
throttling. Nota: el `docker/docker-compose-tika.yml` referido **no está en el repo** (carpeta
`docker/` vacía) → la infra real Tika `[NO CONFIRMADO en disco]`.

## 11. "¿Cómo corregiste y limpiaste todo el proceso?"

Tres fuentes de corrección documentadas:
1. **Límites de sección**: Issue A (subtítulo "Adenda" absorbía páginas → corte al final de la página
   anterior), Issue C (última sección sin fin → marcador `__END_OF_DOCUMENT__`).
2. **Deduplicación**: TOC que coincidía con contenido real (commit `09e9f50`).
3. **Limpieza de texto**: ordena los pasos de `clean_content_field`.

## 12. "Los datos ya están limpios en base: ¿regex o LLM para transformarlos? ¿Cuál tiene sustento académico?"

Esta es la decisión abierta 🟡. Evidencia del proyecto:
- **regex determinista**: reproducible, cero costo, 100% en campos de plantilla (Sesión 16: 7 campos
  portados a regex con subidas de +25 a +88 pp). Respaldo: red flags computables (OCP & Development
  Gateway 2016); "rigged specifications" (World Bank 2011).
- **LLM con postproceso**: cubre semántica; Llama 3.1 8B-AWQ local = **100/100** en golden set humano
  (Sesión 14). Respaldo académico: Katona & Fazekas (2024, GTI-WP) — el texto predice oferta única
  77%→82%; Peppes et al. (2026, Information); Lima et al. (2020, EMNLP).
- **Benchmark del proyecto** (`comparativa_estrategias.csv`): H=65.1 > L_pp=63.1 > R=61.0; el
  postproceso determinista aporta +12pp al LLM.
- **Posición propuesta**: **híbrido** — regex por defecto cuando existe patrón plantilla, LLM como
  fallback para lo semántico, con capa de postproceso determinista compartida. Esto sigue a la vez a
  OCP (estructura computable) y a Katona (texto predictivo).

## 13. "¿Qué sigue?"

- Definir la estrategia regex/LLM para los ~12 campos hybrid restantes (~324K documentos pendientes).
- Modelado de anomalías: Isolation Forest / índices de red flags (CRI) / red de co-bidding (novedad
  Paraguay).
- Validación externa con protestas (1,253 protestas / 967 licitaciones). Ver
  `estrategias-anomaly-deteccion.md`.
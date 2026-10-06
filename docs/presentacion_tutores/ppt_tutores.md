# PPT para tutores — Pipeline de datos DNCP (tesis de detección de anomalías)

Outline de diapositivas con nivel de detalle medio (cómo se hizo cada etapa). Fuente de todos los
números: HOJA DE HECHOS del agente documentador-tutores (verificada contra el repo).

## Slide 1 — Título

**Resumen:** Tesis que construye un pipeline ETL de 7 etapas: de las fuentes públicas de
contrataciones de Paraguay (portal OCDS + PDFs de pliegos) hasta una matriz de features por
licitación para detección de anomalías.

- Cubre datos 2021–2025 (parcial 2026): ~31,387 licitaciones con pliego electrónico, 32,785 PDFs y
  ~2.78M secciones procesadas.
- Se usan dos capas de datos: estructurados (OCDS bulk CSV) y no estructurados (texto de pliegos).
- Todo el flujo es reproducible, idempotente y particionado por año.

## Slide 2 — Pregunta de investigación

**Resumen:** ¿Se puede detectar riesgo de anomalía a partir de los pliegos y de la trayectoria OCDS
sin ground truth? La tesis explora señales de riesgo derivadas del texto y de la competencia.

- No hay etiquetas públicas de corrupción en Paraguay, por lo que se trabaja con "red flags"
  computables: condiciones que restringen competencia, precios atípicos, patrones de adjudicación.
- Se combinan datos estructurados (quién ganó, a qué precio, con qué oferta) con el texto no
  estructurado de los pliegos (requisitos y cláusulas que pueden sesgar la competencia).
- La hipótesis de trabajo sigue a Katona & Fazekas (2024): el texto de los llamados predice riesgo de
  oferta única mejor que la estructura sola (77%→82%).

## Slide 3 — Arquitectura del pipeline (visión general)

**Resumen:** El pipeline va de las fuentes públicas (bulk OCDS + PDFs de pliegos) hasta la matriz de
features por licitación y el algoritmo de detección de anomalías. Los ZIP se cargan a PostgreSQL, el
texto de los pliegos se convierte en secciones, y Python extrae campos con regex o LLM.

- De los ZIP `masivo_{year}.zip` (CSV estructurados) → PostgreSQL: 17 tablas OCDS.
- De los PDFs de pliegos → texto → secciones (~2.78M) → limpieza → `pliegos_secciones`.
- Python (Pipeline 2) extrae 81 campos: 69 con regex determinista + 12 con LLM local (fallback).
- Se construyen 3 matrices de features por licitación (estructura, pliego, economía) que alimentan
  el algoritmo de anomalías.
- `GráficaFull:` graficos/flujo_pipeline.png
## Slide 4 — Etapa 0 · Fuentes de datos (detalle)
**Resumen:** Dos fuentes complementarias del Portal DNCP: el bulk OCDS estructurado y los PDFs de pliegos, unidos por nro_licitacion.
- `GráficaFull:` graficos/etl_1_fuente.png
- Bulk oficial: mismo corte para las 17 tablas, sin límites de API

## Slide 5 — Etapa E · Extracción (detalle)
**Resumen:** Seis pasos llevan del ZIP y los PDFs crudos a 2.78M de secciones limpias, con checkpoints por año.
- `GráficaFull:` graficos/etl_2_extraccion.png
- Tika + PyPDF2 + asyncio · 5 casos de límite resueltos

## Slide 6 — Etapa T · Transformación (detalle)
**Resumen:** 81 campos por licitación: 69 por regex determinista y 12 por LLM híbrido con postproceso compartido.
- `GráficaFull:` graficos/etl_3_transformacion.png
- Híbrido H = 68.3 supera a regex puro (67.0) y a LLM solo (63.1) sobre GT humano

## Slide 7 — Etapa L · Carga (detalle)
**Resumen:** Todo queda en PostgreSQL dncp: 17 tablas OCDS, secciones particionadas y 3 matrices de features consultables en SQL.
- `GráficaFull:` graficos/etl_4_carga.png
- COPY por lotes · ON CONFLICT DO NOTHING · checkpoints idempotentes

## Slide 8 — Aplicación del algoritmo (candidatos)
**Resumen:** Los insumos ya están calculados; la familia de algoritmos se decide con los tutores según precisión, explicabilidad, novedad y costo.
- `GráficaFull:` graficos/etl_5_algoritmo.png
- 6 familias evaluadas: ensemble, CRI+Benford, screens, co-oferta, supervisado, NLP

## Slide 9 — Origen de los datos (fuentes)

**Resumen:** Los datos estructurados se descargaron como bulk CSV OCDS (`masivo.zip`) en lugar de la
API en vivo, para asegurar una descarga completa, reproducible y sin límites de tasa.

- El portal público `contrataciones.gov.py` publica datos OCDS en CSV agrupados por año como
  `masivo_{year}.zip` (73–462 MB según el año; verificado en disco).
- La elección de bulk y no API en vivo busca reproducibilidad y completitud; el motivo "rate limit"
  exacto no figura en docs `[NO CONFIRMADO — solo confirmada la elección de bulk]`.
- Scripts: `src/etl/extractors/ocds_csv_extractor.py` y `scripts/run_ocds_csv_pipeline.py` con
  `YEARS = [2021..2025]`.
- `Gráfica:` graficos/tamano_masivo_zip.png

## Slide 10 — Filtro inicial: solo pliegos electrónicos

**Resumen:** Antes de descargar nada se filtró la lista de documentos para conservar solo los pliegos
electrónicos en PDF, garantizando que el texto sea extraíble (calidad).

- De `tender_documents` se conservan solo registros con `format == application/pdf` y tipo "Pliego
  Electrónico de Bases y Condiciones" (`src/tasks/extract_tender_documents.py`).
- El filtro deduplica por `nro_licitacion` y deja una whitelist de ~31,387 licitaciones
  (`ten_documents_pliego_pdf_every_year_filtered.csv`).
- Los pliegos escaneados u otros formatos se descartaron antes de la descarga: es un filtro de
  calidad de texto (el PDF electrónico es el único con texto editable).

## Slide 11 — Descarga masiva y óptima de PDFs

**Resumen:** Los pliegos se bajan del portal en paralelo con un descargador asíncrono para completar
la colección de 32,785 PDFs sin saturar el servidor de la DNCP.

- Se usó un descargador asíncrono (`src/utils/file_async_downloader.py` + `async_download_pipeline.py`)
  con batches de 1,000 PDFs y una pausa de 300 s entre batch para respetar el portal.
- Cada archivo bajado se descomprime si es `.zip/.rar` interno y se extrae el PDF, descartando el resto.
- Se renombró cada PDF a `{year}_{categoria_id}_{nro_licitacion}.pdf` y se organizó en
  `data/raw/pdf/{year}/category_{id}/`, dejando el corpus trazable por licitación.
- `Gráfica:` graficos/pdfs_por_anio.png

## Slide 12 — Estrategia de extracción: outlines del PDF

**Resumen:** Además del texto plano, se aprovecha la estructura del propio PDF (su índice/bookmarks)
para delimitar secciones de forma confiable en vez de adivinar por encabezados.

- Cada PDF se parsea con **Apache Tika** (contenedores Podman paralelos, puertos 9998/9999,
  `src/utils/tika_init.py`) → texto línea a línea con su página (~50M líneas, Parquet 1.93 GB).
- Con PyPDF2 + pdfplumber se extraen los **outlines/bookmarks** (`pdf_outline_extractor.py`),
  conservando profundidad 1 y 2 (capítulos y secciones) → `PDFOutline(document_id, title, page, depth, ...)`.
- Nota: el `docker/docker-compose-tika.yml` referido no está en el repo (carpeta `docker/` vacía),
  por lo que la infraestructura real de Tika es `[NO CONFIRMADO en disco]`.

## Slide 13 — Cruce outlines + líneas → secciones

**Resumen:** Cada título del índice se matchea contra su línea en el cuerpo y se delimita la sección
hasta el siguiente título, resolviendo 5 casos de límites para no perder ni duplicar contenido.

- Normalización `normalize_exact` (NFKD, sin acentos/puntuación, minúsculas) para cruzar el título
  del outline contra la línea del cuerpo (`pdf_content_pipeline.py`).
- `determine_section_end` resuelve 5 casos: mismo-página, entre páginas (corte al final de la página
  anterior), límite virtual de nivel 1 y marcador `__END_OF_DOCUMENT__` para la última sección.
- Fixes documentados: Issue A (subtítulo "Adenda" que absorbía páginas ajenas) e Issue C (última
  sección sin fin); deduplicación de TOC vs secciones reales (commit `09e9f50`).
- Resultado: **~2.78M secciones** en `pliegos_secciones`.

## Slide 14 — Limpieza y calidad del texto

**Resumen:** El texto se limpia sistemáticamente para normalizar el contenido y descartar ruido
(números de página, líneas vacías, saltos de línea artificiales).

- `content_cleaning_pipeline.py` (`clean_content_field`) aplica en orden: quita `\r`, une guiones de
  fin de línea (`docu-\nmento` → `documento`), normaliza espacios, elimina líneas vacías y de solo
  números de página, reemplaza caracteres Unicode problemáticos y aplica stemming básico español.
- Salida por sección: `content_clean[]`, `content_text` y `content_length_clean`; se descartan las
  secciones con longitud 0.
- Se calcula `word_count` recalculado para las 2,775,485 secciones (media 204.5 palabras/sección).

## Slide 15 — Carga a PostgreSQL

**Resumen:** Las secciones se cargan a PostgreSQL de forma idempotente, particionada por año y con
checkpoints, para que el pipeline sea re-ejecutable sin duplicar.

- Tabla `pliegos_secciones` **particionada por año** (LIST(year), particiones `_2021.._2025`), con
  índices brin(page), gin trigram sobre `title_normalized` y unique
  `(nro_licitacion, title, line_start, year)`.
- Carga con **COPY** (`execute_values`) en batches de 10,000 y commit por batch para evitar OOM del
  WAL y permitir recuperación parcial.
- Upserts `ON CONFLICT DO NOTHING` + checkpoint por año (`.sections_db_done_{year}`) → idempotente.
- El resto de las tablas OCDS del bulk (17) se carga con el orden de claves foráneas respetado.

## Slide 16 — Volúmenes por capa (resultado)

**Resumen:** Resultado numérico de cada etapa del pipeline, desde las fuentes hasta las features por
licitación.

- Cadena verificada: 31,387 whitelist → 32,785 PDFs → ~2.78M secciones → 31,321 documentos con features.
- `Gráfica:` graficos/volumenes_capas.png

## Slide 17 — Features estructuradas y semánticas

**Resumen:** Se construyen tres matrices de features por licitación: estructurales del pliego,
semánticas (81 campos de texto) y económicas de competencia/colusión.

- `document_features_v2`: 31,321 × 108 — 81 campos de pliego (69 regex + 12 hybrid) sobre 25 títulos
  (323 títulos normalizados distintos).
- `document_economic_features`: 29,811 × 46 — 12 campos económicos derivados (oferta única,
  sobreprecio, pagos, protestas) + 4 de colusión con window functions (winner_category_frequency,
  winner_total_contracts, is_repeat_winner, bidder_diversity).
- `document_features`: 31,321 × 571 — estructurales por título (`has_`, `len_`, `tok_`, `word_`,
  `count_`, `span_`, `page_range_`) + agregados (gini, entropía, páginas, word count).
- `Gráfica:` graficos/campos_schema.png

## Slide 18 — Estrategias de extracción comparadas

**Resumen:** Se compararon 5 estrategias (regex puro, regex+postproceso, LLM crudo, LLM+postproceso e
híbrido) sobre un golden set humano congelado de 5,066 juicios en 26 campos.

- Benchmark `data/processed/alolab/comparativa_estrategias.csv`: media de acc
  **H=68.3 > R=67.0 > Ri=66.9 > L_pp=63.1 > L_raw=51.0**.
- El **postproceso determinista** aporta +12 pp al LLM (51→63.1); el híbrido añade +5 pp más (68.3).
- El regex determinista alcanza 100% en campos de plantilla (pct uniformes, conteo de ítems nivel 1).
- `Gráfica:` graficos/comparativa_estrategias.png

## Slide 19 — regex vs LLM: sustento académico (🟡 decisión)

**Resumen:** La decisión abierta es cómo extraer los campos del texto de pliego. La arquitectura ya
es híbrida, y ambos lados tienen respaldo académico y evidencia en el repo.

- **Regex/determinista**: reproducible y sin costo por documento; red flags computables con datos
  estructurados (OCP & Development Gateway 2016) y "rigged specifications" detectables con patrones
  (World Bank 2011).
- **LLM con postproceso**: cobertura semántica; el texto predice oferta única 77→82% (Katona &
  Fazekas 2024); transformers para indicadores de riesgo en pliegos (Peppes et al. 2026; Lima et al. 2020).
- Evidencia propia: Llama 3.1 8B-AWQ local = **100/100** sobre golden set humano (Sesión 14);
  guided decoding mantiene la precisión y es **7× más rápido** (8.95 vs 1.32 docs/s).
- Propuesta: **híbrido** — regex por defecto cuando hay patrón plantilla, LLM como fallback para lo
  semántico, con capa de postproceso determinista compartida.

## Slide 20 — ¿Qué campos van a regex y cuáles a LLM? Criterios de decisión

**Resumen:** De los 81 campos, **69 son regex y 12 hybrid**. La elección no fue arbitraria: se aplicó
una heurística campo por campo, validada contra el golden set humano (5,066 juicios).

- **Regla**: si el pliego usa un **patrón plantilla estable** y la regex valida **≥90% con 0 falsos
  positivos** → se porta a **regex** (14 campos portados en Sesiones 15-16, con subidas de +25 a +88 pp).
- Si el valor exige **comprensión semántica o desambiguación** (días asociados a una cláusula, ratios
  decimales en prosa, conversión meses→días) → queda **hybrid (LLM)** + postproceso determinista.
- **Portados a regex** (ejemplos): `num_categorias_documentos` (conteo ítems nivel 1, 100%, +88pp),
  `num_referencias_minimo` (98%, +87.5pp), `pct_diferencia_precio_*`, `pct_garantia_cumplimiento`
  (pct uniformes al 100%), `dias_presentacion_garantia` (plantilla "dentro de los N días", 200/200).
- **Se quedan en LLM** (ejemplos): `dias_notificacion_fuerza_mayor` (regex solo 5.5% vs LLM 94.5%),
  `dias_solicitud_anticipo` / `dias_pago_anticipo` (desambiguación abonar vs solicitar),
  `ratio_liquidez_minimo` / `ratio_endeudamiento_maximo` (decimales sin patrón fijo).
- **3 irresolubles**: `num_componentes_formula`, `num_documentos_pago`, `num_pasos_verificacion` —
  ni regex ni LLM cuentan listas/fórmulas con sentido semántico (se mantienen hybrid, rendimiento bajo
  documentado). Detalle completo: `decision_regex_llm.md`.

- `Gráfica:` graficos/portes_regex.png
## Slide 21 — Setup de IA local

**Resumen:** Todo el LLM corre en local: una RTX 4070 Ti SUPER 16 GB con vLLM 0.23.0 en WSL2 (y un
runtime secundario llama.cpp en Windows), sin depender de APIs externas.

- GPU **RTX 4070 Ti SUPER 16 GB** (VRAM útil ~15.6 GB tras matar NVIDIA Overlay, Warp, Edge, SearchApp)
  · 32 GB RAM · Windows 11 + WSL2 Ubuntu · driver 581.29, CUDA 12.4.
- Motor principal: **vLLM 0.23.0** en WSL2 (`/root/vllm-env/bin/python`); se fija 0.23.0 porque
  vLLM 0.25.x no funciona en WSL (MRv2 requiere UVA) y hay que usar `VLLM_USE_V2_MODEL_RUNNER=0`.
- **Formato AWQ nativo** (GGUF está deprecated en vLLM); el kernel `awq_marlin` solo sirve para
  arquitecturas Mistral/Llama (Qwen/Phi usan `awq` normal, más lento).
- Runtime secundario: **llama.cpp `llama-server.exe` en Windows** (puerto 1234) para probar
  Qwen3.8-27B, porque el vhdx de WSL se quedó sin disco en D: (0.26 GB libres).
- Detalle completo: `setup_ia_benchmark.md`.

## Slide 22 — Benchmark de 12+ modelos locales

**Resumen:** Se evaluaron 12+ modelos en GGUF y AWQ (muestras de 5 a 80) hasta elegir el campeón:
Mistral 7B AWQ para throughput y Llama 3.1 8B AWQ para calidad.

- **🥇 Mistral 7B v0.3 AWQ** (awq_marlin): 100% en 10/10, **1.61s**, F1 **94.0%**, 64-77 tok/s,
  3.89 GiB VRAM, 35.22× concurrencia → **campeón de throughput**.
- **🥇 Llama 3.1 8B AWQ** (awq_marlin): 100% en 5/5, **1.93s**, F1 **96.4%**, 55-68 tok/s,
  5.39 GiB VRAM, 29.21× concurrencia → **campeón de calidad** (elegido para producción).
- Alternativas AWQ: Qwen2.5 14B (2.46s, F1 93.3%), Qwen3-8B (4.80s), Qwen3-14B (5.29s), Phi-4 (90%,
  4.47s, problemas dtype), Qwen3.8-27B en Windows (97/100, 1.41s/muestra).
- GGUF deprecado: Mistral 7B GGUF (100%, 4.90s) es 3× más lento que AWQ; Qwen3-14B (100%, 5.10s).
- Proyección para ~324K docs del fallback: **~4h** (Mistral) o **~5.8h** (Llama) con continuous
  batching, vs ~34 días del baseline HTTP original.

- `Gráfica:` graficos/benchmark_modelos.png
- Concurrencia: vLLM agrupa requests con continuous batching (hasta 35× en paralelo); llama-server solo atiende 1 slot secuencial
- Impacto en producción (324K docs): Mistral AWQ 35× ≈ 4 h vs Qwen3.8 llama.cpp secuencial ≈ 5.3 días
## Slide 23 — De 59.6% a 100%: prompts + proceso AloLab

**Resumen:** La precisión del LLM en los campos difíciles subió de 59.6% a 100% iterando con un ciclo
artesanal de diagnóstico de errores sobre un golden set humano.

- Baseline (460 muestras, campos hybrid): 274/460 = **59.6%** → con el prompt "primera línea"
  (número en la 1ª línea + max_tokens=256) → **83.9%** (Sesión 11).
- **Guided decoding** (regex vLLM que obliga a emitir número o NULL): misma precisión 83.9% pero
  **7× más rápido** (51.4s vs 347.5s, 8.95 vs 1.32 docs/s) — Sesión 12.
- **AloLab** (Sesión 14): script exporta los errores del modelo sobre 100 GT humanas → el agente
  diagnostica y reescribe hint o postprocess → re-run. Rondas: 95 → 95 → 95 → **100/100**.
- Los falsos positivos se corrigen mejor en **postprocess determinista** (ej. conversión meses→días
  {1:30,2:61,3:91,6:182,12:365}) que en el prompt; hint con valores concretos induce sobre-extracción.
- Few-shot retrieval se probó y se **descartó** (empeoraba: 95% → 85%). Detalle en `setup_ia_benchmark.md`.

- `Gráfica:` graficos/alolab_rondas.png
## Slide 24 — Siguientes pasos

**Resumen:** Quedan tres frentes abiertos: ejecutar el fallback LLM (~324K docs pendientes), validar
el schema del clustering y definir la ruta del Pipeline 3 de detección de anomalías.

- **Fallback LLM en producción**: 12 campos hybrid con conf < 0.8 (~324K documentos); pipeline
  codificado pero nunca ejecutado (~3h GPU con Llama 3.1 8B AWQ).
- **Schema validation del clustering**: 2/10 títulos validados (faltan 9).
- **Modelado de anomalías**: hoja de ruta Benford → CRI → ISF/ECOD → co-bidding → supervisado con
  protestas (1,253 protestas / 967 licitaciones como validación externa).

- `Gráfica:` graficos/proyeccion_fallback.png
## Slide 25 — Cierre

**Resumen:** El proyecto entrega un corpus limpio, trazable y reproducible, listo para el modelado de
anomalías como aporte de novedad para Paraguay.

- Pipeline reproducible, idempotente y particionado por año (2021–2025, parcial 2026).
- Extracción de campos validada con LLM local (100/100 en golden set humano) + capa determinista.
- Preguntas y comentarios.

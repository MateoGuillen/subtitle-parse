# El proyecto paso a paso — de los pliegos del DNCP a la detección de anomalías

> Documento de lectura para tutores: recorre el proyecto completo (tesis de detección de anomalías en
> contrataciones públicas del Paraguay) etapa por etapa, con los números verificados del repositorio
> (HOJA DE HECHOS del agente `documentador-tutores` + `AGENTS.md` + `PIPELINE_CHECKLIST.md`).

## Resumen ejecutivo

El proyecto construye un pipeline ETL que toma los datos públicos de contrataciones de Paraguay
(portal `contrataciones.gov.py`, formato OCDS) y los textos completos de los pliegos de bases y
condiciones (PDF), los transforma en ~2.78M secciones limpias en PostgreSQL, extrae de ellos **81
variables de riesgo** (69 con regex determinista + 12 híbridas con fallback de LLM local) y construye
features por licitación para alimentar la **detección de anomalías/corrupción** (aislamiento de
licitaciones atípicas, red flags compuestos y redes de co-oferta). Todo es reproducible, idempotente y
particionado por año (2021–2025, con parcial de 2026).

---

## Paso 1 — Origen y adquisición de datos

| Aspecto | Detalle verificado |
|---|---|
| Fuente estructurada | Bulk **CSV OCDS `masivo.zip`** del portal `contrataciones.gov.py` (NO se usa la API en vivo) |
| Scripts | `src/etl/extractors/ocds_csv_extractor.py`, `scripts/run_ocds_csv_pipeline.py` (`YEARS = [2021..2025]`) |
| ZIPs en disco | `data/raw/csv/datasets/masivo_{year}.zip` — 2021=73.0 MB · 2022=258.7 · 2023=164.6 · 2024=319.2 · 2025=462.0 · 2026=5.9 (parcial) |
| Filtro de calidad | De `tender_documents` se conservan solo registros `application/pdf` con tipo **"Pliego Electrónico de Bases y Condiciones"** |
| Whitelist | ~**31,387** licitaciones con pliego electrónico (`ten_documents_pliego_pdf_every_year_filtered.csv`) |
| Descarga de PDFs | Asíncrona (`file_async_downloader.py`, `async_download_pipeline.py`), batch=1,000, pausa=300 s; descomprime `.zip/.rar` internos; renombra `{year}_{categoria_id}_{nro_licitacion}.pdf` |
| PDFs en disco | **32,785** (`data/raw/pdf/{year}/category_{id}/`) — 2021=1,116 · 2022=4,620 · 2023=6,237 · 2024=9,406 · 2025=10,805 · 2026=601 |

> `[NO CONFIRMADO]` el motivo exacto ("rate limit") de elegir bulk CSV en lugar de la API en vivo no
> está escrito en la documentación; lo confirmado es la elección de bulk por reproducibilidad.

---

## Paso 2 — Parsing (Tika + outlines)

- **Texto plano**: cada PDF se convierte con **Apache Tika** (contenedores Podman, puertos 9998/9999,
  `src/utils/tika_init.py`, 8 contenedores). La infraestructura real de Tika es `[NO CONFIRMADO en
  disco]` (la carpeta `docker/` está vacía en el repo).
- **Estrategia estructural**: además del texto, se extraen los **outlines/bookmarks** del PDF con
  PyPDF2 + pdfplumber (`pdf_outline_extractor.py`), manteniendo profundidad 1 y 2 →
  `PDFOutline(document_id, title, page, depth, nro_licitacion, category_id, year, line_number)`.
- **Líneas de texto completo**: `masive_async_convert_pdf_parquet.py` (pdfplumber) genera
  `combine_documents_all_years.parquet` (~1.93 GB, ~50M líneas).
- **Cruce outlines + líneas → secciones** (`pdf_content_pipeline.py` + `pdf_content_transformer.py`):
  normalización `normalize_exact` (NFKD, sin acentos/puntuación, minúsculas) y `determine_section_end`
  con **5 casos de límites** (mismo-página; entre páginas con corte al final de la página anterior;
  caso virtual depth-1; marcador `__END_OF_DOCUMENT__` para la última sección). Incluye deduplicación
  de TOC vs secciones reales (commit `09e9f50`).
- **Resultado**: **~2.78M secciones** de pliego (2,775,485 filas verificadas).

---

## Paso 3 — Limpieza y carga a PostgreSQL

- **Limpieza** (`content_cleaning_pipeline.py`, `clean_content_field`): quita `\r`, **une guiones de
  fin de línea** (`"docu-\nmento"` → `"documento"`), normaliza espacios múltiples, elimina líneas
  vacías y de solo números de página, reemplaza caracteres Unicode problemáticos y aplica stemming
  básico en español → `content_clean[]`, `content_text`, `content_length_clean`.
- Se descartan secciones con `content_length_clean == 0`; luego se calculan `word_count` y features
  estructurales (media 204.5 palabras/sección).
- **Carga** (`sections_to_db_pipeline.py`): tabla `dncp.pliegos_secciones` **particionada por año**
  (LIST(year), particiones `_2021.._2025`), índices brin(page) y gin trigram, unique
  `(nro_licitacion, title, line_start, year)`. Carga con **COPY** (`execute_values`) en batches de
  10,000, commit por batch y upserts `ON CONFLICT DO NOTHING`; checkpoints por año
  (`.sections_db_done_{year}`) → idempotente.
- El resto de las tablas OCDS del bulk se cargan a PostgreSQL respetando el orden de claves foráneas.

---

## Paso 4 — Extracción de campos del pliego (Pipeline 2)

- **Esquema**: 25 títulos de pliego con **81 campos = 69 regex + 12 hybrid**
  (`master_schemas_v2.json`; 7 campos pasaron de hybrid a regex en la Sesión 16).
- **Arquitectura híbrida**: regex determinista primero (CPU, ~70 s, 69 campos) → capa de
  **postproceso determinista compartida** (`src/etl/transformers/extraction_postprocess.py`, fuente
  única para pct deterministas, conversión meses→días, reglas anti-falsos-positivos) → **fallback
  LLM** solo para los campos hybrid con `conf < 0.8` (12 campos).
- **Confianza por campo**: el transformer genera columnas `{field}_conf` (0.0–1.0) para campos
  hybrid; regex acertado = 1.0, ausencia de sección = 1.0, sin match = 0.0.
- **Modelo LLM elegido**: **Llama 3.1 8B AWQ** local (vLLM offline, RTX 4070 Ti SUPER 16 GB),
  validado **100/100** sobre el golden set humano (ciclo AloLab, Sesión 14) con postproceso
  determinista. Guided decoding: misma precisión, **7× más rápido** (8.95 vs 1.32 docs/s).
- **Comparativa de estrategias** (`data/processed/alolab/comparativa_estrategias.csv`, 5,066 juicios
  GT congelados): media acc **H=65.1 > L_pp=63.1 > R=61.0 > L_raw=51.0** (Ri=60.9). El postproceso
  determinista aporta +12 pp al LLM; el híbrido añade +2 pp más.
- **Ahorro documentable**: el fallback LLM solo procesa donde falló regex → ~82% menos llamadas que
  un pipeline 100% LLM.
- **Pendiente**: ~**324K documentos** con `conf < 0.8` en los 12 campos hybrid (el pipeline LLM de
  fallback está codificado pero nunca se ejecutó). 3 de los 12 campos se consideran "irresolubles"
  (`num_componentes_formula`, `num_documentos_pago`, `num_pasos_verificacion`).

---

## Paso 5 — Features y ranking de títulos

- **`document_features`** (`document_features_pipeline.py`): **31,321 × 571** columnas — para cada
  uno de los top-80 títulos normalizados: `has_*`, `len_*`, `tok_*`, `word_*`, `count_*`, `span_*`,
  `page_range_*` + agregados (gini, entropía, avg/max page, sum/avg word count). `word_count`
  recalculado para las 2,775,485 secciones.
- **`document_features_v2`**: **31,321 × 108** — los 81 campos extraídos del pliego (Paso 4).
- **`document_economic_features`** (`document_economic_features_pipeline.py`): **29,811 × 46** —
  12 campos económicos derivados (oferta única, sobreprecio, ratios, pagos, protestas) + 4 de
  colusión con window functions (`winner_category_frequency`, `winner_total_contracts`,
  `is_repeat_winner`, `bidder_diversity`; este último degradado por bug de fuente `cantidad_items`).
- **Ranking de títulos** (`title_ranking_pipeline.py`): **6 estrategias ponderadas** (Outlier
  Frequency 0.20, Random Forest 0.25, Contextual 0.15, Section Isolation Forest 0.15, Document
  Correlation 0.10, Economic Risk 0.15) → ranking de 80 títulos (~11.4 s). Top-1:
  **Fraude Y Corrupcion (0.5432)**; top-10 incluye Confidencialidad De La Informacion, Fuerza Mayor,
  Aclaracion De Las Ofertas, Interpretacion, Formato Y Firma De La Oferta, etc.
- **Clustering de secciones** (Pipeline 1, top-10 títulos del ranking): embeddings
  Sentence-Transformer + UMAP + K-Means con 4 fixes anti-overclustering (silhouette 0.45–0.57,
  3–10 clusters por título). Schema validation: **2/10 títulos** 🟡.

---

## Paso 6 — Detección de anomalías (Pipeline 3)

- **Estado actual**: ❌ **no implementado** — existe solo el diseño en
  `docs/pipeline3_anomaly_detection.md` (actualizado hasta Phase 6) y el mapa completo de
  alternativas en `estrategias-anomaly-deteccion.md` (inventario de la BD verificado por consulta
  directa).
- **Familias documentadas y viabilidad verificada**:
  - **A — Tabular no supervisado** (ISF / LOF / ECOD / ensemble): ✅ alta viabilidad (bloques de
    variables A+B+C completos; ISF validado en Paraguay por Kehler et al. 2019). Estado: 📋 en pausa.
  - **B — Red flags deterministas** (CRI tipo FMI, Benford sobre 1.87M montos de ítems y 265K pagos):
    ✅ alta; Paraguay fue país piloto del FMI.
  - **C — Screens econométricos** (Porter–Zona, Bajari–Ye): ⚠️ parcial — `oferentes` no tiene montos
    por oferta (78,695 ofertas, 5,544 bidders) → solo versiones degradadas.
  - **D — Redes de co-bidding** (Wachs & Kertész 2019): ✅ novedad real para Paraguay (78,695
    ofertas, 5,544 bidders).
  - **E — Supervisado con protestas como proxy** (1,253 protestas / 967 licitaciones): ✅
    recomendado como validación.
  - **F — NLP de pliegos** (Katona & Fazekas 2024): 🟡 el pipeline v2 ya es el preproceso; falta el
    modelo texto → `es_unico_oferente`.
- **Hoja de ruta recomendada**: Benford → CRI → ISF/ECOD → Ensemble + SHAP/DIFFI → Co-bidding
  network → Supervisado con protestas → Katona–Paraguay.

---

## Estado actual y próximos pasos

| Estado | Ítem | Detalle |
|---|---|---|
| ✅ | Ingesta (FASE 0) | Descarga bulk OCDS + filtro pliegos electrónicos + 32,785 PDFs + Tika + outlines + limpieza + carga |
| ✅ | `pliegos_secciones` | ~2.78M secciones en PostgreSQL, particionada por año |
| ✅ | Features | `document_features` 31,321×571 · `document_features_v2` 31,321×108 · `document_economic_features` 29,811×46 |
| ✅ | Ranking de títulos | 6 estrategias, 80 títulos, top-1 Fraude Y Corrupcion (0.5432) |
| ✅ | Clustering (Pipeline 1) | 10 títulos, 3–10 clusters, silhouette 0.45–0.57 |
| ✅ | Pipeline 2 — código | 81 campos (69 regex + 12 hybrid), capa postproceso compartida, fallback LLM codificado |
| ✅ | Benchmark de modelos | 12 modelos evaluados; Llama 3.1 8B AWQ = 100/100 en GT humano; guided decoding 7× |
| 🟡 | Decisión regex vs LLM | La arquitectura es híbrida; falta fijar política final por campo (regex si hay patrón plantilla, LLM si hay semántica) |
| 🟡 | Fallback LLM en producción | ~324K docs con conf<0.8 en 12 campos hybrid — pipeline codificado pero nunca ejecutado (~3h GPU) |
| 🟡 | Schema validation clustering | 2/10 títulos validados; faltan 9 (incluye el 1 validado de "idioma de la oferta") |
| 🟡 | ISF | En pausa esperando decisión del usuario sobre la estrategia de Pipeline 3 |
| 📋 | Ejecutar fallback LLM | Llama 3.1 8B AWQ, vLLM offline, actualizar script a `VLLMOfflineProvider` |
| 📋 | Pipeline 3 — detección de anomalías | Implementar según hoja de ruta: Benford → CRI → ISF/ECOD → ensemble → co-bidding → supervisado |
| 📋 | Commit general | Cambios de las Sesiones 1–16 sin commitear + limpieza de scripts temporales |

---

## Decisiones abiertas

1. **🟡 Ejecutar el fallback LLM (~324K docs, 12 campos hybrid).** El pipeline
   `document_features_v2_llm_pipeline` está codificado pero nunca se ejecutó. Requiere: actualizar el
   entry point para usar `VLLMOfflineProvider` (hoy usa HTTP server sin levantar), correr
   `run_document_features_v2_pipeline.py` primero (regenera la tabla con el schema actual de 108
   cols), y ejecutar en WSL2 con Llama 3.1 8B AWQ (~3h GPU). Decidir además si los 3 campos
   "irresolubles" se excluyen por configuración.

2. **🟡 Schema validation del clustering (2/10 títulos).** Los 10 títulos top del ranking ya tienen
   clustering y prompts de schema; solo 2/10 tienen prompts de extracción validados
   (`validate_extraction_prompts.py`). Decidir si se completa la validación de los 9 restantes o si se
   prioriza Pipeline 3.

3. **🟡 Ruta de Pipeline 3 (detección de anomalías).** Elegir entre la hoja de ruta incremental
   (Benford → CRI → ISF/ECOD → ensemble → co-bidding → supervisado con protestas → Katona–Paraguay) y
   reactivar o no el ISF en pausa. Esta decisión define el aporte de novedad final de la tesis.

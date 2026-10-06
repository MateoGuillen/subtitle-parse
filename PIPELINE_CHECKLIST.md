# Pipeline Completo — Tesis: Detección de Anomalías en Pliegos de Licitaciones Públicas

---

## FASE 0: Ingesta de Datos (Fundación)

| # | Pipeline | Archivos | Descripción | Estado |
|---|----------|----------|-------------|--------|
| 0a | **AsyncDownloadPipeline** | `src/pipelines/async_download_pipeline.py` | Descarga masiva de PDFs/JSONs desde DNCP asincrónicamente en lotes | ✅ Completo |
| 0b | **OcdsCsvPipeline (v2)** | `src/pipelines/ocds_csv_pipeline.py` | Descarga ZIP OCDS por año, extrae 19 CSVs, transforma y upserta a 13 tablas PostgreSQL (convocantes, licitaciones, proveedores, adjudicaciones, contratos, enmiendas, pagos, oferentes, notificados, items, criterios, consultas, protestas) respetando orden de FK | ✅ Completo |
| 0c | **PDFParquetPipeline** | `src/pipelines/pdf_parquet_pipeline.py` | Extrae líneas de texto de PDFs via Tika, convierte a formato Parquet particionado por año | ✅ Completo |
| 0d | **PDFOutlinePipeline** | `src/pipelines/pdf_outline_pipeline.py` | Extrae outlines (tabla de contenidos) de PDFs, procesa por lotes y mergea en CSV + Parquet | ✅ Completo |
| 0e | **PdfContentPipeline** | `src/pipelines/pdf_content_pipeline.py` | Empareja líneas de texto con outlines para extraer secciones estructuradas. Asigna `line_start`/`line_end`. Escribe Parquet particionado por año | ✅ Completo |
| 0f | **ContentCleaningPipeline** | `src/pipelines/content_cleaning_pipeline.py` | Limpia el campo `content` de cada sección (remueve ruido, normaliza whitespace). Descarta secciones que quedan vacías | ✅ Completo |
| 0g | **SectionsToDbPipeline** | `src/pipelines/sections_to_db_pipeline.py` | Carga secciones limpias desde Parquet a `dncp.pliegos_secciones` via PostgreSQL COPY. Usa checkpoint files para idempotencia por año | ✅ Completo |

**Output final:** `dncp.pliegos_secciones` (2.7M filas, 17 columnas)

---

## FASE 1: Feature Engineering (Tablas Pre-computadas)

### 1a. DocumentFeaturesPipeline

| Aspecto | Detalle |
|---------|---------|
| **Archivo** | `src/pipelines/document_features_pipeline.py` |
| **Script** | `scripts/run_document_features_pipeline.py` |
| **Tabla output** | `dncp.document_features` — 31,321 documentos × 571 columnas |
| **Dependencia** | SectionsToDbPipeline (`dncp.pliegos_secciones`) |
| **Estado** | ✅ Completo (Phase 1) |

**Mejoras aplicadas (Phase 1):**
- `word_count` recalculado para las 2,775,485 secciones (avg=204.5, max=475K)
- 571 columnas (era 260): agregados los prefijos `word_*`, `count_*`, `span_*`, `page_range_*` por título
- Agregados: gini, entropía, avg/max page, sum/avg word count
- Chunked loading (200K rows/chunk), int64→int32 downcast

**Lo que hace:** Para cada uno de los top-80 `title_normalized`, crea 7 prefix columns:
- `has_{title}` — 0/1 si el documento contiene ese título
- `len_{title}` — cantidad de caracteres
- `tok_{title}` — cantidad de tokens
- `word_{title}` — word count
- `count_{title}` — frecuencia de aparición
- `span_{title}` — diferencia page_end - page_start
- `page_range_{title}` — tupla (page_start, page_end)

---

### 1b. DocumentEconomicFeaturesPipeline

| Aspecto | Detalle |
|---------|---------|
| **Archivo** | `src/pipelines/document_economic_features_pipeline.py` |
| **Script** | `scripts/run_document_economic_features_pipeline.py` |
| **Tabla output** | `dncp.document_economic_features` — 29,811 documentos × 46 columnas |
| **Dependencia** | OcdsCsvPipeline (13 tablas OCDS) |
| **Estado** | ✅ Completo (Phases 2 + 3) |

**Mejoras aplicadas:**
- **Phase 2:** 12 derived fields económicos (ratios, flags) desde JOIN de 9 tablas OCDS
- **Phase 3:** 4 collusion features con window functions (winner_category_frequency, winner_total_contracts, is_repeat_winner, bidder_diversity)
- Fix: SQLAlchemy immutabledict bug → raw psycopg2
- Fix: NaT serialization → `pd.isna(v)` catch-all

**Campos económicos (12 derived):**
- `es_unico_oferente`, `cantidad_oferentes`, `sobreprecio`, `sobreprecio_porcentaje`
- `overbudget_ratio`, `is_high_value_single_bidder`
- `tiene_adiciones`, `propiedades_modificadas`, `monto_modificacion`
- `pagos_recibidos`, `protestas_recibidas`
- `cantidad_items`, `costo_pliego`, `garantia_porcentaje`

**Campos collusion (4):**
- `winner_category_frequency` — frecuencia del ganador en su categoría
- `winner_total_contracts` — total contratos del ganador
- `is_repeat_winner` — flag si ganó múltiples veces
- `bidder_diversity` — n_oferentes_distintos / cantidad_items

**Problemas conocidos:**
- `bidder_diversity` = 0.0 para todos porque `cantidad_items` = 0 en licitaciones (dato fuente)
- `costo_pliego` = 0 y `garantia_porcentaje` = 0 para todos

---

## FASE 2: Ranking de Títulos

### TitleRankingPipeline

| Aspecto | Detalle |
|---------|---------|
| **Archivo** | `src/pipelines/title_ranking_pipeline.py` |
| **Script** | `scripts/run_title_ranking_pipeline.py` |
| **Output** | `data/processed/title_ranking/title_ranking.csv` (80 filas × 10 columnas), `title_ranking_report.md`, `pipeline_summary.json` |
| **Dependencia** | DocumentFeaturesPipeline + DocumentEconomicFeaturesPipeline |
| **Estado** | ✅ Completo (v2.1, 6 estrategias, 11.4s runtime) |

**6 Estrategias con pesos:**

| Estrategia | Peso | Descripción |
|------------|------|-------------|
| Outlier Frequency | 0.20 | Frecuencia de valores extremos por título |
| Random Forest | 0.25 | Importancia de features desde Random Forest |
| Contextual Effect | 0.15 | Efecto contextual del título en el documento |
| Section Isolation Forest | 0.15 | Anomalías por sección via Isolation Forest |
| Document Correlation | 0.10 | Correlación entre títulos y features documento |
| Economic Risk | 0.15 | Correlación con indicadores de riesgo económico |

**Mejoras aplicadas (Phases 4 + 5):**
- 6ta estrategia: `_strategy_economic_risk()` — correlación Pearson media entre has_{slug} y 7 indicadores económicos (es_unico_oferente, overbudget_ratio, is_high_value_single_bidder, winner_category_frequency, is_repeat_winner, winner_total_contracts, bidder_diversity)
- STRATEGY_WEIGHTS: outlier=0.20, rf=0.25, contextual=0.15, section_if=0.15, corr=0.10, economic=0.15
- Memory fix: `_get_relevant_columns()` carga 245 columnas via information_schema en vez de SELECT * (587 columnas), reducción del 58%
- Reporte muestra 6 estrategias (antes mostraba 5)

**Top-10 ranking actual:**

| # | Título | Score Total |
|---|--------|-------------|
| 1 | Fraude Y Corrupcion | 0.5432 |
| 2 | Confidencialidad De La Informacion | 0.5251 |
| 3 | Fuerza Mayor | 0.5238 |
| 4 | Aclaracion De Las Ofertas | 0.4884 |
| 5 | Interpretacion | 0.4880 |
| 6 | Formato Y Firma De La Oferta | 0.4675 |
| 7 | Impuestos Y Derechos | 0.4541 |
| 8 | Derechos Intelectuales | 0.4474 |
| 9 | Retiro Sustitucion Y Modificacion De Las Ofertas | 0.4429 |
| 10 | Causales De Terminacion Del Contrato | 0.4366 |

---

## PIPELINE 1: Section Clustering + LLM Schema Generation

### ⬅️ ACÁ ESTAMOS

| Aspecto | Detalle |
|---------|---------|
| **Archivos** | `src/pipelines/section_clustering_pipeline.py` |
| | `src/etl/extractors/section_clustering_extractor.py` |
| | `src/etl/transformers/section_clustering_transformer.py` |
| | `src/etl/loaders/section_clustering_loader.py` |
| **Script** | `scripts/run_section_clustering_pipeline.py` |
| **Output** | `data/processed/section_clustering/master_schemas.json` |
| | `data/processed/section_clustering/llm_chat_prompts.json` |
| | `data/processed/section_clustering/clustering_comparison.json` |
| | `data/processed/section_clustering/report_*.md` (por título) |
| **Dependencia** | TitleRankingPipeline (usa ranking CSV para seleccionar 10 títulos) |
| **Estado** | ⏳ **Clustering completado, schemas LLM pendientes** |

### Descripción

Para cada uno del top-10 de títulos más relevantes:

1. Extrae todas las secciones de `dncp.pliegos_secciones`
2. Genera embeddings via Sentence-Transformer (`paraphrase-multilingual-MiniLM-L12-v2`)
3. Reduce dimensionalidad con UMAP (n_components=10, n_neighbors=30)
4. Clustering con K-Means buscando k óptimo (2–20) via silhouette + Davies-Bouldin + Calinski-Harabasz
5. Mergea clusters similares (coseno ≥ 0.95)
6. Samplea ejemplos representativos + extremos por cluster
7. Envía samples a LLM para diseñar JSON schema
8. Guarda `master_schemas.json` + reportes por título

### 4 Anti-Overclustering Fixes (commit `869c86c`)

| Fix | Código | Efecto |
|-----|--------|--------|
| **A — k-penalty** | `penalty = 0.08 * k_norm` en `_find_optimal_k_combined()` | Descarta k altos cuando la ganancia de silhouette es marginal |
| **B — Normalización** | `re.sub(r'\s+', ' ', t).strip().lower()` antes de embedding | Elimina ruido por capitalización/espaciado |
| **C — UMAP n_neighbors** | 15 → 30 (valor default) | Preserva estructura global, reduce falsos splits |
| **D — Merge pre-LLM** | `_merge_similar_clusters()` con coseno ≥ 0.95 antes de generar schema | Fusiona clusters casi idénticos |

### Resultados del Clustering

| Título | Raw K | Merged K | Silhouette | DB | CH | Cluster dominante |
|--------|-------|----------|------------|-----|-----|-------------------|
| Fraude Y Corrupcion | 20 | **4** | 0.452 | 1.168 | 622 | 73.0% |
| Confidencialidad De La Informacion | 19 | **9** | 0.522 | 1.066 | 852 | 45.5% |
| Fuerza Mayor | 19 | **10** | 0.540 | 1.063 | 743 | 30.3% |
| Aclaracion De Las Ofertas | 20 | **5** | 0.463 | 1.164 | 593 | 30.0% |
| Interpretacion | 20 | **6** | 0.490 | 1.178 | 712 | 40.8% |
| Formato Y Firma De La Oferta | 20 | **5** | 0.448 | 1.098 | 554 | 75.1% |
| Impuestos Y Derechos | 19 | **7** | **0.567** | **0.966** | **962** | 49.5% |
| Derechos Intelectuales | 20 | **6** | 0.509 | 1.004 | 674 | 52.1% |
| Retiro Sustitucion Y Modificacion De Las Ofertas | 20 | **4** | 0.524 | 1.054 | 800 | 62.2% |
| Causales De Terminacion Del Contrato | 20 | **3** | 0.483 | 1.146 | 623 | 81.2% |

Silhouette mejoró de ~0.26–0.33 (antes de fixes) → **0.45–0.57** (después de fixes).

### Grupos Post-Merge por Título

**Fraude Y Corrupcion (4 grupos):**
- `0_2_4_6_7_8_10_13_14_15_16_17_19`: 3,652 (73.0%) ← dominante
- `1_12_18`: 644 (12.9%)
- `3_9_11`: 555 (11.1%)
- `5`: 149 (3.0%)

**Confidencialidad De La Informacion (9 grupos):**
- `0_4_10_11_12_13_15_16`: 2,277 (45.5%) ← dominante
- `2_5_6_8`: 873 (17.5%)
- `1`: 512 (10.2%)
- `3`: 393 (7.9%)
- `7`: 242 (4.8%)
- `17`: 218 (4.4%)
- `14`: 217 (4.3%)
- `9`: 150 (3.0%)
- `18`: 118 (2.4%)

**Fuerza Mayor (10 grupos):**
- `2_3_4_5_10_11`: 1,517 (30.3%) ← dominante
- `0_8_13_15`: 988 (19.8%)
- `1`: 622 (12.4%)
- `12`: 404 (8.1%)
- `9_14`: 389 (7.8%)
- `17`: 270 (5.4%)
- `18`: 253 (5.1%)
- `7`: 217 (4.3%)
- `6`: 208 (4.2%)
- `16`: 132 (2.6%)

**Aclaracion De Las Ofertas (5 grupos):**
- `2_10_11_16`: 1,499 (30.0%) ← dominante
- `3_5_8_13_17_18`: 1,439 (28.8%)
- `7_9_12_14_15`: 1,131 (22.6%)
- `0_4_6_19`: 653 (13.1%)
- `1`: 278 (5.6%)

**Interpretacion (6 grupos):**
- `0_4_7_10_12_13_14_15_16`: 2,041 (40.8%) ← dominante
- `5_6_8_9_17_18`: 1,333 (26.7%)
- `1`: 655 (13.1%)
- `3_11`: 622 (12.4%)
- `2`: 258 (5.2%)
- `19`: 91 (1.8%)

**Formato Y Firma De La Oferta (5 grupos):**
- `0_1_2_3_6_7_8_9_10_11_13_14_15_16_18_19`: 3,754 (75.1%) ← dominante
- `4`: 498 (10.0%)
- `5`: 378 (7.6%)
- `12`: 218 (4.4%)
- `17`: 152 (3.0%)

**Impuestos Y Derechos (7 grupos):**
- `2_4_5_8_9_12_13_14_15_16_17_18`: 2,476 (49.5%) ← dominante
- `0`: 1,181 (23.6%)
- `3`: 495 (9.9%)
- `10_11`: 362 (7.2%)
- `6`: 224 (4.5%)
- `1`: 157 (3.1%)
- `7`: 105 (2.1%)

**Derechos Intelectuales (6 grupos):**
- `0_1_5_9_11_17_18`: 2,605 (52.1%) ← dominante
- `2_4_7_10_13_14_15_16_19`: 1,432 (28.6%)
- `3`: 368 (7.4%)
- `6`: 236 (4.7%)
- `12`: 228 (4.6%)
- `8`: 131 (2.6%)

**Retiro Sustitucion Y Modificacion De Las Ofertas (4 grupos):**
- `0_4_5_6_7_12_13_15_17_18_19`: 3,110 (62.2%) ← dominante
- `1_3_9_10_11`: 976 (19.5%)
- `8_16`: 569 (11.4%)
- `2_14`: 345 (6.9%)

**Causales De Terminacion Del Contrato (3 grupos):**
- `0_1_2_3_4_5_6_7_8_9_10_11_13_14_15_17_18`: 4,062 (81.2%) ← dominante
- `12_19`: 656 (13.1%)
- `16`: 282 (5.6%)

### Próximos Pasos — Pipeline 1

```
Paso 1: Generar schemas LLM (MANUAL)
  → Abrir data/processed/section_clustering/llm_chat_prompts.json
  → Para cada título, copiar SYSTEM + USER prompt a DeepSeek/ChatGPT
  → La LLM responde con JSON schema
  → Guardar respuesta en data/external/llm_web_responses/openrouter_<titulo>.txt
  → Insertar con:
    python scripts/insert_llm_schema.py --title "TITULO" --provider deepseek --file "data/external/llm_web_responses/openrouter_TITULO.txt"

Paso 2: (Opcional) Re-ejecutar clustering con LLM
  python scripts/run_section_clustering_pipeline.py --llm-provider openrouter
```

---

## PIPELINE 2: LLM Fallback Extraction (document_features_v2_llm)

### ✅ IMPLEMENTADO (código) — ⏳ EJECUCIÓN PENDIENTE

| Aspecto | Detalle |
|---------|---------|
| **Archivos** | `src/pipelines/document_features_v2_llm_pipeline.py` |
| | `src/etl/extractors/document_features_v2_llm_extractor.py` |
| | `src/etl/transformers/document_features_v2_llm_transformer.py` |
| | `src/etl/loaders/document_features_v2_llm_loader.py` |
| | `scripts/run_document_features_v2_llm_pipeline.py` |
| **Descripción** | Arquitectura híbrida: regex (Phase 1-4, CPU, ~70s) cubre 69 campos; LLM fallback (GPU) rellena los 12 campos híbridos donde conf < 0.8 (Sesión 16: 7 campos portados a regex — ver revisión_26_campos.md) |
| **Estado** | ⚠️ **Código listo pero NUNCA ejecutado** — sin checkpoint, docs pendientes (12 campos hybrid; antes 19) |

**Estado de la DB (`dncp.document_features_v2`, 31,321 filas × 108 cols):**
**Sesión 16**: 7 campos pasaron de `hybrid` → `regex` en `master_schemas_v2.json`
(`num_categorias_documentos`, `num_referencias_minimo`, `num_referencias_tecnicas_minimo`,
`dias_reposicion`, `pct_monto_referencia_tecnica`, `pct_monto_referencia_minimo`,
`anos_experiencia_minima`). Dejan de procesarse por el LLM; sus columnas `_conf`/`_source` ya
no existen en la tabla (108 cols, antes 136). Schema final: **69 regex / 12 hybrid**.

Quedan **12 campos hybrid** = 9 con sentido extraíble por LLM + 3 "irresolubles" (siguen
`hybrid` en el schema porque ninguna estrategia los resuelve):
`num_componentes_formula`, `num_documentos_pago`, `num_pasos_verificacion`.

| Campo | pend (<0.8) | ok (>=0.8) |
|-------|:---:|:---:|
| num_componentes_formula | 31,319 | 2 |
| ratio_liquidez_minimo | 31,286 | 35 |
| ratio_endeudamiento_maximo | 31,145 | 176 |
| dias_aceptacion_rechazo_factura | 30,990 | 331 |
| dias_notificacion_fuerza_mayor | 29,831 | 1,490 |
| dias_solicitud_anticipo | 29,799 | 1,522 |
| dias_pago_maximo | 29,507 | 1,814 |
| dias_pago_anticipo | 29,298 | 2,023 |
| dias_validez_garantia | 23,329 | 7,992 |
| anos_experiencia_minima_tecnica | 24,932 | 6,389 |
| num_pasos_verificacion | 27,396 | 3,925 |
| num_documentos_pago | 5,642 | 25,679 |
| **TOTAL (12 hybrid)** | **~324,475** | — |

*Los 7 portados a regex ya no generan pendientes LLM; sus columnas pasan a regex puro
(`pct_monto_referencia_*`, `num_referencias_*`, `dias_reposicion`, `num_categorias_documentos`, `anos_experiencia_minima`).*

**Cómo ejecutar (WSL2, vLLM 0.23.0, Llama 3.1 8B AWQ):**
```bash
# 1. Escribir script temp .py (workaround quoting PowerShell)
# 2. En WSL: cd /mnt/d/projects/subtitle-parse && VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/run_document_features_v2_llm_pipeline.py
# 3. Usar VLLMOfflineProvider (API offline, NO HTTP server)
```
- Estimado: ~4h con continuous batching (35× concurrencia Mistral / 29× Llama)
- Checkpoint: `data/processed/llm_pipeline_checkpoint.json` (no existe — nunca ejecutado)

**Prerequisito**: decidir modelo (Sesión 14-15 → Llama 3.1 8B AWQ, 100% en las 100 GT humanas con postproceso determinista).

---

## PIPELINE 3: Anomaly Detection

### ❌ NO IMPLEMENTADO

| Aspecto | Detalle |
|---------|---------|
| **Archivos** | No existe ningún archivo de pipeline |
| **Descripción** | Consume `document_features`, `document_economic_features`, y `section_extracted_features` (de Pipeline 2) para correr modelos no supervisados de detección de anomalías y generar reportes |
| **Dependencia** | Pipeline 2 (necesita `section_extracted_features`), DocumentFeaturesPipeline, DocumentEconomicFeaturesPipeline |
| **Documentación** | `docs/pipeline3_anomaly_detection.md` (950 líneas, actualizado hasta Phase 6) |
| **Estado** | ❌ **No iniciado** |

### Modelos Planificados

- Isolation Forest
- Local Outlier Factor (LOF)
- Autoencoder (red neuronal)
- Ensemble de los 3 anteriores

### Outputs Esperados

- `data/processed/anomaly_detection/anomaly_scores.csv`
- `data/processed/anomaly_detection/anomaly_report.md`
- `data/processed/anomaly_detection/pipeline_summary.json`
- Reportes por título, año y categoría

---

## ORDEN DE EJECUCIÓN COMPLETO

```
FASE 0: INGESTA (ya ejecutado en servidor, datos en DB)
  AsyncDownload ──→ PDFParquet ──→ PDFOutline ──→ PdfContent ──→ ContentCleaning ──→ SectionsToDB
  OcdsCsvPipeline (independiente, paralelo)
  ↓
FASE 1: FEATURES (ya ejecutado en servidor, tablas en DB)
  DocumentFeaturesPipeline ──→ dncp.document_features (571 cols)
  DocumentEconomicFeaturesPipeline ──→ dncp.document_economic_features (46 cols)
  DocumentFeaturesV2Pipeline ──→ dncp.document_features_v2 (81 campos, 136 cols)
  ↓
FASE 2: RANKING (ya ejecutado)
  TitleRankingPipeline ──→ title_ranking.csv (80 títulos, 6 estrategias)
  ↓
FASE 3: CLUSTERING (ya ejecutado — 10 títulos, 4-10 clusters)
  SectionClusteringPipeline ──→ master_schemas.json
  Schema validation: 🟡 2/10 títulos (ver §Tareas pendientes)
  ↓
PIPELINE 2 — LLM FALLBACK (código ✅, ejecución ⏳ pendiente)
   DocumentFeaturesV2LLMPipeline ──→ rellena 12 campos hybrid (conf<0.8)
   ~324K docs, ~3h GPU, checkpoint data/processed/llm_pipeline_checkpoint.json
   (Sesión 16: 7 campos portados a regex → 19→12 hybrid, ~449K→~324K)
  ↓
PIPELINE 3 — ANOMALY DETECTION (⬅️ ACÁ ESTAMOS)
  Paso 1: Diseñar modelo ensemble (IF + LOF + Autoencoder)
  Paso 2: Implementar pipeline ETL
  Paso 3: Generar reportes por título/año/categoría
```

---

## TAREAS PENDIENTES (actualizado Sesión 16)

### 1. ⏳ Ejecutar fallback LLM (Pipeline 2) — ~3h GPU
- Código listo (`document_features_v2_llm_*`), **nunca ejecutado** (sin checkpoint).
- ~324K docs con conf<0.8 en 12 campos hybrid (Sesión 16: 7 portados a regex → baja de 19 a 12; 3 de los 12 son irresolubles y podrían excluirse por config).
- Modelo recomendado: **Llama 3.1 8B AWQ** (Sesión 14: 100% en 100 GT humanas con postproceso determinista).
- Ejecución: WSL2, vLLM 0.23.0, `VLLM_USE_V2_MODEL_RUNNER=0`, offline API.
- Prerequisito técnico: actualizar `scripts/run_document_features_v2_llm_pipeline.py` para usar `VLLMOfflineProvider` (hoy usa `LocalLLMProvider` HTTP, sin server corriendo).
- Requiere re-correr `scripts/run_document_features_v2_pipeline.py` antes (regenera `document_features_v2` con los 7 campos regex sin `_conf`, según nuevo schema).

### 2. 🟡 Schema validation clustering — 2/10 títulos
- Validar prompts de extracción LLM para los 9 títulos restantes del clustering.
- Script: `scripts/validate_extraction_prompts.py --title "<título>" --samples 16`.

### 3. 🔴 Commit de todo
- HEAD=`869c86c` en `feature/ocds-csv-pipeline`. Todos los cambios de Sesiones 1-16 sin commitear:
  - Phase 5 pipeline v2 LLM (6 archivos nuevos), benchmark scripts, docs (tesis_b_c, revision_26_campos, phase5, pipeline2_summary).
  - Pipeline 3 (tras este trabajo), fixes Session 14-15.
  - Decenas de scripts `_*.py`, `_tmp_*.py` a limpiar antes de commitear.

### 4. 🟢 (Opcional) Re-validar extracción en producción
- Verificar calidad del fallback LLM en producción con muestra manual de 100 docs.
- Post-procesar valores predeterminados (1.0, 0.01) si persisten.


---

## PARA CONTINUAR EN OTRA PC

### Archivos Trackeados (van con git clone)

| Archivo | Ruta |
|---------|------|
| Pipeline code | `src/pipelines/*.py` (13 archivos) |
| Extractors | `src/etl/extractors/section_clustering_extractor.py` |
| Transformers | `src/etl/transformers/section_clustering_transformer.py` |
| | `src/etl/transformers/llm_provider.py` |
| Loaders | `src/etl/loaders/section_clustering_loader.py` |
| Scripts | `scripts/run_section_clustering_pipeline.py` |
| | `scripts/insert_llm_schema.py` |
| | `scripts/generate_llm_schemas.py` |
| Instrucciones | `data/external/llm_web_responses/INSTRUCCIONES.txt` |
| Config | `config/settings.py` |

### Archivos Ignorados por .gitignore (van en zip)

Debido a que `.gitignore` tiene `data/processed/*`, estos archivos NO se commitean. Están empaquetados en `data/processed/section_clustering_output.zip`:

| Archivo | Tamaño | Descripción |
|---------|--------|-------------|
| `data/processed/section_clustering/master_schemas.json` | 1.9 MB | Datos de clustering + samples |
| `data/processed/section_clustering/llm_chat_prompts.json` | 262 KB | 10 prompts para LLM |
| `data/processed/section_clustering/clustering_comparison.json` | 7.5 KB | Métricas de clustering |
| `data/processed/section_clustering/all_cluster_samples_raw.json` | 1.7 MB | Samples raw por cluster |
| `data/processed/section_clustering/all_cluster_samples_merged.json` | 546 KB | Samples merged por cluster |
| `data/processed/section_clustering/report_*.md` (x16) | varios | Reportes individuales |
| `data/processed/title_ranking/*` (x8) | varios | Ranking outputs |

### Archivos Manuales (NO versionados)

| Archivo | Motivo |
|---------|--------|
| `.env` | Contiene credenciales (DB, OpenRouter API key) |
| `data/external/llm_web_responses/openrouter_*.txt` | Respuestas vacías de calls fallidos a OpenRouter |

### Workflow de Migración

```bash
# 1. Clonar repo en PC nueva
git clone <repo-url>
git checkout feature/clustering-pipeline

# 2. Extraer datos ignorados
# Copiar section_clustering_output.zip a data/processed/ y extraer

# 3. Copiar .env manualmente

# 4. Continuar desde Pipeline 1, Paso 1
# (generar schemas LLM manualmente)
```

---

## FASE 5: Setup Linux vLLM Production

> Guía completa: `docs/setup_linux_vllm_production.md` — llevar código, dependencias,
> BD y modelos a Ubuntu 24.04 nativo + RTX 4070 Ti SUPER 16 GB, con Tailscale para
> acceso SSH desde Mac/Windows. Estado: ⏳ Pendiente (documentado 09-sep-2026).

| # | Componente | Detalle | Estado |
|---|-----------|---------|--------|
| 5a | **Recursos y disco** | 150–200 GB NVMe, raíz única ext4 + swapfile 8 GB | ⏳ Pendiente |
| 5b | **Ubuntu 24.04.4 Server** | `live-server-amd64.iso`, SHA256 verificado | ⏳ Pendiente |
| 5c | **Stack base** | driver NVIDIA, CUDA 12.8, Python 3.12, podman 4.9.3+, Java 21 | ⏳ Pendiente |
| 5d | **venv vLLM** | vllm==0.23.0, torch==2.11.0, transformers==5.12.1 + requirements.txt | ⏳ Pendiente |
| 5e | **Postgres + restore** | `postgres:17`, `pg_restore -Fc` del dump 1.6 GB | ⏳ Pendiente |
| 5f | **Tika ×2** | `apache/tika:latest`, puertos 9998/9999 | ⏳ Pendiente |
| 5g | **Modelos prod** | Llama 3.1 8B + Mistral 7B AWQ (9.3 GB, copia offline) | ⏳ Pendiente |
| 5h | **llama-server v0.4.0** | binario CUDA para GGUF (Top 10: modelos 7/8/10) | ⏳ Pendiente |
| 5i | **Tailscale + SSH** | tailnet privada, llaves ed25519, Mac + Windows clientes | ⏳ Pendiente |
| 5j | **Validación e2e** | smoke ambos AWQ, restore counts, corrida corta con checkpoint | ⏳ Pendiente |

> **Sesión nueva en Ubuntu**: seguir `docs/SESION_NUEVA_UBUNTU.md` (estado git exacto,
> 23 claves `.env`, tabla de rutas WSL→Ubuntu, archivos con paths hardcodeados, primer
> mensaje y checklist post-bootstrap). Actualiza FASE 5 a ✅ por ítem al completar.

# CONTEXTO_TESIS — Documentación Central del Proyecto

> **Propósito**: Documentación integral para re-entrenar a un agente (o incorporar a un nuevo desarrollador/investigador) en el proyecto completo. Cubre: objetivo, datos, pipelines, feature engineering, extracción híbrida, gold standard, benchmark de modelos, sustento académico, agentes, glosario y sesiones históricas.

---

## 1. ¿Qué es este proyecto?

**Tesis de detección de anomalías en contrataciones públicas del Paraguay (DNCP).**

Pipeline ETL completo que procesa ~32,785 pliegos de licitación (PDFs) desde la fuente OCDS del portal `contrataciones.gov.py` hasta tablas precomputadas para detección de anomalías no supervisada (Isolation Forest, ECOD, co-bidding networks, etc.).

**Stack tecnológico:**
- **ETL**: Python, PostgreSQL, Apache Tika, PyPDF2, pdfplumber, Parquet
- **ML**: scikit-learn, vLLM 0.23.0 (WSL2), llama.cpp (Windows)
- **GPU**: NVIDIA RTX 4070 Ti SUPER 16 GB
- **BD**: PostgreSQL 16 en contenedor Podman, `<DB_HOST>:5433/dncp`

**Objetivo final**: Identificar licitaciones con perfil anómalo (riesgo de corrupción/colusión) combinando:
- Datos estructurados OCDS (licitaciones, oferentes, contratos, pagos, protestas)
- Texto de pliegos (81 campos extraídos vía regex + LLM)
- Tablas precomputadas (document_features_v2, document_economic_features, title_ranking)

---

## 2. Estado Global del Proyecto (Septiembre 2026)

| Componente | Estado | Detalle |
|---|---|---|
| **Fase 0 — Ingesta** | ✅ Completo | 7 pipelines: async download, OCDS CSV, PDF parquet, outlines, content, cleaning, sections to DB |
| **Fase 1 — Feature Engineering** | ✅ Completo | `document_features` (571 cols), `document_features_v2` (108 cols), `document_economic_features` (46 cols) |
| **Fase 2 — Extracción v2 (regex+LLM)** | ✅ Completo | 81 campos, 69 regex + 12 hybrid, `master_schemas_v2.json` |
| **Fase 2 — Fallback LLM** | 🟡 Pendiente | Pipeline `document_features_v2_llm_*` creado pero nunca ejecutado (~324K docs) |
| **Fase 3 — Ranking** | ✅ Completo | 6 estrategias, top-80 títulos, ~11.4s |
| **Fase 3 — Anomaly Detection** | ❌ No implementado | Diseño en `docs/pipeline3_anomaly_detection.md`, 15-20 días estimados |
| **Schema Validation** | 🟡 2/10 | Validación de esquemas de clustering (2 títulos de 10) |
| **Benchmark Modelos** | ✅ Completo | 12 modelos evaluados, 21 configuraciones, 100% en GT humano |
| **Sustento Académico** | ✅ Completo | 22 fuentes, 19 PDFs, 8 variables con respaldo académico |
| **Gold Standard** | ✅ Completo | 26 campos (5,066 juicios) + 8 variables académicas (900 filas) |
| **Backup BD** | ✅ Completo | `backup_dncp/backup_dncp.dump` 1.6 GB (pg_dump `-Fc -Z9`, 03-sep-2026) |
| **Setup Linux production (FASE 5)** | 🟡 Documentado | `docs/setup_linux_vllm_production.md` + FASE 5 en checklist; sin ejecutar |
| **Contexto exportable** | ✅ Completo | `docs/PROMPT_CONTEXTO_LLM.md` + `proyecto_tesis_export.zip` (11.4 MB) |

**Branch activo**: `feature/clustering-pipeline` (HEAD `869c86c`, ahead 1 de origin; 11 modificados + ~150 untracked — ver `docs/SESION_NUEVA_UBUNTU.md` §1 para el traslado exacto a otra máquina).

---

## 3. Datos — Fuentes y Base de Datos

### 3.1 Fuente primaria: OCDS Paraguay

El portal `contrataciones.gov.py` publica datos en formato **Open Contracting Data Standard (OCDS)**. Se descargan **bulk ZIPs** (`masivo_{year}.zip`), NO se usa la API en vivo (motivo: rate limit).

| Año | Tamaño ZIP | PDFs en disco |
|-----|-----------|---------------|
| 2021 | 73.0 MB | 1,116 |
| 2022 | 258.7 MB | 4,620 |
| 2023 | 164.6 MB | 6,237 |
| 2024 | 319.2 MB | 9,406 |
| 2025 | 462.0 MB | 10,805 |
| 2026 | 5.9 MB (parcial) | 601 |
| **Total** | | **32,785** |

### 3.2 Filtro "solo pliegos electrónicos"

De `tender_documents` se conservan solo registros con:
- `tender_documents_format == 'application/pdf'`
- Tipo = "Pliego Electrónico de Bases y Condiciones"

Esto reduce de ~300K documentos a **~31,387 licitaciones** con pliego electrónico.

### 3.3 Tablas principales en PostgreSQL (`dncp`)

| Tabla | Filas | Columnas | Descripción |
|-------|-------|----------|-------------|
| `pliegos_secciones` | ~2,775,485 | 17 | Secciones de pliegos con texto limpio, particionada por año |
| `document_features` | 31,321 | 571 | Features estructurales (has_*, len_*, tok_*, word_*, count_*, span_*, page_range_*) |
| `document_features_v2` | 31,321 | 108 | Features v2 — 81 campos extraídos (69 regex + 12 hybrid) |
| `document_economic_features` | 29,811 | 46 | Features económicas + colusión (ratios, flags, winner frequency) |
| `licitaciones` | 29,811 | — | Datos maestros de licitaciones |
| `oferentes` | 78,695 | — | Oferentes por licitación (5,544 bidders únicos, SIN montos por oferta) |
| `adjudicaciones` | 39,463 | — | Solo monto del ganador |
| `contratos` | 39,691 | — | Contratos adjudicados |
| `pagos_contrato` | 276,868 | — | Pagos (265,725 con monto no-nulo) |
| `enmiendas_contrato` | 7,143 | — | 4,414 licitaciones con enmiendas |
| `protestas` | 1,253 | — | 967 licitaciones con protestas (proxy de anomalía) |
| `consultas_llamado` | 123,283 | — | 119,740 con respuesta, 12,353 licitaciones |
| `items_licitacion` | 1,878,237 | — | 1.87M montos 100% no-nulos (para Benford) |

### 3.4 Datos NO disponibles (verificados)

| Campo | Estado | Impacto |
|-------|--------|---------|
| `cantidad_items` | Siempre 0 en licitaciones | `bidder_diversity` = 0.0 para todas las filas |
| Montos por oferente individual | No existe en `oferentes` | Screens Porter-Zona/Bajari-Ye no implementables completos |
| `costo_pliego`, `presupuesto_monto`, `garantia_porcentaje` | Vacíos | Descartados |
| `proveedores.tamanio` | NULL | Descartado |
| `hitos_contrato` | 0 filas | Descartado |
| `metodo_contratacion` | 99.98% "open" | Usar `metodo_detalle` (CD 9,000, Menor cuantía 10,384, LPN 5,677, Concurso 4,547) |

### 3.5 Backup de la BD (verificado 09-sep-2026)

| Dato | Valor |
|---|---|
| Archivo | `backup_dncp/backup_dncp.dump` (raíz del repo) |
| Tamaño / fecha | **1,646,170,018 bytes (~1.6 GB)**, 03-sep-2026 02:12 |
| Formato | `pg_dump -U postgres -d dncp -Fc -Z9` (custom comprimido, incluye DDL+datos+índices) |
| Origen | contenedor podman `dncp` = PG **17.9** (imagen local `postgres-dncp:17` = tag de `docker.io/library/postgres:17`, 461 MB) |
| Restore | `podman cp backup_dncp.dump dncp:/tmp/ && podman exec dncp pg_restore -U postgres -d dncp -Fc --clean --if-exists /tmp/backup_dncp.dump` (NO necesita zstd: el custom ya va comprimido) |
| En destino Linux | `podman run -d --name dncp -p 5433:5432 -e POSTGRES_DB=dncp -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=<DB_PASSWORD> -v dncp_data:/var/lib/postgresql/data docker.io/library/postgres:17`, luego restore; verificar con `select count(*) from pliegos_secciones` (~2.78M) |

Detalle completo del traslado a otra máquina en `docs/setup_linux_vllm_production.md` §4.

---

## 4. Arquitectura de Pipelines

### Fase 0 — Ingesta de Datos (7 pipelines)

```
AsyncDownloadPipeline → descarga PDFs en batches de 1,000 con pausa de 300s
    → OcdsCsvPipeline → descarga ZIP OCDS, extrae 19 CSVs, upserta 13 tablas PostgreSQL
    → PDFParquetPipeline → Tika extrae texto línea a línea → Parquet particionado por año
    → PDFOutlinePipeline → PyPDF2 extrae bookmarks (outlines) → CSV + Parquet
    → PdfContentPipeline → cruza outlines + líneas → secciones estructuradas
    → ContentCleaningPipeline → limpia texto (guiones, Unicode, stemming)
    → SectionsToDbPipeline → COPY a PostgreSQL en batches de 10K, checkpoint por año
```

**Output**: `dncp.pliegos_secciones` (~2.78M filas, 17 columnas)

### Fase 1 — Feature Engineering

| Pipeline | Output | Descripción |
|----------|--------|-------------|
| `DocumentFeaturesPipeline` | `document_features` (31,321 × 571) | word_count, has_*, len_*, tok_*, word_*, count_*, span_*, page_range_*, gini, entropy |
| `DocumentEconomicFeaturesPipeline` | `document_economic_features` (29,811 × 46) | Ratios, flags, colusión (winner frequency, is_repeat_winner, bidder_diversity) |
| `DocumentFeaturesV2Pipeline` | `document_features_v2` (31,321 × 108) | 81 campos extraídos (69 regex + 12 hybrid) |

### Fase 2 — Extracción de Campos del Pliego (Pipeline 2)

Arquitectura **híbrida**: regex determinista (85% de campos) + LLM fallback (15% de campos).

```
master_schemas_v2.json (81 campos, 25 títulos)
    → DocumentFeaturesV2Transformer (regex _re_* methods)
    → extraction_postprocess.py (capa determinista compartida)
    → document_features_v2 (DB)
    → [si conf < 0.8] DocumentFeaturesV2LLMPipeline (LLM fallback, no ejecutado)
```

### Fase 3 — Detección de Anomalías (diseñada, no implementada)

Ver `docs/pipeline3_anomaly_detection.md` (950 líneas). Métodos propuestos:
- Isolation Forest (primary)
- LOF, DBSCAN, One-Class SVM, Autoencoder
- Ensemble (recomendado)
- Evaluación: inyección de anomalías sintéticas, Jaccard cross-method

### Fase 5 — Setup Linux vLLM Production (documentada 09-sep-2026, no ejecutada)

Ver `docs/setup_linux_vllm_production.md` (12 secciones) + FASE 5 en `PIPELINE_CHECKLIST.md`.
Llevar código + deps + BD + modelos a Ubuntu 24.04 Server nativo + RTX 4070 Ti SUPER 16 GB:
pins medidos (vllm 0.23.0, torch 2.11.0, transformers 5.12.1, podman 4.9.3, PG 17.9),
modelos por fases (2 AWQ prod 9.3 GB offline → resto con embudo), matriz VRAM del Top 10,
llama-server v0.4.0 para GGUF, Tailscale + SSH (Mac/Windows clientes).

---

## 5. Feature Engineering — Tablas Precomputadas

### 5.1 `document_features` (571 columnas)

Evolucionó de 260 a 571 columnas en la Phase 1:
- Prefijos por título: `has_*`, `len_*`, `tok_*`, `word_*`, `count_*`, `span_*`, `page_range_*`
- Agregados por documento: `gini_content_length`, `title_entropy`, `avg_page`, `max_page`, `sum_word_count`, `avg_word_count`
- Chunked loading (200K rows/chunk), int64→int32 downcast

### 5.2 `document_features_v2` (108 columnas)

81 campos de extracción (69 regex + 12 hybrid):
- 55 `binary_*` (presencia/ausencia de cláusulas)
- 16 `int_*` (días, cantidades, porcentajes enteros)
- 10 `float_*` (ratios, porcentajes decimales)
- 1 `categorical_tipo_garantia`

Columnas por campo: `{field}` (valor), `{field}_conf` (confianza 0.0-1.0, solo hybrid), `{field}_source` (solo hybrid).

### 5.3 `document_economic_features` (46 columnas)

12 campos derivados + 4 de colusión:
- `bidding_urgency`, `es_unico_oferente`, `n_oferentes_distintos`, `oferentes_por_item` (bug: 0.0)
- `overbudget_ratio`, `is_high_value_single_bidder`, `enmienda_ratio`, `has_protesta`
- `winner_category_frequency`, `winner_total_contracts`, `is_repeat_winner`, `bidder_diversity` (bug: 0.0)
- `metodo_detalle` codificado, `tiene_consulta`, `n_consultas`, `tiempo_respuesta_consultas`

---

## 6. Ranking de Títulos (6 Estrategias)

**Pipeline**: `TitleRankingPipeline` (~11.4s para 31,321 documentos)

### Pesos de las 6 estrategias

| Estrategia | Peso | Descripción |
|-----------|------|-------------|
| Outlier Frequency | 0.20 | Frecuencia de valores extremos por título |
| Random Forest Importance | 0.25 | Importancia Gini de cada título para predecir `es_unico_oferente` |
| Contextual Effect | 0.15 | Correlación título con variable de riesgo |
| Section Isolation Forest | 0.15 | IF sobre features de sección por título |
| Document Correlation | 0.10 | Correlación entre documentos |
| Economic Risk | 0.15 | Correlación media con indicadores económicos (6 variables) |

### Top-10 real (6 estrategias)

| Rank | Título | Score Total |
|------|--------|-------------|
| 1 | Fraude Y Corrupcion | 0.5432 |
| 2 | Confidencialidad De La Informacion | 0.5250 |
| 3 | Fuerza Mayor | 0.5238 |
| 4 | Aclaracion De Las Ofertas | 0.4884 |
| 5 | Interpretacion | 0.4875 |
| 6 | Formato Y Firma De La Oferta | 0.4677 |
| 7 | Impuestos Y Derechos | 0.4539 |
| 8 | Derechos Intelectuales | 0.4467 |
| 9 | Retiro Sustitucion Y Modificacion De Las Ofertas | 0.4425 |
| 10 | Causales De Terminacion Del Contrato | 0.4367 |

---

## 7. Pipeline 2 — Extracción de Campos (Arquitectura Híbrida)

### 7.1 `master_schemas_v2.json`

**Ubicación**: `data/processed/section_clustering_v2/master_schemas_v2.json`
**Estructura**: 25 títulos → 81 campos con `name`, `type`, `description`, `extraction_method`, `extraction_hint`

**Evolución del método de extracción**:

| Sesión | Regex | Hybrid | Total |
|--------|-------|--------|-------|
| Original (26 GT) | 0 | 26 | 26 |
| Después de Sesión 15 | 62 | 19 | 81 |
| Después de Sesión 16 | **69** | **12** | **81** |

### 7.2 Capa de Postproceso Determinista (`extraction_postprocess.py`)

**Ubicación**: `src/etl/transformers/extraction_postprocess.py` (659 líneas)

**Single source of truth** para toda la lógica de postproceso. Exporta:
- `NO_RULE` — centinela para "no hay regla determinista"
- `KNOWN_DEFAULTS` — valores válidos conocidos por campo
- `PCT_INT_SCALE` — 5 campos porcentuales que requieren ×100
- `deterministic_pct()` — extracción autoritativa para 5 campos uniformes
- `postprocess()` — correcciones y validaciones deterministas
- `parse_response()` — pipeline completo: determinista → parse → postprocess

**Reglas autoritativas** (9 campos, 200/200 en GT):
- `deterministic_pct`: `pct_interes_mora_diario`, `pct_multa_diaria`, `pct_diferencia_precio_bajo`, `pct_diferencia_precio_alto`, `pct_garantia_cumplimiento`
- `_deterministic_ratio_liquidez`: número después de "liquidez" con ">= / igual o mayor"
- `_deterministic_primer_n_dias`: primer "N días" literal
- `_deterministic_validez_garantia`: N días o conversión meses→días (mapa exacto)
- `_deterministic_solicitud_anticipo`: plazo de solicitud
- `_deterministic_pago_anticipo`: plazo de pago
- `_deterministic_endeudamiento`: "Endeudamiento: ... No deberá ser mayor a N"
- `_deterministic_experiencia_tecnica`: primer "N años" con contexto "experiencia"
- `_deterministic_notificacion_fuerza_mayor`: "siete (7) días" → 7.0

### 7.3 Los 12 Campos Hybrid (y por qué siguen en LLM)

| Campo | Tipo | Acc LLM | Por qué no se porta a regex |
|-------|------|---------|------------------------------|
| `dias_notificacion_fuerza_mayor` | int | 94.5% | Regex solo 5.5%; wording variable |
| `dias_solicitud_anticipo` | int | 96.5% | Desambiguación vs `dias_pago_anticipo` |
| `dias_pago_maximo` | int | 88.5% | Asociación semántica a "factura" |
| `dias_aceptacion_rechazo_factura` | int | 85.0% | Proximidad semántica |
| `dias_pago_anticipo` | int | 86.5% | Desambiguación (solo "abonar/pagar") |
| `dias_validez_garantia` | int | 82.0% | Conversión meses→días |
| `ratio_endeudamiento_maximo` | float | 86.5% | Decimal en prosa, sin patrón plantilla |
| `ratio_liquidez_minimo` | float | 82.0% | Idem |
| `anos_experiencia_minima_tecnica` | int | 78.0% | Desambiguación vs personal y antigüedad |
| `num_componentes_formula` | int | 3.0% | **Irresoluble** — LLM no cuenta componentes |
| `num_documentos_pago` | int | 2.5% | **Irresoluble** — conteo semántico |
| `num_pasos_verificacion` | int | 1.5% | **Irresoluble** — conteo semántico |

### 7.4 Comparativa de Estrategias (5,066 juicios GT)

| Estrategia | Descripción | Acc Media |
|-----------|-------------|-----------|
| **R** | Regex puro | 67.0% |
| **Ri** | Regex + postproceso | 66.9% |
| **L_raw** | LLM bruto (primer número) | 51.0% |
| **L_pp** | LLM + postproceso `parse_response` | 63.1% |
| **H** | Híbrido arquitectural | **68.3%** |

**Ganador por campo**: H en 13/26, Ri en 8/26, L_pp en 3/26, L_raw en 2/26.

---

## 8. Top de Variables Regenerado + Las 8 Variables Académicas Nuevas

> Fuente maestro: `docs/07_top_variables_enriquecido.md` (v2, 2026-08-27) — regenera `docs/05_seleccion_variables_sustento_academico.md` y `estrategias-anomaly-deteccion.md §2`. El v2 incorpora 4 PDFs nuevos (Ferwerda 2016, Decarolis 2022 EPJ, Azevedo 2021 Benford, TI Handbook 2006).

### 8.1 Top General — Niveles 1-4 (20 variables, OCDS + pliego)

**Nivel 1 — Núcleo (competencia y precio):**
| # | Variable | Fuentes v2 |
|---|---|---|
| 1 | `es_unico_oferente` / `n_oferentes_distintos` | Fazekas 2016 + Kocsis 2020 (+9.6% precio con single-bid); Ferwerda `number of offers` r=-0.178 |
| 2 | `precio_vs_estimado` / `overbudget_ratio` | Ferwerda `all bids > projected` r=0.160 + `substantial changes` r=0.337; Azevedo Benford |
| 3 | `metodo_detalle` (CD/LPN/Concurso/Menor cuantía) | OECD 2007 + NBER 28209 (+1.9pp con negotiated+few invited); EPJ negotiated 41% discrecional |
| 4 | `duracion_oferta_dias` / `bidding_urgency` | **Ferwerda `shortened time span` r=0.200 (p=0.005)** + EPJ `open tender days` negativo (evasión) |

**Nivel 2 — Colusión y ejecución:**
| # | Variable | Fuentes v2 |
|---|---|---|
| 5 | `n_enmiendas` / `enmienda_ratio` (7,143 / 4,414 licit.) | Ferwerda `substantial changes` **r=0.337** (top-3); Decarolis renegociación ≥50% ahorro |
| 6 | `winner_total_contracts` / `winner_category_frequency` / `is_repeat_winner` | Fazekas winner share; EPJ criminal firm (15% IT) |
| 7 | `n_consultas_llamado` / `tiempo_respuesta_consultas` (12,353 licit.) | EPJ information completeness como predictor RF |
| 8 | `pago_vs_contrato_ratio` / `n_pagos` / `demora_media_pago_dias` (276K pagos) | TI execution risks; nota: no usar extra cost/time como proxy (EPJ §5) |

**Nivel 3 — Pliego (Bloque B):**
| # | Variable | Fuentes v2 |
|---|---|---|
| 9 | `anos_experiencia_minima` / `anos_experiencia_minima_tecnica` | Ferwerda `preferred supplier` **r=0.487 #1** + WB rigged specs |
| 10 | `ratio_liquidez_minimo` / `ratio_endeudamiento_maximo` | Ferwerda 0.487 + EPJ firm preference 28% |
| 11 | `pct_interes_mora_diario` / `pct_multa_diaria` | Penalizaciones asimétricas (OCP) |
| 12 | `dias_pago_maximo` / `pct_garantia_cumplimiento` / `dias_validez_garantia` | Garantías (OCP) |
| 13 | **`num_categorias_documentos` / `num_documentos_pago`** | Ferwerda `amount missing info` **r=0.431 (#3 global)** — SUBE de MEDIA a ALTA |
| 14 | `pct_diferencia_precio_bajo` / `pct_diferencia_precio_alto` | `all bids > projected` + Conley-Decarolis |
| 15 | `dias_reposicion` / `dias_solicitud_anticipo` / `dias_pago_anticipo` | EPJ WV/DV hours share (0.95/0.77) |

**Nivel 4 — Estructural + Benford (promovido):**
| # | Variable | Fuentes v2 |
|---|---|---|
| 16 | `gini_content_length` / `title_entropy` / `sum_word_count` | EPJ RF: word/page count top predictor — **SUBE a Nivel 3.5** |
| 17 | `n_protestas` / `has_protesta` (967 licit.) | Kehler validación externa; Ferwerda complaints n.s. |
| 18 | `n_unspsc_distintos` / `n_items` | Ferwerda `tender exceptionally large` 0.330 |
| 19 | `razon_notificados_a_oferentes` | Wachs cohesion/exclusivity |
| 20 | **`benford_mad_first_two` / `benford_chi2`** | Azevedo MAD 0.001/χ² 11.9 (agregado); Nigrini thresholds 0.006/0.012/0.022 |

### 8.2 Las 8 Variables Nuevas de Pliego (N1-N8)

Seleccionadas con metodología **top-down** (literatura primero → DB verification → método de extracción). Prioridad v2: **N5 (MEAT) > N4 (worksite) > N1 (time span) > N7 (preferred supplier) > N2 > N3 > N6**.

| Var | Campo | Tipo | Título en BD | Hits | Población | Fuente Académica Clave |
|-----|-------|------|-------------|------|-----------|----------------------|
| N1 | `dias_validez_oferta` | int | periodo de validez de las ofertas | 100% | 32,182 | Fazekas 2016: short submission period → +8.4-9pp single-bid |
| N2 | `dias_validez_garantia_mantenimiento_oferta` | int | garantia de mantenimiento de oferta | 100% | 32,182 | World Bank 2011: onerous guarantee → bidder exclusion |
| N3 | `dias_plazo_entrega_bien` | int | plan de entrega de los bienes | 19% | 22,547 | Decarolis 2022 EPJ: worksite delivery time = signal |
| N4 | `requiere_visita_tecnica` | binary | visita al sitio de ejecucion del contrato | 100% | 26,909 | EPJ: Naples Camorra usó WV para filtrar; WV hours share 0.95 |
| N5 | `criterio_adjudicacion` | categorical | criterios de adjudicacion | 100% | 32,182 | **EPJ: MEAT = predictor #1 (2× next best)** + Ferwerda 0.487 |
| N6 | `multa_jornales_incumplimiento` | int | multas por incumplimiento | 100% | 664 | Fazekas penalidades + EPJ obstacles + TI 10-25% daño |
| N7 | `tiene_requisito_eliminatorio` | binary | requisitos de calificacion | 100% | 32,182 | Ferwerda `preferred supplier` r=0.487 + EPJ firm preference 28% |
| N8 | `pct_subcontratacion_maximo` | float | subcontratacion | 2% | 23,414 | WB 2011: excessive subcontracting = red flag |

### 8.3 Las 900 muestras generadas (sesión de gold standard N1-N8)

Generadas con `scripts/preparar_gt_nuevo_7variables.py` (seed 42, `DISTINCT ON (nro_licitacion)`), anotadas con `scripts/anotar_pseudo_gt.py` + modelo de frontera DeepSeek v4 flash (agente `anotador-pseudo-gt`, capa 1 regex → capa 2 modelo). 3 CSVs en `data/processed/alolab/`:

| Archivo | Filas | Anotación | Notas |
|---------|-------|-----------|-------|
| `gold_standard_pliego_nuevo_8variables.csv` | 900 | 29 pre-anotadas, resto vacío | CSV base (muestreador) |
| `gold_standard_pliego_nuevo_8variables_deepseek_v4_flash.csv` | 900 | **680 con valor + 0 REVISAR** | **Pseudo-GT definitivo** |
| `gold_standard_pliego_nuevo_8variables_pseudoGT.csv` | 900 | 610 con valor, 140 REVISAR | Solo regex (capa 1) — N3 falla (135 REVISAR) |

**Distribución real por variable (DeepSeek v4 flash, verificado en CSV):**

| Var | Muestras | Con valor | NULL | Distribución de valores |
|-----|----------|-----------|------|------------------------|
| N1 | 100 | 100 | 0 | 90:48, 60:28, 30:16, 120:6, 45:2 |
| N2 | 100 | 100 | 0 | 120:34, 90:31, 60:22, 150:7, 50:3, 61:2, 31:1 |
| N3 | 200 | 78 | 122 | 10:16, 5:15, 30:11, 15:11, 3:5, 20:4, 45:3... |
| N4 | 100 | 100 | 0 | 0 (no requiere): 75, 1 (sí requiere): 25 |
| N5 | 100 | 100 | 0 | `precio_mas_bajo`: 52, `calidad_precio`: 48 |
| N6 | 100 | 100 | 0 | **constante `2` (dos jornales): 100** |
| N7 | 100 | 100 | 0 | **constante `1` (tiene requisito eliminatorio): 100** |
| N8 | 100 | 2 | 98 | 60.0: 1, 50.0: 1 |

**Hallazgos de la sesión**:
- N3 es la única con varianza real y NULLs (122/200) — el plazo de entrega no siempre se explicita → la varianza misma es señal (días cortos = entrega restrictiva).
- N6 y N7 son **constantes en toda la BD** (100% "2 jornales" en 664 docs; 100% requisito eliminatorio en 32,182) → señal débil por diseño, documentadas como constantes.
- N8 casi siempre NULL (98/100) — la subcontratación rara vez tiene % máximo explícito en el texto.
- N4 = 25% "sí requiere visita" y N5 = 52/48 (precio vs calidad) son las de mayor utilidad discriminante.

---

## 9. Proceso de Gold Standard

### Pipeline de GT (2 agentes encadenados)

```
Variables (definición)
  → [@muestreador-gt] → CSV base (texto + prompt + columnas vacías)
  → [@anotador-pseudo-gt] → CSV anotado (ground_truth + confianza + es_revisar)
  → [@documentador-tutores] → material de presentación
```

### Archivos de GT en `data/processed/alolab/`

| Archivo | Filas | Campos | Propósito |
|---------|-------|--------|-----------|
| `gold_standard_26campos.csv` | 5,066 | 26 | GT humano original (25×200 + pct_garantia×66) |
| `gt_extension.csv` / `_filled.csv` | 500 | 5 | Extensión 5 campos difíciles (Sesión 14) |
| `gold_standard_pliego_nuevo_8variables.csv` | 900 | 8 | Muestras nuevas (N1-N8) |
| `gold_standard_pliego_nuevo_8variables_deepseek_v4_flash.csv` | 900 | 8 | **Pseudo-GT anotado** (0 REVISAR) |
| `gold_standard_pliego_nuevo_8variables_pseudoGT.csv` | 900 | 8 | Pseudo-GT vía regex |

### Ciclo AloLab (Sesión 14)

Proceso artesanal de mejora: `alolab_collect_errors.py` → diagnóstico humano → reescritura de hint/postprocess → re-run.

| Ronda | Score | Fix aplicado |
|-------|-------|-------------|
| 1 | 95/100 | 3 hints reescritas + fix regex `personales?`→`personal(?:es)?` |
| 2 | 95/100 | 2 reglas postprocess vs ítems de lista + regla `dias_solicitud_anticipo` |
| 3 | 95/100 | Hint limpia + conversión determinista meses→días |
| 4 | **100/100** | ✅ |

---

## 10. Benchmark de Modelos Locales

### Hardware

- **GPU**: NVIDIA RTX 4070 Ti SUPER — 16 GB VRAM (~15.6 GB usable)
- **RAM/SO**: 32 GB, Windows 11 + WSL2 (Ubuntu), driver 581.29, CUDA 12.4
- **Motor**: vLLM 0.23.0 (V1 engine) en WSL2, entorno `/root/vllm-env/bin/python`
- **Restricción WSL**: `VLLM_USE_V2_MODEL_RUNNER=0` obligatorio (sin UVA)
- **vLLM 0.25.x**: NO funciona en WSL (MRv2 requiere UVA)

### Ranking Final (AWQ, ordenado por calidad × velocidad)

| # | Modelo | Formato | Precisión | Velocidad | VRAM | Concurrencia |
|---|--------|---------|-----------|-----------|------|-------------|
| 🥇 | **Llama 3.1 8B AWQ** | awq_marlin | **F1 96.4%** | 1.93s | 5.39 GiB | 29.21× |
| 🥇 | **Mistral 7B v0.3 AWQ** | awq_marlin | F1 94.0% | **1.61s** | **3.89 GiB** | **35.22×** |
| 3 | Qwen2.5 14B AWQ | awq | F1 93.3% | 2.46s | 9.38 GiB | 8.18× |
| 4 | Qwen3-8B AWQ | awq | 100% | 4.80s | 5.71 GiB | 24.58× |
| 5 | Qwen3-14B AWQ | awq | 100% | 5.29s | 9.44 GiB | 9.00× |
| 6 | Phi-4 14B AWQ | awq | 90% (9/10) | 4.47s | 8.51 GiB | 9.95× |

### Proyección para ~324K secciones del fallback LLM

| Modelo | Tiempo estimado | Continuous batching |
|--------|----------------|---------------------|
| Mistral 7B AWQ | **~4 horas** | 35.22× |
| Llama 3.1 8B AWQ | **~5.8 horas** | 29.21× |
| Mistral 7B GGUF | ~1 día | — |
| HTTP baseline (sin batching) | ~34 días | — |

### Sesión Qwen3.8-27B (Windows nativo, 2026-08-22)

Probado fuera de WSL2 (disco D: lleno, vhdx sin espacio) con `llama-server` en Windows:

| Aspecto | Resultado |
|---------|-----------|
| Modelo | `Qwen3.8-27B-UD-Q3_K_XL.gguf` 12.24 GB (llama.cpp b10566, CUDA 12.4) |
| Precisión | **97/100** en 100 GT humanas (RAW 98%, PP 97%) — no supera a Llama 3.1 8B-AWQ (100%) |
| Velocidad | 1.41s/muestra · 33-39 tok/s gen · 1015-1432 tok/s prompt (2× más lento que Llama AWQ) |
| Estabilidad | Bug KV LCP + DeltaNet → OOM tras ~60 req grandes; fix `cache_prompt False` + batch 10 + reinicio |
| Veredicto | Viable como 2º modelo pero no justifica 27B para +0pp |

Doc completo: `docs/benchmark_qwen38_session.md`.

### Estudio de modelos locales (38 candidatos, 16 GB VRAM)

**Documentación**: `docs/benchmark_modelos/` (README + 4 archivos) + `docs/modelos_locales_investigacion.md` (investigación previa julio 2026).

**Metodología de cálculo de VRAM**:
- AWQ (W4A16): `params × 0.5 + 1.5-2 GB overhead`
- FP8 (W8A8): `params × 1.0 + 1.5-2 GB overhead`
- GGUF Q4_K_M: `params × 0.56`
- KV cache por token: `2 × num_layers × num_kv_heads × head_dim × dtype_bytes` (fp8 = mitad)
- Concurrencia = KV cache disponible ÷ (KV/token × max_model_len)

**5 zonas de modelos que caben**:

| Zona | Rango | Concurrencia | Ejemplos |
|------|-------|--------------|----------|
| ALTA CALIDAD | 14B-24B denso | 5-15× | Mistral Small 3.1 24B, Qwen3-14B, Gemma 4 12B, Granite 4.2 8B, Mistral Nemo 12B |
| MEDIA (sweet spot) | 7B-11B | 15-35× | **Llama 3.1 8B**, **Qwen3-8B**, **Mistral 7B v0.3**, EXAONE-3.5-7.8B, Bielik-11B, Qwen3.5-9B, GLM-5.2-FP8 |
| BORDERLINE | 27B-30B | 0-2× | Granite 4.2 30B, Qwen3.8-27B, gemma-3-27b (requieren offload) |
| ECONÓMICA (MoE) | pocos activos | variable | Instella-MoE-16B-A3B, Mellum2-12B-A2.5B, Gemma 4 26B-A4B |
| BÁSICA | ≤4B | 40-100× | Qwen3-4B, Phi-4-mini, MiniCPM5-1B, Qwen3-0.6B |

**Top 10 sugerido para el benchmark (embudo de 2 fases)**:
1. Llama 3.1 8B AWQ (baseline F1 96.4%)
2. Qwen3-14B AWQ (100% probado)
3. Mistral 7B v0.3 AWQ (throughput 35×)
4. Llama 3.1 8B FP8 (comparación AWQ vs FP8)
5. Qwen3-8B FP8 (FP8 oficial Qwen)
6. Mistral Small 3.1 24B AWQ (calidad máxima, 4K ctx)
7. Granite 4.2 8B GGUF (español IBM, ⚠️ smoke test thinking)
8. Gemma 4 12B GGUF (⚠️ smoke test guided decoding #39130)
9. EXAONE-3.5-7.8B AWQ (2026 text-only, ⚠️ validar español)
10. Ministral-3-14B GGUF (oficial Mistral)

**Estrategia**: Fase 1 smoke test 30-50 muestras (descartar por criterios) → Fase 2 900 muestras solo top 5-8. Criterios de descarte: <70% acierto, OOM, diferencia <1% (queda el más rápido), offload peor que 8B, y **nuevo**: `schema_valid_json_rate` alto + `campos_correctos` bajo = guided decoding bypaseado → investigar.

### Optimizaciones del pipeline (antes de cambiar de modelo)

Del análisis de `docs/modelos_locales_investigacion.md` y `docs/benchmark_vllm_vs_lmstudio.md`:
- **Batching real**: `LLM(...).generate(lista_prompts)` offline con continuous batching interno (hasta 23× vs secuencial). Nunca `vllm serve` + requests secuenciales.
- **Guided JSON**: `GuidedDecodingParams(json=schema)` vía xgrammar (hasta ~5× en throughput de salida, elimina parseos fallidos).
- **`--max-model-len` sobredimensionado**: bajar de 8192 a 2048 (medir p99 real) libera GB para más concurrencia.
- **Chunking**: guardar progreso cada 10-20K secciones (job reanudable).

### Hallazgos clave del benchmark

- **GGUF es experimental/deprecated** en vLLM 0.23.0 → AWQ es el formato nativo recomendado
- **awq_marlin** es el diferenciador clave (solo funciona con MistralForCausalLM/LlamaForCausalLM)
- **Guided decoding** (StructuredOutputsParams) NO mejora precisión por sí solo, pero da **7× más velocidad** (mismo accuracy, output ~2 tokens vs ~50+)
- **Few-shot retrieval** descartado: empeoraba el baseline (95% → 85%)
- **Qwen3** requiere `enable_thinking=False` para extracción directa (thinking mode activado por defecto)
- **Backends de guided decoding distintos**: AWQ/FP8 usan xgrammar, GGUF usa GBNF (llama.cpp) — NO comparar precisión entre formatos sin nota
- **llama-server fijado**: v0.4.0 (release 04-sep-2026, verificado en GitHub) — binario `llama-*-bin-ubuntu-x64.zip` con CUDA, sin compilar; rige para los GGUF del Top 10 (modelos 7/8/10)
- **Matriz de memoria Top 10** (ver `docs/setup_linux_vllm_production.md` §7-bis): todos caben en 16 GB salvo 24B+ con ctx recortado; descartados por OOM: Qwen3.8-27B AWQ, Granite 30B, Qwen3-14B-FP8, phi-4-FP8, Muse-Glimmer-30B, Qwen3.x-27B-FP8

---

## 11. Sustento Académico — 22 Fuentes

### Familias bibliográficas

| Familia | Fuentes | Enfoque |
|---------|---------|---------|
| **A. Frameworks generales** | WB 2011, TI 2006, OECD 2007/2015/2016, OCP 2016, IACRC | Taxonomía de red flags |
| **B. CRI y oferta única** | Fazekas 2016/2019/2020, Ferwerda 2016, Cingolani & Fazekas | CRI, single-bidding, ML red flags |
| **C. Colusión y screens** | Wachs & Kertész 2019, Conley & Decarolis 2016, Decarolis 2014/2022, Porter-Zona 1993, Bajari-Ye 2003, Kawai-Nakabayashi 2022, Abdou IMF 2022 | Co-bidding, screens económicos, piloto Paraguay |
| **D. Benford** | Nigrini 2012/1996/2009, Azevedo 2021, Sampaio 2022 | Ley de Benford, MAD, second-order |
| **E. Texto y Paraguay** | Katona & Fazekas 2024, Kehler 2019, Ley 7021/2022 | NLP de pliegos, ISF Paraguay |

### Evidencia cuantitativa clave

- **Fazekas 2016** (CRI, 2.8M contratos, 28 países): submission period 5d vs 15d → +8.4-9pp single-bid; weekend-abuse → +19.8-21.6pp; non-price criteria weight 0.4-0.556 → +4.8pp, >0.556 → +9.6pp
- **Ferwerda 2016** (192 casos, 8 países): `preferred supplier` r=0.487 (más alta), `missing info` r=0.431, `enmiendas` r=0.337; corruptos=4.6 flags vs clean=1.8 (p<0.05)
- **Decarolis 2022** (EPJ Data Science, 12,786 contratos): **MEAT = predictor #1 (2× next best)**; page/word count emerge en RF flexible
- **Bali case**: eliminar visita técnica obligatoria → precios caen 35-40%
- **Azevedo 2021** (13.4M pagos Bolsa Familia): individual χ² 6.48M >> 15.5, agregado por municipio MAD 0.001 < 0.0022

### Biblioteca de referencias descargada (19 PDFs + 22 MDs)

**Directorio**: `docs/referencias/` → `pdf/` (originales) y `md/` (convertidos). Índice maestro: `docs/referencias/md/00_INDICE.md`.

| # | Fuente | MD | PDF | Págs | Estado |
|---|--------|----|-----|-----:|--------|
| 1 | WB 2011 Curbing Fraud & Corruption & Collusion in Roads | `01_WB_2011_Curbing_Fraud_Corruption_Collusion_Roads.md` | `01_....pdf` | 61 | ✅ |
| 2 | WB 2011 Curbing Fraud (WP 64283) | `01b_WB_2011_Curbing_Fraud_WP64283.md` | `01b_....pdf` | 61 | ✅ |
| 3 | WB 2011 Fraud & Corruption Awareness Handbook | `02_WB_2011_Fraud_Corruption_Awareness_Handbook.md` | `02_....pdf` | 84 | ✅ |
| 4 | WB Warning Signs of Fraud & Corruption | `03_WB_Warning_Signs_Fraud_Corruption_Procurement.md` | `03_....pdf` | 4 | ✅ |
| 5 | OCP & Development Gateway 2016 Red Flags OCDS | `04_OCP_DG_2016_Red_Flags_Integrity_OCDS.md` | `04_....pdf` | 42 | ✅ |
| 6 | OECD 2016 Preventing Corruption | `05_OECD_2016_Preventing_Corruption_Procurement.md` | `05_....pdf` | 32 | ✅ |
| 7 | OECD 2007 Bribery Typology | `06_OECD_2007_Bribery_Public_Procurement_Typology.md` | `06_....pdf` | 107 | ✅ |
| 8 | Conley & Decarolis 2016 Detecting Bidders Groups (AEJ:Micro) | `07_Conley_Decarolis_2016_Detecting_Bidders_Groups_AEJMicro.md` | `07_....pdf` | 38 | ✅ |
| 9 | Fazekas 2019 Single Bidding DG REGIO | `08_Fazekas_2019_Single_Bidding_ESIF_EU_DGREGIO.md` | `08_....pdf` | 90 | ✅ |
| 10 | Ley 7021/2022 Paraguay | `09_Ley_7021_2022_Paraguay_Suministro_Contrataciones.md` | `09_....pdf` | 65 | ✅ |
| 11 | Bandiera & Decarolis 2021 NBER 28209 | `10_Bandiera_Decarolis_2021_Rules_Discretion_Corruption_NBER28209.md` | `10_....pdf` | 60 | ✅ |
| 12 | Wachs & Kertész 2019 (SciRep) | `11_Wachs_Kertesz_2019_Network_Cartel_Detection_SciRep.md` | `11_....pdf` | 10 | ✅ |
| 13 | Wachs & Kertész 2019 (arXiv) | `11b_Wachs_Kertesz_2019_arXiv.md` | `11b_....pdf` | 18 | ✅ |
| 14 | Decarolis 2022 Corruption Red Flags (EPJ Data Science) | `12_Corruption_RedFlags_EPJDataScience_2022_Decarolis.md` | `12_....pdf` | 38 | ✅ |
| 15 | Azevedo 2021 Benford (MethodsX) | `13_Benford_Azevedo_MethodsX_2021.md` | `13_....pdf` | 10 | ✅ |
| 16 | Abdou/Fisman IMF WP/22/94 Paraguay | `14_IMF_WP22-94_Paraguay_Identifying_Cartels.md` | `14_....pdf` | — | ⏳ PLACEHOLDER (pendiente) |
| 17 | Ferwerda 2016 Finding Right Indicators (EJCPR) | `15_Ferwerda_2016_Finding_Right_Indicators_EJCPR.md` | `15_....pdf` | 23 | ✅ |
| 18 | OECD 2008 Recommendation | `16_OECD_2008_Recommendation_Public_Procurement.md` | `16_....pdf` | 8 | ✅ |
| 19 | Fazekas, Tóth & King 2016 CRI (EJCPR) | `17_Fazekas_Toth_King_2016_CRI_EJCPR.md` | `17_....pdf` | 22 | ✅ |
| 20 | TI 2006 Handbook Curbing Corruption | `18_TI_2006_Handbook_Curbing_Corruption_Procurement.md` | `18_....pdf` | 232 | ✅ |
| 21 | Fazekas & Kocsis 2020 (BJPS) | `19_Fazekas_Kocsis_2020_BJPS.md` | `19_....pdf` | — | ⏳ PLACEHOLDER (pendiente) |
| 22 | Decarolis 2014 Awarding Price (AEJ:Applied) | `20_Decarolis_2014_Awarding_Price_AEJApplied.md` | `20_....pdf` | — | ⏳ PLACEHOLDER (pendiente) |

**Rutas completas**: todos los PDF en `docs/referencias/pdf/` y todos los MD en `docs/referencias/md/` con el nombre exacto de la tabla. Los 3 pendientes tienen DOI verificado y guía de descarga en `docs/referencias/COMO_DESCARGAR_FALTANTES.md`.

---

## 12. Estrategias de Detección de Anomalías (A-F)

| Alternativa | Método | Viabilidad | Novedad PY |
|-------------|--------|-----------|------------|
| **A** | ISF, LOF, ECOD, COPOD, Ensemble | ✅ Alta | ISF estándar (Kehler 2019); ECOD/COPOD benchmark SOTA |
| **B1** | CRI (Fazekas) | ✅ Alta | Paraguay fue país piloto FMI (Abdou 2022) |
| **B3** | Benford (ítems 1.87M + pagos 265K) | ✅ Alta | **Novedad PY** — nunca aplicado a OCDS Paraguay |
| **C** | Screens Porter-Zona, Bajari-Ye | ⚠️ Parcial | Sin montos por bidder → versión degradada |
| **D1** | Co-bidding network (Wachs) | ✅ Alta | **Novedad PY real** — 78K ofertas, 5,544 bidders |
| **E** | Supervisado con protestas proxy | ✅ Alta | RF/GBM 77-91% (Fazekas GTI-WP); 1,253 protestas (4%) |
| **F1** | Katona-Paraguay (texto→single-bid) | 🟡 Pendiente | Pipeline v2 listo; falta modelo texto→single-bid |

**Roadmap recomendado**: Benford → CRI → ISF/ECOD → Ensemble+DIFFI/SHAP → Co-bidding → Supervisado → Katona-PY.

---

## 13. Glosario con Contexto

### Términos del proyecto

| Término | Definición | Contexto en el proyecto |
|---------|-----------|------------------------|
| **DNCP** | Dirección Nacional de Contrataciones Públicas (Paraguay) | Ente rector de contrataciones. Portal: `contrataciones.gov.py` |
| **OCDS** | Open Contracting Data Standard | Estándar internacional para datos de contratación. Los datos fuente del proyecto están en formato OCDS |
| **SICP** | Sistema de Información de Contrataciones Públicas | Sistema interno de la DNCP. Algunos datos (costos, garantías, presupuestos) están en el SICP, NO en los PDFs → no extraíbles |
| **Pliego** | Pliego de bases y condiciones | Documento PDF que define los términos de una licitación. ~32,785 pliegos procesados |
| **Sección** | Fragmento del pliego delimitado por un título del outline | ~2.78M secciones extraídas. Cada sección tiene un `title_normalized` (323 valores únicos) |
| **Título** | Nombre de la sección (ej. "fuerza mayor", "criterios de adjudicacion") | 25 títulos en el esquema actual, cada uno con 2-5 campos |
| **Outline** | Índice/bookmarks del PDF | Extraído con PyPDF2 `reader.outline`. Profundidad 1 y 2. Usado para delimitar secciones |
| **Tika** | Apache Tika — extractor de texto de documentos | Ejecutado en contenedores Podman (puertos 9998/9999). Extrae línea a línea del PDF |
| **Document Features** | Tabla de features estructurales por documento | `document_features` (571 cols) y `document_features_v2` (108 cols) |
| **Economic Features** | Tabla de features económicas y de colusión | `document_economic_features` (46 cols). Ratios, flags, winner frequency |
| **Hybrid** | Método de extracción que combina regex + LLM | 12 campos en `master_schemas_v2.json` con `extraction_method: "hybrid"` |
| **Regex** | Método de extracción basado en patrones | 69 campos en `master_schemas_v2.json`. Usa `_re_*` methods en el transformer |
| **Conf** | Columna de confianza en `document_features_v2` | `{field}_conf REAL DEFAULT 1.0`. Si conf < 0.8, entra el fallback LLM |
| **Guided Decoding** | Técnica de vLLM que restringe la salida a una regex | `StructuredOutputsParams(regex=r'^(-?\d+...|NULL)$')`. Impone formato a nivel de logits. 7× más rápido |
| **AWQ** | Activation-aware Weight Quantization | Formato de cuantización nativo de vLLM. W4A16. Recomendado sobre GGUF |
| **GGUF** | GPT-Generated Unified Format | Formato de cuantización de llama.cpp. Experimental/deprecated en vLLM 0.23.0 |
| **FP8** | Floating Point 8-bit | Formato W8A8. Requiere Ada Lovelace+. Mitad de KV cache vs FP16 |
| **awq_marlin** | Kernel optimizado para AWQ en vLLM | Solo compatible con MistralForCausalLM/LlamaForCausalLM. 3× más rápido que AWQ normal |
| **vLLM** | Motor de inferencia de LLM de alto rendimiento | Versión 0.23.0 fijada (0.25.x no funciona en WSL por falta de UVA) |
| **WSL2** | Windows Subsystem for Linux 2 | Entorno donde corre vLLM. Limitación: sin UVA (Unified Virtual Addressing) |
| **Pseudo-GT** | Ground truth generado por modelo de frontera (DeepSeek) | Anotado con `anotador-pseudo-gt`. 900 filas, 0 REVISAR |
| **GT** | Ground Truth / Gold Standard | Anotación humana. 5,066 juicios en `gold_standard_26campos.csv` |
| **AloLab** | Ciclo artesanal de mejora de extracción | Collect errors → diagnose → rewrite hint → re-run. 100% en 4 rondas |
| **CRI** | Corruption Risk Indicator (Fazekas 2016) | Metodología de 50+ red flags. Base del enfoque de detección |
| **ISF** | Isolation Forest | Algoritmo de detección de anomalías no supervisada (Liu 2008) |
| **ECOD** | Empirical Cumulative Distribution-based Outlier Detection | Método SOTA sin parámetros (92% AUC DTE 2025) |
| **Benford** | Ley de Benford (distribución esperada de primeros dígitos) | Aplicable a 1.87M montos de ítems + 265K pagos |
| **MEAT** | Most Economically Advantageous Tender | Criterio de adjudicación que no es solo precio. Predictor #1 en Decarolis 2022 |

### Términos técnicos y workarounds

| Término | Definición | Workaround |
|---------|-----------|------------|
| **UVA** | Unified Virtual Addressing | No disponible en WSL2. Requiere `VLLM_USE_V2_MODEL_RUNNER=0` |
| **VLLM_USE_V2_MODEL_RUNNER** | Variable de entorno de vLLM | Forzar a 0 para WSL. Necesario para Qwen3, Llama, Mistral |
| **enable_thinking** | Parámetro de Qwen3 para razonamiento | `False` para extracción directa. Por defecto está activado |
| **reasoning-parser** | Flag de vLLM para modelos con razonamiento | `gemma4` con `enable_thinking=false` bypasea xgrammar (bug #39130) |
| **PowerShell bug** | Problemas de quoting con Python inline en WSL | NO usar `python -c "..."`. Escribir script temporal `.py` y ejecutar |
| **NaT serialization** | `pd.NaT` no es serializable a JSON | Usar `pd.isna(v)` como catch-all para None/NaN/NaT |
| **OOM en WSL** | VRAM insuficiente | `taskkill /F /IM "NVIDIA Overlay.exe" /T` + Warp + msedge + SearchApp → libera ~1.3 GB |
| **Cantidad_items bug** | Siempre 0 en BD | `bidder_diversity` = 0.0 para todas las filas. Documentado como limitación |

---

## 14. Agentes de opencode

> **PORTABILIDAD**: Esta sección contiene la **implementación completa (verbatim)** de cada agente, comando y skill. Para llevar los agentes a otro LLM/herramienta, copiar el bloque de código íntegro a un archivo `.md` con el nombre indicado y el frontmatter incluido. El comportamiento quedará idéntico porque el cuerpo (propósito, reglas, contrato, convenciones) está completo.

### 14.1 Pipeline de agentes (cadena)

```
variables (definición)
  → [@muestreador-gt] → CSV base
  → [@anotador-pseudo-gt] → CSV anotado (pseudo-GT)
  → [@documentador-tutores] → material de presentación
```

Los agentes se encadenan produciendo y consumiendo CSVs en `data/processed/alolab/`. El contrato entre muestreador y anotador son las columnas: `nro_licitacion, campo, variable, title_slug, title_original, text, word_count, page, prompt, db_value, llm_raw, llm_value, ground_truth, nota, prioridad, fuente_academica, confianza, es_revisar`.

### 14.2 `muestreador-gt` — implementación completa

**Archivo**: `.opencode/agents/muestreador-gt.md`

```markdown
---
name: muestreador-gt
description: Selecciona muestras representativas de la BD `dncp.pliegos_secciones` para un conjunto de variables de pliego y genera el CSV de trabajo del gold standard (filas con texto + prompt + columnas para anotar). Úsalo cuando se pida "crear muestras", "preparar el GT", "muestrear variables de pliego", "generar el CSV de gold standard" o "seleccionar N muestras por variable".
mode: all
permission:
  bash: allow
  edit: allow
---

Eres el agente **muestreador** del proyecto de detección de anomalías en contrataciones públicas del Paraguay (DNCP), en `D:\projects\subtitle-parse`.

Tu trabajo es el PRIMER paso del pipeline de gold standard: dado un conjunto de variables de pliego, extraes de la base muestras aleatorias (una por licitación) y generas un CSV listo para anotar. **NO anotas** — solo muestreas. El agente que anota es `anotador-pseudo-gt` (trabajan en cadena: primero tú, luego él).

# Cómo funciona el pipeline

```
variables (N1-N8 o nuevas) 
   → [TÚ: muestreador] → CSV base (texto + prompt + columnas vacías)
   → [anotador-pseudo-gt] → CSV anotado (ground_truth + confianza + es_revisar)
```

# Qué debes hacer

## 1. Recibir o construir la definición de variables

Las variables se definen como una lista de dicts con estos campos (ver `scripts/preparar_gt_nuevo_7variables.py` como plantilla canónica):

```python
VARIABLES = [
  {
    "variable": "N9",                      # etiqueta corta
    "campo": "dias_validez_garantia_fiel", # nombre del campo
    "type": "int" | "float" | "binary" | "categorical",
    "title_slug": "garantia de fiel cumplimiento",  # title_normalized EXACTO en BD
    "description": "qué mide la variable (una línea)",
    "extraction_hint": "guía de extracción con patrones concretos, rangos válidos y qué retornar si no aparece (NULL)",
    "prioridad": "ALTA #N",
    "fuente_academica": "referencias (autor + evidencia, ej: Ferwerda 2016 r=0.487)",
  },
  ...
]
```

Si el usuario solo da variables "a secas" (ej. "días de validez de la oferta"), tú debes:
- Verificar el `title_normalized` EXACTO consultando la BD:
  ```sql
  SELECT title_normalized, COUNT(*) FROM dncp.pliegos_secciones GROUP BY 1 ORDER BY 2 DESC;
  ```
- Redactar `description` y `extraction_hint` con el mismo estilo de las variables N1-N8 ya existentes (patrones concretos del español jurídico paraguayo, rango válido, y regla NULL explícita). NO inventes patrones sin mirar el texto real: muestrea 3-5 secciones de prueba y ajusta la hint a lo que ves.
- Mapear a una fuente académica de las ya documentadas en `docs/07_top_variables_enriquecido.md` o `docs/06_sustento_academico_respaldo.md` (si no hay match, pon "sin fuente directa" y avísalo).

## 2. Escribir y ejecutar el script de muestreo

**Usa SIEMPRE la BD `<DB_HOST>:5433/dncp`** (usuario `postgres`, password `<DB_PASSWORD>`) vía psycopg2. NO uses otro host.

Reglas del muestreo (las mismas que `scripts/preparar_gt_nuevo_7variables.py`):
- Por cada variable: `SELECT DISTINCT ON (nro_licitacion) nro_licitacion, title_normalized, content_text, word_count, page FROM dncp.pliegos_secciones WHERE title_normalized = %s ORDER BY nro_licitacion, document_id`
- `random.seed(42)` fijo para reproducibilidad.
- Muestras por variable: **100 por defecto**; si el usuario pide más (ej. N3=200), respétalo.
- **Preservación**: si ya existe el CSV de salida anotado (con ground_truth/confianza/es_revisar), NO remuestrear las filas ya anotadas: mantenlas tal cual y solo completa hasta el objetivo con filas nuevas. Esto es crítico para no perder anotaciones.
- Columnas del CSV de salida (compatibles con `benchmark_multi_vs_gt.py`):
  `nro_licitacion, campo, variable, title_slug, title_original, text, word_count, page, prompt, db_value, llm_raw, llm_value, ground_truth, nota, prioridad, fuente_academica, confianza, es_revisar`
- El `prompt` se construye así: `Campo a extraer: {description}\nTipo esperado: {type}\nGuia de extraccion: {extraction_hint}`.
- Ordenar por variable y luego `nro_licitacion`. Escribir UTF-8.

Reutiliza el código de `scripts/preparar_gt_nuevo_7variables.py` como base; si las variables cambian, modifica la lista `VARIABLES` (puedes escribir un script temporal `_tmp_muestrear.py` en la raíz del repo, ejecutarlo con el Python de Windows, y borrarlo al terminar).

**IMPORTANTE (PowerShell)**: NO uses `python -c "..."` con comillas anidadas ni backticks. Escribe un archivo `.py` temporal con la herramienta edit/write y ejecuta `python _tmp_muestrear.py`.

## 3. Salida

Escribe el CSV en `data/processed/alolab/gold_standard_pliego_nuevo_{N}variables.csv` (o el nombre que pida el usuario). Al terminar reporta:
- Ruta del CSV y nº total de filas
- Tabla resumen por variable: `variable | campo | total | población en BD`
- ADVERTENCIAS: variables con población < objetivo, title_slug sin match, duplicados.

# Contrato con el anotador

Deja claro al terminar: "listo, corré anotador-pseudo-gt sobre `<ruta>`". El CSV que produces es el único contrato entre ambos agentes — no modifiques columnas que el anotador necesite (ground_truth, nota, confianza, es_revisar deben existir, aunque vacías).
```

### 14.3 `anotador-pseudo-gt` — implementación completa

**Archivo**: `.opencode/agents/anotador-pseudo-gt.md`

```markdown
---
name: anotador-pseudo-gt
description: Toma el CSV generado por `muestreador-gt` y produce un pseudo-GT anotado (ground_truth, confianza, es_revisar) usando extracción determinista (regex) Y modelos de frontera (DeepSeek, GPT, etc. via API). Úsalo cuando se pida "anotar el GT", "generar pseudo-GT", "poner ground truth", "usar DeepSeek como anotador", "completar la anotación" o "revisar casos ambiguos del GT".
mode: all
permission:
  bash: allow
  edit: allow
---

Eres el agente **anotador** (pseudo-GT) del proyecto de detección de anomalías en contrataciones públicas del Paraguay (DNCP), en `D:\projects\subtitle-parse`.

Tu trabajo es el SEGUNDO paso del pipeline de gold standard: tomas el CSV que produjo `muestreador-gt` (con texto + prompt + columnas vacías) y produces un CSV completo con `ground_truth`, `confianza`, `es_revisar` y `nota`. Actúas como un anotador humano que usa modelos de frontera para decidir. Trabajas en cadena con `muestreador-gt`.

```
CSV base (de muestreador-gt)
   → [TÚ: anotador] → CSV anotado (ground_truth + confianza + es_revisar)
```

# Estrategia de anotación en 3 capas

## Capa 1 — Anotación determinista (regex)

Para cada fila, aplica primero reglas de extracción basadas en el `extraction_hint` del campo. Reutiliza el patrón de `scripts/anotar_pseudo_gt.py` (anotadores N1-N8 como referencia). Las reglas deterministas cubren:
- **Patrones tabulares**: `"por: | N"`, `"sera de: | N"`, `"N dias"`, `"N%"`
- **Patrones de texto**: valores numéricos con contexto semántico (ej. `"validez de N dias"`, `"multa de N jornales"`)
- **Palabras clave**: `"No Aplica"` → NULL, `"seran desechadas"` → 1, etc.

Si la regex encuentra un match claro → `confianza=0.95+`, `es_revisar=0`, siguiente fila.
Si la regex no encuentra match → dejar `confianza=0.5`, `es_revisar=1`, `nota="REVISAR: sin match regex"`.

## Capa 2 — Revisión como modelo de frontera

Para las filas con `es_revisar=1` (es decir, donde la regex falló), **actúas tú mismo como el anotador humano** (usas tu propio juicio como modelo de frontera para analizar el texto, aplicar el extraction_hint, y decidir la respuesta):

1. Para cada fila, lee el `text`, el `prompt` (que contiene `description` + `type` + `extraction_hint`), y el `campo`.
2. Aplica la `extraction_hint` como regla de anotación: revisa manualmente el texto palabra por palabra, identifica patrones, decide el valor correcto.
3. Si el valor es claro: escribe `ground_truth`, `nota` explicando el patrón encontrado, `confianza=0.9`, `es_revisar=0`.
4. Si el valor no está presente en el texto (el pliego no contiene esa información): `ground_truth=""`, `nota="NULL: no hay evidencia en el texto"`, `confianza=0.85`, `es_revisar=0`.
5. Si es ambiguo (el texto sugiere un valor pero no es 100% claro): `ground_truth=""`, `nota="REVISAR [humano]: ambiguo — ...explique por qué..."`, `confianza=0.5`, `es_revisar=1`.
6. **Importante — regla de NULL**: si el texto NO contiene el valor explícitamente, responde NULL. NO intentes inferir valores de cláusulas legales, leyes, o fechas de calendario que no sean el número explícito. Si el texto dice "conforme a especificaciones técnicas" sin un número, → NULL. Si dice "En caso de que aplique..." sin %, → NULL.
7. **Regla de falsos positivos**: NO extraigas números de ítems de lista ("1. El oferente...", "a) ..."), años calendario (2021, 2024), números de leyes (7021, 7781, 2006, 2051), montos en guaraníes (Gs. 1.000.000), ni fechas (15 de enero, 31/12/2024).
8. **Regla de desambiguación**: si el texto tiene múltiples números (ej. varios plazos), elige el que esté semánticamente asociado al campo (por proximidad de palabras clave en el texto). Si ninguno está claramente asociado → NULL.

## Capa 3 — API de frontera (opcional)

Si el usuario lo pide explícitamente (ej. "usa DeepSeek API", "usa Tokengo"), puedes fallback a una llamada API de un modelo de frontera para corroborar. Las APIs disponibles (ver `.env`):

| API | Base URL | Key | Modelos |
|---|---|---|---|
| B.AI | `https://api.b.ai/v1` | `B_AI_API_KEY` (env) | deepseek-v4-flash, hy3, gpt-5.2 |
| DeepSeek | `https://api.deepseek.com` | `DEEPSEEK_API_KEY` | deepseek-chat, deepseek-reasoner |
| OpenRouter | `https://openrouter.ai/api/v1` | `OPEN_ROUTER_API_KEY` | deepseek-v4, qwen-72b, etc. |
| Tokengo | `https://api.tokengo.com/v1` | `TOKENGO_API_KEY` | glm-5.1, deepseek-v3.2 |

Usa la clase `OpenRouterProvider` o `TokengoProvider` de `src/etl/transformers/llm_provider.py` para la llamada. El prompt de extracción es:

```
system: "Eres un anotador de ground truth para pliegos de licitación paraguayos (DNCP). Dado el texto de una sección y una guía de extracción, responde SOLO con el valor numérico o NULL. NO expliques, NO añadas texto adicional."
user: "Campo: {campo}\nTipo: {type}\nGuía: {extraction_hint}\n\nTexto:\n{text}"
```

Procesa en batches (ThreadPoolExecutor, max_workers=8) para acelerar.

# Cómo ejecutar la anotación

Escribe un script temporal `_tmp_anotar.py` en la raíz del repo que:
1. Lee el CSV de muestreador-gt
2. Aplica la Capa 1 (regex) para cada fila
3. Para las filas REVISAR, aplica la Capa 2 (tú mismo analizas cada una) o Capa 3 (API)
4. Escribe el CSV anotado con nombre: `gold_standard_pliego_nuevo_{N}variables_{modelo}.csv`

**IMPORTANTE (PowerShell)**: No uses `python -c "..."` con comillas anidadas. Escribe un archivo `.py` temporal y ejecútalo con `python _tmp_anotar.py`.

# Convenciones de salida

| Campo | Regla |
|---|---|
| `ground_truth` | Valor como string: `"90"`, `"1"`, `"0.5"`; vacío para NULL |
| `confianza` | `0.5` = REVISAR; `0.85` = NULL seguro; `0.9`+ = valor seguro |
| `es_revisar` | `"1"` si necesita revisión humana; `"0"` si está listo |
| `nota` | Explicación breve de por qué elegiste ese valor, ej: "plazo 90 días (patrón 'sera de: 90')" |

# Contrato con el usuario

Al terminar reporta:
- Ruta del CSV anotado y nº total de filas
- Tabla resumen por variable: `variable | total | con valor | NULL | REVISAR`
- 3-5 ejemplos de casos REVISAR que dejaste (nro_licitacion, explicación)
- Si usaste Capa 3 (API), cuántas llamadas y qué modelo
```

### 14.4 `documentador-tutores` — implementación completa

**Archivo**: `.opencode/agents/documentador-tutores.md`

```markdown
---
name: documentador-tutores
description: Genera material de comunicación para presentar la tesis a los tutores (correos ejecutivos, resumen PPT por diapositiva, documento del flujo de datos con gráficas, y respuestas a preguntas típicas de reunión). Úsalo cuando pidan resúmenes "tipo correo", "para tutores", "para presentar en reunión", "PPT", "gráficas del pipeline", "explicar de dónde vienen los datos" o "cómo se extrajeron los datos de los pliegos".
mode: all
permission:
  bash: allow
  edit: allow
---

Eres el agente de documentación/comunicación de la tesis de detección de anomalías en concontrataciones públicas del Paraguay (DNCP), proyecto `D:\projects\subtitle-parse`.

Tu rol: producir material claro, veraz y listo para mostrarse a tutores académicos. Solo debes usar HECHOS verificados (ver HOJA DE HECHOS abajo y el repositorio); nunca inventes números, pasos ni fuentes. Cuando algo no esté confirmado, márcalo `[NO CONFIRMADO]`.

# Entregables que produces (según lo que pidan)

Escribe todo en `docs/presentacion_tutores/` (crédalo si no existe). Usa rutas con barras normales.

1. **Correo ejecutivo** → `correo_tutores.md` — un correo en markdown, 3-6 párrafos, tono académico pero directo. Estructura: 1) qué se entregó (resultado), 2) decisiones tomadas y por qué, 3) estado actual / bloqueo, 4) qué se decidirá a continuación y qué aporta.
2. **Resumen PPT (diapositivas)** → `ppt_tutores.md` — outline numerado diapositiva por diapositiva con **nivel de detalle medio**: cada slide debe explicar el *cómo* (herramienta, script, parámetro y cifra), no solo etiquetas. Máximo ~20 diapositivas. Formato exacto por slide:

   ```markdown
   ## Slide N — Título corto y claro

   **Resumen:** 1-2 frases que explican QUÉ se hizo en esta etapa y POR QUÉ (la idea detrás del paso).

   - Bullet 1 — CÓMO: acción concreta con herramienta/script + 1 parámetro + 1 cifra verificada.
   - Bullet 2 — siguiente detalle operativo del proceso (qué hace, no una keyword suelta).
   - ...
   - `Gráfica:` graficos/xxx.png  ← solo si aplica (lo ubica el generador pptx)
   ```

   **Reglas del nivel de detalle** (objetivo: "breve pero detallado" — el tutor debe poder decir cómo se hizo cada cosa al leer el slide):
   - Cada bullet es una **oración completa** de 1-2 líneas que describe una acción concreta. NO uses fragmentos sueltos ("Descarga asíncrona, batch 1,000") — escribe la oración: "Se descargaron los PDFs con un descargador asíncrono en batches de 1,000 y pausa de 300 s para no saturar el portal."
   - Incluye SIEMPRE: la herramienta o script que lo hace, un parámetro relevante y una cifra verificada de la HOJA DE HECHOS.
   - Si hay un fix o decisión de diseño en esa etapa, menciónalo en una línea (ej. "para evitar OOM del WAL", "fix del Issue A: ...").
   - Un slide tiene **3-6 bullets**; si el material excede, prioriza los pasos que explican el "cómo".
   - Nunca inventes pasos ni números: todo debe rastrearse a la HOJA DE HECHOS o al repo; si algo no está confirmado, anótalo `[NO CONFIRMADO]`.
   - Si `python -c "import pptx"` funciona en Windows, genera también el `.pptx` real ejecutando `docs/presentacion_tutores/generar_pptx.py` (título + resumen + bullets por slide) → `ppt_tutores.pptx`.
   - **Incluye obligatoriamente 3-4 slides sobre el setup de IA** (hardware RTX 4070 Ti SUPER 16GB, WSL2, vLLM 0.23.0, llama-server Windows), el benchmark de modelos (AWQ final ranking con velocidad y F1), y la evolución de prompts + AloLab (59.6% → 100%). Usa la tabla de datos de la HOJA DE HECHOS "Setup de IA local y benchmark de modelos".

   **Ejemplo del nivel esperado:**

   ```markdown
   ## Slide 5 — Descarga masiva y óptima de PDFs

   **Resumen:** Los pliegos se bajan del portal en paralelo con un descargador asíncrono para completar la colección de 32,785 PDFs sin saturar el servidor de la DNCP.

   - Se usó un descargador asíncrono (`src/utils/file_async_downloader.py` + `async_download_pipeline.py`) con batches de 1,000 PDFs y una pausa de 300 s entre batch para respetar el portal.
   - Cada archivo bajado se descomprime si es `.zip/.rar` interno y se extrae el PDF, descartando el resto.
   - Se renombró cada PDF a `{year}_{categoria_id}_{nro_licitacion}.pdf` y se organizó en `data/raw/pdf/{year}/category_{id}/`, dejando el corpus trazable por licitación.
   - `Gráfica:` graficos/pdfs_por_anio.png
   ```
3. **Flujo de datos con gráficas** → `flujo_datos.md` + `graficos/` — documento que explica de dónde viene cada dato y qué transformación sufrió, con una figura Mermaid del flujo y gráficas matplotlib (véase más abajo). Máximo 2 gráficas por sección y solo con números de la HOJA DE HECHOS.
4. **Hoja de respuestas de reunión** → `preguntas_reunion.md` — para cada pregunta típica (las del usuario y equivalentes), respuesta en 3-6 líneas: qué hiciste exactamente, qué herramienta, qué archivo/script lo hace, y una cifra verificada. Si la pregunta cae en regex vs LLM, incluye el análisis académico (Sección "regex vs LLM" abajo).
5. **Detalle de setup de IA y benchmark** (solo si piden profundizar en modelos/GPU/benchmark) → `setup_ia_benchmark.md` — documento independiente con tablas de todos los modelos, muestras, velocidad, VRAM, concurrencia, evolución de prompts, proceso AloLab y proyecciones de tiempo. Crea este archivo solo si el usuario pide explícitamente detalles del benchmark.

Regla de formato: respuestas en español, tablas markdown para comparativas, usar `🟡 decidir` para lo abierto, `✅` para lo hecho y `📋` para lo planificado.

# HOJA DE HECHOS (verificada en el repo — fuente de verdad)

## Origen y adquisición de datos
- Fuente OCDS: portal paraguayo `https://www.contrataciones.gov.py`. Datos estructurados llegan como **CSV bulk ZIP** `masivo.zip` (NO se usa la API en vivo del DNCP — verificar si el motivo "rate limit" está en alguna doc; si no está, documeéntelo como `[NO CONFIRMADO el motivo, solo la elección de bulk CSV]`).
- Script: `src/etl/extractors/ocds_csv_extractor.py`, `scripts/run_ocds_csv_pipeline.py` con `YEARS = [2021..2025]`. ZIPs: `data/raw/csv/datasets/masivo_{year}.zip` (2021=73.0 MB, 2022=258.7 MB, 2023=164.6 MB, 2024=319.2 MB, 2025=462.0 MB, 2026=5.9 MB parcial).
- **Filtro "solo pliegos electrónicos"**: de `tender_documents` se conservan solo registros con `tender_documents_format == application/pdf` y tipo **"Pliego Electrónico de Bases y Condiciones"** → archivos `ten_documents_pliego_pdf_{year}.csv` → mergeado `ten_documents_pliego_pdf_every_year.csv` → deduplicado por `nro_licitacion` en `ten_documents_pliego_pdf_every_year_filtered.csv` (~31,387 licitaciones). Lógica en `src/tasks/extract_tender_documents.py` y en `OcdsCsvPipeline` (log "Filtradas %d licitaciones sin pliego electrónico").
- **Descarga PDFs**: asíncrona `src/utils/file_async_downloader.py` + `src/pipelines/async_download_pipeline.py`. batch=1000, pause=300s. Descomprime `.zip/.rar` internos, extrae el PDF, renombra `{year}_{categoria_id}_{nro_licitacion}.pdf`. En disco: `data/raw/pdf/{year}/category_{id}/`. **32,785 PDFs**: 2021=1,116 · 2022=4,620 · 2023=6,237 · 2024=9,406 · 2025=10,805 · 2026=601.

## Parsing (Tika + outlines)
- El stack oficial de producción usa **Apache Tika** (contenedores Podman, puertos 9998 y 9999, `src/utils/tika_init.py`, 8 contenedores total). El compose es teóricamente `docker/docker-compose-tika.yml` → carpeta `docker/` está vacía en el repo, así que la infra Tika real es `[NO CONFIRMADO en disco]`.
- Transformación: `src/etl/transformers/pdf_transformer.py` — parseo de XHTML, omite líneas de número de página (`PAGE_NUM_PATTERN ^\d{1,3}/\d{1,3}$`), `Semaphore(64)`.
- **Estrategia por outlines**: `src/etl/extractors/pdf_outline_extractor.py` extrae el índice/bookmarks del PDF con PyPDF2 (`reader.outline`) + pdfplumber; mantiene profundidad 1 y 2; genera `PDFOutline(document_id, title, page, depth, nro_licitacion, category_id, year, line_number)`. Pipeline: `src/pipelines/pdf_outline_pipeline.py`.
- **Extracción de líneas (texto completo)**: `masive_async_convert_pdf_parquet.py` (pdfplumber) en memoria, sellos de cabeceras de 2 líneas, `combine_documents_all_years.parquet` (~1.93 GB, ~50M líneas).
- **Cruce outlines + líneas → secciones**: `src/pipelines/pdf_content_pipeline.py` + `src/etl/transformers/pdf_content_transformer.py`. Normalización `normalize_exact` (NFKD, sin acentos/puntuación, minúsculas). `determine_section_end` resuelve **5 casos de límites** (README_FIX_SECTION_EXTRACT.md): Caso 1 mismo-página; Caso 2/3 entre páginas → corte al final de la página anterior (fix del Issue A: subtítulo "Adenda" que absorbía páginas); Caso 4 virtual depth-1; Caso 5 marcador `__END_OF_DOCUMENT__` para que la última sección tenga fin (Issue C). Además deduplicación TOC vs reales (commit `09e9f50`).

## Limpieza
- `src/pipelines/content_cleaning_pipeline.py` (`clean_content_field`): quita `\r`, **une guiones de fin de línea** (`"docu-\nmento"` → `"documento"`), normaliza espacios múltiples, elimina líneas vacías y de solo números de página, reemplaza caracteres Unicode problemáticos, stemming básico español → `content_clean[]`, `content_text`, `content_length_clean`. Descarta secciones con `content_length_clean == 0`.
- Luego `word_count` y otras features estructurales (Sesión 1: media 204.5 palabras/sección).

## Carga a PostgreSQL
- `src/pipelines/sections_to_db_pipeline.py` → `dncp.pliegos_secciones` **particionada por año** (LIST(year), particiones `_2021.._2025`), 17-18 cols, índices brin(page), gin trigram, unique `(nro_licitacion,title,line_start,year)`. Carga vía COPY (`execute_values`) en batches de 10,000, commit por batch, upserts `ON CONFLICT DO NOTHING`. Checkpoints por año (`.sections_db_done_{year}` en `data/processed/sections_clean/`). ~2.78M secciones.
- El resto de tablas OCDS (17 total) se cargan desde los CSVs del `masivo.zip` con FKs ordenadas.

## Volúmenes verificados (para gráficas)
| Métrica | Valor |
|---|---|
| Licitaciones con pliego electrónico (whitelist) | ~31,387 |
| PDFs en disco | 32,785 |
| `pliegos_secciones` | ~2.78M filas |
| `document_features` | 31,321 × 571 cols |
| `document_features_v2` | 31,321 × 108 cols |
| `document_economic_features` | 29,811 × 46 cols |
| Esquema campos v2 | 25 títulos / **81 campos = 69 regex + 12 hybrid** |
| `title_normalized` únicos | 323 |
| Fallback LLM pendiente (12 campos hybrid, conf<0.8) | ~324K documentos |

## Setup de IA local y benchmark de modelos (toda la evidencia del repo)

### Hardware y software (verified)
| Componente | Detalle |
|---|---|
| GPU | NVIDIA **RTX 4070 Ti SUPER — 16 GB VRAM** (VRAM útil ~15.6 GB tras matar NVIDIA Overlay, Warp, Edge, SearchApp) |
| RAM / SO | 32 GB · Windows 11 + **WSL2** (Ubuntu), driver 581.29, CUDA 12.4 |
| Motor principal | **vLLM 0.23.0** (V1 engine) en WSL2, entorno `/root/vllm-env/bin/python` (único con vLLM+pandas+psycopg2) |
| Restricción WSL | `VLLM_USE_V2_MODEL_RUNNER=0` obligatorio (WSL2 no tiene UVA); **vLLM 0.25.x no funciona** (MRv2 requiere UVA) → se fija 0.23.0 |
| Formato | **AWQ nativo recomendado**; GGUF experimental/deprecated en vLLM. `awq_marlin` solo con arquitectura Mistral/Llama; Qwen/Phi caen a `awq` normal (más lento) |
| Runtime secundario | **llama.cpp `llama-server.exe` en Windows** (`C:\models\llama_cuda\`, nightly b10566, CUDA 12.4, puerto 1234) — usado para probar Qwen3.8-27B porque WSL vhdx se quedó sin disco en D: (0.26 GB libres) |
| VRAM cleanup | `taskkill /F /IM "NVIDIA Overlay.exe" /T` + Warp + msedge + SearchApp → libera ~1.3 GB |

### Evolución del benchmark (muestras por modelo, en orden cronológico)
| Sesión | Modelo | Formato | Muestras | Precisión | Velocidad |
|---|---|---|---|---|---|
| 4 | Qwen2.5 14B Q5_K_L | GGUF | 40 + 80 | 97.5% (39/40) → 100% (80/80) | 7.1s → 6.5s |
| 4 | Qwen3-14B Q5_K_M | GGUF | 60 | **100% (60/60)** | 5.10s avg · 13-19 tok/s · 15.23× conc |
| 5 | Mistral 7B v0.3 Q5_K_M | GGUF | 60 | **100% (60/60)** | 4.90s avg (nuevo campeón) |
| 5 | Qwen3-8B Q5_K_M | GGUF | 60 | 100% (60/60) (fix `enable_thinking=False`) | 4.50s |
| 5 | Phi-4 14B Q4_K_M | GGUF | 60 | 100% (60/60) | 6.09s |
| 5 | Llama 3.1 8B | GGUF | — | ❌ tokenizer gated | — |
| 5 | Gemma2 27B | GGUF | — | ❌ OOM (>16 GB) | — |
| 6 | **Mistral 7B v0.3 AWQ** | AWQ (awq_marlin) | 10 | **100%** · F1 94.0% | **1.61s** · 64-77 tok/s · 35.22× · 3.89 GiB |
| 6 | Qwen3-8B AWQ | AWQ | 60 | 100% | 4.80s (sin marlin) |
| 6 | Qwen3-14B AWQ | AWQ | 60 | 100% | 5.29s (sin marlin) |
| 6 | Phi-4 14B AWQ | AWQ | 10 | 90% (9/10) | 4.47s (dtype float16) |
| 7 | **Llama 3.1 8B AWQ** | AWQ (awq_marlin) | 5 | **100%** · F1 **96.4%** | **1.93s** · 55-68 tok/s · 29.21× · 5.39 GiB |
| 7 | Qwen2.5 14B AWQ | AWQ | 5 | 100% · F1 93.3% | 2.46s · 8.18× · 9.38 GiB |
| 7 | Gemma 3 12B | AWQ | — | ❌ requiere bfloat16, AWQ usa float16 | — |
| Qwen38 (2026-08) | Qwen3.8-27B UD-Q3_K_XL | GGUF (llama-server Windows) | 100 GT | **97/100 = 97%** | 1.41s/muestra · 33-39 tok/s gen · 1015-1432 tok/s prompt |

**Veredicto final (AWQ, ordenado por velocidad × concurrencia)**: 🥇 **Mistral 7B AWQ** (1.61s, 35.22×, F1 94.0%, THROUGHPUT) · 🥇 **Llama 3.1 8B AWQ** (1.93s, 29.21×, F1 96.4%, CALIDAD) · Qwen2.5 14B AWQ (2.46s, 8.18×) · Qwen3-8B AWQ (4.80s) · Qwen3-14B AWQ (5.29s) · Phi-4 AWQ (90%) · Qwen3.8-27B (97%, Windows).

**Proyección para ~324K secciones del fallback LLM**: Mistral 7B AWQ ~**4 horas** (35× continuous batching) · Llama 3.1 8B AWQ ~**5.8 horas** · Mistral GGUF ~1 día · HTTP baseline ~34 días.

### Benchmark del fallback LLM sobre 460 muestras (26 campos hybrid, conf<0.8)
| Etapa | Precisión | Notas |
|---|---|---|
| Baseline Sesión 9 | 274/460 = **59.6%** | 106 fallos analizados: 51 eran NULL legítimos, 55 perdidos de verdad |
| Sesión 10 (seed fijo) | 271/460 = **58.9%** | Llama 3.1 8B AWQ, 23 campos |
| Sesión 11 — Mistral 7B | 289/460 = **62.8%** | peor que Llama |
| Sesión 11 — **"primera línea"** | **386/460 = 83.9%** | prompt pide el número en la primera línea + max_tokens=256 + parser first-line; +14.8 pp |
| Sesión 12 — **guided decoding** | **386/460 = 83.9%** | regex vLLM (StructuredOutputsParams); misma precisión pero **7× más rápido** (8.95 vs 1.32 docs/s; 51.4s vs 347.5s) |
| Sesión 13 — few-shot retrieval | ❌ descartado | empeoraba: 95% → 85%/80% |
| Sesión 14 — **AloLab manual** | **100/100** | golden set humano (ver abajo) |

### Proceso AloLab (Sesión 14, agente como meta-analista) → 100%
Ciclo artesanal: `scripts/alolab_collect_errors.py` corre Llama 3.1 8B-AWQ sobre las 100 GT humanas y exporta solo los errores → el agente diagnostica (con texto, hint actual y salida LLM) → reescribe hint o postprocess → re-run. Rondas:
1. **95/100** → 3 hints reescritas (validez_garantia, experiencia_tecnica, solicitud_anticipo) + fix regex `personales?`→`personal(?:es)?`.
2. **95/100** → 5 FPs nuevos (el modelo sobre-extrae de ítems de lista "1. El oferente...") → 2 reglas de postprocess + regla `dias_solicitud_anticipo` (v∉(5,20) exige contexto "días" + "solicitud").
3. **95/100** → los 5 errores son el modelo repitiendo el multiplicador "30.42" de la hint → hint limpia + **conversión determinista** meses→días (mapa {1:30,2:61,3:91,6:182,12:365}).
4. **100/100** ✅.
Insights: los falsos positivos (GT=NULL) se arreglan mejor en **postprocess determinista** que en el prompt; una hint que enseña un valor concreto induce sobre-extracción en pliegos de plantilla vacía.

### Ingeniería de prompts (cómo se mejoró)
- Sesión 9: 10 hints de campo mejoradas en `master_schemas_v2.json` (+40pp en `anos_experiencia_minima`, +20 en `num_documentos_pago`, +35 en `ratio_liquidez_minimo`); reglas agresivas del system prompt REVERTIDAS (mataban campos: 60%→0%).
- Sesión 11: hint verbose vs corta → **+4.5 pp** las verbosas con ejemplos de patrones (se mantienen verbosas).
- Sesión 12: **guided decoding** (regex `^(-?\d+(?:[.,]\d+)?|NULL)$`) impone el formato a nivel de logits: output ~2 tokens, sin explicaciones, 7× más rápido, mismo accuracy.
- Sesión 14: hints + reglas postprocess vía ciclo AloLab.
- Sesiones 15-16: capa de postproceso determinista compartida (`extraction_postprocess.py`) y portes hybrid→regex (7 campos, +25 a +88 pp).
- Comparativa final (`comparativa_estrategias.csv`, 26 campos, 5,066 juicios GT): **H=65.1 > L_pp=63.1 > R=61.0 > L_raw=51.0** (Ri=60.9).

# regex vs LLM — el análisis académico que debes presentar (estado actual: 🟡 decidir)

Contexto del usuario: **los datos ya están limpios en la base**. La pregunta abierta es si la transformación de campos (extracción de variables del texto de pliegos) se hace con **regex** o con **LLM**, y cuál tiene más sustento académico. Presenta el estado real de tu repo:

- **Estado hoy: arquitectura híbrida** — regex con determinismo primero, LLM como fallback para `conf < 0.8` (`src/etl/transformers/document_features_v2_transformer.py` usa patrones `_re_*`; capa postproceso compartida `src/etl/transformers/extraction_postprocess.py`; fallback `document_features_v2_llm_*`). En Sesión 16 se movieron 7 campos hybrid→regex (69 regex / 12 hybrid).
- **Evidencia de tu repo (benchmarks)**:
  - Regex determinista = 100% en campos de plantilla (pct uniformes, conteo de ítems nivel 1).
  - LLM Llama 3.1 8B-AWQ llegó a **100/100** sobre el golden set humano en Sesión 14 (ciclo AloLab).
  - Comparativa (`data/processed/alolab/comparativa_estrategias.csv`): media acc **H=65.1 > L_pp=63.1 > L_raw=51.0 > R=61.0** según campo; el **determinista/postproceso es lo que más contribuye**.
  - LLM fallback: ~82% menos llamadas que un pipeline 100% LLM (solo procesa donde falló regex).
- **Sustento académico para cada lado** (todos verificados en `auditor-top-pliegos/contexto_auditor/`):
  - A favor de regex/determinista: OCP & Development Gateway (2016) — los red flags computables con datos estructurados; World Bank (2011) — "rigged specifications" detectables con patrones; reproducibilidad y sin costo por documento.
  - A favor de LLM: Katona & Fazekas (2024, GTI-WP) — el **texto** de los llamados predice oferta única mejor que solo estructura (77%→82%); Lima et al. (2020, EMNLP) — Bi-LSTM sobre textos brasileños; Peppes et al. (2026, Information) — transformers para indicadores de riesgo en pliegos.
  - Conclusión equilibrada que debes ofrecer: el **híbrido** (regex primero + LLM fallback para el resto) combina la reproducibilidad del determinista con la cobertura semántica del LLM, y es lo que sugiere tanto OCP (estructura) como Katona (texto). Fórmula para guiarte: si el campo tiene patrón plantilla → regex; si necesita comprensión semántica (p.ej. "experiencia técnica en X", "liquidez mayor o igual a N") → LLM con postproceso determinista.

# Decisión de campos: qué variable se extrae con regex y cuál con LLM (hybrid)

De los **81 campos** de `document_features_v2`: **69 regex + 12 hybrid**. Todos empezaron como hybrid (26 evaluados en el golden set); los portes a regex se decidieron con evidencia del GT. Esta es la metodología que debes poder explicar:

## Regla de decisión (heurística aplicada campo por campo)

1. **¿El texto del pliego usa un patrón plantilla estable para ese campo?** (p.ej. "garantía de fiel cumplimiento del 10%", "dentro de los 10 días", "mínimo de N referencias") → probar **regex**. Si la regex valida **≥90% sobre el GT con 0 falsos positivos** → **regex (definitivo)**.
2. **¿El valor exige comprensión semántica, desambiguación entre cláusulas similares o conversión contextual?** (p.ej. "experiencia técnica en X", "ratio de liquidez mayor o igual a N", meses→días) → **hybrid (LLM)** con postproceso determinista.
3. **¿El valor requiere "contar" elementos semánticos?** (componentes de una fórmula, documentos de pago, pasos de verificación) → el LLM NO sabe contar listas; el regex cuenta solo ítems numerados de primer nivel. Si no hay ítems nivel-1 → **irresoluble** (se mantiene hybrid pero con rendimiento bajo; documentado en `revision_26_campos.md`).

## Evidencia que justificó los portes a regex (Sesiones 15-16, golden set 26 campos)

- `pct_*` uniformes (5) + `dias_presentacion_garantia` + `pct_anticipo`: regex/determinista = **100%** con 0 FP → portados en Sesión 15.
- `num_categorias_documentos` (regex conteo ítems nivel 1, **100%**, fp0), `num_referencias_minimo` (**98%**, fp0), `num_referencias_tecnicas_minimo` (**97%**, fp0), `dias_reposicion` (**92%**), `pct_monto_referencia_minimo` (**90.5%**), `pct_monto_referencia_tecnica` (**90%**), `anos_experiencia_minima` (**80%**, fp6) → portados en Sesión 16 con subidas de **+25 a +88 pp** vs la mejor estrategia LLM previa.
- Lección: el regex de conteo de ítems nivel-1 supera al LLM (100% vs 12%); los campos de referencias tenían FPs LLM altos (50-172) que el regex elimina (FP 0).

## Los 12 campos que quedan como hybrid (y por qué NO se portan)

| Campo | Tipo | Por qué sigue en LLM |
|---|---|---|
| `dias_notificacion_fuerza_mayor` | int | regex solo 5.5%; wording variable ("notificar fehacientemente dentro de los N días"); LLM 94.5% |
| `dias_pago_maximo` | int | hay que asociar el número a "después de la presentación de la factura" (no a otros plazos); LLM 88.5% |
| `dias_aceptacion_rechazo_factura` | int | proximidad semántica a "aceptar/rechazar/factura"; LLM 85% |
| `dias_solicitud_anticipo` | int | desambiguación fuerte vs `dias_pago_anticipo` y vs plazo de reclamo (punto 5); reglas postprocess; LLM 96.5% |
| `dias_pago_anticipo` | int | desambiguación vs `dias_solicitud_anticipo` (solo "abonar/pagar"); LLM 86.5% |
| `dias_validez_garantia` | int | conversión meses/años→días con palabra en paréntesis; determinista en postprocess; LLM 82% |
| `anos_experiencia_minima_tecnica` | int | desambiguación vs cantidad de personal y antigüedad en meses; anti-FP postprocess; LLM 78% |
| `ratio_liquidez_minimo` | float | decimal en prosa ("mayor que N", ">= N"); sin patrón plantilla; LLM 82% |
| `ratio_endeudamiento_maximo` | float | idem; LLM 86.5% |
| `num_documentos_pago` | int | irresoluble (el LLM no cuenta documentos de primer nivel) — se mantiene hybrid sin excluir |
| `num_pasos_verificacion` | int | irresoluble (conteo semántico) |
| `num_componentes_formula` | int | irresoluble (el LLM no cuenta componentes de fórmula) |

Fuente: `data/processed/section_clustering_v2/master_schemas_v2.json` (`extraction_method`), `docs/revision_26_campos.md` (acc_best y método por campo), `comparativa_estrategias.csv`.

# Cómo generar las gráficas

Crea un script temporal Python en `docs/presentacion_tutores/generar_graficas.py` y ejecútalo con el Python de Windows (`python`, ya verificado con matplotlib 3.9.4 y pandas 2.3.3). Guarda las imágenes en `docs/presentacion_tutores/graficos/` (PNG, 100 dpi, tamaño ~ (9,5) pulgadas, estilo limpio). Gráficas sugeridas (usa SIEMPRE números de la HOJA DE HECHOS; cada gráfica debe citar la fuente en pie de figura):
1. `pdfs_por_anio.png` — barras: PDFs 2021=1116, 2022=4620, 2023=6237, 2024=9406, 2025=10805, 2026=601.
2. `tamano_masivo_zip.png` — barras: MB del bulk OCDS 2021=73.0, 2022=258.7, 2023=164.6, 2024=319.2, 2025=462.0, 2026=5.9.
3. `volumenes_capas.png` — barras horizontales de volúmenes por capa: 31,387 whitelist → 32,785 PDFs → 2.78M secciones → 31,321 docs features.
4. `campos_schema.png` — barras: 81 campos = 69 regex + 12 hybrid (y un stack bar sesión a sesión si está en AGENTS.md).
5. `comparativa_estrategias.png` — leer `data/processed/alolab/comparativa_estrategias.csv` (si existe) y graficar la media de acc por estrategia (R, Ri, L_raw, L_pp, H). Si el archivo no existe, omítenla y anótalo.

La figura Mermaid del flujo va embebida en `flujo_datos.md` como bloque ```mermaid```.

# Proceso de ejecución

1. Crea `docs/presentacion_tutores/` y los archivos pedidos.
2. Genera `generar_graficas.py`, ejecútalo, y refiere las imágenes por ruta relativa en los .md.
3. Verifica que cada cifra usada esté en la HOJA DE HECHOS (o en el repo). Si necesitas un dato extra, búscalo en el repo o la BD con los hechos antes de ponerlo.
4. Si te piden solo un entregable (p.ej. solo el correo), haz solo ese.
5. Al terminar, resume en tu mensaje final: qué archivos creaste, qué gráficas, y 1-2 decisiones abiertas 🟡 que dejaste señaladas para que el usuario decida.
```

### 14.5 Comando `/tutores` — implementación completa

**Archivo**: `.opencode/commands/tutores.md`

```markdown
---
description: Genera material para tutores (correo, PPT, flujo de datos con gráficas, respuestas de reunión) sobre el pipeline de la tesis.
agent: documentador-tutores
---

Genera el material de presentación para los tutores sobre el pipeline de la tesis.

$ARGUMENTS

Reunión: el tutor preguntará desde el inicio: cómo se extrajeron los datos de los pliegos, cómo se metieron en la base de datos, cuál fue la estrategia (por outlines dentro del PDF), ejemplo de extracción de variables, todas las limpiezas y filtros, solo pliegos electrónicos, los ZIP bajados, la transferencia de OCDS a la base (sin usar la API de la DNCP por rate limit), descarga óptima de PDFs, transformación con Tika, correcciones y limpieza de todo el proceso, y la decisión abierta de transformar los datos en base con regex o LLM (sustento académico y qué estrategia aplicar para continuar con los datos en limpio).

Crea los entregables según tus instrucciones de agente. Responde al final con un resumen de archivos creados.
```

### 14.6 Skill `evaluate-local-models` — implementación completa

**Archivo**: `.opencode/skills/evaluate-local-models/SKILL.md`

```markdown
---
name: evaluate-local-models
description: Benchmark LLMs for structured extraction using local vLLM on RTX 4070 Ti SUPER 16GB. Covers Qwen 14B/7B, gpt-oss-20b GGUF models with CUDA graph configs, VRAM cleanup, and quality/throughput evaluation via pipeline 2 extraction test.
---

# evaluate-local-models

Benchmark LLMs for structured extraction (Pipeline 2) using local vLLM + RTX 4070 Ti SUPER 16GB.

## Inventory — GGUF Models

| Model | Size | VRAM est. | Path |
|-------|------|-----------|------|
| **Qwen 2.5 14B Q5_K_L** ✅ | 10.2 GB | ~15.7 GB | `lmstudio-community/.../Qwen2.5-14B-Instruct-Q5_K_L.gguf` |
| Qwen 2.5 14B Q4_K_S ⚠️ | 8.0 GB | ~12 GB | `bartowski/Qwen2.5-14B-Instruct-GGUF/Qwen2.5-14B-Instruct-Q4_K_S.gguf` |
| Qwen 2.5 7B Q4_K_M ❌ | 4.4 GB | ~7 GB | `bartowski/Qwen2.5-7B-Instruct-GGUF/Qwen2.5-7B-Instruct-Q4_K_M.gguf` |
| gpt-oss-20b-MXFP4 ❌ | 11.3 GB | N/A | `lmstudio-community/gpt-oss-20b-GGUF/gpt-oss-20b-MXFP4.gguf` |
| Qwen 2.5 Coder 14B Q4_K_M | 8.4 GB | ~12 GB | `lmstudio-community/.../Qwen2.5-Coder-14B-Instruct-Q4_K_M.gguf` |
| Qwen 2.5 Coder 14B Q6_K | 11.3 GB | ~16 GB | `Lucy-in-the-Sky/.../qwen2.5-coder-14b-instruct-q6_k.gguf` |

## vLLM Config Templates

### Safe config (no CUDA graphs, reliable)
```bash
VLLM_SCRIPT=/tmp/vllm_safe.sh
MODEL="<gguf_path>"
TOKENIZER="<hf_name>"

cat > "$VLLM_SCRIPT" << 'EOF'
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
/root/vllm-env/bin/vllm serve "$MODEL" \
    --tokenizer "$TOKENIZER" \
    --host 0.0.0.0 --port 8000 \
    --max-model-len 8192 \
    --gpu-memory-utilization 0.85 \
    --enforce-eager \
    --kv-cache-dtype fp8 \
    --enable-prefix-caching
EOF
```

### CUDA graphs config (speed, may hang during graph capture)
```bash
VLLM_SCRIPT=/tmp/vllm_optimized.sh
MODEL="<gguf_path>"
TOKENIZER="<hf_name>"

cat > "$VLLM_SCRIPT" << 'EOF'
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
/root/vllm-env/bin/vllm serve "$MODEL" \
    --tokenizer "$TOKENIZER" \
    --host 0.0.0.0 --port 8000 \
    --max-model-len 14000 \
    --gpu-memory-utilization 0.90 \
    --kv-cache-dtype fp8 \
    --enable-prefix-caching
EOF
```

### Low VRAM config (for Q4_K_S or tight memory)
```bash
VLLM_SCRIPT=/tmp/vllm_q4.sh
MODEL="<gguf_path>"
TOKENIZER="<hf_name>"

cat > "$VLLM_SCRIPT" << 'EOF'
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
/root/vllm-env/bin/vllm serve "$MODEL" \
    --tokenizer "$TOKENIZER" \
    --host 0.0.0.0 --port 8000 \
    --max-model-len 8192 \
    --gpu-memory-utilization 0.80 \
    --enforce-eager \
    --kv-cache-dtype fp8
EOF
```

### Aggressive VRAM cleanup (when Windows processes eat VRAM)
```powershell
# Kill these processes from PowerShell (admin)
taskkill /F /IM "NVIDIA Overlay.exe" /T
taskkill /F /IM "Warp.exe" /T
taskkill /F /IM "msedge.exe" /T
taskkill /F /IM "SearchApp.exe" /T
# Can free 1.3 GB -> 15.6 GB
```

## Known Issues

| Issue | Symptom | Workaround |
|-------|---------|------------|
| **CUDA graph hang** | Stuck at `Capturing CUDA graphs (decode, FULL): 0/35` or `43%` | Kill vLLM + retry. Intermittent — usually works 2nd attempt |
| **gpt-oss MXFP4** | GGUF reader crash parsing MXFP4 metadata | Not compatible with vLLM. MXFP4 quant not supported |
| **Q4_K_S crash** | vLLM dies silently after `non-default args` | Probable incompat with vLLM 0.23.0. Try `--enforce-eager` |
| **WSL `localhost`** | `Connection refused` on `localhost:8000` | Use `127.0.0.1:8000` instead |
| **VLLM_USE_V1=0** | Process exits immediately | v0.23.0 doesn't support V0 engine |
| **OOM on startup** | `CUDA out of memory` | Kill Windows GPU processes (see above) + set `expandable_segments:True` |

## Benchmark Workflow

### 1. Start vLLM (via tmux)
```bash
# Kill previous + check VRAM
pkill -f vllm 2>/dev/null
sleep 5
nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits

# Write config script (use template above)
# Adjust MODEL and TOKENIZER paths for target model
bash "$VLLM_SCRIPT"

# OR use tmux for persistence
tmux new-session -d -s vllm "bash $VLLM_SCRIPT"
```

### 2. Wait for ready (poll API)
```bash
# Check every 10s
curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8000/v1/models
```

### 3. Run evaluation
```bash
source /root/vllm-env/bin/activate
cd /mnt/d/projects/subtitle-parse

# Quick check (10 samples per title)
python scripts/evaluate_extraction.py --provider vllm --samples-per-title 10 --skip-sample

# Full eval (20 samples per title)
python scripts/evaluate_extraction.py --provider vllm --samples-per-title 20 --skip-sample
```

### 4. Collect metrics
- Report at `eval/comparison_report.md`
- Key metrics: `OK/N`, `F1`, `MAE`, `total_time`
- For throughput: check avg `response_time` per sample

### 5. Shutdown
```bash
tmux send-keys -t vllm C-c
sleep 3
tmux kill-session -t vllm 2>/dev/null
pkill -f vllm 2>/dev/null
```

## Expected Results (RTX 4070 Ti SUPER 16GB)

| Model | Quality | s/sample | 315K sections | Status |
|-------|---------|----------|---------------|--------|
| Qwen 14B Q5_K_L | 97.5-100% | ~7s | ~25 days seq | **Current best** |
| gpt-oss-20b-MXFP4 | ? | ~8s est. | ~29 days | Untested |
| Qwen 7B Q4_K_M | 0% | ~3s est. | ~11 days | Too weak |
| Qwen 14B Q4_K_S | ? | ~5s est. | ~18 days | Crashed in vLLM |
| gpt-oss-20b-MXFP4 | N/A | N/A | N/A | GGUF reader incompatible |

## Key Scripts & Files

- `scripts/evaluate_extraction.py` — benchmark runner (`--provider vllm`)
- `config/settings.py` — DB config, API keys
- `src/etl/transformers/llm_provider.py` — `LocalLLMProvider` class
- `docs/wsl2_vllm_setup.md` — full vLLM install guide
- `eval/comparison_report.md` — latest benchmark results
- `.env` — API keys
```

### 14.7 Índice de agentes (`README.md`)

**Archivo**: `.opencode/agents/README.md` — contiene el resumen de los 3 agentes, sus propósitos, modos, archivos de salida, cómo invocarlos y el pipeline. Ver implementaciones completas arriba en 14.2-14.6.

---

## 15. Sesiones Históricas (Resumen)

### Sesiones 1-3 — Feature Improvements (Phases 1-3)
- **Phase 1**: word_count recalculado (2.78M secciones, avg 204.5), 571 columnas (era 260), gini, entropía, chunked loading
- **Phase 2**: `document_economic_features` (29,811 × 46), JOIN 9 tablas OCDS, 12 campos derivados
- **Phase 3**: Colusión — window functions (`winner_category_frequency`, `is_repeat_winner`), `bidder_diversity`

### Sesión 4 — 6ta Estrategia de Ranking (Economic Risk)
- 6 estrategias con pesos: outlier 0.20, RF 0.25, contextual 0.15, section IF 0.15, corr 0.10, economic 0.15
- Memory fix: `_get_relevant_columns()` carga 245 cols (vs 587) via `information_schema`

### Sesiones 4-5 — Benchmark vLLM
- Qwen3-14B GGUF: 100% (60/60), 5.10s, 15.23×
- Mistral 7B GGUF: 100% (60/60), 4.90s — nuevo campeón
- Fix: Qwen3 thinking mode, offline API, UVA workaround

### Sesión 6 — AWQ Benchmark
- **Mistral 7B AWQ (awq_marlin)**: 1.61s, 35.22×, 3.89 GiB — campeón throughput
- GGUF deprecado en vLLM → AWQ nativo recomendado
- Phi-4 AWQ: 90% (dtype float16)

### Sesión 7 — AWQ Retest
- **Llama 3.1 8B AWQ**: F1 96.4%, 1.93s, 29.21× — campeón calidad
- Qwen2.5 14B AWQ: F1 93.3%, 2.46s, 8.18×

### Sesión 8 — Phase 5: Confidence Tracking + LLM Fallback Pipeline
- Columnas `{field}_conf` (0.0-1.0) para campos hybrid
- Pipeline LLM fallback creado: extractor → transformer → loader
- ~324K docs pendientes, 12 campos hybrid

### Sesión 9 — LLM Fallback Benchmark & Prompt Engineering
- Baseline 59.6% (274/460). 51/106 fallos eran NULL legítimos
- 10 field hints mejoradas en `master_schemas_v2.json`
- System prompt calibración: reglas agresivas revertidas

### Sesión 10 — Benchmark Final Determinista
- Llama 3.1 8B AWQ: 271/460 = 58.9%, 304.5s
- Campos 100%: 8 de 23. Problemáticos: anos_experiencia_minima_tecnica (0%), num_componentes_formula (0%)

### Sesión 11 — "Primera Línea" Approach → 83.9%
- Prompt: "Escribe el número o NULL en la PRIMERA línea"
- **83.9% (386/460)**, +14.8 pp vs baseline
- Hint verbose: +4.5 pp (se mantienen verbosas)

### Sesión 12 — Guided Decoding → 83.9% @ 7× velocidad
- StructuredOutputsParams con regex: misma precisión, **7× más rápido** (51.4s vs 347.5s)
- Guided decoding NO mejora precisión por sí solo, pero la mantiene y acelera

### Sesión 13 — Few-shot Retrieval ❌ Descartado
- Embeddings BGE-M3-Legal-Spanish, corpus de 2,073+1,537 ejemplos
- Empeoraba: 95% → 85%/80%. Las secciones son casi idénticas (cos 0.95-0.98)

### Sesión 14 — AloLab Manual → 100% ✅
- 4 rondas: 95/100 → 95/100 → 95/100 → **100/100**
- Fixes: regex `personal(es)?`, reglas anti-FP para ítems de lista, conversión determinista meses→días

### Sesión 15 — Capa de Postproceso Compartida + 3 Campos a Regex
- `extraction_postprocess.py` creado: single source of truth
- 3 campos portados a regex (pct_garantia_cumplimiento, pct_diferencia_precio_bajo/alto)
- Comparativa estrategias: **H=65.1 > L_pp=63.1 > R=61.0 > L_raw=51.0**

### Sesión 16 — 7 Campos Más a Regex → 69 regex / 12 hybrid
- Portes: num_categorias_documentos (+88pp), num_referencias_minimo (+87.5pp), num_referencias_tecnicas_minimo (+66.5pp), etc.
- `document_features_v2` recreado: 31,321 × 108 cols

### Sesión 17 — Panorama de Estrategias + Mapa de Variables
- `estrategias-anomaly-deteccion.md` creado
- BD verificada por consulta directa (SELECT, no estimados)
- 20 variables top (niveles 1-4), viabilidad A-F, hoja de ruta

### Sesión 18 — Agente de Comunicación para Tutores
- `.opencode/agents/documentador-tutores.md` + comando `/tutores`
- Material generado: 4 .md + 5 gráficas matplotlib + PPT

### Sesiones posteriores — Benchmark Modelos 2025-2026
- Investigación de 38 modelos que caben en 16 GB VRAM
- Documentación: `docs/benchmark_modelos/` (README, AWQ, GGUF, FP8, ranking combinado)
- Avisos de validación: Gemma 4 bug #39130, Granite 4.2 thinking, Qwen3.8-27B stretch goal

### Sesión Final — Sustento Académico + Pseudo-GT 8 Variables
- 22 fuentes, 19 PDFs, `docs/06_sustento_academico_respaldo.md` (respaldo probatorio punto por punto)
- 8 variables N1-N8 con respaldo académico top-down: literatura (Ferwerda 0.487, EPJ MEAT #1, WB red flags) → DB verification → método de extracción
- `docs/07_top_variables_enriquecido.md` regenerado: top 20 variables niveles 1-4, evidencia cuantitativa actualizada, prioridades v2
- **900 muestras generadas** con `scripts/preparar_gt_nuevo_7variables.py` (seed 42, N3=200, resto=100)
- **Pseudo-GT DeepSeek v4 flash**: 900 filas, 680 con valor, 0 REVISAR. Archivo: `gold_standard_pliego_nuevo_8variables_deepseek_v4_flash.csv`
- Pipeline de agentes: `@muestreador-gt` → `@anotador-pseudo-gt` (3 capas: regex + modelo de frontera + API opcional)
- Agentes creados: `.opencode/agents/muestreador-gt.md`, `anotador-pseudo-gt.md`, `README.md` (índice de agentes)
- Detalle por variable: N1 100/100 (90/60/30/120 días), N2 100/100 (120/90/60/150), N3 78/122 (valores 3-60), N4 25% sí, N5 52/48 (precio/calidad), N6 constante 2, N7 constante 1, N8 2/98 (% subcontratación)

### Sesión 19 — Backup de BD + Contexto Exportable (03-sep-2026)
- `pg_dump -Fc -Z9` dentro del contenedor `dncp` (PG 17.9) → `backup_dncp/backup_dncp.dump` 1.6 GB (copiado con `podman cp`; el `.dump.zst` intermedio no hizo falta: el custom ya va comprimido)
- `docs/PROMPT_CONTEXTO_LLM.md` creado (prompt de 6 secciones para incorporar otro LLM) + `proyecto_tesis_export.zip` (11.4 MB, 1050 archivos, sin datos grandes)
- Aclarado: `/tmp` del contenedor no tiene ruta Windows — se extrae con `podman cp`

### Sesión 20 — Setup Linux vLLM Production (09-sep-2026)
- `docs/setup_linux_vllm_production.md` (12 secciones) + FASE 5 (5a–5j) en `PIPELINE_CHECKLIST.md`
- Decisiones: Ubuntu 24.04.4 **Server** (3.2 GB, no Desktop: ahorra ~300–500 MB VRAM), misma RTX 4070 Ti SUPER 16 GB, full stack, ambos AWQ, solo documento
- Medido: venv 9.6 GB, CUDA 12.8 6.7 GB, modelos 9.3 GB, `data/` 28 GB → disco 150–200 GB NVMe, raíz única + swapfile
- Matriz VRAM/disco/servidor del Top 10 (4 GGUF van por llama-server, no vLLM); descargas por fases con embudo (nunca 75 GB juntos)
- Dependencias inventariadas: podman 4.9.3, Python 3.12.3, Java 21, venv único (ya trae pandas/psycopg2/sklearn/sentence-transformers), llama-server v0.4.0, Tailscale + SSH ed25519 (Mac/Windows clientes)
- `postgres-dncp:17` es tag local de la oficial (misma imagen 461 MB) → en destino usar `docker.io/library/postgres:17` directo

---

## 16. Mapa de Directorios del Proyecto

### 16.1 Estructura raíz

```
D:\projects\subtitle-parse\
├── config\                # Configuración vía env vars (settings.py)
├── data\
│   ├── raw\               # PDFs crudos, ZIPs OCDS, CSVs descargados
│   ├── processed\         # TODO el producto del pipeline (ver 16.2)
│   └── external\          # Datos externos (respuestas LLM web, categorías)
├── docs\                  # Documentación completa (ver 16.5)
├── src\
│   ├── core\              # Entidades (pdf_models, db_models) + excepciones
│   ├── etl\               # extractors/ + transformers/ + loaders/
│   ├── pipelines\         # 15 orquestadores ETL
│   ├── tasks\             # 42 scripts legacy/one-off (scrap bin)
│   └── utils\             # Helpers (file, csv, tika, llm_client, text_cleaner...)
├── scripts\               # 73 scripts de ejecución + 45 temporales (_tmp/_debug)
├── auditor-top-pliegos\   # Auditoría de variables para Isolation Forest (ver 16.3)
├── eval\                  # Evaluación de modelos (comparison_report, raw_results)
├── backup-dncp\           # Backup/restore de PostgreSQL (Docker + scripts)
├── backup_dncp\           # Dump real verificado: backup_dncp.dump 1.6 GB (pg_dump -Fc -Z9, 03-sep-2026)
├── proyecto_tesis_export.zip  # Código exportable 11.4 MB (1050 archivos, sin datos grandes)
├── estrategias-anomaly-deteccion.md  # Panorama de estrategias con BD verificada (raíz)
├── docker\                # (vacío, solo .gitkeep — infra Tika real en Podman)
├── tmp\                   # start_vllm.sh (helper)
├── logs\                  # Logs de todos los pipelines (40+ archivos)
├── presentacion_tutores\  # Material gráfico generado (duplicado de docs/presentacion_tutores/)
├── .opencode\             # Agentes, comandos, skills (sección 14)
├── AGENTS.md              # Contexto de sesiones (49 KB)
├── PIPELINE_CHECKLIST.md  # Checklist del pipeline
├── *.sql                  # DDL de las tablas OCDS (3 versiones)
└── requirements.txt       # Dependencias
```

### 16.2 `data/processed/` — producto del pipeline

| Subdirectorio | Contenido | Notas |
|---|---|---|
| `alolab\` | GT 26 campos, GT 8 variables, errores AloLab (round1-5), comparativa estrategias, revision 26 campos | **Núcleo del gold standard** |
| `alolab\gt_por_campo\` | 10 CSVs `*_GT.csv` (200 filas c/u) + 26 `errores_*.csv` | GT por campo individual |
| `csv\` | Tender data mergeado por año (json/pdf), whitelist de pliegos | `ten_documents_pliego_pdf_every_year_filtered.csv` = input descarga |
| `fewshot_retrieval\` | Corpus + embeddings BGE-M3 (corpus.json, *_emb.npy) + resultados k2/k4/static | **Few-shot descartado** (Sesión 13) |
| `isf_candidatos\` | Muestras OCDS/pliegos para ISF | Pequeños |
| `llm_benchmark\` | Resultados benchmark multi-modelo (460 muestras), GT 5 campos, Qwen38 | Resultados por modelo + multimodel_results/ |
| `outlines\` | Outlines por año (2021-2026) + merged parquet | Del PDFOutlinePipeline |
| `parquet\` | PDF texto por año + `combined_documents_all_years.parquet` (1.93 GB) | Líneas extraídas |
| `pipeline2_checkpoints\` | Checkpoint del Pipeline 2 | `Test_Title_checkpoint.json` |
| `schema_design_v2\` | Muestras de 25 títulos (8 c/u) + title_samples.json | Para diseño de schemas |
| `section_clustering\` | Master schemas v1 + 30 reportes por título + clusters | Clustering original |
| `section_clustering_v2\` | **`master_schemas_v2.json`** (81 campos) + few_shot_db.json | **Esquema oficial** |
| `sections\` | Secciones por año (parquet) | Antes de limpieza |
| `sections_clean\` | Secciones limpias + checkpoints `.sections_db_done_{year}` | Input de carga a BD |
| `test_2021_pipeline\` | Test del pipeline para 2021 | — |
| `title_diagnostics\` | title_diagnostics.csv + report | Diagnóstico de títulos |
| `title_ranking\` | Ranking v1 (CSV + report + bootstrap + k_evaluation) | 6 estrategias |
| `title_ranking_v2\` | Ranking v2.2 (CSV + report + filtro D) | Versión final |
| `validation\` | GT web + respuestas + raw de modelos (base/few-shot/strict) | Validación externa |

### 16.3 `src/` — código fuente

| Ruta | Contenido | Descripción |
|---|---|---|
| `src/core/entities/` | `pdf_models.py`, `db_models.py`, `document_models.py` | Dataclasses con schema PyArrow |
| `src/core/exceptions/` | `exceptions.py` | PipelineError, ExtractionError, TransformationError, LoadError |
| `src/etl/extractors/` | 20 extractores | Lectura de BD/PDF/CSV (ver 16.4) |
| `src/etl/transformers/` | 26 transformadores | **`extraction_postprocess.py`** (capa determinista), `llm_provider.py`, `document_features_v2_*` |
| `src/etl/loaders/` | 16 cargadores | Escritura a BD/parquet/CSV |
| `src/pipelines/` | 15 pipelines | Orquestadores ETL (ver 16.4) |
| `src/tasks/` | 42 scripts legacy | Descarga, Tika, migraciones, clustering temprano |
| `src/utils/` | 17 helpers | `file_async_downloader.py`, `csv_utility.py`, `tika_init.py`, `llm_client.py`, `text_cleaner.py`, `prompt_utils.py` |

### 16.4 Pipelines y extractores clave

**Pipelines de Fase 0 (ingesta):** `ocds_csv_pipeline.py` (ZIP→13 tablas), `async_download_pipeline.py` (PDFs), `pdf_parquet_pipeline.py` (Tika→parquet), `pdf_outline_pipeline.py` (bookmarks), `pdf_content_pipeline.py` (outlines+líneas→secciones), `content_cleaning_pipeline.py` (limpieza), `sections_to_db_pipeline.py` (COPY→BD).

**Pipelines de Fases 1-2 (features y extracción):** `document_features_pipeline.py` (571 cols), `document_features_v2_pipeline.py` (81 campos regex, `FINAL_TITLES`=25 títulos), `document_economic_features_pipeline.py` (46 cols), `document_features_v2_llm_pipeline.py` (fallback LLM, **no ejecutado**), `llm_extraction_pipeline.py` (legacy LLM).

**Pipelines de análisis:** `title_ranking_pipeline.py` (6 estrategias, `top_n=80`, `section_sample=200K`), `section_clustering_pipeline.py` (UMAP+KMeans, 10 títulos, schema LLM).

**Config `config/settings.py`:** lee `.env` — `DB_CONFIG` (user/password/host/port/database), paths (`BASE_OUTPUT_*`), API keys (`OPENAI`, `TOKENGO`, `SAMBANOVA`, `OPENROUTER`, `DEEPSEEK`, `GOOGLE`), `LLM_ENDPOINT/USERNAME/PASSWORD`, `DEFAULT_BATCH_SIZE/RETRIES`.

### 16.5 `docs/` — documentación completa

| Carpeta | Contenido |
|---|---|
| `docs/` (raíz) | 43 .md: planes (plan_actual, plan_futuro, plan_mejora_pipeline_seleccion_titulos), pipeline 2 (pipeline2_summary, execution_plan, optimized_strategy, bottleneck_analysis), pipeline 3 (anomaly_detection), benchmark (benchmark_resultados, benchmark_vllm_vs_lmstudio, benchmark_qwen38_session), WSL2 (wsl2_vllm_setup, wsl2_vllm_errors), extracción (revision_26_campos, tesis_b_c_resultados, phase5_llm_fallback_report, schema_pipeline2_campos), sustento (05, 06, 07), clustering (pipeline_llm_context, reporte_validacion_*), **setup_linux_vllm_production (FASE 5), CONTEXTO_TESIS (este archivo), PROMPT_CONTEXTO_LLM** |
| `docs/benchmark_modelos/` | 5 archivos: README (38 modelos) + modelos_awq/gguf/fp8 + ranking_combinado |
| `docs/presentacion_tutores/` | Material para tutores: correo, PPT, flujo de datos, preguntas, gráficas, generar_graficas.py, generar_pptx.py |
| `docs/referencias/` | **Literatura académica** (sección 11): `pdf/` (18 PDFs), `md/` (22 MDs + índice), COMO_DESCARGAR_FALTANTES.md |

### 16.6 Directorios auxiliares

**`auditor-top-pliegos/`** — Auditoría de variables para Isolation Forest:
- `auditoria_variables_isolation_forest_contrataciones_publicas.md` (36 KB, auditoría completa)
- `agents.md` (instrucciones del auditor), `glosario_variables.md` (glosario técnico de contratación), `resumen_variables_candidatas.md`, `campos_tesis_isolation_forest_clasificacion.csv` (15 MB)
- `contexto_auditor/` — 6 fases: `diccionario_base_datos.md` (44 KB), `fase1_clasificacion_taxonomica.md`, `fase2_relevancia_isolation_forest.md`, `fase3_configuracion_modelo.md`, `fase4_limitaciones_cierre.md`, `fase5_mapeo_bd_candidatos.md`, `fase6_sustento_academico.md`

**`eval/`** — Evaluación de modelos:
- `comparison_report.md`, `ground_truth.json`, `sample_sections.json`
- `raw_results/` — respuestas crudas DeepSeek-V3.2, gemma-4-31B, qwen14b

**`backup-dncp/`** — Backup/restore de la BD:
- `Dockerfile`, `init_ddl.sql` (DDL completo), `backup.sh`, `restore.sh`, `pack.sh`, `custom.conf`, `README.md`

**`backup_dncp/`** — Dump real verificado (ver §3.5):
- `backup_dncp.dump` 1.6 GB (pg_dump `-Fc -Z9` del contenedor `dncp`/PG 17.9, 03-sep-2026). Restore con `pg_restore -Fc --clean --if-exists`.

**`tmp/`** — `start_vllm.sh` (script de arranque de vLLM).

---

## 17. Índice de Archivos Clave

### Documentación principal
| Archivo | Descripción |
|---------|-------------|
| `AGENTS.md` | Contexto de sesiones (49 KB, 615 líneas) |
| `PIPELINE_CHECKLIST.md` | Checklist completo del pipeline (22 KB) |
| `estrategias-anomaly-deteccion.md` | Panorama de estrategias con BD verificada |
| `docs/06_sustento_academico_respaldo.md` | 22 fuentes, 19 PDFs, respaldo por variable |
| `docs/07_top_variables_enriquecido.md` | Top 20 variables v2 con evidencia cuantitativa |
| `docs/revision_26_campos.md` | 26 campos: 14 regex 100%, 9 hybrid, 3 irresolubles |
| `docs/tesis_b_c_resultados.md` | Arquitectura híbrida de extracción |
| `docs/benchmark_resultados.md` | Benchmark completo (705 líneas, 21 configuraciones) |
| `docs/benchmark_modelos/README.md` | 38 modelos, 5 ejes, avisos de validación |
| `docs/benchmark_modelos/modelos_awq.md` | Ranking AWQ (vLLM nativo, awq_marlin, repos HF) |
| `docs/benchmark_modelos/modelos_gguf.md` | Ranking GGUF 2026 (llama-server, avisos Gemma 4/Granite 4.2) |
| `docs/benchmark_modelos/modelos_fp8.md` | Ranking FP8 (vLLM nativo, 8-bit, calidad ~fp16) |
| `docs/benchmark_modelos/ranking_combinado.md` | Rankings multi-criterio: precisión, concurrencia, balance, por formato, Top 10 benchmark |
| `docs/modelos_locales_investigacion.md` | Investigación de modelos locales 2026 y optimizaciones vLLM |
| `docs/benchmark_qwen38_session.md` | Sesión Qwen3.8-27B (Windows nativo, llama-server, 97/100) |
| `docs/benchmark_vllm_vs_lmstudio.md` | Comparativa vLLM optimizado vs LM Studio |
| `docs/wsl2_vllm_setup.md` | Guía completa de instalación vLLM en WSL2 |
| `docs/wsl2_vllm_errors.md` | Catálogo de errores vLLM en WSL2 y soluciones |
| `docs/vllm_production.md` | Formatos de cuantización para vLLM en producción |
| `docs/plan_finetune_qlora_test.md` | Plan de fine-tuning QLoRA (innecesario tras AloLab 100%) |
| `docs/plan_feature_improvements.md` | Plan de mejoras de features (6 fases) |
| `docs/pipeline3_anomaly_detection.md` | Diseño de Pipeline 3 (950 líneas) |
| `docs/pipeline2_summary.md` | Resumen de descubrimientos del Pipeline 2 |
| `docs/pipeline2_execution_plan.md` | Plan de ejecución y análisis de rendimiento |
| `docs/pipeline2_optimized_strategy.md` | Estrategia de ejecución optimizada |
| `docs/pipeline2_bottleneck_analysis.md` | Análisis de cuello de botella del Pipeline 2 |
| `docs/schema_pipeline2_campos.md` | Explicación campo por campo del schema |
| `docs/prompt_actual.md` | Prompt actual de clustering y schema design |
| `docs/phase5_llm_fallback_report.md` | Reporte de Phase 5 LLM Fallback |
| `docs/alolab_progreso_9campos.md` | Progreso AloLab en 9 campos hybrid |
| `docs/csv_analisis_monitoreo_9campos.md` | CSV de análisis de monitoreo (281K filas) |
| `docs/presentacion_tutores/` | Material para tutores (6 .md + PPT + gráficas) |
| `docs/setup_linux_vllm_production.md` | Setup Ubuntu 24.04 + vLLM + Tailscale en máquina dedicada (FASE 5, 12 secciones) |
| `docs/SESION_NUEVA_UBUNTU.md` | Bootstrap de sesión fresca opencode en Ubuntu (estado git, .env, rutas, primer mensaje) |
| `docs/PROMPT_CONTEXTO_LLM.md` | Prompt de 6 secciones para incorporar otro LLM al proyecto |

### Literatura académica descargada (`docs/referencias/`)
| Archivo | Descripción |
|---------|-------------|
| `docs/referencias/md/00_INDICE.md` | Índice maestro: 19 PDFs + 22 MDs, páginas y estado |
| `docs/referencias/pdf/` (18 PDFs) | Originales descargados (WB 2011, OCP 2016, OECD, Conley-Decarolis, Fazekas, Ley 7021, Wachs, Decarolis EPJ, Azevedo, Ferwerda, TI 2006...) |
| `docs/referencias/md/` (22 MDs) | PDFs convertidos a Markdown (buscables) |
| `docs/referencias/COMO_DESCARGAR_FALTANTES.md` | Guía para los 3 pendientes (IMF WP/22/94, Fazekas-Kocsis 2020, Decarolis 2014) con DOI |

### Datos y esquemas
| Archivo | Descripción |
|---------|-------------|
| `data/processed/section_clustering_v2/master_schemas_v2.json` | 81 campos, 25 títulos, método de extracción |
| `data/processed/alolab/comparativa_estrategias.csv` | 5 estrategias × 26 campos (5,066 juicios) |
| `data/processed/alolab/gold_standard_26campos.csv` | GT humano original (5,066 filas) |
| `data/processed/alolab/gold_standard_pliego_nuevo_8variables_deepseek_v4_flash.csv` | Pseudo-GT 8 variables (900 filas) |
| `data/processed/alolab/revision_26_campos.csv` | Revisión de método por campo |
| `backup_dncp/backup_dncp.dump` | Dump BD `dncp` 1.6 GB (`-Fc -Z9`, 03-sep-2026, ver §3.5) |
| `proyecto_tesis_export.zip` | Código exportable 11.4 MB (1050 archivos, sin datos grandes) |

### Agentes
| Archivo | Descripción |
|---------|-------------|
| `.opencode/agents/muestreador-gt.md` | Muestreo de BD para GT |
| `.opencode/agents/anotador-pseudo-gt.md` | Anotación pseudo-GT con modelo de frontera |
| `.opencode/agents/documentador-tutores.md` | Material de comunicación para tutores |
| `.opencode/agents/README.md` | Índice de agentes |
| `.opencode/commands/tutores.md` | Comando para invocar documentador-tutores |
| `.opencode/skills/evaluate-local-models/SKILL.md` | Skill de benchmark de modelos |

### Scripts clave
| Script | Propósito |
|--------|-----------|
| `scripts/benchmark_multi_vs_gt.py` | Benchmark multi-modelo vs GT (100 muestras) |
| `scripts/benchmark_llm_fallback.py` | Benchmark fallback LLM (460 muestras, guided decoding) |
| `scripts/alolab_collect_errors.py` | Recolección de errores para ciclo AloLab |
| `scripts/comparativa_estrategias.py` | Comparativa 5 estrategias (GPU-free) |
| `scripts/preparar_gt_nuevo_7variables.py` | Sampler de 8 variables académicas |
| `scripts/anotar_pseudo_gt.py` | Anotador regex N1-N8 |
| `scripts/run_document_features_v2_llm_pipeline.py` | Entry point fallback LLM (no ejecutado) |
| `scripts/build_fewshot_corpus.py` | Corpus para few-shot retrieval |
| `scripts/benchmark_fewshot_retrieval.py` | Benchmark few-shot (descartado) |

### Código fuente
| Archivo | Propósito |
|---------|-----------|
| `src/etl/transformers/extraction_postprocess.py` | Capa de postproceso determinista compartida |
| `src/etl/transformers/llm_provider.py` | Providers: VLLMOffline, Local, OpenRouter, Tokengo |
| `src/etl/transformers/document_features_v2_transformer.py` | Transformer v2 con métodos `_re_*` |
| `src/etl/transformers/document_features_v2_llm_transformer.py` | Transformer LLM fallback |
| `src/etl/extractors/document_features_v2_llm_extractor.py` | Extractor LLM fallback |
| `src/etl/loaders/document_features_v2_llm_loader.py` | Loader LLM fallback |

---

## 18. Workarounds Técnicos (IMPORTANTE)

### WSL2 + vLLM
- `VLLM_USE_V2_MODEL_RUNNER=0` obligatorio (sin UVA)
- vLLM fijado a 0.23.0 (0.25.x no funciona en WSL)
- Entorno único: `/root/vllm-env/bin/python` (tiene vLLM + pandas + psycopg2)
- NUNCA crear venv nuevo para scripts vLLM
- VRAM: `taskkill /F /IM "NVIDIA Overlay.exe" /T` + Warp + msedge + SearchApp → libera ~1.3 GB

### PowerShell
- NO usar `python -c "..."` con comillas anidadas ni backticks
- Siempre escribir script temporal `.py` y ejecutar `python _tmp_script.py`
- Luego borrar el script temporal

### BD
- NaT serialization: `pd.isna(v)` como catch-all para None/NaN/NaT
- `pandas SQLAlchemy`: UserWarning al leer SQL con psycopg2 (cosmético)
- BD: `<DB_HOST>:5433/dncp` (usuario `postgres`, password `<DB_PASSWORD>`)

### Qwen3
- `enable_thinking=False` obligatorio para extracción directa (thinking mode activado por defecto)
- HTTP server crashes en WSL (UVA + multiprocessing) → usar offline API
- AWQ sin awq_marlin (Qwen no compatible) → más lento que GGUF

### Gemma 4
- Bug #39130: `--reasoning-parser gemma4` + `enable_thinking=false` bypasea xgrammar silenciosamente
- Fix mergeado en PR #45553 (16/06/2026). Validar con smoke test.
- Gemma 3 12B: requiere bfloat16, AWQ usa float16 → incompatible

### Granite 4.2
- Razonamiento nativo con thinking modes → desactivar con `--default-chat-template-kwargs '{"enable_thinking": false}'` + `--reasoning-parser granite_thinking_parser`

---

## 19. Próximos Pasos (Priorizados)

1. **Ejecutar fallback LLM** (`document_features_v2_llm_pipeline`): ~324K docs con conf<0.8 en 12 campos hybrid. Modelo: Llama 3.1 8B AWQ. WSL2, vLLM 0.23.0. ~3h con continuous batching. (Alternativa: correrlo ya en la máquina Linux de FASE 5.)
2. **Ejecutar FASE 5 (Setup Linux)**: instalar Ubuntu 24.04.4 Server según `docs/setup_linux_vllm_production.md` (5a–5j), restaurar BD desde `backup_dncp/backup_dncp.dump`, validar smoke de ambos AWQ.
3. **Commit**: todos los cambios sin commitear (Sesiones 1-16). Limpiar scripts `_tmp_*` antes.
4. **Benchmark Top 10** (embudo 2 fases, ver §10 + setup doc §7-bis): smoke 30–50 muestras c/u → 900 muestras top 5–8. Pre-vuelos obligatorios: Gemma 4 (`schema_valid_json_rate`), Granite (thinking desactivado).
5. **Clustering**: schema validation para 9 títulos restantes (2/10 completados).
6. **Pipeline 3 (Anomaly Detection)**: implementar según diseño en `docs/pipeline3_anomaly_detection.md`. 15-20 días estimados.
7. **Benford**: aplicar a 1.87M items + 265K pagos (agregado por convocante o departamento).
8. **Co-bidding network**: red de 78K ofertas / 5,544 bidders.

---

## 20. Metadatos del Documento

- **Creado**: 2026-09-02
- **Propósito**: Documentación central para re-entrenar agentes en el proyecto de tesis
- **Fuentes**: AGENTS.md, PIPELINE_CHECKLIST.md, docs/*.md, agentes, scripts, src/etl/transformers/
- **Actualizado**: 2026-09-09 (Sesiones 19-20: backup BD verificado + setup Linux/FASE 5 + matriz Top 10 + Tailscale + prompt exportable + bootstrap SESION_NUEVA_UBUNTU)
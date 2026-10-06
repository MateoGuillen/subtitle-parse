# Estrategias de Detección de Anomalías — Alternativas, Novedades y Variables

> Documento de investigación consolidado — mapa entre **todas** las familias de
> detección de anomalías en contratación pública (no solo Isolation Forest), cruzadas con la base real
> `dncp` (OCDS + pliegos) y con el **top de variables** que las alimentan, indicando **cuáles** usar en
> cada alternativa y **por qué**.
>
> - Fecha: 2026-08-20. Modo: build (datos verificados por consulta directa a PostgreSQL `<DB_HOST>:5433/dncp`).
> - Toda la literatura citada fue verificada en web (sesiones previas + esta investigación).

---

## 1. Inventario de datos verificado en la BD `dncp` (conteos reales)

Conteos verificados hoy (SELECT directo a la base, no estimados):

| Tabla | Filas | Clave | Dato verificable para anomalías |
|---|---|---|---|
| `licitaciones` | 29,811 | `nro_licitacion` | `metodo_detalle` (**el método real**: Menor cuantía 10,384; Contratación Directa 9,000; Lic. Pública 5,677; Concurso 4,547; Subasta 195). ⚠️ `metodo_contratacion` casi todo `open` → **usar `metodo_detalle`**. Ventanas: `duracion_consultas_dias`, `duracion_oferta_dias`, `duracion_contrato_dias`. `cantidad_oferentes` 93%. ⚠️ `costo_pliego`, `presupuesto_monto`, `garantia_porcentaje`, `cantidad_items` vacíos/bug. |
| `oferentes` | 78,695 | (id) `nro_licitacion`, `proveedor_id` | **5,544 proveedores distintos ofertando**, 1–65 ofertas por licitación. ⚠️ **NO tiene montos de oferta** → solo sirve para conteo/red/colusión, NO para screens basados en montos por bidder. |
| `proveedores_notificados` | 76,879 | `nro_licitacion`, `proveedor_id` | Razón **notificados vs ofertaron** (listas de invitación en directas). 5 invitados en la mayoría (11,105 licit.). |
| `adjudicaciones` | 39,463 | `award_id`, `nro_licitacion` | `monto_adjudicado` (39,456 no-nulos) — **solo el monto del ganador**. `fecha_adjudicacion`. 1 adjudicatario en 22,807 licitaciones, hasta 80. |
| `contratos` | 39,691 | `contrato_id`, `nro_licitacion` | `monto_contrato`, `fecha_firma/inicio/fin`, `duracion_dias`, `estado`, `cantidad_pagos`, `monto_total_pagado`. ⚠️ `cantidad_enmiendas` bug (todo 0) → usar `enmiendas_contrato`. |
| `pagos_contrato` | 276,868 | `pago_id`, `contrato_id` | `monto_pagado` (265,725 no-nulos), `monto_factura`, `fecha_pago`/`fecha_solicitud`/`fecha_factura` (para demoras), `retencion_*`, `multa`. 15,513 pagos en 2026. |
| `enmiendas_contrato` | 7,143 | `enmienda_id`, `nro_licitacion` | `monto_enmienda`, `fecha`. Cubren **4,414 licitaciones distintas**. |
| `protestas` | 1,253 | `protesta_id`, `nro_licitacion` | **967 licitaciones con protesta**. `resultado`, `motivo`, `recurrente`. = ground truth externo (proxy). |
| `consultas_llamado` | 123,283 | `consulta_id`, `nro_licitacion` | **119,740 con respuesta**, `fecha`/`fecha_respuesta` (→ `tiempo_respuesta_consultas`). Cubren 12,353 licitaciones. `respuesta` = texto para NLP. |
| `items_licitacion` | 1,878,237 | `item_id`, `nro_licitacion` | `monto` (100% no-nulo = **1.87M montos PYG**), `descripcion`, `unspsc_id` (5,085 distintos). Para Benford, complejidad, heterogeneidad. |
| `criterios_llamado` | 45,779 | `criterio_id`, `nro_licitacion` | `requisito_valor`, `grupo_descripcion` — umbrales de specs (validación cruzada de Bloque B). |
| `proveedores` | 13,256 | `proveedor_id` | `tipo_entidad`, `tipo_actividad`, `direccion/email/telefono` (conexiones), `region/localidad`. ⚠️ `tamanio/escala` vacíos. |
| `pliegos_secciones` | 2,775,485 | `document_id`, `nro_licitacion` | `title_normalized`, `content_text`, `word_count`, `page`, `line_start/end`, `cluster_id`. |
| `document_features` | 31,321 × 571 | `nro_licitacion` | Estructural del pliego: `gini_content_length`, `title_entropy`, `sum_word_count`, `has_*/len_*/word_*/count_*/span_*/page_range_*` (~60 títulos). |
| `document_features_v2` | 31,321 × 108 | `nro_licitacion` | Semántico del pliego (regex + LLM): `pct_*`, `dias_*`, `ratio_*`, `anos_*`, `num_*`, `binary_*`. 100% poblada. |
| `document_economic_features` | 29,811 × 46 | `nro_licitacion` | OCDS + colusión + ejecución agregados por licitación. |

**Unidad de análisis maestra**: licitación (`nro_licitacion`, 29,811). Las tablas 1:N (oferentes, pagos,
consultas, items, enmiendas, protestas) requieren agregación a nivel licitación.

---

## 2. TOP GENERAL DE VARIABLES (consolidado OCDS + pliego)

Top unificado por **importancia para detectar anomalías**, priorizando: respaldo académico, varianza
real en los datos, no-redundancia. Cada variable indica su bloque y tabla de origen.

### 🥇 Nivel 1 — Núcleo (competencias y precio; el más validado por la literatura)

| # | Variable | Bloque | Tabla/col | Por qué (justificación) |
|---|---|---|---|---|
| 1 | `es_unico_oferente` + `n_oferentes_distintos` | A | `document_economic_features` | El red flag #1 de Fazekas/OCP/BM: oferta única = menor competencia. 6 fuentes académicas. Single-bidder crece 280→3,470 (2021→2025, verificable). |
| 2 | `precio_vs_estimado` + `overbudget_ratio` | C | `monto_adjudicado`/`monto_estimado` | Desviación de precio vs referencial: sobreprecio y "bajas anormales". 3 fuentes (Fazekas 2016, OCP 2016, BM). |
| 3 | `metodo_detalle` (one-hot) | A | `licitaciones` | Modalidad directa/restricción = mayor riesgo. ⚠️ usar `metodo_detalle` (Contratación Directa 9,000), no `metodo_contratacion` (todo "open"). |
| 4 | `duracion_oferta_dias` / `bidding_urgency` | A | `licitaciones` | Plazos cortos reducen competencia (EFC/OECD). ⚠️ Decarolis-matiz: urgencia correlaciona negativo en algunos casos → validar. |

### 🥈 Nivel 2 — Colusión y recurrencia (correlato de cartel)

| # | Variable | Bloque | Tabla/col | Por qué |
|---|---|---|---|---|
| 5 | `n_enmiendas` + `enmienda_ratio` | C | `enmiendas_contrato` (no `contratos`) | Adendas = renegociación post-firma, sobreprecio. 3 fuentes. Verificado: 7,143 enmiendas / 4,414 licitaciones. |
| 6 | `winner_total_contracts` + `winner_category_frequency` + `is_repeat_winner` | A | `document_economic_features` | Peso/concentración del ganador en su mercado = favoritismo. 2 fuentes. |
| 7 | `n_consultas_llamado` + `tiempo_respuesta_consultas` | A | `consultas_llamado` | Interés y transparencia del proceso. Verificado: 12,353 licitaciones con consultas. |
| 8 | `pago_vs_contrato_ratio` + `n_pagos` | C | `contratos` | Ejecución real vs firmada: contrato estancado = anomalía de ejecución. |

### 🥉 Nivel 3 — Pliego (Bloco B: specs y condiciones; el aporte de tu pipeline v2)

| # | Variable | Bloque | Tabla/col | Por qué |
|---|---|---|---|---|
| 9 | `anos_experiencia_minima` + `anos_experiencia_minima_tecnica` | B | `document_features_v2` | Rigged/tailored specs: experiencia excesiva excluye competidores. 13-20 valores. BM (2011) "rigged specifications". |
| 10 | `ratio_liquidez_minimo` + `ratio_endeudamiento_maximo` | B | `document_features_v2` | Umbrales financieros a medida (specs): liquidez muy alta = barrera. 8/3 valores. |
| 11 | `pct_interes_mora_diario` + `pct_multa_diaria` | B | `document_features_v2` | Condiciones punitivas que solo tolera el "indicado". 35 valores. |
| 12 | `dias_pago_maximo` + `pct_garantia_cumplimiento` + `dias_validez_garantia` | B | `document_features_v2` | Condiciones de pago/garantías asimétricas. 15/5/25 valores. |
| 13 | `num_categorias_documentos` + `num_documentos_pago` | B | `document_features_v2` | Burocracia/exigencias documentales = barrera de entrada. |
| 14 | `pct_diferencia_precio_bajo` + `pct_diferencia_precio_alto` | B | `document_features_v2` | Umbrales de "precios anormales" — como se define la desviación permitida. |
| 15 | `dias_reposicion` + `dias_solicitud_anticipo` + `dias_pago_anticipo` | B | `document_features_v2` | Plazos de reposición/anticipo — presión o ventaja. |

### Nivel 4 — Estructural del pliego (document_features) y derivables

| # | Variable | Bloque | Tabla/col | Por qué |
|---|---|---|---|---|
| 16 | `gini_content_length` + `title_entropy` + `sum_word_count` | B | `document_features` | Pliego truncado/incompleto vs completo: `sum_word_count` bajo en directas. |
| 17 | `demora_media_pago_dias` (solicitud→pago) | C | `pagos_contrato` | Retrasos de pago = presión financiera/favoritismo (Wittberg 2023). **Verificado: pares fecha disponibles en 276K pagos.** |
| 18 | `n_protestas` / `has_protesta` | A | `protestas` | Competencia disputada. Y clave de **validación externa**. |
| 19 | `n_unspsc_distintos` + `n_items` reales | A | `items_licitacion` | Heterogeneidad/especificidad de lo contratado (evita bug `cantidad_items`). |
| 20 | `razon_notificados_a_oferentes` | A | `proveedores_notificados` vs `oferentes` | De N invitados solo ofertó 1 repetidamente = trato directo. |

**⚠️ Descartadas por datos**: `bidder_diversity`, `oferentes_por_item` (bug `cantidad_items`=0),
`costo_pliego`, `presupuesto_monto`, `garantia_porcentaje` (vacíos), `pct_anticipo`, `num_componentes_formula`
(constantes), `antiguedad_empresa` (no disponible), `convocantes.region` (vacío).

---

## 3. MATRIZ: variable → alternativas que la usan

| Variable (top) | A. ISF/ECOD | B. CRI/Red flags | C. Screens | D. Redes | E. Superv. protestas | F. NLP pliegos |
|---|---|---|---|---|---|---|
| `es_unico_oferente`/`n_oferentes_distintos` | ✅ | ✅ | ✅ (base) | ✅ (grado) | ✅ | target (F1) |
| `precio_vs_estimado`/`overbudget_ratio` | ✅ | ✅ | ✅ | — | ✅ | — |
| `metodo_detalle` | ✅ | ✅ | ✅ | — | ✅ | — |
| `duracion_oferta_dias`/`bidding_urgency` | ✅ | ✅ | ✅ | — | ✅ | — |
| `n_enmiendas`/`enmienda_ratio` | ✅ | ✅ | — | — | ✅ | — |
| `winner_total_contracts`/`winner_category_frequency`/`is_repeat_winner` | ✅ | ✅ | — | ✅ | ✅ | — |
| `n_consultas`/`tiempo_respuesta` | ✅ | — | — | — | ✅ | ✅ (texto) |
| `pago_vs_contrato_ratio`/`n_pagos` | ✅ | ✅ | — | — | ✅ | — |
| `anos_experiencia_minima(_tecnica)` | ✅ | ✅ | — | — | ✅ | — |
| `ratio_liquidez/endeudamiento` | ✅ | ✅ | — | — | ✅ | — |
| `pct_interes_mora_diario`/`pct_multa_diaria` | ✅ | ✅ | — | — | ✅ | — |
| `dias_pago_maximo`/`pct_garantia`/`dias_validez` | ✅ | ✅ | — | — | ✅ | — |
| `num_categorias_documentos`/`num_documentos_pago` | ✅ | ✅ | — | — | ✅ | — |
| `pct_diferencia_precio_*` | ✅ | ✅ | ✅ | — | ✅ | — |
| `gini_content_length`/`title_entropy`/`sum_word_count` | ✅ | ✅ | — | — | ✅ | ✅ |
| `demora_media_pago_dias` | ✅ | ✅ | — | — | ✅ | — |
| `n_protestas` | ✅ (validación) | ✅ | — | — | target (proxy) | — |
| `n_unspsc_distintos`/`n_items` | ✅ | — | — | — | ✅ | — |
| `razon_notificados_a_oferentes` | ✅ | ✅ | — | — | ✅ | — |
| Columnas de red (co-bidding, sin montos) | — | — | parcial | ✅ | ✅ | — |
| `items_licitacion.monto` (1.87M) | — | ✅ (Benford) | parcial | — | — | — |
| `pagos_contrato.monto_pagado` (265K) | — | ✅ (Benford) | — | — | — | — |
| `consultas_llamado.respuesta` (texto) | — | — | — | — | — | ✅ |
| `pliegos_secciones.content_text` | — | — | — | — | — | ✅ |

---

## 4. Detalle por alternativa: qué usa, por qué, y viabilidad en tu BD

### A. Anomalías tabulares no supervisadas (ISF, LOF, ECOD, ensemble) — núcleo sin etiquetas

**Qué es**: detectores que marcan la licitación "rara" por combinación atípica de variables, sin
necesitar ground truth. ISF es el estándar (validado en Paraguay por Kehler et al. 2019, >90% vs
protestas); ECOD/COPOD son los modernos que ganan benchmarks tabulares (AAAI-25).

**Variables que usa** (todas las del TOP Nivel 1–4, perfil por licitación):
- Bloque A: `es_unico_oferente`, `n_oferentes_distintos`, `metodo_detalle` (one-hot),
  `duracion_oferta_dias`/`bidding_urgency`, `n_consultas_llamado`, `tiempo_respuesta_consultas`,
  `razon_notificados_a_oferentes`, `n_protestas`.
- Bloque C: `precio_vs_estimado`, `overbudget_ratio`, `n_enmiendas`, `enmienda_ratio`,
  `winner_total_contracts`, `winner_category_frequency`, `is_repeat_winner`, `pago_vs_contrato_ratio`,
  `n_pagos`, `demora_media_pago_dias`.
- Bloque B: `anos_experiencia_minima(_tecnica)`, `ratio_liquidez_minimo`, `ratio_endeudamiento_maximo`,
  `pct_interes_mora_diario`, `pct_multa_diaria`, `dias_pago_maximo`, `pct_garantia_cumplimiento`,
  `dias_validez_garantia`, `num_categorias_documentos`, `num_documentos_pago`, `pct_diferencia_precio_*`,
  `gini_content_length`, `title_entropy`, `sum_word_count`.

**Por qué**: cluster de competencia rota + specs a medida + ejecución desviada es exactamente el perfil
que estos detectores aíslan con subespacios de baja densidad. ISF con `n_estimators=100`,
`max_samples=256`, umbral percentil 99. Preproceso: log-transform montos, one-hot `metodo_detalle`,
imputación (B es 100%). ECOD/COPOD mejoran a ISF en AUC (92% benchmark DTE 2025) — añaden robustez.

**Viabilidad** (verificada): ✅ alta. Todas las variables existen; agregaciones simples.
Protestas permiten validar (967 licitaciones).

**Estado**: ISF en 📋 pausa; ECOD/COPOD ✖ nuevo.

---

### B. Red flags / índice compuesto determinista (CRI, Red flags OCDS, Benford) — sin ML

**B1 — CRI (Corruption Risk Index, Fazekas 2016 / FMI 2022)**
- **Qué**: promedio ponderado de red flags normalizados por licitación. Paraguay fue **país piloto del
  FMI** (Abdou et al. 2022) → replicarlo es tesis por sí mismo.
- **Variables** (todas en `document_economic_features`/`licitaciones`/`v2`):
  oferta única (`es_unico_oferente`), compartida del ganador (`winner_category_frequency`,
  `winner_total_contracts`), plazo corto (`duracion_oferta_dias`), modalidad (`metodo_detalle`),
  especificaciones a medida (Bloque B: `anos_experiencia_*`, `ratio_liquidez_minimo`,
  `num_categorias_documentos`), no-publicación de documentos, enmiendas (`enmienda_ratio`).
- **Por qué**: cada red flag mide un **canal de corrupción distinto** (competencia, favoritismo,
  restricción, renegociación); combinarlos en índice da un score 0–1 interpretable por institución.

**B3 — Benford / análisis digital (montos)**
- **Qué**: distribución del primer/segundo dígito de los montos; desviación → manipulación.
- **Datos verificados**: `items_licitacion.monto` (1,878,237 PYG, 100% no-nulos) ✓;
  `pagos_contrato.monto_pagado` (265,725) ✓; `adjudicaciones.monto_adjudicado` (39,456) ✓.
- **Tests**: first-digit, second-digit, first-two; z-stat, MAD, χ², KS, Kuiper.
- **Por qué**: Cero ML, riguroso, aplicado en Macedonia/Portugal/Brasil COVID. Detecta el redondeo y
  digitos "diseñados" en montos. **Novedad**: nunca aplicado a OCDS PYG.
- **Esfuerzo**: muy bajo (solo agregación + `benfordslaw`/script propio).

**B4 — TAB (Test of Abnormal Bids)** sobre ítems: mismo Benford aplicado a precios unitarios de ítems.

**Viabilidad**: ✅ alta. CRI requiere solo columnas ya derivadas; Benford solo `monto` (existe).

---

### C. Screens econométricos de colusión (Porter–Zona, Bajari–Ye, missing bids) — ⚠️ PARCIAL

**Qué**: tests estadísticos sobre la **distribución completa de las ofertas** para detectar cover bids,
independencia condicional y "masa faltante" de ofertas cercanas a la ganadora.

**Variables/requisitos**: montos de **cada oferta por bidder** (`oferentes` + monto), no solo del ganador.

**⚠️ Viabilidad VERIFICADA — LIMITADA en tu BD**:
- `oferentes` (78,695) **NO tiene montos** → no puedes reconstruir la distribución de ofertas.
- Solo tienes `adjudicaciones.monto_adjudicado` (el ganador, 39,456) y `items_licitacion.monto`
  (referencial).
- **Consecuencia**: Porter–Zona y Bajari–Ye (necesitan todas las ofertas firmadas) **NO se pueden
  implementar tal cual**. Solo puedes hacer *versiones degradadas*: análisis de la **distancia relativa
  de la oferta ganadora al referencial** por método/categoría (proxy de "bid shooting").
- **Excepción**: Kawai–Nakabayashi (missing bids) y screens de *entrada/recurrencia* usan conteo de
  oferentes y rotación — **sí** computables con `oferentes` + `winner_category_frequency`.

**Por qué las mencionamos igual**: son el marco académico de las autoridades (JPE, Econometrica); las
citas enriquecen el marco teórico incluso si no las implementas completas.

**Esfuerzo de la versión degradada**: bajo–medio. **Estado**: ✖, documentar como limitación.

---

### D. Redes / grafos — lo MÁS novedoso para Paraguay

**D1 — Co-bidding network (Wachs & Kertész 2019)**
- **Qué**: grafo bipartita oferente↔licitación → proyección por pares que ofertan juntos; métricas de
  **cohesión** (beneficio de coludir) y **exclusividad** (falta de competencia externa).
- **Datos verificados**: `oferentes` con `nro_licitacion` + `proveedor_id` = **78,695 ofertas, 5,544
  bidders, 1–65 por licitación** ✓. Idóneo (no necesita montos).
- **Variables/derivadas por licitación**: grado (nº de co-bidders), densidad de vecindario, Jaccard
  promedio, cluster de colusión (proveedores que siempre se restan/ganan juntos).
- **Por qué**: **Nadie ha publicado red de co-bidding para Paraguay** → novedad real y pura del tamaño
  exacto que ya tienes. Puedes cruzarlo con `protestas` (¿los que protestan son los excluidos de la
  red?).
- **Esfuerzo**: bajo–medio (networkx, 78K filas). **Estado**: ✖ nuevo, muy recomendado.

**D2 — Anomalía en red de pagos/valor** (estilo TBFY/Your Work): proveedor pequeño gana contrato grande
→ `winner_tipo_entidad` × `monto_adjudicado`/`monto_contrato`; ratio monto/empleados (sin tamaño) →
usar `tipo_entidad` (persona física vs jurídica) × montos.

**D3 — GNN / R-GCN / GraphSAGE**: estado del arte (Gomes 2024, Imhof 2025, 60–95% accuracy) **pero
requiere etiquetas de colusión que NO tienes** → descartar para ejecución; citar como trabajo futuro.

**Por qué en tesis**: la capa de red es la **una** que complementa competitivamente a ISF (estructura,
no perfil) y es un diferencial frente al paper Kehler.

---

### E. ML supervisado con protestas como proxy (RF/GBM, VigIA, LAIRM)

**Qué**: modelos que aprenden a predecir "mal resultado" usando un proxy de verdad. Tus 1,253 protestas
(967 licitaciones) son el proxy dominante (Kehler 2019): la protesta de un perdedor indica competencia
disputada/irregularidad percibida.

- **Targets posibles** (definir con cuidado): `has_protesta` (967 positivos), desviación de precio
  extrema, `terminated`/incumplimiento, alto `enmienda_ratio`.
- **Variables**: todos los bloques A/B/C (mismas que ISF) + validación cruzada con CRI.
- **Algoritmos**: Random Forest/GBM con screens (Fazekas GTI-WP 2023: 77–91% accuracy con 5 indicadores);
  **LAIRM 2026** (F1 15% > World Bank Red Flags) → adaptable a PY; VigIA (Colombia, IRICP índice).
- **Por qué**: transforma la detección no supervisada en un modelo con métricas de precision/recall
  validadas contra un proxy; da ranking de importancia (SHAP) comparable contra el top teórico.
- ⚠️ **Riesgo**: clases muy desbalanceadas (1,253/29,811 ≈ 4%); protestas = señal de competencia, no
  prueba de corrupción → tratar como proxy imperfecto (fuera del alcance de "ground truth").
- **Esfuerzo**: medio. **Estado**: ✖ nuevo, recomendado como etapa de validación/complemento.

---

### F. NLP sobre texto de pliegos (tu pipeline v2 como preproceso) — estado del arte

**F1 — Katona & Fazekas (2024)**: el **texto de los llamados predice oferta única** mejor que
estructura sola (77→82%, Hungría, 119K). **Tu `document_features_v2` es exactamente el preproceso**:
extrae los 13–19 campos hybrid/regex del pliego.
- **Arquitectura**: TF-IDF n-grams sobre `pliegos_secciones.content_text` (o features v2) → RF/XGB/LR
  → target `es_unico_oferente`.
- **Por qué**: vincula texto restringente (specs) con oferta única real; es la justificación académica
  de todo el Bloque B. **Novedad**: nunca hecho para Paraguay.
- **Esfuerzo**: medio (tu v2 ya está extraído). **Estado**: 🟡 pipeline v2 listo; falta modelo.

**F2 — NLP sobre las 123,283 consultas** (`respuesta`): consultas acusatorias/ambiguas → señales.
Ecuador SOCE 2025 (embeddings + GMM + RF).

**F3 — Transformers supervisados** (BERT/RoBERTa/DeBERTa, Peppes 2026; Bi-LSTM, Lima 2020):
**requiere etiquetas** → descartar para ejecución; citar como estado del arte.

---

## 5. Análisis de novedad (lo que nadie ha hecho en Paraguay)

1. **ISF/ECOD sobre mezcla A+B+C + validación con protestas** — recupera el precedente Kehler pero
   **añade texto de pliegos (Bloque B)**, que Kehler no usó. Es el aporte de menor riesgo.
2. **Red de co-bidding (D1)** sobre 5,544 bidders — **nunca publicado para Paraguay**; esfuerzo bajo,
   aporte alto.
3. **Benford (B3)** sobre 1.87M montos PYG de ítems y 265K pagos — trivial, riguroso, con precedente
   Brasil, **nuevo para PY**.
4. **CRI replicado para Paraguay (B1)** — el FMI lo aplicó a nivel país piloto; tú lo harías a nivel
   licitación con más variables (Bloque B/C) y validado con protestas.
5. **Ensemble (ISF + ECOD + LOF) + DIFFI/SHAP** — interpretabilidad que compara ranking empírico vs el
   top teórico de la Sección 2 (método de Dos Santos 2025: los ensembles rinden mejor).

**Combinaciones descartadas por falta de datos/etiquetas**: screens de distribución completa (C1–C2,
sin montos por bidder), GNN (D3) y transformers supervisados (F3) requieren etiquetas de colusión.

---

## 6. Hoja de ruta recomendada (incremental, de menor a mayor novedad)

| Etapa | Alternativa | Variables | Esfuerzo | Validación |
|---|---|---|---|---|
| 1 | **Benford (B3)** | `items_licitacion.monto`, `pagos_contrato.monto_pagado`, `adjudicaciones.monto_adjudicado` | muy bajo | MAD/χ² benchmark |
| 2 | **CRI (B1)** | Bloque A + C (compuesto ponderado) | bajo | distribución por `metodo_detalle`, protestas |
| 3 | **ISF/ECOD (A)** | Bloques A+B+C completos | bajo–medio | percentil, protestas, DIFFI/SHAP |
| 4 | **Ensemble + interpretabilidad (A4)** | A+B+C | medio | rank-mean de detectores × protestas |
| 5 | **Co-bidding network (D1)** | `oferentes` (78K) | bajo–medio | cruce con protestas; clusters |
| 6 | **Supervisado con protestas (E)** | A+B+C + target proxy | medio | precision/recall vs 1,253 protestas |
| 7 | **Katona–Paraguay (F1)** | `document_features_v2` + textos | medio | AUC predicción oferta única |

---

*Fuentes clave citadas: Liu et al. (2008, ICDM); Kehler, Paciello & Pane (2019, IEEE/CEUR — ISF OCDS
Paraguay); Chandola et al. (2009, CSUR); Fazekas, Tóth & King (2016, EJCPR); Abdou et al. (2022, IMF
WP/22/94 — Paraguay piloto FMI); OCP & Development Gateway (2016, red flags OCDS); World Bank (2011,
rigged specifications); Katona & Fazekas (2024, GTI-WP — texto→oferta única); Wachs & Kertész (2019, red
co-bidding); Kawai & Nakabayashi (2022, JPE); Chassang, Kawai & Nakabayashi (2022, Econometrica);
Huber, Imhof & Ishii (2022/2023, JRSS-A); Porter & Zona (1993, JPE); Gallego et al. (2021, Colombia 2M);
Dos Santos et al. (2025, EPJ Data Science — SLM 93 estudios); LAIRM (2026, SciELO); Peppes et al.
(2026, Information); Lima et al. (2020, EMNLP); majilana (2025, POTRAZ Zimbabue). Verificación del 100%
de fuentes en sesiones previas.*
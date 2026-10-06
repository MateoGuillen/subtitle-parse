# Selección de Variables con Sustento Académico — Documento de Investigación

> Documento maestro de la tesis: lista completa de variables (origen, método, fuente académica,
> referencia Paraguay) para detección de anomalías en contrataciones públicas (DNCP, Paraguay).
>
> - Fecha: 2026-08-27. Proyecto: `subtitle-parse`.
> - Variables verificadas contra la BD `dncp` (consulta directa PostgreSQL, contenedor arriba).
> - Enfoque metodológico: **literatura primero (top-down)**, clustering como origen bottom-up de los
>   81 campos existentes, validado a posteriori contra la literatura.

---

## 0. Metodología — de dónde sale cada variable

Existen **dos orígenes** para las variables de esta tesis. **La literatura es siempre la fuente
primaria** de justificación; el clustering es el *mecanismo* que descubrió 81 campos paraguayos que
luego se mapean ex-post a la literatura.

| Enfoque | Dirección | Qué hace | Punto de partida | Validador |
|---|---|---|---|---|
| **Literatura (top-down)** | Red flag → variable | Traduce un red flag documentado a una variable medible tipada | Artículos/guías académicas (World Bank, IMF, OCP, IACRC, Fazekas, Katona) | BD: ¿existe la sección? ¿tiene varianza numérica real? |
| **Clustering (bottom-up)** | Texto → campo | El LLM diseña campos que capturan diferencias entre variantes de una sección | `pliegos_secciones` (2.78M secciones) → embeddings → UMAP → K-Means → LLM | Lit. ex-post: ¿qué red flag mide cada campo? |

**Regla de decisión**: ninguna variable entra al modelo sin sustento. Las de literatura llevan la
fuente directamente. Las de clustering se mapean a posteriori al red flag que miden; si no hay red flag
que las justifique, se marcan de baja prioridad.

---

## 1. Variables NUEVAS derivadas de la literatura (top-down) — 7

Estas variables **no existen** en `document_features_v2`. Nacen de red flags académicos y se
confirmó su extraibilidad en la BD (sección + varianza numérica real verificada por regex sobre
muestras de texto). Requieren **gold standard manual nuevo** (100 muestras/campo) y **benchmark LLM**.

| # | Variable | Tipo | Sección pliego (título) | Fuente académica | Referencia Paraguay (columna aparte) | Varianza verificada |
|---|---|---|---|---|---|---|
| N1 | `dias_validez_oferta` | int | `periodo de validez de las ofertas` (32,182) | **World Bank (2011)** rigged specs: validez de oferta anormalmente corta/larga restringe o señala favoritismo | Ley 7021/2022 (plazo mínimo de validez); Reglamento DNCP | 100%: 90·60·30·120·45·50·59 |
| N2 | `dias_validez_garantia_mantenimiento_oferta` | int | `periodo de validez de la garantia de mantenimiento de oferta` (32,182) | **IACRC** bid security: período de garantía de oferta fuera de rango | Reglamento DNCP (garantía de mantenimiento de oferta) | 100%: 120·90·60·150·50·61·31 |
| N3 | `dias_plazo_entrega_bien` | int | `plan de entrega de los bienes` (22,547) | **World Bank (2011)**: plazo de entrega restrictivo | Ley 7021/2022 (plazo de entrega según objeto) | 19%: 15·5·30·10·3·20·45 |
| N4 | `requiere_visita_tecnica` | binary | `visita al sitio de ejecucion del contrato` (26,909) | **IACRC** red flags: visita obligatoria excluye oferentes foráneos | Reglamento DNCP | Texto "visita ... No Aplica" vs indicaciones → binary |
| N5 | `criterio_adjudicacion` | categorical | `criterios de adjudicacion` (32,182) | **OCP & Development Gateway (2016)**: criterio de adjudicación ambiguo o sesgado | Ley 7021/2022 (menor precio vs más conveniente) | Complementa binarios existentes |
| N6 | `multa_jornales_incumplimiento` | int | `multas por incumplimiento` (664) | **Fazekas, Tóth & King (2016)**: penalidades desproporcionadas (no %) | Ley 7021/2022 (límite de multas) | "2 (dos) jornales diarios" → jornales |
| N7 | `tiene_requisito_eliminatorio` | binary | `requisitos de calificacion` (32,182) | **World Bank (2011)**: especificaciones/requisitos eliminatorios a medida | Ley 7021/2022 (calificación no discriminatoria) | Texto "serán desechadas las ofertas que..." |

> Las 7 pasan al **gold standard manual** (`data/processed/alolab/gold_standard_pliego_nuevo.csv`,
> 100 muestras/campo) y al **benchmark LLM** (Llama 3.1 8B-AWQ y Mistral 7B-AWQ, guided decoding).

---

## 2. Variables derivadas del CLUSTERING (bottom-up) — 81 existentes

Origen: Pipeline 1 (Section Clustering + LLM schema design). El LLM vio 2 muestras por cluster de cada
sección y diseñó campos que miden las diferencias normativas entre variantes de redacción. No nacieron
de la literatura, pero **cada uno se mapea ex-post al red flag que mide**.

### 2.1 Agrupación por sección (25 títulos) con mapeo ex-post

| Sección (título) | Campos que creó el clustering | Red flag que miden (ex-post, literatura) | Prioridad anomalía |
|---|---|---|---|
| `analisis_de_precios_ofertados` | `pct_diferencia_precio_bajo`, `pct_diferencia_precio_alto`, `binary_solicita_explicacion_precios`, `binary_permite_declarar_inaceptable`, `binary_tiene_multiplicidad_criterios` | **World Bank (2011)** precios anormales; **Fazekas (2016)** baja anormal | ALTA |
| `porcentaje_de_multas` | `pct_multa_diaria`, `binary_permite_rescision_por_multas` | **Fazekas (2016)** penalidades | ALTA |
| `fuerza_mayor` | `dias_notificacion_fuerza_mayor`, `binary_incluye_ejemplos_tipicos`, `binary_obliga_continuar_cumplimiento` | Cláusula estándar (bajo) | MEDIA |
| `porcentaje_de_garantia_de_fiel_cumplimiento_de_contrato` | `pct_garantia_cumplimiento`, `dias_presentacion_garantia` | **World Bank (2011)** garantía excesiva | ALTA |
| `aclaracion_de_las_ofertas` | `binary_comite_puede_solicitar_aclaracion`, `binary_prohibe_modificacion_sustancia`, `binary_solicitud_respuesta_por_escrito` | Transparencia (OCP) | MEDIA |
| `experiencia_requerida` | `anos_experiencia_minima`, `num_referencias_minimo`, `pct_monto_referencia_minimo`, `binary_tiene_requisitos_por_lote`, `binary_requiere_experiencia_especifica_marca` | **World Bank (2011)** rigged specs: experiencia a medida | ALTA |
| `plazo_de_reposicion_de_bienes` | `dias_reposicion`, `binary_dias_habiles`, `binary_plazo_segun_autoridad` | **World Bank (2011)** plazos | MEDIA |
| `documentacion_requerida_para_la_firma_del_contrato` | `num_categorias_documentos`, `binary_requiere_certificado_tributario`, `binary_requiere_certificado_ips`, `binary_incluye_condiciones_contractuales` | **OCP (2016)** burocracia/barreras de entrada | MEDIA |
| `causales_de_terminacion_del_contrato` | 4 × `binary_incluye_terminacion_*` | Cláusula estándar (bajo) | BAJA |
| `confidencialidad_de_la_informacion` | `binary_tiene_excepciones_confidencialidad`, `binary_menciona_plazo_confidencialidad` | Transparencia (OCP) | MEDIA |
| `impuestos_y_derechos` | `binary_responsabilidad_integral_proveedor`, `binary_menciona_iva`, `binary_incluye_exoneraciones` | Cláusula estándar | BAJA |
| `suministros_y_especificaciones_tecnicas` | `binary_menciona_normas_oficiales`, `binary_permite_cambios_codigos`, `binary_proveedor_puede_rehusar_responsabilidad` | **World Bank (2011)** specs técnicas | MEDIA |
| `derechos_intelectuales` | `binary_cesion_derechos_contratante`, `binary_prohibe_reproduccion_no_autorizada` | Cláusula estándar | BAJA |
| `periodo_de_validez_de_la_garantia_de_los_bienes` | `dias_validez_garantia`, `binary_cubre_defectos_fabricacion`, `binary_tiene_cobertura_especifica` | **OCP (2016)** garantía | MEDIA |
| `capacidad_tecnica` | `anos_experiencia_minima_tecnica`, `num_referencias_tecnicas_minimo`, `pct_monto_referencia_tecnica` | **World Bank (2011)** specs a medida | ALTA |
| `formas_y_condiciones_de_pago` | `dias_pago_maximo`, `dias_aceptacion_rechazo_factura`, `num_documentos_pago`, `binary_requiere_cuenta_bancaria_estatal` | **OCP (2016)** condiciones de pago | ALTA |
| `plan_de_entrega_de_los_bienes` | `binary_tiene_plan_entrega`, `binary_incluye_cronograma`, `binary_requiere_documentos_embarque` | **OCP (2016)** | MEDIA |
| `capacidad_financiera` | `binary_tiene_requisitos_financieros`, `ratio_liquidez_minimo`, `ratio_endeudamiento_maximo`, `binary_requiere_estados_auditados` | **World Bank (2011)** barreras financieras | ALTA |
| `reajuste` | `binary_tiene_formula_reajuste`, `num_componentes_formula`, `binary_referencia_ipc`, `binary_no_reajuste_si_atraso` | Fórmula (bajo; `num_componentes_formula` irresoluble) | BAJA |
| `criterios_de_adjudicacion` | `binary_criterio_menor_precio`, `binary_multiplicidad_criterios`, `binary_contrato_abierto`, `binary_reserva_disminuir_cantidad` | **Katona & Fazekas (2024)** criterios | ALTA |
| `forma_de_instrumentacion_de_garantia_de_anticipo` | `binary_requiere_garantia_anticipo`, `categorical_tipo_garantia`, `pct_anticipo` | **OCP (2016)** anticipo | MEDIA |
| `requisitos_de_calificacion` | `num_pasos_verificacion`, `binary_verifica_sinarh`, `binary_requiere_declaracion_conflictos`, `binary_requiere_registro_proveedores` | **World Bank (2011)** barreras de entrada | MEDIA |
| `solicitud_de_pago_de_anticipo` | `binary_tiene_condiciones_anticipo`, `dias_solicitud_anticipo`, `dias_pago_anticipo`, `binary_anticipo_solo_mipymes` | **OCP (2016)** anticipo | MEDIA |
| `tasa_de_interes_por_mora` | `pct_interes_mora_diario`, `binary_mora_desde_dia_siguiente` | **Fazekas (2016)** intereses punitivos | ALTA |
| `criterios_de_desempate_de_ofertas` | `binary_menciona_criterios_dncp`, `binary_incluye_criterios_especificos_Adicionales` | **OCP (2016)** desempate | MEDIA |

**Resumen**: 81 campos = 69 regex + 12 hybrid. Prioridad ALTA (bloques A/B de `estrategias-anomaly-deteccion.md`):
`experiencia_requerida`, `capacidad_tecnica`, `capacidad_financiera`, `formas_y_condiciones_de_pago`,
`criterios_de_adjudicacion`, `analisis_de_precios_ofertados`, `porcentaje_de_multas`, `tasa_de_interes_por_mora`,
`porcentaje_de_garantia_de_fiel_cumplimiento_de_contrato`.

### 2.2 Los 12 hybrid (LLM fallback) con sustento académico directo

| Campo | Método | Fuente académica | Referencia Paraguay |
|---|---|---|---|
| `anos_experiencia_minima_tecnica` | hybrid | **World Bank (2011)** rigged specs | Ley 7021/2022 (requisitos de calificación proporcionados) |
| `dias_pago_maximo` | hybrid | **OCP (2016)** condiciones de pago | Ley 7021/2022 (plazo de pago) |
| `ratio_liquidez_minimo` | hybrid | **World Bank (2011)** barrera financiera | Reglamento DNCP (liquidez) |
| `ratio_endeudamiento_maximo` | hybrid | **World Bank (2011)** | Reglamento DNCP |
| `dias_validez_garantia` | hybrid | **OCP (2016)** garantía | Reglamento DNCP |
| `dias_solicitud_anticipo` | hybrid | **OCP (2016)** anticipo | Ley 7021/2022 (anticipo) |
| `dias_pago_anticipo` | hybrid | **OCP (2016)** | Ley 7021/2022 |
| `dias_notificacion_fuerza_mayor` | hybrid | Cláusula estándar (MEDIA) | — |
| `dias_aceptacion_rechazo_factura` | hybrid | **Wittberg (2023)** demoras de pago | Ley 7021/2022 |
| `num_componentes_formula` | hybrid | Fórmula (irresoluble, <3% acc) | — |
| `num_documentos_pago` | hybrid | **Kehler (2019)** burocracia (irresoluble) | — |
| `num_pasos_verificacion` | hybrid | Burocracia (irresoluble, <2%) | — |

---

## 3. Variables OCDS-derivadas (separadas, NO van al benchmark LLM) — 4

Nacen de la literatura y se computan desde el masivo (OCDS). Van a **Pipeline 3** como feature
engineering, no requieren extracción de texto.

| Variable | Fórmula / columna OCDS | Fill rate | Fuente académica | Referencia Paraguay |
|---|---|---|---|---|
| `dias_plazo_presentacion_ofertas` | `fecha_apertura − fecha_publicacion` | 100% (29,810) | **World Bank (2011)** ventana corta | Ley 7021/2022 (plazos por modalidad) |
| `dias_plazo_consultas` | `fecha_fin_consultas − fecha_publicacion` | 100% (29,807) | **OCP (2016)** | Ley 7021/2022 |
| `dias_publicacion_llamado` | `fecha_publicacion` (directa) | 100% | **Fazekas (2016)** publicación tardía | Ley 7021/2022 (publicidad) |
| `criterio_adjudicacion_ocds` | `criterio_adjudicacion` (`priceOnly` 29,803 / `ratedCriteria` 7) | 100% | **Katona & Fazekas (2024)** peso de precio | Ley 7021/2022 |

> Además de estas, `document_economic_features` (46 cols) ya agrega el resto de variables OCDS
> (oferta única, overbudget, colusión, pagos, protestas) — ver `estrategias-anomaly-deteccion.md`.

---

## 4. Variables NO disponibles (limitación documentada) — 3

| Variable | Motivo (verificado en BD) |
|---|---|
| `pct_garantia_seriedad_oferta` | `garantia_porcentaje` = 0/29,811 en OCDS; sin texto en pliego (el % está en SICP) |
| `num_lotes` | `cantidad_lotes` = 0/29,811 en OCDS; solo 17 secciones genéricas en pliego |
| `pct_anticipo_maximo` | `pct_anticipo` ya existe en v2 y es todo-NULL; el % real está en el SICP |

---

## 5. Resumen consolidado (95 variables)

| Grupo | Origen | Cant. | Método | ¿LLM? | ¿GT? |
|---|---:|---|---|---|---|
| Nuevas de literatura (pliego) | literatura top-down | 7 | nueva (regex/hybrid) | ✅ | **nuevo manual** |
| Existentes de clustering | clustering bottom-up | 81 | 69 regex / 12 hybrid | 12 hybrid | ✅ 26 campos (5,066 juicios) |
| OCDS-derivables | literatura | 4 | feature eng. | ❌ | ❌ (no LLM) |
| No disponibles | literatura | 3 | — | ❌ | ❌ |
| **Total** | | **95** | | | |

---

## 6. Referencias

### 6.1 Internacionales (académicas / oficiales)

- **World Bank (2011)**: *Curbing Fraud, Corruption, and Collusion in the Road Sector* (rigged
  specifications, red flags). `documents.worldbank.org`
- **Open Contracting Partnership & Development Gateway (2016)**: *Red Flags for Integrity: Giving the
  Green Light to Open Data Solutions* + *Red Flags to OCDS Mapping*. `open-contracting.org`
- **IACRC (International Anti-Corruption Resource Center)**: *Guide to Combating Corruption &
  Fraud in Development Projects* — red flags de bid rigging (specs too narrow/broad, visita obligatoria).
  `guide.iacrc.org`
- **Fazekas, Tóth & King (2016)**: *An Objective Corruption Risk Index Using Public Procurement Data*
  (EJCPR) — CRI.
- **Abdou, Fisman, et al. (2022)**: IMF Working Paper 22/94 — *Identifying Cartels in the Field*
  (Paraguay piloto).
- **Katona & Fazekas (2024)**: GTI-WP — *Tender text predicts single bidding* (NLP de pliegos).
- **Kehler, Paciello & Pane (2019)**: Isolation Forest sobre OCDS Paraguay (IEEE/CEUR).
- **Wittberg (2023)**: demoras de pago y favoritismo.

### 6.2 Normativa y fuentes oficiales de Paraguay

- **Ley 7021/2022** *De la Contratación Pública* (DNCP) — plazos, calificación no discriminatoria,
  garantías, anticipo, criterios de adjudicación, publicidad.
- **Reglamento DNCP** (decretos reglamentarios de la Ley 7021/2022).
- **SICP — Sistema de Información de Contrataciones Públicas**: `contrataciones.gov.py` (datos que no
  están en el pliego: plazos exactos, % de garantías, fechas).
- **Bulk OCDS**: `masivo_{year}.zip` (fuente de las variables OCDS).

---

*Documento generado con verificación directa en BD `dncp` (agosto 2026). Las variables nuevas (Sección 1)
requieren gold standard manual y benchmark LLM; el resto alimenta `document_features_v2` y Pipeline 3.*

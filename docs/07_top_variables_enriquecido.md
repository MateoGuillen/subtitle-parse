# Top de Variables Regenerado — Sustento Enriquecido v2

> **Versión:** 2026-08-27 v2 — **regenera por completo** `05_seleccion_variables_sustento_academico.md` y `estrategias-anomaly-deteccion.md §2`.
> **Qué cambió:** incorpora los 4 PDFs nuevos descargados el 2026-08-27 (Ferwerda 2016, EPJ Data Science 2022 Decarolis, Azevedo MethodsX 2021 Benford, TI Handbook 2006) + 15 PDFs previos. Total **19 PDFs en `docs/referencias/pdf/` + 19 MD en `docs/referencias/md/` + 3 pendientes con DOI verificado**.
> **Estado BD:** verificado directo `dncp` <DB_HOST>:5433 (31,321 pliegos; 29,811 licitaciones; 78,695 ofertas; 1.87M ítems). **81 campos** clustering = 69 regex + 12 hybrid.

---

## 0. Metodología v2 — cómo se regeneró

**Mismo principio:** literatura → variable tipada → verificación BD (sección + varianza). **Novedad:** cada nivel del top ahora pondera **fuerza cuantitativa** de la nueva literatura:

| Fuente nueva | n / método | Hallazgo cuantitativo que mueve el top |
|---|---|---|
| **Ferwerda et al. 2016 EJCPR** (192 casos: 96 corrupt / 96 clean, 8 países, 5 sectores, probit ordenado) | pseudo-R² 0.40 (15 flags), 0.38 (8 flags) | **Corrupt=4.6 flags, Clean=1.8** (p<0.05). 18/28 flags correlacionan. Top r: `preferred supplier` **0.487**, `negative media` 0.528, `amount missing info` 0.431, `substantial changes after award` 0.337, `tender exceptionally large` 0.330, `shortened time span` **0.200** (p=0.005), `accelerated tender` 0.172. Bid rigging → más predecible que kickback. |
| **Decarolis & Giorgiantonio 2022 EPJ Data Science** (12,786 + 3,553 contratos roadworks IT 2009-15, ML LASSO/Ridge/RF) | Random Forest gana con *oblivious* indicators | **MEAT (scoring rule) = predictor #1 (2× siguiente)**. `urgency` y `open tender days` **negativos** (evitan lo monitoreado). `worksite verification hours share` y `document verification` entran en RF top. Caso Naples: worksite visit usada por Camorra para filtrar. `page/word count` emerge en RF flexible. |
| **Azevedo et al. 2021 MethodsX** (13.4M pagos Bolsa Familia, 5,570 municipios) | Z / χ² / MAD + summation test, R `benford.analysis` | Falla en valores individuales (χ² 6.48M >> 15.5), **pasa al agregar por municipio** (χ² 11.9 < 15.5, MAD 0.001 < 0.0022). Lección: Benford requiere **agregación geográfica** para N grande. |
| **TI Handbook 2006** (TI + EU-Asia Urbs, Indonesia/Malaysia/Pakistan) | 232 p., casos por fase | Daño 10-25% contrato. Integrity Pacts, transparencia por fase (needs→award→execution). Sostiene marco general para todos los niveles. |

**Regla de re-priorización:** sube todo lo que *acorta tiempo*, *señala proveedor preferido*, *usa MEAT*, *exige worksite/document verification*, *deja huecos de información* o *desvía montos Benford*.

---

## 1. Top General Regenerado — Niveles 1-4 (20 variables, OCDS+pliego)

### 🥇 Nivel 1 — Núcleo (competencia y precio) — INTACTO pero reforzado

| # | Variable | Bloque | Fuentes v2 | Evidencia nueva que refuerza | BD PY | Acción |
|---|---|---|---|---|---|---|
| 1 | `es_unico_oferente` / `n_oferentes_distintos` | A | Fazekas 2016 + Kocsis 2020 (**+9.6% precio** con single-bid, 2.8M contratos) + **Ferwerda** `number of offers` r=-0.178 + EPJ *competition curbs corruption* | Ferwerda: menos ofertas → más corrupción; EPJ conclusión: *enhancing private firm competition* es herramienta #1 | 29,811 licit., single-bid 280→3,470 (2021→25) | **#1 fijo** |
| 2 | `precio_vs_estimado` / `overbudget_ratio` | C | WB A1 focal point + **Ferwerda** `all bids > projected costs` r=0.160 (p=0.026) + `substantial changes` r=0.337 + Azevedo Benford | 1.87M montos `items_licitacion` para Benford first-two digits (MAD <0.012) | Verificado | **#2 fijo** |
| 3 | `metodo_detalle` (CD/LPN/Concurso/Menor cuantía) | A | OECD 2007 + NBER 28209 discretion×competition + **EPJ** `negotiated procedures` (41% discrecional, 13% sin tender, 9% sin notice) | NBER: negotiated con too few invited **+1.9pp** investigated winner; EPJ: negotiated sin tender es discrecional | `metodo_contratacion` todo `open` → usar `metodo_detalle` (CD 9,000) | **#3 sube** |
| 4 | `duracion_oferta_dias` / `bidding_urgency` | A | **Ferwerda** `shortened time span` **0.200** + `time-to-bid not conform` 0.179 + B1 weekend-abuse +19.8pp + **EPJ** `open tender days` **negativo** (evasión) + OTD violation 11% en IT | Ferwerda p=0.005 (bid rigging); EPJ explica signo negativo: lo obvio se evita si es monitoreado → mide sofisticación | 100% fill | **Evidencia más fuerte v2** |

### 🥈 Nivel 2 — Colusión y ejecución — REORDENADO

| # | Variable | Fuentes v2 | Por qué sube/baja |
|---|---|---|---|
| 5 | `n_enmiendas` / `enmienda_ratio` (`enmiendas_contrato` 7,143 / 4,414 licit.) | **Ferwerda** `substantial changes scope/costs after award` **r=0.337** (top-3) + C3 Decarolis renegociación ≥50% ahorro perdido | **Sube a #5** (antes #5, ahora con r más alto) |
| 6 | `winner_total_contracts` / `winner_category_frequency` / `is_repeat_winner` | Fazekas winner share + EPJ *criminal firm* measure (15% licit. IT con firma investigada) | Mantiene |
| 7 | `n_consultas_llamado` / `tiempo_respuesta_consultas` (12,353 licit. con consultas) | **EPJ** `page count` 25p / `word count` 9K + `ANAC info not available` 42% + `tender call absence` 33% — compleción documental como predictor RF | **Sube**: EPJ muestra que *information completeness* predice en RF flexible |
| 8 | `pago_vs_contrato_ratio` / `n_pagos` / `demora_media_pago_dias` (276K pagos) | TI execution risks + EPJ `extra cost/time` no correlacionan con criminal (medida pobre → no usar como proxy) | Mantiene, pero nota: no usar extra cost/time como target (EPJ §5) |

### 🥉 Nivel 3 — Pliego (Bloque B) — MAYOR CAMBIO v2

| # | Variable (document_features_v2) | Fuentes v2 | Movimiento |
|---|---|---|---|
| 9 | `anos_experiencia_minima` / `anos_experiencia_minima_tecnica` | **Ferwerda** `preferred supplier` **0.487 #1** + WB rigged specs | **ALTA — sube a top-3 de B** |
| 10 | `ratio_liquidez_minimo` / `ratio_endeudamiento_maximo` | idem + EPJ `firm list preference` 4% vs `other preference` 28% | ALTA |
| 11 | `pct_interes_mora_diario` / `pct_multa_diaria` | Penalizaciones asimétricas (OCP) — sin cambio | ALTA |
| 12 | `dias_pago_maximo` / `pct_garantia_cumplimiento` / `dias_validez_garantia` | Garantías (OCP) — sin cambio | ALTA |
| 13 | **`num_categorias_documentos` / `num_documentos_pago`** | **Ferwerda** `amount missing info` **r=0.431** + `awarding authority did not fill TED/CAN` 0.152 + `award docs not public` 0.258 + EPJ `information completeness` 33% missing | **SUBE FUERTE: de MEDIA a ALTA** — missing info es #3 predictor global |
| 14 | `pct_diferencia_precio_bajo` / `pct_diferencia_precio_alto` | `all bids > projected` + C3 | ALTA |
| 15 | `dias_reposicion` / `dias_solicitud_anticipo` / `dias_pago_anticipo` | **EPJ** `DV hours share` 0.77 / `WV hours share` 0.95 — *share* de tiempo para cumplir verificaciones | MEDIA, pero con nueva métrica futura: horas_share |

### Nivel 4 — Estructural + Benford — PROMOVIDO

| # | Variable | Fuentes v2 | Movimiento |
|---|---|---|---|
| 16 | `gini_content_length` / `title_entropy` / `sum_word_count` | **EPJ** random forest: `page/word count` pasa de marginal (LASSO) a **top predictor** en RF flexible | **SUBE de Nivel 4 a Nivel 3.5** — document length como oblivious indicator |
| 17 | `n_protestas` / `has_protesta` (967 licit.) | Validación externa (Kehler) — Ferwerda `complaints non-winning` r=0.114 n.s. (no tan fuerte) | Mantiene como proxy, pero nota Ferwerda n.s. |
| 18 | `n_unspsc_distintos` / `n_items` | **Ferwerda** `tender exceptionally large` **0.330** + `artificial bids` 0.221 | Sube levemente |
| 19 | `razon_notificados_a_oferentes` | `connections bidders` 0.178 + Wachs cohesion/exclusivity | Mantiene |
| 20 | **NUEVO** `benford_mad_first_two` / `benford_chi2` sobre `items_licitacion.monto` 1.87M + `pagos_contrato.monto_pagado` 265K | **Azevedo** MAD 0.001 / χ² 11.9 (agregado), thresholds Nigrini 0.006/0.012/0.022 | **Entra al top por primera vez** — pipeline replicable en PY con agregación por convocante |

> **Descartadas v2 igual:** `bidder_diversity` (bug), `costo_pliego` etc. vacíos, `pct_anticipo` constante — validadas contra Ferwerda `EU funding` n.s.

---

## 2. Variables NUEVAS de Pliego Regeneradas (7) — sustento v2

| # | Variable | Tipo | Sección (n) | v1 (Ferwerda/EPJ faltaban) | **v2 sustento enriquecido** | Paraguay | Var. |
|---|---|---|---|---|---|---|---|
| **N1** | `dias_validez_oferta` | int | `periodo de validez...` 32,182 | WB + B1 weekend | **+ Ferwerda** `shortened time span` 0.200 + `time-to-bid not conform` 0.179 + **EPJ** OTD negativo (evasión) | Ley 7021 art.33 plazos | 100% 90·60·30·120 |
| **N2** | `dias_validez_garantia_mantenimiento_oferta` | int | `periodo de validez garantia...` 32,182 | IACRC bid security | **+ TI** integrity in contract management + Ferwerda `tender exceptionally large` (garantía como barrera) | Regl. DNCP + Circ.08/23 | 100% 120·90·60·150 |
| **N3** | `dias_plazo_entrega_bien` | int | `plan de entrega...` 22,547 | WB entrega restrictiva | **+ EPJ** `ad hoc subcontract` 42% + `no subcontract 21%` (plazo + subcontratación como combo) + C3 ≥50% | Ley 7021 art.13 + SICP cronogr. | 19% 15·5·30·10 |
| **N4** | `requiere_visita_tecnica` | binary | `visita al sitio...` 26,909 | IACRC mandatory visit | **+ EPJ §3/8: WV 14.1-14.4** (CRÍTICO): Naples Camorra usó WV con individuo específico para filtrar; `WV hours share` 0.95 + `WV specific dates` + `DV hours share` 0.77. **Pasa de MEDIA a ALTA** | Regl. DNCP "No Aplica" vs dirección | binary |
| **N5** | `criterio_adjudicacion` | categorical | `criterios adjudic.` 32,182 | OCP + B1 non-price weight | **+ EPJ hallazgo #1**: MEAT **2× siguiente predictor** (LASSO/Ridge), `MEAT tech score` + `qual score`, `firm list preference` + **Ferwerda** `preferred supplier` **0.487** (top) | Ley 7021 menor precio vs más conveniente | cat |
| **N6** | `multa_jornales_incumplimiento` | int | `multas por incumplimiento` 664 | Fazekas penalidades | **+ EPJ** `obstacles to participation` como grupo + TI 10-25% daño | Ley 7021 tope multas; jornales = peculiaridad PY | int 2 jornales |
| **N7** | `tiene_requisito_eliminatorio` | binary | `requisitos calif.` 32,182 | WB rigged specs | **+ Ferwerda** `preferred supplier` 0.487 + **EPJ** `firm other preference` 28% + `local regulations` 33% + *prohibition pooling* | Ley 7021 no discriminatoria; cruce `criterios_llamado` | binary |

> **Prioridad v2 entre las 7:** N5 (MEAT) > N4 (worksite) > N1 (time span) > N7 (preferred supplier) > N2 > N3 > N6. Para gold standard, empezar por N5+N4+N1 (más varianza + más sustento).

---

## 3. 81 Campos de Clustering Regenerados (25 títulos) — mapeo con nueva literatura

> ✅ Validado: 81/81 campos del schema `master_schemas_v2.json` presentes. Prioridad actualizada con Ferwerda/EPJ.

| # | Sección | Campos clustering | Red flag ex-post v2 | Fuentes v2 | Prio v2 | Δ vs v1 |
|---|---|---|---|---|---|---|
| 1 | `analisis_de_precios_ofertados` | `pct_diferencia_precio_bajo/alto` + 3 binary | Bajas anormales + sobreprecio | Ferwerda `all bids > costs` 0.16 + A1 focal point + Azevedo | **ALTA** | = |
| 2 | `porcentaje_de_multas` | `pct_multa_diaria` + `binary_...rescision` | Penalidades asimétricas | TI 10-25% | **ALTA** | = |
| 3 | `fuerza_mayor` | `dias_notificacion_fuerza_mayor` + 2 binary | Estándar | — | MEDIA | = |
| 4 | `porcentaje_de_garantia_de_fiel_cumplimiento_de_contrato` | `pct_garantia_cumplimiento` + `dias_presentacion_garantia` | Garantía barrera | Ferwerda large tender 0.33 | **ALTA** | = |
| 5 | `aclaracion_de_las_ofertas` | 3 binary | Transparencia | OCP + Ferwerda docs not public 0.258 | MEDIA | = |
| 6 | `experiencia_requerida` | `anos_experiencia_minima` + `num_referencias_minimo` + `pct_monto_referencia_minimo` + 2 binary | **Rigged specs — preferencia** | **Ferwerda 0.487 #1** + WB | **ALTA** | ↑ |
| 7 | `plazo_de_reposicion_de_bienes` | `dias_reposicion` + 2 binary | Plazos + **EPJ WV share** | EPJ 14.1-14.4 | MEDIA | = |
| 8 | **`documentacion_requerida_para_la_firma_del_contrato`** | `num_categorias_documentos` + 3 binary | **Missing info** | **Ferwerda 0.431 #3** + 0.258 + EPJ completeness | **ALTA** | **MEDIA→ALTA** |
| 9 | `causales_de_terminacion_del_contrato` | 4 binary | Estándar | — | BAJA | = |
| 10 | `confidencialidad_de_la_informacion` | 2 binary | Transparencia | — | MEDIA | = |
| 11 | `impuestos_y_derechos` | 3 binary | — | — | BAJA | = |
| 12 | `suministros_y_especificaciones_tecnicas` | 3 binary | Specs técnicas | EPJ `page/word count` RF | MEDIA↑ | = |
| 13 | `derechos_intelectuales` | 2 binary | — | — | BAJA | = |
| 14 | `periodo_de_validez_de_la_garantia_de_los_bienes` | `dias_validez_garantia` + 2 binary | Garantía | — | MEDIA | = |
| 15 | `capacidad_tecnica` | `anos_experiencia_minima_tecnica` + `num_referencias_tecnicas_minimo` + `pct_monto_referencia_tecnica` | **Specs a medida — técnica** | **Ferwerda 0.487** | **ALTA** | ↑ |
| 16 | `formas_y_condiciones_de_pago` | `dias_pago_maximo` + `dias_aceptacion_rechazo_factura` + `num_documentos_pago` + binary | Pago + **missing info** | Ferwerda 0.431 | **ALTA** | = |
| 17 | `plan_de_entrega_de_los_bienes` | 3 binary | Plan + subcontratación | EPJ ad hoc 42% | MEDIA | = |
| 18 | `capacidad_financiera` | `ratio_liquidez_minimo` + `ratio_endeudamiento_maximo` + 2 binary | Barrera financiera | EPJ firm preference | **ALTA** | = |
| 19 | `reajuste` | 4 campos (irresoluble 3%) | Fórmula | — | BAJA | = |
| 20 | **`criterios_de_adjudicacion`** | 4 binary | **MEAT — scoring** | **EPJ #1 (2×)** + Ferwerda 0.487 | **ALTA** | **ALTA↑ (top de B)** |
| 21 | `forma_de_instrumentacion_de_garantia_de_anticipo` | 3 campos | Anticipo | — | MEDIA | = |
| 22 | `requisitos_de_calificacion` | `num_pasos_verificacion` + 3 binary | Barreras + **EPJ pooling prohibition** | EPJ 16. | MEDIA | = |
| 23 | `solicitud_de_pago_de_anticipo` | 4 campos | Anticipo | EPJ WV/DV share | MEDIA | = |
| 24 | `tasa_de_interes_por_mora` | `pct_interes_mora_diario` + binary | Punitivos | — | **ALTA** | = |
| 25 | `criterios_de_desempate_de_ofertas` | 2 binary | Desempate | — | MEDIA | = |

**12 hybrid (LLM fallback) — sustento directo v2:**

| Campo | Mide | Nueva evidencia que justifica LLM | Prio |
|---|---|---|---|
| `anos_experiencia_minima_tecnica` | experiencia técnica a medida | Ferwerda 0.487 | ALTA |
| `dias_pago_maximo` | pago tardío | Ferwerda missing info 0.431 | ALTA |
| `ratio_liquidez_minimo` / `ratio_endeudamiento_maximo` | barrera financiera | EPJ firm preference 28% | ALTA |
| `dias_validez_garantia` | garantía asimétrica | — | MEDIA |
| `dias_solicitud_anticipo` / `dias_pago_anticipo` | anticipo + **horas share** futuro | EPJ WV/DV hours share | MEDIA↑ |
| `dias_notificacion_fuerza_mayor` | estándar | — | MEDIA |
| `dias_aceptacion_rechazo_factura` | demoras | — | MEDIA |
| `num_componentes_formula` / `num_documentos_pago` / `num_pasos_verificacion` | irresolubles <3% | Ferwerda missing info (pero no contables) | BAJA |

---

## 4. Variables OCDS-derivadas (4 + 46) — con nuevo pipeline Benford

| Variable | Fórmula OCDS | Sustento v2 | Fill | Novedad v2 |
|---|---|---|---|---|
| `dias_plazo_presentacion_ofertas` | `fecha_apertura − fecha_publicacion` | Ferwerda **shortened 0.200** + EPJ OTD negativo | 100% | RF flexible |
| `dias_plazo_consultas` | `fecha_fin_consultas − fecha_pub` | Ferwerda `time-to-bid not conform` 0.179 + OCP | 100% | = |
| `dias_publicacion_llamado` | `fecha_publicacion` | Benford + publication value | 100% | = |
| `criterio_adjudicacion_ocds` | `priceOnly` 29,803 / `ratedCriteria` 7 | EPJ MEAT #1 | 100% | **Top** |
| `benford_mad` / `benford_chi2` / `benford_summation` | `items_licitacion.monto` 1.87M + `pagos.monto_pagado` 265K agregados por convocante | Azevedo MethodsX 2021 (MAD 0.001, χ² 11.9 agregado) + Nigrini thresholds | 100% | **NUEVO** — pipeline R disponible |
| `document_economic_features` 46 cols | oferta única, overbudget, colusión etc. | Wachs cohesion/exclusivity (78K ofertas, 5,544 bidders) | 29,811 | = |

> **Nota Azevedo:** Benford en 13.4M individuales falla (χ² 6.48M); pasa al agregar por municipio → en PY agregar por `convocante` o `departamento`.

---

## 5. Matriz Actualizada Variable → Alternativa (con nuevo peso)

| Variable (top) | ISF/ECOD | CRI | Benford | Redes | Superv. protestas | NLP |
|---|---|---|---|---|---|---|
| `es_unico_oferente` | ✅ | ✅ |  | ✅ grado | ✅ | target Katona |
| `metodo_detalle` | ✅ **↑** | ✅ |  |  | ✅ NBER |  |
| `duracion_oferta` | ✅ **↑** | ✅ Ferwerda 0.20 |  |  | ✅ |  |
| `n_enmiendas` | ✅ | ✅ **↑** 0.33 |  |  | ✅ |  |
| `winner_*` | ✅ | ✅ |  | ✅ | ✅ EPJ criminal |  |
| `n_consultas` + **word count** | ✅ **↑** |  |  |  | ✅ | ✅ EPJ RF |
| `num_categorias_documentos` | ✅ **↑ 0.43** | ✅ |  |  | ✅ |  |
| `criterio_adjudicacion` / MEAT | ✅ **↑ #1** | ✅ |  |  | ✅ |  |
| `requiere_visita_tecnica` (WV share futuro) | ✅ **↑ EPJ** | ✅ |  |  | ✅ |  |
| Benford 1.87M montos |  | ✅ | **✅ Azevedo** |  |  |  |

---

## 6. Bibliografía Enriquecida v2 (22 fuentes — 19 PDFs OK)

| # | Cita | Estado PDF |
|---|---|---|
| A1-4 WB/OCP | World Bank 2011 (3) + OCP 2016 | OK |
| A5-7 OECD | OECD 2007 + 2008 Rec + 2016 Preventing | OK |
| A8 TI | TI Handbook 2006 | **OK (nuevo)** `18_...232p.pdf` |
| B1 | Fazekas 2016 CRI | OK |
| B2 | Fazekas & Kocsis 2020 BJPS | ⏳ pendiente `19_...` (DOI `10.1017/S0007123418000700` verificado) |
| B3 | DG REGIO 2019 Single bidding | OK |
| B4 | EC Scoreboard | Web |
| B5 | **Ferwerda 2016** Finding Right Indicators | **OK (nuevo)** `15_...23p.pdf` |
| B6 | PMC8933377 / | EPJ Data Science 2022 → **OK (nuevo)** `12_...38p.pdf` (open access, reemplaza PMC JS) |
| C1 | Wachs & Kertész 2019 | OK (2 pdfs) |
| C2-4 | Conley/Decarolis/NBER | OK (C3 `20_...` pendiente `10.1257/app.6.1.108`) |
| C5 | IMF WP/22/94 Paraguay | ⏳ pendiente `14_...` |
| D1-2 | Nigrini + **Azevedo 2021 Benford** | **OK (nuevo)** `13_...10p.pdf` |
| E3 | Ley 7021/2022 | OK |

Ver `docs/referencias/md/00_INDICE.md` y `docs/06_sustento_academico_respaldo.md §7` para rutas exactas.

---

## 7. Qué Cambió vs Top v1 (05)

| Decisión v1 | v2 | Motivo con nueva literatura |
|---|---|---|
| N4 `requiere_visita_tecnica` binary, MEDIA | **ALTA**, prioridad #2 entre nuevas | EPJ WV case Naples + hours share 0.95 — la visita es instrumento de captura documentado |
| N5 `criterio_adjudicacion` categorical, ALTA | **ALTA #1** entre nuevas | EPJ MEAT 2× predictor + Ferwerda preferred supplier 0.487 |
| `num_categorias_documentos` MEDIA | **ALTA** | Ferwerda missing info 0.431 (#3 global) + EPJ completeness 33% missing |
| `gini/title_entropy/sum_word_count` Nivel 4 | **Nivel 3.5** (promovido) | EPJ RF: word/page count top predictor en modelo flexible |
| Benford solo mencionado | **Variable explícita** `benford_mad` con pipeline Azevedo + threshold MAD | Azevedo agregación por municipio leccionable |
| `n_consultas` sin peso textual | **+ word/page count** como proxy | EPJ §4 |
| Top general sin cuantificación | **Con r y p-values Ferwerda + EPJ** | Permite ponderar CRI v2 |

---

## 8. Próximos Pasos

1. **Gold standard v2:** 100 muestras/campo para N5+N4+N1 primero (más sustento + varianza). Usar `scripts/preparar_gt_nuevo.py`.
2. **Feature engineering v2:** añadir `benford_mad` por convocante + `word_count` como feature ISF.
3. **Benchmark LLM:** Llama 3.1 8B vs Mistral 7B sobre 7 nuevas + 12 hybrid (guided decoding) — N4/N5 son ahora los más críticos para validar.
4. **CRI v2:** recalcular pesos Fazekas incorporando `shortened time span` (Ferwerda 0.20) y `MEAT` (EPJ) con mayor peso.

*Regenerado 2026-08-27. Verifica 81/81 campos. Cita este v2 como Top maestro; `05` queda como v1 histórico y `06` como respaldo probatorio.*

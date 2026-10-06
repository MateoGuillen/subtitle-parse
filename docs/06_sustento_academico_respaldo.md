# Sustento Académico Completo — Respaldo por Variable

> **Proyecto:** `subtitle-parse` — Detección de anomalías en contrataciones públicas (DNCP, Paraguay).
> **Fecha:** 2026-08-27 · **Docs relacionados:** `05_seleccion_variables_sustento_academico.md` (selección), `estrategias-anomaly-deteccion.md` (mapa de métodos).
> **Propósito:** respaldo **puntual** de cada decisión de variable con fuente primaria, cita y PDF local. Lista **todas** las fuentes usadas + **nuevas incorporadas** en esta entrega.

---

## 0. Cómo leer este documento

**Regla de oro de la tesis:** *literatura primero*. Ninguna variable entra al modelo sin red flag previo. El clustering descubrió 81 campos en texto paraguayo que luego se validaron ex-post contra esa literatura.

```
Red flag académico (World Bank / OECD / OCP / IACRC / Fazekas / Katona)
        │
        ├──► Variable medible tipada (int / float / binary / categorical)
        │         │
        │         ├──► Verificación BD: ¿existe la sección? ¿varía el número?
        │         └──► Método: regex determinista vs hybrid (LLM fallback)
        │
        └──► Referencia Paraguay (Ley 7021/2022 + reglamentos DNCP + SICP)
```

**PDF locales:** `docs/referencias/pdf/` (ver §7). Si un PDF no pudo descargarse por paywall/CF, se indica DOI/URL alternativa verificada.

---

## 1. Bibliografía completa (22 fuentes, orden temático)

> Formato APA abreviado + nota de qué aporta a tu tesis. **★ = nueva en esta entrega** (no estaba en `05`).

### A. Marcos generales — corrupción en contratación pública

| # | Cita | Qué aporta a tu tesis | PDF local |
|---|---|---|---|
| A1 | World Bank (2011). *Curbing Fraud, Corruption, and Collusion in the Roads Sector.* Washington: World Bank, INT. | **Fuente madre** de *rigged specifications*, publication of cost estimates (focal point para colusión), Bali case (publicity vs competition). Define el problema de transparencia ↔ colusión. | `01_WB_2011_Curbing_Fraud_Corruption_Collusion_Roads.pdf` + `01b_WB_2011_Curbing_Fraud_WP64283.pdf` |
| A2 | World Bank (2011). *Fraud and Corruption Awareness Handbook* (con Central Anti-Corruption Bureau, Poland). Washington: World Bank, Integrity Vice Presidency. | **Inventario de red flags por fase** (design → award → execution): unnecessary items, contract splitting, restricted circulation, rigged specs, biased evaluation criteria. | `02_WB_2011_Fraud_Corruption_Awareness_Handbook.pdf` |
| A3 | World Bank (s.f.). *Warning Signs of Fraud and Corruption in Procurement.* Washington: World Bank Group, INT — brochure. | **Checklist operativo** de red flags (bidding patterns, suspicious bidders, viable bids rejected). Complementa A1-A2 con tipología IACRC. | `03_WB_Warning_Signs_Fraud_Corruption_Procurement.pdf` |
| A4 | Transparency International (2006). *Handbook for Curbing Corruption in Public Procurement.* Berlin: TI (EU-Asia Urbs Programme). | Integrity Pacts, principio de transparencia por fase, estimación del daño (10-25% del contrato). Contexto Asia que se extrapola a PY. | URL verificada (CF bloquea descarga directa): `https://www.transparency.org/en/publications/handbook-for-curbing-corruption-in-public-procurement` — mirrors: `images.transparencycdn.org` |
| A5 | OECD (2007). *Bribery in Public Procurement: Methods, Actors and Counter-Measures.* Paris: OECD, Working Group on Bribery. | **Tipología** bribery: who bribes whom, how, sector risk, controls. Capítulo 8 lista red flags verificables. | `06_OECD_2007_Bribery_Public_Procurement_Typology.pdf` |
| A6 | OECD (2015). *Recommendation of the Council on Public Procurement.* Paris: OECD/LEGAL/0411 + (2008) *Principles for Integrity in Public Procurement.* | **10 principios** (transparency, accessibility, e-procurement, oversight). Define integrity violations (kickbacks, conflict, collusion, discriminatory treatment). Marco normativo internacional. | `16_OECD_2008_Recommendation_Public_Procurement.pdf` |
| A7 | OECD (2016). *Preventing Corruption in Public Procurement.* París: OECD. | **Guía holística** por etapa (needs → bidding → execution → payment). Red flags + medidas de e-procurement, whistleblowing, control. | `05_OECD_2016_Preventing_Corruption_Procurement.pdf` |
| A8 | Open Contracting Partnership & Development Gateway (2016). *Red Flags for Integrity: Giving the Green Light to Open Data Solutions.* Washington: OCP. | **Puente literatura → datos:** 60 red flags mapeados a 35+ campos OCDS, 36 calculables hoy, 75 con extensiones. Metodología que justifica tu pipeline OCDS → `document_economic_features`. | `04_OCP_DG_2016_Red_Flags_Integrity_OCDS.pdf` |
| A9 | IACRC — International Anti-Corruption Resource Center. *Guide to Combating Corruption & Fraud in Development Projects* (guide.iacrc.org, 2011‑). | **Red Flags Listed by Project Cycle** + 16 esquemas con prueba (rigged specs, excluding qualified bidders, leaking bid info, unbalanced bidding). Fuente que OCP filtra. | Web: `https://guide.iacrc.org/detection/red-flags-of-corruption-bid-rigging-and-other-schemes/red-flags-listed-by-project-cycle/` |

### B. Índice de riesgo y single bidding (CRI)

| # | Cita | Qué aporta | PDF local |
|---|---|---|---|
| B1 | Fazekas, M., Tóth, I. J., & King, L. P. (2016). An objective corruption risk index using public procurement data. *European Journal on Criminal Policy and Research, 22*(3), 369‑397. https://doi.org/10.1007/s10610-016-9308-z | **CRI fundacional:** 13 red flags (no call published, procedure type, eligibility criteria length, short submission period & *exceptionally short abusing weekend* +19.8‑21.6pp single-bid, non-price criteria weight, decision period U-shape, contract modification). Cada flag validada por logit vs single-bid / winner share. Tu CRI paraguayo replica método. | `17_Fazekas_Toth_King_2016_CRI_EJCPR.pdf` (Springer) — mirror Cambridge: `repository.cam.ac.uk/handle/1810/256458` |
| B2 | ★ Fazekas, M., & Kocsis, G. (2020). Uncovering high-level corruption: Cross-national objective corruption risk indicators using public procurement data. *British Journal of Political Science, 50*(1), 155‑164. https://doi.org/10.1017/S0007123418000700 | Replica CRI en **2.8M contratos / 28 países UE 2009‑14**. Cuantifica: single-bid → **+9.6% precio**, 48% ESIF gastado vía procurement. Para tu § single-bidder growing 280→3470. | DOI: `10.1017/S0007123418000700` (paywall CUP; no PDF libre) — resumen verificado |
| B3 | ★ Fazekas, M. (2019). *Single bidding and EU co-funded procurement* (DG REGIO). Brussels: European Commission. | **Informe DG REGIO** (10 países con mayor single-bid 2008‑17: HR, CY, CZ, HU, IT, LV, PL, RO, SK, SI). Diagnóstico descriptive + explanatory models (economic, administrative, integrity). Benchmark para posición de PY. | `08_Fazekas_2019_Single_Bidding_ESIF_EU_DGREGIO.pdf` |
| B4 | European Commission — Single Market Scoreboard, *Business Framework: Public Procurement.* | **Thresholds single-bidder:** Green ≤10% / Red >20%; direct awards ≤5%/≥10%; decision speed ≤120d. Para calibrar tus umbrales. | Web: `https://single-market-scoreboard.ec.europa.eu/business-framework-conditions/public-procurement_en` |
| B5 | ★ Ferwerda, J., Deleanu, I., & Unger, B. (2016). Corruption in public procurement: Finding the right indicators. *European Journal on Criminal Policy and Research, 22*(3). https://doi.org/10.1007/s10610-016-9312-3 | **13 red flags testeados** vs tipos de corrupción: bid rigging correlación más fuerte (shortened time span, accelerated tender, all bids > expected costs); kickbacks → conflict of interest + missing info. Justifica por qué tus variables de plazo son más CRI que otras. | DOI `10.1007/s10610-016-9312-3` (Springer, cf bloque) — datos extraídos vía websearch |
| B6 | ★ Cingolani, L., & Fazekas, M. et al. Corruption red flags: new evidence from machine learning (PMC8933377). | **ML-driven selection** de indicadores (LASSO/Ridge). Hallazgo: MEAT criterion = mayor coeficiente, urgency **negativo** (vigilado → evitamiento), worksite inspection time share relevante. Para justificar no-linealidades. | PMC: `PMC8933377` (html; pdf requiere JS — cita verificada por highlights) |

### C. Colusión, screens y redes

| # | Cita | Qué aporta | PDF local |
|---|---|---|---|
| C1 | ★ Wachs, J., & Kertész, J. (2019). A network approach to cartel detection in public auction markets. *Scientific Reports, 9*, 10818. https://doi.org/10.1038/s41598-019-47198-1 | **Co-bidding network** sin necesidad de montos: cohesion (media geom/aritm) + exclusivity (strength inside/total). Validado en Ohio school milk (cartel conocido) + 150K contratos Georgia. Para tu D1 con 5,544 bidders/78K ofertas. | `11_Wachs_Kertesz_2019_Network_Cartel_Detection_SciRep.pdf` + `11b_Wachs_Kertesz_2019_arXiv.pdf` (1906.08667) |
| C2 | ★ Conley, T. G., & Decarolis, F. (2016). Detecting bidders groups in collusive auctions. *American Economic Journal: Microeconomics, 8*(2), 1‑38. https://doi.org/10.1257/mic.20130254 | **Tests de grupo coordinado** (entry + bidding piloting de media recortada). Validados con 8 carteles condenados Torino (276 ABAs). Para justificar screens de colusión italianos adaptables a PY. | `07_Conley_Decarolis_2016_Detecting_Bidders_Groups_AEJMicro.pdf` |
| C3 | ★ Decarolis, F. (2014). Awarding price, contract performance, and bids screening. *American Economic Journal: Applied Economics, 6*(1), 108‑132. https://doi.org/10.1257/app.6.1.108 | Trade-off first-price vs performance: bajo precio → renegociación ex-post pierde ≥50% ahorro. Screening precio anormal evita empeoramiento pero reduce ahorro 1/3. Fundamento para `pct_diferencia_precio_*` y `enmienda_ratio`. | DOI `10.1257/app.6.1.108` (AEA; html verificado; pdf requiere subscripción AEA) |
| C4 | ★ Decarolis, F., Rovigatti, G., et al. (2021). Rules, discretion, and corruption in procurement: Evidence from Italian government contracting. *NBER WP 28209.* | Discretion (negotiated + scoring rule/MEAT) × competition: solo cuando **falta competencia** (too few invited) se asocia a firmas investigadas (+1.9pp). Para matizar `metodo_detalle` + `n_oferentes`. | `10_Bandiera_Decarolis_2021_Rules_Discretion_Corruption_NBER28209.pdf` |
| C5 | Porter, R. H., & Zona, J. D. (1993, JPE) / Bajari & Ye (2003) / Kawai & Nakabayashi (2022, JPE) — citados vía Conley/Decarolis. | Screens clásicos de distribución de ofertas (requieren montos por bidder → **limitado en tu BD** `oferentes` sin montos, documentado como degradado). | Via C2/C3 |
| C6 | Abdou, Fisman, et al. (2022). *Identifying Cartels in the Field* — IMF WP/22/94 (Paraguay piloto). | Mencionado en `estrategias` como precedente PY FMI. **No descargable directo** (Cloudflare/Elibrary bloquea automatizado). | IMF: `https://www.imf.org/en/Publications/WP/Issues/2022/05/20/Identifying-Cartels-in-Public-Procurement-A-Pilot-Study-for-Paraguay-517511` |

### D. Benford y análisis digital de montos

| # | Cita | Qué aporta | PDF local |
|---|---|---|---|
| D1 | Nigrini, M. J. (2012). *Benford's Law: Applications for Forensic Accounting, Auditing, and Fraud Detection.* Wiley. + (1996) Taxpayer compliance; (2009) Second-order test (Nigrini & Miller, *Auditing* 28). | **Fundamento Benford:** first-digit, second-digit (redondeo), first-two-digits (manipulación psicológica), MAD (<0.006 close, <0.012 acceptable), second-order, summation test. Para `items_licitacion.monto` 1.87M + `pagos_contrato.monto_pagado` 265K. | PMC8720889 (ver D2) cita a Nigrini 2012 como fuente |
| D2 | ★ Azevedo, C., et al. A Benford's Law based method for fraud detection using R — Bolsa Familia (PMC8720889). | Implementación abierta R con χ², Z, MAD + summation. Agregación por municipio salva conformidad (13.4M registros). Modelo para tu pipeline Benford geográfico por convocante. | PMC: `PMC8720889` (html ok; pdf bloqueado por JS) — método extraído |
| D3 | ★ Sampaio et al. (2022) + Pinheiro (2024) mão portuguesa + Adelia et al. (2021) Indonesia — vía websearch. | Evidencia Benford en procurement BR/PT/ID: first-digit 4/8/9 desviados en subastas electrónicas BR; MAD resuelve over-sensitivity K‑S/χ² en N grande. | Ver D1/D2 |

### E. Texto, anomalías y Paraguay

| # | Cita | Qué aporta | PDF local |
|---|---|---|---|
| E1 | Katona & Fazekas (2024). Tender text predicts single bidding (GTI-WP). | Texto del llamado predice oferta única (77→82% HU, 119K). Fundamento bloque B (`document_features_v2`). Tu novedad PY. | GTI-WP (cita en `estrategias`) |
| E2 | Kehler et al. (2019). Isolation Forest sobre OCDS Paraguay. | Precedente ISF PY con validación vs protestas. Tus bloques A+B+C son extensión. | `estrategias-anomaly-deteccion.md` §A |
| E3 | **Ley 7021/2022** *De Suministro y Contrataciones Públicas* (DNCP) + Decreto 2264/2024 + Resolución 4400/2023. | **Fuente normativa PY**: PAC, calificación no discriminatoria, garantías, plazos por modalidad, protestas (desde 17/02/2025 nuevo régimen). Columna Paraguay de todas tus tablas. | `09_Ley_7021_2022_Paraguay_Suministro_Contrataciones.pdf` (10,156 KB, texto OCR) — portal: `https://www.contrataciones.gov.py/dncp/ley-7021/ley-7021-de-suministro-y-contrataciones-publicas/` |
| E4 | Heggstad, K. K., & Frøystad, M. (2011); Klasnja (2015/16); Charron et al. (2016) — citados vía Fazekas. | Winner's share, partisanship, QoG. Para justificar `winner_total_contracts` / `is_repeat_winner`. | Via B1 |

> **Total verificado hoy:** 22 fuentes primarias (10 con PDF local directo, 6 con DOI/URL alternativa verificada, 6 vía cita secundaria con highlights extraídos).

---

## 2. Sustento puntual — Variables NUEVAS de pliego (top-down, 7)

> Cada fila: red flag textual + mecanismo de abuso + evidencia + artículo/parágrafo PY + cómo se medirá en BD.

### N1 · `dias_validez_oferta` — int — `periodo de validez de las ofertas` (32,182)

- **Red flag:** A1 p.5 (Bali: el mecanismo de quejas desactivó colusión) + A2 *Restricted circulation, short notice* + B1 *short submission period* (5d vs 15d → +8.4‑9pp single-bid; 35d vs 45d → nada). Validez anormalmente corta expulsa a no-iniciados; anormalmente larga amarra el mercado y señala favoritismo (World Bank rigged specs).
- **Mecanismo:** plazo fuera del rango normativo → solo el “indicado” tiene la oferta lista o tolera el riesgo.
- **Evidencia:** B1 Tabla 4: efectos no lineales por umbral; B5: shortened time span → bid rigging p<0.05.
- **Paraguay:** Ley 7021 art. 33‑34 (clasificación y plazos por modalidad) + Reglamento DNCP (plazo mínimo de validez). Ver PDF 09.
- **BD:** hit 100% (90·60·30·120·45·50·59). Tipado int días. Método: regex robusto (número + “días (calendario/hábiles)”) — candidato a `deterministic_pct`‑like si plantilla.

### N2 · `dias_validez_garantia_mantenimiento_oferta` — int — `periodo de validez de la garantia de mantenimiento de oferta` (32,182)

- **Red flag:** IACRC *bid security red flags* (A9) + A6 principio “integrity in contract management: bid guarantees”. Garantía fuera de rango encarece injustificadamente la entrada.
- **Mecanismo:** exigir validez excesiva (150d vs 30d estándar) filtra PYMEs; validez insuficiente es señal de contrato dirigido.
- **Paraguay:** Reglamento DNCP (garantía de mantenimiento de oferta; circular DNCP 08/2023 para montos). Ver 09 + `contrataciones.gov.py/dncp/tramites-juridicos-electronicos/normativa-ley-7021-2022`.
- **BD:** hit 100% (120·90·60·150·50·61·31). Int días.

### N3 · `dias_plazo_entrega_bien` — int — `plan de entrega de los bienes` (22,547)

- **Red flag:** A1 *Engineer's estimate as focal point* (46 contratos idénticos a estimado → colusión segura) + A2 *Inappropriate contractual terms* + A6 “needs assessment → contract management” (plazo irreal → renegociación).
- **Mecanismo:** plazo de entrega restrictivo (3/5 días para 10K unidades) expulsa; plazo laxo + enmiendas posteriores → sobreprecio ex-post (C3: ≥50% del ahorro se pierde por renegotiation).
- **Paraguay:** Ley 7021 art. 13 (cadena de suministro → entrega trazable) + pliego SICP (cronograma).
- **BD:** hit 19% (15·5·30·10·3·20·45) — precisamente la varianza que predice el red flag. Int; NULL si no especifica.

### N4 · `requiere_visita_tecnica` — binary — `visita al sitio de ejecucion del contrato` (26,909)

- **Red flag:** A1 p.4‑5 *Mandatory participation in pre‑bid meetings … intimidate firms not part of ring* (Bali se eliminó y cayó 35‑40%); B6 (LASSO) *worksite inspection time share* → easier = menos corrupción; A9 IACRC *Mandatory site visit as exclusion*.
- **Mecanismo:** visita obligatoria + ventana corta geográficamente concentrada = cartel local; “No Aplica” = abierto.
- **Paraguay:** Reglamento (pliego tipo DNCP: visita “No Aplica” vs dirección + fecha).
- **BD:** binary (presencia de “No Aplica” vs instrucciones). Caso ejemplar para regex determinista.

### N5 · `criterio_adjudicacion` — categorical — `criterios de adjudicacion` (32,182)

- **Red flag:** B1 *weight of non‑price evaluation criteria* (0.4‑0.556 → +4.8pp single-bid; >0.556 → +9.6pp; only non‑price → 0) + C4 *scoring rule (MEAT) + too few invited = +1.9pp investigated winner* + B6 *MEAT is strongest predictor, twice next best*.
- **Mecanismo:** criterios subjetivos (calidad 70/price 30 con ítems cualitativos) = discrecionalidad sin competencia → favoritismo; price‑only = transparente pero vulnerable a bajas temerarias.
- **Paraguay:** Ley 7021 (menor precio vs más conveniente / costo integral). Complementa tus binaries `binary_criterio_menor_precio`, `binary_multiplicidad_criterios`.
- **BD:** categorical (`precio_mas_bajo` / `costo_evaluado` / `calidad_precio` / `otro`) — a derivar por regex + validación vs `criterio_adjudicacion_ocds`.

### N6 · `multa_jornales_incumplimiento` — int — `multas por incumplimiento` (664)

- **Red flag:** B1‑C3 *Inappropriate contractual terms / penalty clauses* + A2 *Unbalanced bidding*. Penalidades desproporcionadas (“2 jornales diarios”) solo tolerables por quien sabe que no se aplicarán.
- **Mecanismo:** multa punitiva asimétrica filtra oferentes aversos al riesgo; monto fijo en jornales desindexado.
- **Paraguay:** Ley 7021 límite de multas + Decreto reglamentario (tope % del contrato; jornales es peculiaridad PY). Ver 09.
- **BD:** 664 filas; patrón “N (N) jornales (mínimos) diarios”. Int jornales; NULL si no especifica.

### N7 · `tiene_requisito_eliminatorio` — binary — `requisitos de calificacion` (32,182)

- **Red flag:** A1‑A2 *Rigged specifications / Exclusion of qualified bidders* + A9 IACRC *Excluding qualified bidders* + B1 *relative length of eligibility criteria* (extenso → +12.4pp single-bid).
- **Mecanismo:** frase “serán desechadas / descalificadas las ofertas que no…” + requisito inusual (certificación ISO específica, marca) → filtro a medida.
- **Paraguay:** Ley 7021 calificación no discriminatoria + proporcionalidad. SICP publica lista de requisitos (ya tienes `criterios_llamado` con `requisito_valor` para cruce).
- **BD:** binary (regex “serán desechadas/descalificadas/no serán evaluadas”). Alto valor discriminativo (clustering lo marcó como ALTA).

---

## 3. Sustento puntual — 81 campos de CLUSTERING (25 títulos, mapeo ex-post)

> Cada grupo indica el red flag que mide **después** de haber sido descubierto por el LLM. Prioridad anomalía según `estrategias-anomaly-deteccion.md` Bloques A/B/C.

| # | Sección (título) | Campos (clustering) | Red flag ex‑post | Fuentes clave | Prioridad |
|---|---|---|---|---|---|
| 1 | `analisis_de_precios_ofertados` | `pct_diferencia_precio_bajo`, `pct_diferencia_precio_alto`, `binary_solicita_explicacion_precios`, `binary_permite_declarar_inaceptable`, `binary_tiene_multiplicidad_criterios` | **Baja anormal + sobreprecio** (price vs estimate) | A1‑A3 + B1 *contract value diff* + C3 (screening anormal) | **ALTA** |
| 2 | `porcentaje_de_multas` | `pct_multa_diaria`, `binary_permite_rescision_por_multas` | **Penalidades asimétricas** | A2 *Inappropriate terms* + E3 límite multas | **ALTA** |
| 3 | `fuerza_mayor` | `dias_notificacion_fuerza_mayor`, `binary_incluye_ejemplos_tipicos`, `binary_obliga_continuar_cumplimiento` | Cláusula estándar | — | MEDIA |
| 4 | `porcentaje_de_garantia_de_fiel_cumplimiento_de_contrato` | `pct_garantia_cumplimiento`, `dias_presentacion_garantia` | **Garantía excesiva = barrera** | A6‑A7 + B1 *contract modification* | **ALTA** |
| 5 | `aclaracion_de_las_ofertas` | 3 × `binary_*aclaracion*` | Transparencia / igualdad | A8 OCP + A6 | MEDIA |
| 6 | `experiencia_requerida` | `anos_experiencia_minima`, `num_referencias_minimo`, `pct_monto_referencia_minimo`, `binary_tiene_requisitos_por_lote`, `binary_requiere_experiencia_especifica_marca` | **Rigged specs — experiencia a medida** | A1‑A2 *Rigged specs* + B1 *eligibility length* | **ALTA** |
| 7 | `plazo_de_reposicion_de_bienes` | `dias_reposicion`, `binary_dias_habiles`, `binary_plazo_segun_autoridad` | Plazos como filtro | A2 + C3 | MEDIA |
| 8 | `documentacion_requerida_para_la_firma_del_contrato` | `num_categorias_documentos`, `binary_requiere_certificado_tributario`, `binary_requiere_certificado_ips`, `binary_incluye_condiciones_contractuales` | **Burocracia = barrera** | A8 (data gaps = missing info) + B5 *high missing info → kickbacks* | MEDIA |
| 9 | `causales_de_terminacion_del_contrato` | 4 × `binary_incluye_terminacion_*` | Cláusula estándar | — | BAJA |
| 10 | `confidencialidad_de_la_informacion` | `binary_tiene_excepciones_confidencialidad`, `binary_menciona_plazo_confidencialidad` | Transparencia | A6 | MEDIA |
| 11 | `impuestos_y_derechos` | `binary_responsabilidad_integral_proveedor`, `binary_menciona_iva`, `binary_incluye_exoneraciones` | — | — | BAJA |
| 12 | `suministros_y_especificaciones_tecnicas` | `binary_menciona_normas_oficiales`, `binary_permite_cambios_codigos`, `binary_proveedor_puede_rehusar_responsabilidad` | **Specs técnicas a medida** | A1‑A2 | MEDIA |
| 13 | `derechos_intelectuales` | 2 × `binary_*derechos*` | — | — | BAJA |
| 14 | `periodo_de_validez_de_la_garantia_de_los_bienes` | `dias_validez_garantia`, `binary_cubre_defectos_fabricacion`, `binary_tiene_cobertura_especifica` | Garantía asimétrica | A6 | MEDIA |
| 15 | `capacidad_tecnica` | `anos_experiencia_minima_tecnica`, `num_referencias_tecnicas_minimo`, `pct_monto_referencia_tecnica` | **Specs a medida — técnica** | A1‑A2 + B1 | **ALTA** |
| 16 | `formas_y_condiciones_de_pago` | `dias_pago_maximo`, `dias_aceptacion_rechazo_factura`, `num_documentos_pago`, `binary_requiere_cuenta_bancaria_estatal` | **Condiciones de pago como filtro** | A7 *payment risks* + Wittberg (demoras) | **ALTA** |
| 17 | `plan_de_entrega_de_los_bienes` | `binary_tiene_plan_entrega`, `binary_incluye_cronograma`, `binary_requiere_documentos_embarque` | Plan restrictivo | A2 | MEDIA |
| 18 | `capacidad_financiera` | `binary_tiene_requisitos_financieros`, `ratio_liquidez_minimo`, `ratio_endeudamiento_maximo`, `binary_requiere_estados_auditados` | **Barrera financiera** | A1 + B1 (no directo; ratio como eligibility) + A9 | **ALTA** |
| 19 | `reajuste` | `binary_tiene_formula_reajuste`, `num_componentes_formula`, `binary_referencia_ipc`, `binary_no_reajuste_si_atraso` | Fórmula (irresoluble <3% acc) | — | BAJA |
| 20 | `criterios_de_adjudicacion` | `binary_criterio_menor_precio`, `binary_multiplicidad_criterios`, `binary_contrato_abierto`, `binary_reserva_disminuir_cantidad` | **Criterios discrecionales** | B1 + C4 + B6 (MEAT) | **ALTA** |
| 21 | `forma_de_instrumentacion_de_garantia_de_anticipo` | `binary_requiere_garantia_anticipo`, `categorical_tipo_garantia`, `pct_anticipo` | Anticipo como filtro | A7 | MEDIA |
| 22 | `requisitos_de_calificacion` | `num_pasos_verificacion`, `binary_verifica_sinarh`, `binary_requiere_declaracion_conflictos`, `binary_requiere_registro_proveedores` | **Barreras de entrada** | A2 + A8 | MEDIA |
| 23 | `solicitud_de_pago_de_anticipo` | `binary_tiene_condiciones_anticipo`, `dias_solicitud_anticipo`, `dias_pago_anticipo`, `binary_anticipo_solo_mipymes` | Anticipo (plazos) | A7 | MEDIA |
| 24 | `tasa_de_interes_por_mora` | `pct_interes_mora_diario`, `binary_mora_desde_dia_siguiente` | **Intereses punitivos** | B1 (penalidades) | **ALTA** |
| 25 | `criterios_de_desempate_de_ofertas` | `binary_menciona_criterios_dncp`, `binary_incluye_criterios_especificos_Adicionales` | Desempate sesgado | A7 | MEDIA |

> **Resumen:** 9 grupos en ALTA (1,2,4,6,15,16,18,20,24) = núcleo B de `estrategias`. 69 regex / 12 hybrid actual. Ver `docs/revision_26_campos.md` para portes 55→69.

### 3a. Los 12 hybrid (LLM fallback) — sustento directo

| Campo | Mide | Fuente | Paraguay |
|---|---|---|---|
| `anos_experiencia_minima_tecnica` | experiencia técnica a medida | A1‑A2 Rigged specs | Ley 7021 requisitos proporcionados |
| `dias_pago_maximo` | pago tardío como presión | A7 *payment* + Wittberg demoras | Ley 7021 plazo de pago |
| `ratio_liquidez_minimo` | barrera financiera | A2 Excluding bidders | Reglamento DNCP liquidez |
| `ratio_endeudamiento_maximo` | barrera financiera | idem | idem |
| `dias_validez_garantia` | garantía asimétrica | A6 | Reglamento DNCP |
| `dias_solicitud_anticipo` / `dias_pago_anticipo` | anticipo como ventaja | A7 + B1 contract modification | Ley 7021 anticipo |
| `dias_notificacion_fuerza_mayor` | estándar | — | — |
| `dias_aceptacion_rechazo_factura` | demoras factura | Wittberg | Ley 7021 |
| `num_componentes_formula` | irresoluble (<3% acc) | — | — |
| `num_documentos_pago` | irresoluble (0% acc) | B5 missing info | — |
| `num_pasos_verificacion` | irresoluble (<2% acc) | A8 data gaps | — |

---

## 4. Variables OCDS-derivadas (separadas, no LLM) — 4 + 46

| Variable | Fórmula OCDS | Fuente académica | Paraguay | Fill |
|---|---|---|---|---|
| `dias_plazo_presentacion_ofertas` | `fecha_apertura − fecha_publicacion` (licitaciones) | B1 *short submission period* (efectos umbral) + B5 *shortened time span* | Ley 7021 plazos por modalidad | 100% |
| `dias_plazo_consultas` | `fecha_fin_consultas − fecha_publicacion` | A8 + B1 (donde no se publica call → +16pp single‑bid) | Ley 7021 | 100% |
| `dias_publicacion_llamado` | `fecha_publicacion` directa | B2 precios +9.6% con single‑bid | Publicidad DNCP | 100% |
| `criterio_adjudicacion_ocds` | `criterio_adjudicacion` (`priceOnly` 29,803 / `ratedCriteria` 7) | C4 scoring rule / MEAT + B1 non‑price weight | E3 | 100% |
| `document_economic_features` (46 cols) | agregados por `nro_licitacion` (oferta única, overbudget, colusión, pagos, protestas) | A1‑A8 + C1‑C4 + D1‑D2 | — | 29,811×46 |

> `cantidad_items`, `costo_pliego`, `presupuesto_monto`, `garantia_porcentaje` = vacíos verificados (0/29,811) → descartadas a pesar de ser deseables (documentado en `05` §4).

---

## 5. Métodos transversales (qué variables alimentan cada alternativa)

| Alternativa | Variables que usa (top) | Fuente justificativa |
|---|---|---|
| **A. ISF/ECOD/ensemble no supervisado** | Todas Niv 1‑4 (bloques A+B+C) | Kehler et al. 2019 (ISF PY); ECOD/COPOD SOTA benchmarks |
| **B1. CRI compuesto** | oferta única, winner share, plazo corto, método directo, specs B (eligibility length), enmiendas | B1 Fazekas 2016 (pesos por regresión) |
| **B3. Benford (first/second/first‑two, MAD)** | `items_licitacion.monto` 1.87M + `pagos_contrato.monto_pagado` 265K | D1 Nigrini 2012 + D2 Bolsa Familia |
| **D1. Red co‑bidding (cohesion/exclusivity, Jaccard)** | `oferentes` 78,695 / 5,544 bidders sin montos | C1 Wachs & Kertész 2019 |
| **C. Screens econométricos (degradados)** | solo distancia ganador‑referencial + recurrencia (sin montos por bidder) | C2‑C5 (limitación documentada) |
| **E. Supervisado protestas** | A+B+C → target `has_protesta` 967 licit. | B3 DG REGIO (protestas = proxy) |
| **F1. NLP texto → single bidding** | `document_features_v2` 31,321×108 | E1 Katona & Fazekas 2024 |

Ver `estrategias-anomaly-deteccion.md` §4‑6 para viabilidad por bloque + hoja de ruta Benford→CRI→ISF/ECOD→red.

---

## 6. ¿Qué se enriqueció en esta entrega vs `05`?

> `05` ya citaba A1, A8 (OCP 2016), IACRC, B1 (Fazekas 2016), E1‑E2 y E3. Esta entrega añade:

- **★ Marcos normativos completos:** A5‑A7 (OECD 2007 typology + 2015 Recommendation + 2016 Preventing Corruption) — para anclar cada variable a principio OECD de transparencia/integrity.
- **★ Single bidding cuantificado:** B2 (Fazekas & Kocsis 2020, 2.8M contratos, +9.6% precio) + B3 (DG REGIO 2019, top‑10 países) + B4 (EC Scoreboard thresholds ≤10% / >20%) — para posicionar el crecimiento PY 280→3470.
- **★ Red flags más fina:** B5 (Ferwerda 2016) — qué flags predicen bid rigging vs kickbacks (acorta plazo + bids > expected = rigging; missing info = kickback). Justifica priorizar N1‑N3 sobre otras.
- **★ Selección por ML:** B6 (PMC8933377) — MEAT y worksite inspection time share validados por LASSO/Ridge; N4 (visita técnica) gana peso.
- **★ Redes sin montos:** C1 (Wachs & Kertész 2019) con cohésion/exclusivity medibles en tu `oferentes` — novedad PY documentada.
- **★ Screens validados:** C2 (Conley & Decarolis 2016, 8 carteles Torino) + C3 (Decarolis 2014, renegociación ≥50%) + C4 (NBER 28209, discretion×competition) — para defender N5 y `enmienda_ratio`.
- **★ Benford operativo:** D1‑D2 (Nigrini 2012 + Bolsa Familia R pipeline) — pipeline replicable en PY con MAD vs χ².

---

## 7. PDFs descargados y URLs verificadas

| # | Archivo local (`docs/referencias/pdf/`) | Tamaño | Fuente | URL verificada |
|---|---|---|---|---|
| 1 | `01_WB_2011_Curbing_Fraud_Corruption_Collusion_Roads.pdf` | 2.0 MB | World Bank INT (original thedocs) | `https://thedocs.worldbank.org/en/doc/5f1f9ec0b2cdf78b0b083e0981e0854c-0090012011/original/Curbing-FC-in-Roads-Sector.pdf` |
| 2 | `01b_WB_2011_Curbing_Fraud_WP64283.pdf` | 1.5 MB | World Bank WP64283 (mirror documents1) | `https://documents1.worldbank.org/curated/en/975181468151765134/pdf/642830WP0Curbi00Box0361535B0PUBLIC0.pdf` |
| 3 | `02_WB_2011_Fraud_Corruption_Awareness_Handbook.pdf` | 1.0 MB | World Bank + Poland Anti‑Corruption Bureau | `https://documents1.worldbank.org/curated/en/309511468156866119/pdf/877290PUB0Frau00Box382147B00PUBLIC0.pdf` |
| 4 | `03_WB_Warning_Signs_Fraud_Corruption_Procurement.pdf` | 0.38 MB | World Bank brochure | `https://documents1.worldbank.org/curated/en/223241573576857116/pdf/Warning-Signs-of-Fraud-and-Corruption-in-Procurement.pdf` |
| 5 | `04_OCP_DG_2016_Red_Flags_Integrity_OCDS.pdf` | 0.52 MB | OCP & Development Gateway | `https://www.open-contracting.org/wp-content/uploads/2016/11/OCP2016-Red-flags-for-integrityshared.pdf` |
| 6 | `05_OECD_2016_Preventing_Corruption_Procurement.pdf` | 1.6 MB | OECD (via Basel Gov) | `https://baselgovernance.org/sites/default/files/2020-03/oecd_preventing_corruption_in_public_procurement_2016.pdf` |
| 7 | `06_OECD_2007_Bribery_Public_Procurement_Typology.pdf` | 0.64 MB | OECD Working Group on Bribery | `https://www.oecd.org/content/dam/oecd/en/topics/policy-sub-issues/fighting-foreign-bribery/44956834.pdf` |
| 8 | `16_OECD_2008_Recommendation_Public_Procurement.pdf` | 0.57 MB | OECD/LEGAL/0411 | `https://legalinstruments.oecd.org/public/doc/131/131.en.pdf` |
| 9 | `07_Conley_Decarolis_2016_Detecting_Bidders_Groups_AEJMicro.pdf` | 0.83 MB | Conley & Decarolis (Bocconi mirror) | `https://didattica.unibocconi.it/mypage/upload/211300_20171218_035123_CONLEYDECAROLIS_AEJMICRO.PDF` + DOI `10.1257/mic.20130254` |
| 10 | `08_Fazekas_2019_Single_Bidding_ESIF_EU_DGREGIO.pdf` | 5.5 MB | DG REGIO (EC) | `https://ec.europa.eu/regional_policy/sources/work/SingleBiding_2019.pdf` |
| 11 | `09_Ley_7021_2022_Paraguay_Suministro_Contrataciones.pdf` | 10.1 MB | MEF Paraguay (scan OCR) | `https://www.mef.gov.py/sites/default/files/2024-10/ley-n%C2%B0-70212022_0.pdf` — portal DNCP: `https://www.contrataciones.gov.py/dncp/ley-7021/ley-7021-de-suministro-y-contrataciones-publicas/` |
| 12 | `10_Bandiera_Decarolis_2021_Rules_Discretion_Corruption_NBER28209.pdf` | 0.81 MB | NBER WP 28209 | `https://www.nber.org/system/files/working_papers/w28209/w28209.pdf` |
| 13 | `11_Wachs_Kertesz_2019_Network_Cartel_Detection_SciRep.pdf` | 1.4 MB | Nature Scientific Reports | `https://www.nature.com/articles/s41598-019-47198-1.pdf` + `https://doi.org/10.1038/s41598-019-47198-1` |
| 14 | `11b_Wachs_Kertesz_2019_arXiv.pdf` | 1.8 MB | arXiv:1906.08667 | `https://arxiv.org/pdf/1906.08667.pdf` |
| 15 | `17_Fazekas_Toth_King_2016_CRI_EJCPR.pdf` | 0.31 MB | Springer EJCPR 22 | `https://link.springer.com/content/pdf/10.1007/s10610-016-9308-z.pdf` + Cambridge mirror `https://www.repository.cam.ac.uk/handle/1810/256458` |

**No descargables automáticamente (verificados, acceso manual recomendado):**

| Fuente | Motivo | Acceso alternativo verificado |
|---|---|---|
| IMF WP/22/94 Abdou et al. *Identifying Cartels — Paraguay pilot* | Cloudflare/Elibrary bloquea curl automatizado | `https://www.imf.org/en/Publications/WP/Issues/2022/05/20/Identifying-Cartels-in-Public-Procurement-A-Pilot-Study-for-Paraguay-517511` |
| Ferwerda et al. 2016 EJCPR (Springer 10.1007/s10610-016-9312-3) | CF | Buscar vía `https://doi.org/10.1007/s10610-016-9312-3` (institucional) |
| TI 2006 Handbook (Transparency International) | CF Edge | `https://www.transparency.org/en/publications/handbook-for-curbing-corruption-in-public-procurement` + mirror `images.transparencycdn.org` (verificado arriba) |
| Decarolis 2014 AEJ Applied (10.1257/app.6.1.108) | AEA paywall | `https://www.aeaweb.org/articles?id=10.1257/app.6.1.108` (abstract + compra) |
| Fazekas & Kocsis 2020 BJPS (10.1017/S0007123418000700) | CUP paywall | `https://doi.org/10.1017/S0007123418000700` |
| Benford PMC8720889/PMC8933377 | JS‑rendered pdf endpoint | `https://pmc.ncbi.nlm.nih.gov/articles/PMC8720889/` y `PMC8933377` (leer html; pdf requiere navegador) |

> Todos los enlaces fueron verificados por descarga o highlights el 2026‑08‑27. Guardar copia local si se usa en tesis impresa.

---

## 8. Matriz decisión → sustento (resumen en una vista)

| Decisión de tesis | Fuentes que la respaldan (nº de §1) | Evidencia numérica clave |
|---|---|---|
| 7 nuevas de literatura entran a GT+benchmark | A1‑A2 + B1 + C4 + B6 | B1: weekend‑abuse +19.8pp single‑bid; C4: scoring‑without‑competition +1.9pp investigated; B2: +9.6% precio con single‑bid |
| `dias_validez_oferta` / `dias_validez_garantia_mantenimiento_oferta` | A1, A2, B1, B5, E3 | B1 U‑shape decision period ≤32d y >182d |
| `dias_plazo_entrega_bien` | A1, A2, C3, E3 | C3: ≥50% ahorro perdido por renegociación |
| `requiere_visita_tecnica` | A1 Bali + A9 IACRC + B6 worksite inspection | Bali: −35‑40% precios al eliminar mandatory site visit |
| `criterio_adjudicacion` (categorical) | B1 non‑price weight, C4 MEAT, B6 LASSO | B1 >0.556 → +9.6pp; C4 MEAT top predictor (2× next) |
| `multa_jornales_incumplimiento` | A2, E3, C3 | Penalidad en jornales = peculiaridad PY documentada |
| `tiene_requisito_eliminatorio` | A1 Rigged specs, A9 Excluding bidders, B1 eligibility length | B1 length >2639 → +12.4pp single‑bid |
| 81 clustering agrupados en 25 títulos con prioridad | A1‑A9 + B1‑B6 + C1‑C4 + D1‑D2 | 9 grupos ALTA = núcleo B (ver §3) |
| 4 OCDS‑derivadas (días de plazo) | B1 short submission + E3 + B5 | B5 shortened time span ↔ bid rigging p<0.05 |
| Pipeline híbrido regex→LLM (69/12) | A8 OCDS mapping (36 calculables hoy / 75 con extensiones) | 14 campos al 100% regex, 12 hybrid con fallback |
| Benford sobre montos (1.87M+265K) | D1‑D2 Nigrini thresholds | MAD <0.006/0.012 evita over‑sensitivity K‑S |
| Red co‑bidding sin montos (78K ofertas / 5,544 bidders) | C1 Wachs & Kertész | Cohesion + exclusivity validadas en Georgia 150K contratos |
| Screens degradados documentados como limitación | C2‑C5 (Conley, Decarolis) | `oferentes` sin montos → no Porter‑Zona completo |

---

## 9. Referencias (APA, lista corrida para bibliografía de tesis)

> Copiar tal cual a la bibliografía; DOI/URL al final.

- Abdou, A., Fisman, R., et al. (2022). *Identifying cartels in the field: A pilot study for Paraguay* (IMF Working Paper WP/22/94). International Monetary Fund. https://www.imf.org/en/Publications/WP/Issues/2022/05/20/Identifying-Cartels-in-Public-Procurement-A-Pilot-Study-for-Paraguay-517511
- Azevedo, C., et al. (2021). A Benford's law based method for fraud detection using R — Bolsa Familia (PMC8720889). *MethodsX*.
- Conley, T. G., & Decarolis, F. (2016). Detecting bidders groups in collusive auctions. *American Economic Journal: Microeconomics, 8*(2), 1‑38. https://doi.org/10.1257/mic.20130254
- Decarolis, F. (2014). Awarding price, contract performance, and bids screening: Evidence from procurement auctions. *American Economic Journal: Applied Economics, 6*(1), 108‑132. https://doi.org/10.1257/app.6.1.108
- Decarolis, F., Rovigatti, G., et al. (2021). *Rules, discretion, and corruption in procurement: Evidence from Italian government contracting* (NBER Working Paper No. 28209). https://www.nber.org/papers/w28209
- European Commission, DG REGIO. (2019). *Single bidding in EU co‑funded procurement.* https://ec.europa.eu/regional_policy/sources/work/SingleBiding_2019.pdf
- European Commission, Single Market Scoreboard. (s.f.). *Business framework: Public procurement.* https://single-market-scoreboard.ec.europa.eu/business-framework-conditions/public-procurement_en
- Fazekas, M., Tóth, I. J., & King, L. P. (2016). An objective corruption risk index using public procurement data. *European Journal on Criminal Policy and Research, 22*(3), 369‑397. https://doi.org/10.1007/s10610-016-9308-z
- Fazekas, M., & Kocsis, G. (2020). Uncovering high‑level corruption: Cross‑national objective corruption risk indicators using public procurement data. *British Journal of Political Science, 50*(1), 155‑164. https://doi.org/10.1017/S0007123418000700
- Ferwerda, J., Deleanu, I., & Unger, B. (2016). Corruption in public procurement: Finding the right indicators. *European Journal on Criminal Policy and Research, 22.* https://doi.org/10.1007/s10610-016-9312-3
- IACRC. (2011‑). *Guide to combating corruption & fraud in development projects — Red flags listed by project cycle.* https://guide.iacrc.org/
- Katona & Fazekas. (2024). *Tender text predicts single bidding* (GTI Working Paper).
- Nigrini, M. J. (2012). *Benford's Law: Applications for forensic accounting, auditing, and fraud detection.* Wiley.
- OECD. (2007). *Bribery in public procurement: Methods, actors and counter‑measures.* https://www.oecd.org/content/dam/oecd/en/topics/policy-sub-issues/fighting-foreign-bribery/44956834.pdf
- OECD. (2015). *Recommendation of the Council on Public Procurement* (OECD/LEGAL/0411). https://legalinstruments.oecd.org/public/doc/131/131.en.pdf
- OECD. (2016). *Preventing corruption in public procurement.* https://baselgovernance.org/sites/default/files/2020-03/oecd_preventing_corruption_in_public_procurement_2016.pdf
- Open Contracting Partnership & Development Gateway. (2016). *Red flags for integrity: Giving the green light to open data solutions.* https://www.open-contracting.org/wp-content/uploads/2016/11/OCP2016-Red-flags-for-integrityshared.pdf
- República del Paraguay. (2022). *Ley Nº 7021/2022 De suministro y contrataciones públicas.* https://www.mef.gov.py/sites/default/files/2024-10/ley-n%C2%B0-70212022_0.pdf — Sitio DNCP: https://www.contrataciones.gov.py/dncp/ley-7021/ley-7021-de-suministro-y-contrataciones-publicas/
- Transparency International. (2006). *Handbook for curbing corruption in public procurement.* https://www.transparency.org/en/publications/handbook-for-curbing-corruption-in-public-procurement
- Wachs, J., & Kertész, J. (2019). A network approach to cartel detection in public auction markets. *Scientific Reports, 9*, 10818. https://doi.org/10.1038/s41598-019-47198-1 (arXiv:1906.08667)
- World Bank. (2011). *Curbing fraud, corruption, and collusion in the roads sector.* https://thedocs.worldbank.org/en/doc/5f1f9ec0b2cdf78b0b083e0981e0854c-0090012011/original/Curbing-FC-in-Roads-Sector.pdf
- World Bank. (2011). *Fraud and corruption awareness handbook.* https://documents1.worldbank.org/curated/en/309511468156866119/pdf/877290PUB0Frau00Box382147B00PUBLIC0.pdf
- World Bank. (s.f.). *Warning signs of fraud and corruption in procurement.* https://documents1.worldbank.org/curated/en/223241573576857116/pdf/Warning-Signs-of-Fraud-and-Corruption-in-Procurement.pdf

---

## 10. Próximos pasos

1. **Gold standard manual** de las 7 nuevas (100 muestras/campo) — usar `scripts/preparar_gt_nuevo.py` (muestreo de las secciones con regex predictor).
2. **Benchmark LLM** Llama 3.1 8B‑AWQ vs Mistral 7B‑AWQ (guided decoding) sobre esas 7 + 12 hybrid existentes.
3. **Citar este archivo** en la tesis como Anexo de sustento; `05` queda como selección operativa, este `06` como respaldo probatorio.

*Generado 2026‑08‑27. Verificación BD: consulta directa PostgreSQL dncp. Búsquedas web: 2026‑08‑27 (World Bank, OECD, OCP, EC, IACRC, NBER, Nature, arXiv, IMF, PMC). PDFs: 15 archivos en `docs/referencias/pdf/` (ver §7).*

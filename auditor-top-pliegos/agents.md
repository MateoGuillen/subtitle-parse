# Auditoría de Variables - Isolation Forest para Contrataciones Públicas (Paraguay)

## Contexto General
- **Archivo analizado**: `campos_tesis_isolation_forest_clasificacion.csv`
- **Metodología**: Basada en `auditoria_variables_isolation_forest_contrataciones_publicas.md`
- **Objetivo**: Clasificar variables según taxonomía de Fazekas et al. (2016) adaptada, identificar relevancia para Isolation Forest, documentar sesiones y decisiones para replicabilidad

## Estructura del CSV
- **Columnas**: field, index_in_field, repeat_count, nro_licitacion, title_normalized, content_text, value, field_type, scope_corruption, correlation_corruption, anomaly_risk, anomaly_features, preview
- **Campo principal**: `pct_garantia_cumplimiento` (porcentaje de garantía de cumplimiento)
- **Registros**: ~187+ entradas con 3 nro_licitacion distintos (434669, 469279, 405512)
- **Secciones documentales (preview)**: 
  1. "Margen de Preferencia Local - CPS" (nro_licitacion 434669)
  2. "Cobertura de Seguro de los bienes" (nro_licitacion 469279)
  3. "Derechos Intelectuales" (nro_licitacion 405512)
- **Valores**: 10.0000 o 5.0000 (porcentaje), field_type: "porcentaje"
- **Anomalía actual**: Todas marcadas como `out_of_range` con `anomaly_features: ['out_of_range']`

## FASES DE TRABAJO

### FASE 1: Inventario y Clasificación Taxonómica ✅ COMPLETADA
**Objetivo**: Identificar todas las variables únicas (title_normalized), clasificarlas en la taxonomía de 8 categorías, inferir tipo de dato, y asignar relevancia para Isolation Forest (Alta/Media/Baja/Descartar).

**Estado**: Completa - 125 variables únicas clasificadas en 8 categorías taxonómicas

**Entregable**: `contexto_auditor/fase1_clasificacion_taxonomica.md`

### FASE 2: Evaluación de Relevancia para Isolation Forest ✅ COMPLETADA
**Objetivo**: Para cada variable, justificar relevancia con 3 criterios: (a) respaldo bibliográfico, (b) expresión como valor extremo/raro vs. contextual, (c) redundancia. Recomendar preprocesamiento.

**Estado**: Completa - Las 125 variables evaluadas por relevancia ISF:
- 10 variables **Alta** relevancia
- 22 variables **Media** relevancia
- 16 variables **Baja** relevancia
- 17 variables **Descartar** relevancia

**Entregable**: `contexto_auditor/fase2_relevancia_isolation_forest.md`

### FASE 3: Variables Ausentes y Configuración del Modelo ✅ COMPLETADA
**Objetivo**: Identificar red flags de la Sección B no presentes, sugerir derivación o solicitud. Configuración inicial Isolation Forest (hiperparámetros, umbral).

**Estado**: Completa - Identificando 6 red flags críticos no presentes + configurando hiperparámetros ISF:
- R1: `cantidad_oferentes` (puesto #1 ranking ISF) - derivable de `ofertentes en consorcio` + conteo texto
- R2: `monto_adjudicado` - derivable de `value` + precio base del pliego
- R3: `%_desviacion` (puesto #2 ranking ISF, red flag oficial DNCP) - derivable de `analisis precios` + precio referencial
- R4: `modalidad_contratacion` - derivable de `criterios adjudicacion` + palabras clave
- R5: `historial_alertas_entidad` - requiere solicitud a DNCP
- R6: `antigüedad_empresa` - requiere solicitud a DNCP

**Configuración ISF sugerida**: `n_estimators=100`, `max_samples=256`, umbral basado en percentil 99 (1% superior de puntajes)
- Conjunto base: 10 variables Alta relevancia para modelo inicial
- 22 variables Media como conjunto ampliado

**Entregable**: `contexto_auditor/fase3_configuracion_modelo.md`

### FASE 4: Limitaciones y Cierre ✅ COMPLETADA
**Objetivo**: Documentar limitaciones éticas/metodológicas, falsos positivos, sesgo de disponibilidad, umbral arbitrario.

**Estado**: Completa - 10 limitaciones metodológicas + 3 consideraciones éticas + 10 limitaciones de datos documentadas para el capítulo de limitaciones de la tesis

**Entregable**: `contexto_auditor/fase4_limitaciones_cierre.md`

### FASE 5: Mapeo a Base de Datos dncp + Lluvia de Ideas de Variables Candidatas ✅ COMPLETADA
**Objetivo**: Cruzar las 125 variables teóricas con la base real `dncp` (26 tablas, 16+ principales) y generar ranking de variables candidatas para ISF por licitación.

**Estado**: Completa - Resueltos 5 de 6 red flags (R1-R5 por cómputo; R6 antigüedad_empresa no disponible), las 10 variables Alta mapeadas 1:1 a columnas reales, 3 bloques finales:
- 🥇 **Bloque A** (núcleo validado por literatura): n_oferentes_distintos, es_unico_oferente, precio_vs_estimado, overbudget_ratio, metodo_contratacion, duracion_oferta_dias/bidding_urgency, n_enmiendas/enmienda_ratio, winner_total_contracts/is_repeat_winner, winner_category_frequency, n_consultas_llamado
- 🥈 **Bloque B** (estructural del pliego document_features_v2): pct_interes_mora_diario, pct_multa_diaria, anos_experiencia_*, dias_validez_garantia, dias_reposicion, dias_pago_maximo, ratio_liquidez/endeudamiento, pct_garantia_cumplimiento, num_categorias_documentos, pct_diferencia_precio_*
- 🥉 **Bloque C** (derivadas OCDS nuevas): demora_media_pago_dias (pagos_contrato), pago_vs_contrato_ratio, total_multas/has_multas, n_protestas, tiempo_respuesta_consultas, n_unspsc_distintos, proveedor_region

**Limitaciones clave del cruce**: bidder_diversity/oferentes_por_item constantes (bug cantidad_items=0), costo_pliego/presupuesto/garantia vacíos, proveedores.tamanio y convocantes.region vacíos, R6 no disponible.

**Entregable**: `contexto_auditor/fase5_mapeo_bd_candidatos.md`

### FASE 6: Sustento Académico del Corpus ✅ COMPLETADA
**Objetivo**: Verificar vía web que cada afirmación central de Fases 1-5 tenga respaldo académico real (sin fuentes inventadas).

**Estado**: Completa - ✅ **Todas las afirmaciones verificadas** con 12 fuentes confirmadas y citadas en APA:
- Marco corrupción: Fazekas, Tóth & King (2016, EJCPR); Fazekas & Kocsis (2020, BJPS 50(1)); Abdou, Basdevant, David-Barrett & Fazekas (2022, IMF WP 2022/094 — **incluye Paraguay** como país piloto); David-Barrett & Fazekas (2016, GTI-WP 2016:02); Wittberg & Fazekas (2023, Public Choice); OECD (2025 Guidelines for Fighting Bid Rigging)
- Algoritmo/anomalías: Liu, Ting & Zhou (2008, ICDM); Liu, Ting & Zhou (2012, ACM TKDD); Chandola, Banerjee & Kumar (2009, ACM CSUR); Carletti, Terzi & Susto (2020, DIFFI arXiv:2007.11117); Herreros-Martínez et al. (2025, Information 16(3):177)
- Precedente hispanoamericano: Torres Berru (2023, Tesis doctoral Universidad de Salamanca — SERCOP Ecuador)

**Ranking resultante por respaldo académico**: oferta única/n° de oferentes (4 fuentes) > desviación de precio (2) > modalidad directa (2) > recurrencia ganador (2) > plazo corto/urgencia (2) > adendas (2) > retrasos de pago (emergente, Wittberg 2023).

### FASE 6b: Ampliación del corpus con 8 fuentes adicionales verificadas ✅ COMPLETADA
**Objetivo**: Enriquecer el sustento académico con búsquedas de fuentes extra (autorías y DOIs confirmados en línea).

**Estado**: Completa - 8 referencias nuevas (total corpus: **20 fuentes verificadas**):
- **Paraguay/mismo caso**: Kehler, Paciello & Pane (2019, AMW/CEUR-2369 — ISF sobre **OCDS de Paraguay**, >90% validado con protestas); Auriol, Straub & Flochel (2016, World Development 77:395-407 — rent-seeking en **Paraguay**, contratos sin competencia en entidades corruptas)
- **Red flags del TEXTO de pliegos (Bloque B)**: Decarolis & Giorgiantonio (2022, EPJ Data Science 11:16 — red flags de llamados italianos; ⚠️ matiz: urgencia negativamente correlacionada tras escrutinio → refuerza DIFFI/SHAP); Lima et al. (2020, Findings EMNLP — Bi-LSTM sobre texto de 15M registros brasileños)
- **Colusión/competencia**: Wallimann, Imhof & Huber (2023, Computational Economics 62(4):1669-1720 — screens + RF, carteles incompletos)
- **Early warning / OCDS**: Gallego, Rivero & Martínez (2021, Int. J. of Forecasting 37(1):360-377 — **2M contratos SECOP Colombia**, extensiones = red flag de eficiencia); Dos Santos et al. (2025, EPJ Data Science 14:52 — SLM de 93 estudios)
- **Validación ISF**: Majilana (2025, IJCSMC 14(7) — POTRAZ Zimbabue, precision 94.6%/recall 54.7%)

**Efecto en ranking**: oferta única → **6 fuentes**; adendas → **3 fuentes**; **Bloque B pasa de apoyo indirecto a directo**; caso Paraguay → **3 referencias** (Abdou 2022, Kehler 2019, Auriol 2016).

**Entregable**: `contexto_auditor/fase6_sustento_academico.md` (Sección 6)

### FASE 6c: Fuentes OCDS + Pliegos (la tabla extra sobre el estándar) ✅ COMPLETADA
**Objetivo**: Documentar con fuentes el argumento estructural de la tesis — OCDS estructurado + tabla extra de pliegos (texto no estandarizado extraído con regex+LLM) para detectar anomalías.

**Estado**: Completa - **todas las fuentes confirmadas** (0 pendientes), total **27 fuentes únicas verificadas**:
- **Katona & Fazekas (2024, GTI-WP 2024:01)**: el TEXTO de los llamados predice oferta única mejor que solo variables estructuradas (77% → 82%, Hungría, 119K contratos). Validación empírica directa de la arquitectura regex+LLM sobre pliegos.
- **OCP & Development Gateway (2016), "Red flags for integrity"**: de 60 red flags, **36 son computables en el esquema OCDS actual y 75 requieren campos context-specific**. La tabla `pliegos_secciones`/`document_features_v2` implementa esos campos faltantes para Paraguay. Incluye concepto de "triangulación" (Fazekas, Cingolani & Tóth).
- **World Bank (2011)**: "rigged specifications" y "tailored specifications" como red flags oficiales — conecta campos del pliego (anos_experiencia_minima, ratio_liquidez, num_categorias_documentos) con el esquema de specs manipuladas.
- **Ortega Nieto, Fazekas, Vaz Mondo, Tóth & Braem Velasco (2023, GRAS World Bank)**: 60 red flags en 4 grupos implementados en Brasil; single bidding como red flag más usado. Referencia operativa moderna que justifica los bloques.
- **World Bank INT (2020), "Warning signs of fraud and corruption in procurement"**: señales oficiales conectadas a winner_total_contracts, enmiendas, pct_diferencia_precio_*.
- Reemplazo de la entrada "World Bank 2021 pendiente" por la GRAS 2023 (confirmada) — sin fuentes no verificadas.

**Entregable**: `contexto_auditor/fase6_sustento_academico.md` (Sección 7)

### FASE 6d: Diccionario de datos completo + muestras de la BD ✅ COMPLETADA
**Objetivo**: Documentar tabla por tabla y columna por columna las 25 tablas de la base `dncp` con su significado en licitaciones paraguayas, y generar CSV de muestras reales con texto acompañante.

**Estado**: Completa
- **Diccionario completo**: `contexto_auditor/diccionario_base_datos.md` — 25 tablas, cada columna con significado en contratación paraguaya (OCDS v1.1), bloques A/B/C de la tesis, y **calidad de datos** de cada tabla (bugs conocidos: `contratos.cantidad_enmiendas`=0, `licitaciones.cantidad_items`=0, `convocantes.region` vacío, `bidder_diversity`=0.0, `pct_anticipo`/`num_componentes_formula`=0, `bidding_urgency` escala atípica).
- **Muestras OCDS**: `data/processed/isf_candidatos/muestras_ocds.csv` — 4 licitaciones reales (oferta única 412142, varios oferentes 437443, con enmiendas 408087, pliego restrictivo 473963) × 34 cols (OCDS + derivadas económicas).
- **Muestras pliegos por campo**: `data/processed/isf_candidatos/muestras_pliegos_por_campo.csv` — **5 muestras por campo × 13 campos** con el **texto de la sección** que da origen a la variable (65 filas). Campos: dias_pago_maximo, dias_validez_garantia, pct_garantia_cumplimiento, dias_presentacion_garantia, dias_solicitud_anticipo, dias_pago_anticipo, anos_experiencia_minima, num_referencias_minimo, num_categorias_documentos, ratio_liquidez_minimo, pct_interes_mora_diario, pct_anticipo, num_componentes_formula.

**Entregable**: `contexto_auditor/diccionario_base_datos.md` + 2 CSV en `data/processed/isf_candidatos/`

---

## RESUMEN EJECUTIVO DEL TRABAJO REALIZADO

**Objetivo cumplido**: Auditar y clasificar 125 variables extraídas de pliegos de bases y condiciones de contrataciones públicas paraguayas, clasificadas según la taxonomía de Fazekas et al. (2016) adaptada, evaluar su relevancia para la detección de anomalías con Isolation Forest, mapearlas a la base real `dncp` y verificar su sustento académico.

**Hallazgos principales**:
1. **125 variables únicas** identificadas en el CSV, agrupadas en 8 categorías taxonómicas (Competencia/Concurrencia, Económica/Precio, Procedimental, Proveedor, Entidad contratante, Temporal, Ejecución contractual, Conexión Política)
2. **Graduación de relevancia ISF**: 10 variables Alta, 22 Media, 16 Baja, 17 Descartar
3. **6 red flags críticos** de la Sección B del documento metodológico no presentes en los datos identificados (R1-R6), con derivaciones propuestas; **5 de 6 resueltos con la BD completa**
4. **Configuración ISF recomendada**: `n_estimators=100`, `max_samples=256`, umbral basado en percentil 99
5. **10 limitaciones metodológicas** y **3 consideraciones éticas** documentadas para el capítulo de limitaciones de la tesis
6. **Marco reproducible** creado para guiar a futuros investigadores en el trabajo continuo
7. **Ranking final de 3 bloques** (A núcleo validado, B estructural pliego, C derivadas OCDS) listo para el modelado ISF por licitación
8. **12 fuentes académicas verificadas** que respaldan el corpus completo (incluida la validación directa del caso Paraguay)

**Archivos generados en `contexto_auditor/`**:
- `fase1_clasificacion_taxonomica.md` - Clasificación de 125 variables en 8 categorías
- `fase2_relevancia_isolation_forest.md` - Evaluación de relevancia ISF con 3 criterios por variable
- `fase3_configuracion_modelo.md` - Variables ausentes y configuración de modelo
- `fase4_limitaciones_cierre.md` - Limitaciones metodológicas y éticas para tesis
- `fase5_mapeo_bd_candidatos.md` - Mapeo variables→BD dncp + ranking 3 bloques (A/B/C)
- `fase6_sustento_academico.md` - Verificación de sustento académico con 12 referencias APA
- `agents.md` - Contexto general y registro de decisiones para continuidad del trabajo

**Próximos pasos recomendados**:
1. Crear `data/processed/isf_candidatos/features_isf.csv` (matriz licitación × Bloques A+B+C)
2. Ejecutar ISF piloto con Bloque A y contrastar ranking empírico (DIFFI/SHAP) vs ranking teórico
3. Añadir Bloque C (derivadas OCDS) y medir contribución
4. Redactar capítulo de marco teórico usando las referencias APA de la Fase 6
5. Solicitar red flag restante a DNCP/SIE (antigüedad_empresa, R6)
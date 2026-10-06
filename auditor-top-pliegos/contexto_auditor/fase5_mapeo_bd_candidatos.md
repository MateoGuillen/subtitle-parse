# FASE 5: Mapeo a Base de Datos dncp + Lluvia de Ideas de Variables Candidatas para Isolation Forest

**Fecha**: Sesión 17 — cruce del directorio `auditor-top-pliegos/` (125 variables / 8 categorías taxonómicas / ranking ISF) con la base real `dncp`.

**Contexto**: El caso de estudio ahora NO trabaja solo con el CSV de 3 pliegos (pct_garantia_cumplimiento), sino con los pliegos de bases y condiciones **ya estructurados** en `dncp.document_features_v2` (31,321 × 108 columnas) cruzados con las tablas OCDS y la tabla derivada `dncp.document_economic_features` (29,811 × 46 columnas).

---

## 1. Resolución de los 6 red flags "ausentes" (R1–R6) de la Fase 3

La Fase 3 indicó que 6 red flags críticos de la Sección B **no estaban presentes** en el CSV de 3 pliegos y debían solicitarse a DNCP. **Con la base de datos completa, 5 de 6 ya están resueltos por cómputo directo y con cobertura alta:**

| Red flag (Fase 3) | Puesto ISF | Columna real en dncp | Cobertura | Estado |
|---|---|---|---|---|
| R1. cantidad_oferentes | #1 | `licitaciones.cantidad_oferentes` | 93.1% (27,758/29,811) | ✅ Resuelto |
| R1. oferta única | #1 | `document_economic_features.es_unico_oferente` | 100% | ✅ Resuelto + derivado |
| R1. n oferentes distintos | #1 | `document_economic_features.n_oferentes_distintos` | 100% | ✅ Resuelto |
| R2. monto_adjudicado | #2 | `document_economic_features.monto_adjudicado` | 100% | ✅ Resuelto |
| R3. %_desviacion precio | #2 | `document_economic_features.precio_vs_estimado` | 100% | ✅ Resuelto |
| R3. sobreprecio | #2 | `document_economic_features.overbudget_ratio`, `is_high_value_single_bidder` | 100% | ✅ Resuelto |
| R4. modalidad_contratacion | #3 | `licitaciones.metodo_contratacion` (direct/open), `criterio_adjudicacion` (priceOnly/ratedCriteria) | 99.99% | ✅ Resuelto |
| R5. historial_alertas_entidad | lado demanda | `document_economic_features.winner_category_frequency`, `winner_total_contracts`, `is_repeat_winner` | 100% | ✅ Parcial (Proveedor, no entidad) |
| R6. antigüedad_empresa | lado proveedor | — (proveedores.tamanio vacío; sin fecha de constitución) | 0% | ❌ No disponible |

**Nuevo hallazgo**: la derivación "R1 cantidad_oferentes desde texto" ya no es necesaria — la tabla `oferentes` tiene 78,695 filas con los proveedores que se registraron por licitación (`n_oferentes_distintos` agregado por licitación en `document_economic_features`).

---

## 2. Mapeo de las 10 variables "Alta relevancia" de la Fase 2 → columnas reales

| Variable auditor (Fase 2 Alta) | Categoría | Columna real en dncp | Disponible |
|---|---|---|---|
| analisis de precios ofertados | Económica/Precio | `pct_diferencia_precio_bajo`, `pct_diferencia_precio_alto` (v2) + `precio_vs_estimado`, `overbudget_ratio` (eco) | ✅ en v2 y eco |
| oferentes en consorcio | Competencia | `n_oferentes_distintos`, `es_unico_oferente` (eco) | ✅ |
| criterios de adjudicacion | Procedimental | `binary_multiplicidad_criterios`, `binary_criterio_menor_precio` (v2) + `criterio_adjudicacion` (licitaciones) | ✅ |
| moneda de la oferta y pago | Económica | `moneda` en licitaciones/adjudicaciones/contratos | ✅ |
| tasa de interes por mora | Económica | `pct_interes_mora_diario` (v2, 35 valores) | ✅ |
| causales de terminacion del contrato | Ejecución | `binary_incluye_terminacion_incumplimiento/fraude/quiebra/conveniencia` (v2) | ✅ (binarias) |
| adenda | Ejecución | `n_enmiendas`, `total_monto_enmiendas`, `enmienda_ratio` (eco) + `enmiendas_contrato` (7,143) | ✅ |
| periodo validez garantía cumplimiento | Ejecución | `dias_validez_garantia` (v2) + `pct_garantia_cumplimiento` (v2) | ✅ |
| capacidad financiera | Proveedor | `ratio_liquidez_minimo`, `ratio_endeudamiento_maximo`, `binary_requiere_estados_auditados` (v2) | ✅ |
| capacidad tecnica | Proveedor | `anos_experiencia_minima`, `anos_experiencia_minima_tecnica`, `num_referencias_*` (v2) | ✅ |

**Conclusión**: las 10 variables Alta de la Fase 2 **todas** tienen columna equivalente real en `dncp`. La tesis puede operacionalizar el ranking teórico directamente con datos.

---

## 3. Mapeo de variables "Media (22)" → columnas reales (resumen)

- `experiencia requerida` → `anos_experiencia_minima`, `num_referencias_minimo` (v2)
- `periodo validez garantia bienes` → `dias_validez_garantia`, `binary_tiene_cobertura_especifica` (v2)
- `plazo reposicion bienes` → `dias_reposicion`, `binary_dias_habiles` (v2)
- `idioma oferta y contrato` → no en v2 (texto en `pliegos_secciones.title_normalized="idioma de la oferta e idioma del contrato"`); derivable
- `condiciones de participacion` → `criterio_elegibilidad` (licitaciones) + `binary_requiere_registro_proveedores` (v2)
- `requisitos doc. capacidad financiera/tecnica/experiencia` → `num_categorias_documentos`, `binary_requiere_certificado_tributario`, `binary_requiere_certificado_ips` (v2)
- `notificaciones/aclaracion/difusion (Baja → descartar)` → se **regeneran como variables numéricas** del lado proceso: ver Sección 4 (consultas, enmiendas)

---

## 4. LLUVIA DE IDEAS — Nuevas variables candidatas derivadas del cruce con la BD (más allá de los pliegos)

Variables accesibles solo por el cruce con tablas OCDS que **no existen en los pliegos** y que la literatura lista como red flags:

### 4.1 Competencia/Concurrencia (categoría 1)
| Variable candidata | Fuente | Cobertura | Nota ISF |
|---|---|---|---|
| `n_oferentes_distintos` | eco | 100% | Núcleo ISF; vallado directo |
| `es_unico_oferente` (bin) | eco | 100% | Oferta única = proxy #1 Fazekas |
| `oferentes_por_item` | eco | ⚠️ CONST (1 valor) | ❌ no usable — `cantidad_items`=0 (bug fuente) |
| `bidder_diversity` | eco | ⚠️ CONST (0.0) | ❌ no usable (mismo bug) |

### 4.2 Económica/Precio (categoría 2)
| Variable candidata | Fuente | Cobertura | Nota ISF |
|---|---|---|---|
| `precio_vs_estimado` | eco | 100% | Desviación % adjudicado vs estimado |
| `overbudget_ratio` | eco | 100% | Ratio sobrepresupuesto |
| `contract_value_ratio` | eco | 100% | Valor contrato / estimado |
| `monto_estimado` | eco + licitaciones | 99.4% | Log-transform por cola pesada |
| `costo_pliego` | licitaciones | ❌ 0% (costo_pliego vacío) | intentar derivar de `items_licitacion` |
| `costo_pliego_postext` | derivable de v2 | — | si un campo del pliego lo explicita |

### 4.3 Procedimental/proceso (categoría 3 — regenera las "Baja/Descartar" como numéricas útiles)
| Variable candidata | Fuente | Cobertura | Nota ISF |
|---|---|---|---|
| `bidding_urgency` | eco | 100% | Urgencia del proceso (duración oferta) |
| `duracion_oferta_dias` | licitaciones | 100% | Plazo publicación→apertura; OCDE colusión |
| `n_consultas_llamado` | consultas_llamado | ~12,353 licit. | ⚠️ 0 para las demás (1–950, media ~10) |
| `tiempo_respuesta_consultas` (promedio días) | consultas_llamado | 119,740 con respuesta | Pocas consultas+rápido = sospechoso |
| `metodo_contratacion` (direct/open) | licitaciones | 99.99% | Categórica baja cardinalidad → one-hot |
| `criterio_adjudicacion` | licitaciones | 99.99% | priceOnly vs ratedCriteria |

### 4.4 Ejecución contractual / post-adjudicación (categoría 7) — solo disponible por OCDS
| Variable candidata | Fuente | Cobertura | Nota ISF |
|---|---|---|---|
| `n_enmiendas` | eco | 100% (0–23) | Adendas = mecanismo de corrección |
| `enmienda_ratio` | eco | 100% | Monto enmiendas / contrato |
| `n_pagos` / `total_pagado` | eco | 100% | Fluid ratio contrato |
| `pago_vs_contrato_ratio` | eco | 100% | Ejecución real vs firmado |
| `demora_media_pago_dias` (solicitud→pago) | **pagos_contrato** | 276,485 pares | **NUEVA** — retrasos de pago = red flag |
| `n_multas` / `total_multas` / `has_multas` | eco | 100% | Sanciones por incumplimiento |
| `avg_retencion` | eco | 100% | Retenciones IVA/renta/DNCP |

### 4.5 Proveedor (categoría 4) — reconstruida por agregación
| Variable candidata | Fuente | Cobertura | Nota ISF |
|---|---|---|---|
| `winner_category_frequency` | eco | 100% (1–85) | Recurrencia proveedor en categoría |
| `winner_total_contracts` | eco | 100% (1–129) | Concentración proveedor |
| `is_repeat_winner` (bin) | eco | 100% | Gana repetidamente en la misma entidad |
| `proveedor_region` geográfica | proveedores.region (18 regiones) | 8,491 | Categórica; match proveedor-convocante |

### 4.6 Temporal (categoría 6)
| Variable candidata | Fuente | Cobertura | Nota ISF |
|---|---|---|---|
| `year` / mes de publicación | licitaciones | 2003–2025 | Estacionalidad cierre fiscal |
| `duracion_contrato_dias` | eco | 100% | Extremos cortos/largos |
| `duracion_consultas_dias` | eco | 100% | Plazo consultas |

### 4.7 Protestas (categoría 7 — nuevo)
| Variable candidata | Fuente | Cobertura | Nota ISF |
|---|---|---|---|
| `has_protesta` / `n_protestas` | eco | 967 licit. con protesta | La protesta de un perdedor = señalizo |

### 4.8 Variables estructurales del pliego (document_features_v2) útiles para ISF
Las 108 columnas v2 son 100% pobladas (todo documento tiene valor). Mapa por categoría:
- **Económica**: `pct_garantia_cumplimiento` (5 valores), `pct_anticipo` (CONST 1 valor ❌), `pct_interes_mora_diario` (35), `pct_multa_diaria` (35)
- **Proveedor/requisitos**: `anos_experiencia_minima` (13), `anos_experiencia_minima_tecnica` (20), `num_referencias_minimo` (9), `num_referencias_tecnicas_minimo` (6), `ratio_liquidez_minimo` (8), `ratio_endeudamiento_maximo` (3), `num_categorias_documentos` (6), `num_documentos_pago` (7)
- **Procedimental**: `binary_multiplicidad_criterios`, `binary_criterio_menor_precio`, `binary_contrato_abierto` (CONST ❌), `categorical_tipo_garantia` (3), `num_componentes_formula` (CONST ❌)
- **Plazos/días**: `dias_presentacion_garantia`, `dias_validez_garantia` (25), `dias_reposicion` (21), `dias_pago_maximo` (15), `dias_solicitud_anticipo` (13), `dias_pago_anticipo` (13), `dias_aceptacion_rechazo_factura` (3)
- **Binarias de terminación/responsabilidad**: 20+ `binary_*` (cierre, fraude, quiebra, conveniencia, IVA, exoneraciones, etc.) → cargan de información dumónica de riesgo

**⚠️ Columnas v2 CONSTANTES (no aportan a ISF — descartar)**: `dias_notificacion_fuerza_mayor` (1 valor), `binary_incluye_exoneraciones` (1), `binary_prohibe_reproduccion_no_autorizada` (1), `num_componentes_formula` (1), `binary_criterio_menor_precio` (1), `binary_contrato_abierto` (1), `binary_reserva_disminuir_cantidad` (1), `pct_anticipo` (1).

---

## 5. RANKING FINAL CANDIDATO — Variables para Isolation Forest (caso de estudio)

Tres bloques combinables en el modelo (perfil por **licitación**, clave `nro_licitacion`):

### 🥇 Bloque A — Núcleo validado por literatura (10–12 vars)
1. `n_oferentes_distintos` (competencia)
2. `es_unico_oferente` (competencia)
3. `precio_vs_estimado` (precio) — log/abs
4. `overbudget_ratio` (precio)
5. `metodo_contratacion` one-hot (procedimiento)
6. `bidding_urgency` / `duracion_oferta_dias` (temporal)
7. `n_enmiendas` + `enmienda_ratio` (ejecución)
8. `winner_total_contracts` / `is_repeat_winner` (proveedor)
9. `wan_categoría_frequency` (proveedor-categoría)
10. `n_consultas_llamado` (proceso)

### 🥈 Bloque B — Estructural del pliego (document_features_v2)
11. `pct_interes_mora_diario`; 12. `pct_multa_diaria`; 13. `anos_experiencia_minima`; 14. `anos_experiencia_minima_tecnica`; 15. `dias_validez_garantia`; 16. `dias_reposicion`; 17. `dias_pago_maximo`; 18. `ratio_liquidez_minimo`; 19. `ratio_endeudamiento_maximo`; 20. `pct_garantia_cumplimiento`; 21. `num_categorias_documentos`; 22. `pct_diferencia_precio_*`

### 🥉 Bloque C — Nuevas derivadas de OCDS (recomendadas como aporte)
23. `demora_media_pago_dias` (NUEVA, de pagos_contrato)
24. `pago_vs_contrato_ratio`
25. `total_multas` / `has_multas`
26. `n_protestas`
27. `tiempo_respuesta_consultas` (avg días)
28. `n_unspsc_distintos` (heterogeneidad de ítems — derivable de items_licitacion)
29. `proveedor_region` (si se cruza convocante/proveedor geográfico)

### Configuración ISF (heredada de Fase 3/academia)
- `n_estimators=100`, `max_samples=256`, umbral = percentil 99 (1% superior de puntajes)
- Preproceso: imputar nulos solo para Bloque B (v2 es 100%), log-transform para montos/distribuciones cola pesada, one-hot para categóricas de baja cardinalidad, target encoding (mediana/moda) para proveedor/categoría.
- **Interpretabilidad**: SHAP (local) o DIFFI (económico) para contrastar ranking empírico vs este ranking teórico → aporte de tesis.

---

## 6. Limitaciones detectadas en el cruce (para el capítulo de limitaciones)

1. **`bidder_diversity` y `oferentes_por_item` = constantes** (bug fuente: `cantidad_items`=0 en licitaciones) → excluidas.
2. **`withher cantidad_oferentes` 93%**: 2,053 licitaciones sin conteo (candidatas a "no publicado" = posible señal en sí misma).
3. **`costo_pliego`, `presupuesto_monto`, `garantia_porcentaje` = vacíos** en licitaciones → no usables.
4. **`proveedores.tamanio` vacío**, `convocantes.region` vacío → geografía parcial (solo proveedor).
5. **`antigüedad_empresa` (R6) no disponible** → permanece como red flag a solicitar a DNCP/SIE.
6. **Documentos no del pliego** (ej. `pct_anticipo` CONST) aportan 0 varianza → sacar del modelo.
7. **unidad de análisis**: aunque hay 31,321 filas v2, la unidad correcta es **licitación** (29,811) — merge 1:1 por `nro_licitacion`; `oferentes` y `pagos` requieren agregación previa por licitación.

---

## Próximos Pasos Sugeridos
1. Crear `data/processed/isf_candidatos/features_isf.csv` — matriz licitación × (Bloque A+B+C con agregaciones ya realizadas).
2. Correr Isolation Forest piloto con Bloque A (núcleo) y comparar ranking.
3. Aplicar DIFFI/SHAP → tabla de importancia por bloque.
4. Añadir Bloque C (derivadas OCDS) → comparar qué aportan.
5. Documentar en `docs/` informe ISF.

**Archivo generado**: `auditor-top-pliegos/contexto_auditor/fase5_mapeo_bd_candidatos.md`
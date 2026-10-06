# Resumen: variables candidatas para detectar anomalías en licitaciones (Isolation Forest)

> Documento de lectura rápida (menos técnico). Explica qué variables podemos usar para detectar licitaciones "sospechosas" con el algoritmo **Isolation Forest**, combinando lo que está en los **pliegos** (texto convertido a números con **regex + LLM**) y lo que ya tenemos calculado en la **base de datos**.

---

## 1. La idea en una frase

Cada licitación pública nos deja su "huella" en dos lugares: el **pliego de bases y condiciones** (texto libre que convertimos a números con regex y LLM) y las **tablas de datos** (montos, fechas, oferentes, pagos). El Isolation Forest busca licitaciones cuya huella es **muy rara** comparada con las demás — eso es lo que llamamos *anomalía* y que la literatura asocia a riesgo de corrupción.

---

## 2. De dónde salen las variables

### A. Desde los pliegos (texto → números con regex + LLM)
Los pliegos tienen secciones que repiten el mismo formato en casi todos los llamados. De esa repetición sacamos números comparables:

- **Porcentajes**: garantía de cumplimiento, interés por mora, multa diaria, anticipo, diferencia de precio.
- **Ratios financieros exigidos**: liquidez mínima, endeudamiento máximo.
- **Plazos en días**: validez de garantía, reposición de bienes, pago máximo, solicitud/pago de anticipo, aceptación de factura, presentación de garantía.
- **Requisitos de experiencia**: años mínimos, número de referencias, categorías de documentos exigidos.
- **Interruptores sí/no (binarios)**: si el pliego permite rescindir por multas, si exige certificado tributario/IPS, si incluye causales de terminación, si menciona IVA, si requiere estados auditados, etc.

Estas variables ya están calculadas en la tabla `document_features_v2` del pipeline 2, con columnas de confianza (`_conf`) que indican qué tan segura fue la extracción.

### B. Desde la base de datos (tablas OCDS + tabla económica)
Más allá del texto del pliego, la base ya tiene datos listos para usar:

| Bloque | Qué contiene | Ejemplos de columnas |
|---|---|---|
| **Licitaciones** | Datos generales del llamado | cantidad de oferentes, método de contratación, fechas, montos, criterios de adjudicación |
| **Adjudicaciones** | Quién ganó y por cuánto | monto adjudicado, fecha, proveedor |
| **Contratos** | Ejecución del contrato | monto, duración, enmiendas, pagos |
| **Oferentes** | Quiénes se presentaron | nro. de oferentes distintos por llamado |
| **Pagos** | Flujo de dinero del contrato | fechas de factura/solicitud/pago, montos, multas, retenciones |
| **Enmiendas** | Modificaciones al contrato | cantidad y monto de adendas |
| **Protestas** | Reclamos de oferentes perdedores | si hubo protesta y cuántas |
| **Consultas** | Preguntas sobre el pliego | cuántas consultas y cuánto tardaron en responder |
| **Tabla económica** (`document_economic_features`) | Un resumen ya armado por llamada | oferentes, ratios de precio, urgencia del proceso, recurrencia del ganador |

---

## 3. Las mejores variables (el top)

Aquí está la clasificación final, pensada para Isolation Forest (detecta valores raros/extremos mejor que combinaciones raras).

### 🥇 Núcleo (más respaldo en la literatura, ya calculado)
1. **Cantidad de oferentes** (`n_oferentes_distintos`) — una licitación con 1 o 2 oferentes es rara si casi todas tienen varios. Es el indicador más validado.
2. **Oferta única** (`es_unico_oferente`) — directamente "solo se presentó uno".
3. **Desvío de precio** (`precio_vs_estimado`, `overbudget_ratio`) — cuánto se desvió lo adjudicado del precio estimado. La DNCP ya alerta con +20%/-30%.
4. **Modalidad de contratación** (`metodo_contratacion`) — directa vs. abierta (las directas concentran riesgo).
5. **Urgencia del proceso** (`bidding_urgency`, `duracion_oferta_dias`) — plazos anormalmente cortos reducen competencia.
6. **Enmiendas/adendas** (`n_enmiendas`, `enmienda_ratio`) — modificar el contrato después de adjudicado es el mecanismo final de "corrección" de una mala adjudicación.
7. **Recurrencia del ganador** (`winner_total_contracts`, `is_repeat_winner`) — el mismo proveedor gana seguido a la misma entidad.

### 🥈 Estructurales del pliego (de `document_features_v2`)
8. **Interés por mora** (`pct_interes_mora_diario`) — tasas inusuales.
9. **Multa diaria** (`pct_multa_diaria`).
10. **Años de experiencia mínimos** (`anos_experiencia_minima`, `anos_experiencia_minima_tecnica`) — exigencias raras que pueden "direccionar" el pliego.
11. **Días de validez de garantía** (`dias_validez_garantia`) y **días de reposición de bienes** (`dias_reposicion`).
12. **Ratios financieros exigidos** (`ratio_liquidez_minimo`, `ratio_endeudamiento_maximo`).
13. **Porcentaje de garantía de cumplimiento** (`pct_garantia_cumplimiento`).
14. **Interruptores de riesgo** — causales de terminación, certificados exigidos, IVA, etc. (las ~20 columnas `binary_*` aportan información aunque cada una sola sea débil).

### 🥉 Derivadas nuevas de la base (solo posibles por el cruce de datos)
15. **Demora en pagos** (`demora_media_pago_dias`) — días entre solicitud y pago real (retrasos sistemáticos = red flag).
16. **Consultas sobre el pliego** (`n_consultas_llamado`) — muy pocas consultas o respuestas demasiado rápidas pueden indicar que el interesado ya conocía el pliego.
17. **Heterogeneidad de ítems** (`n_unspsc_distintos`) — cuántos rubros distintos entran en un llamado (muy pocos para un monto grande = extraño).
18. **Protestas** (`has_protesta`, `n_protestas`) — cuando el perdedor reclama.
19. **Multas en pagos** (`total_multas`, `has_multas`) — incumplimientos sancionados.
20. **Ejecución del contrato** (`pago_vs_contrato_ratio`) — cuánto se pagó realmente vs. lo firmado.

---

## 4. Cómo se clasificaron (la taxonomía)

Todas las variables se agrupan en 8 categorías (basadas en Fazekas et al. 2016, adaptadas a Paraguay):

| # | Categoría | Qué mide | Ejemplos en nuestros datos |
|---|---|---|---|
| 1 | **Competencia** | Si hubo real acceso de oferentes al mercado | oferentes, oferta única |
| 2 | **Económica/Precio** | Si el precio se desvió del valor de mercado | desvío vs. estimado, montos |
| 3 | **Procedimental** | Si el diseño del proceso fue manipulado | modalidad, urgencia, consultas |
| 4 | **Proveedor** | Riesgo del lado del que vende | recurrencia del ganador, años de experiencia |
| 5 | **Entidad contratante** | Riesgo del lado del que compra | región/historial de la entidad |
| 6 | **Temporal** | Presión de tiempo y estacionalidad | plazo oferta, demora pagos, cierre fiscal |
| 7 | **Ejecución contractual** | Manipulación después de firmar | enmiendas, protestas, multas |
| 8 | **Conexión política** | Vínculos de poder (poca disponibilidad) | — (no disponible en datos abiertos) |

---

## 5. Qué NO sirve (y por qué)

Al revisar la base completa encontramos columnas que **no aportan** al Isolation Forest porque son constantes o vacías:

| Variable | Problema |
|---|---|
| `bidder_diversity`, `oferentes_por_item` | Siempre el mismo valor (bug de `cantidad_items` en la fuente) |
| `costo_pliego`, `presupuesto_monto`, `garantia_porcentaje` | Vacías en la tabla de licitaciones |
| `pct_anticipo`, `num_componentes_formula`, `binary_contrato_abierto`, `binary_criterio_menor_precio`, `binary_reserva_disminuir_cantidad`, `dias_notificacion_fuerza_mayor`, `binary_prohibe_reproduccion_no_autorizada`, `binary_incluye_exoneraciones` | Solo tienen un valor en los datos (sin variación → no hay anomalía que detectar) |
| `antigüedad_empresa`, `prov_tamanio`, `convocantes.region` | No disponibles aún (habría que pedirlos a DNCP) |

Esto ya está documentado para que el modelo no desperdicie columnas.

---

## 6. Cómo se alimentará el modelo (en resumen)

1. **Unidad de análisis**: una fila por **licitación** (29,811 en total).
2. **Se combinan 3 bloques**:
   - Bloque A: núcleo ya calculado en `document_economic_features`.
   - Bloque B: estructuras del pliego de `document_features_v2` (las que pasaron por regex/LLM).
   - Bloque C: nuevas sumas/agregaciones de OCDS (pagos, consultas, protestas, ítems).
3. **Isolation Forest**: `n_estimators=100`, `max_samples=256`, umbral = 1% superior de puntajes.
4. **Luego**: SHAP o DIFFI para explicar *por qué* un caso salió anómalo (qué variable pesó más).
5. Recuerda: **anomalía ≠ corrupción** — es una señal para revisión humana, no una acusación.

---

*Documento base completo (técnico) en `auditor-top-pliegos/contexto_auditor/fase5_mapeo_bd_candidatos.md`.*
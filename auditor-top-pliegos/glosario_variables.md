# Glosario técnico: Contrataciones Públicas, Pliegos de Bases y Variables de la Base dncp

> Referencia completa para entender qué significa cada campo del pliego, cada columna de la base de datos, y cada término técnico del proyecto de detección de anomalías con Isolation Forest. Complementa `resumen_variables_candidatas.md` (versión simple) y `contexto_auditor/fase5_mapeo_bd_candidatos.md` (versión técnica).

**Convenciones del proyecto:**
- `regex` = extracción de texto mediante patrones (rápida, determinista).
- `hybrid` = extracción por regex + LLM (regex primero, LLM solo si falla).
- `_conf` = confianza de la extracción LLM (0.0–1.0).
- `_source` = origen de la extracción (regex / llm / none).
- Unidades: `pct_*` = porcentaje, `dias_*` = días, `ratio_*` = índice adimensional, `num_*` = conteo, `binary_*` = 0/1.

---

## 1. Términos generales de contrataciones públicas

| Término | Sigla | Significado |
|---|---|---|
| **Licitación / llamado** | — | Proceso público para seleccionar un proveedor de bienes/servicios. En nuestros datos, clave `nro_licitacion`. |
| **Pliego de bases y condiciones (PBC)** | — | Documento principal del llamado: normas, requisitos, garantías, plazos, criterios. Es la fuente de texto que convertimos a números. |
| **Convocante / unidad operativa** | UOC | Entidad pública que compra (ministerio, municipio, empresa pública, etc.). Tabla `convocantes`. |
| **Oferente** | — | Empresa o persona que presenta una oferta en la licitación. Tabla `oferentes`. |
| **Adjudicación** | — | Decisión de asignar el contrato al oferente ganador. Tabla `adjudicaciones`. |
| **Contrato** | — | Acuerdo formal firmado entre la entidad y el proveedor ganador. Tabla `contratos`. |
| **Enmienda / adenda** | — | Modificación del contrato después de firmado (monto, plazo). Tabla `enmiendas_contrato`. |
| **Anticipo** | — | Pago al proveedor antes de entregar los bienes, contra garantía. |
| **Garantía de fiel cumplimiento** | — | Porcentaje del monto contratado que el proveedor deposita como respaldo de que cumplirá. |
| **Garantía de anticipo** | — | Respaldo del anticipo recibido por el proveedor. |
| **Protesta** | — | Reclamo administrativo de un oferente perdedor contra la adjudicación. Tabla `protestas`. |
| **Monto referencial / estimado** | — | Precio base fijado por la entidad como referencia del valor del bien/servicio. |
| **Precio ofertado / adjudicado** | — | Precio propuesto por el ganador (adjudicado). |
| **Desviación de precio** | — | Diferencia porcentual entre precio adjudicado y precio referencial. La DNCP alerta si pasa **+20% / −30%**. |
| **RSD** | — | "Red flags" (banderas rojas): indicadores estadísticos asociados a riesgo de corrupción. |
| **SICP** | — | Sistema de Información de la Contratación Pública (Paraguay), donde se registran proveedores. |
| **SINARH** | — | Sistema Nacional de Recursos Humanos; `binary_verifica_sinarh` verifica si el pliego exige consultarlo. |
| **PAC** | — | Programa Anual de Contrataciones; nivel de ejecución es un red flag oficial de la DNCP. |
| **UNSPSC** | — | Código estándar internacional de clasificación de bienes y servicios (columna `unspsc_id` en ítems). |
| **Criterio de adjudicación** | — | Regla para elegir ganador: `priceOnly` (menor precio) o `ratedCriteria` (evaluación por puntaje). |
| **Modalidad / método de contratación** | — | Tipo de procedimiento: `open` (licitación pública abierta) o `direct` (contratación directa). |
| **Cierre de ejercicio fiscal** | — | Fin del año presupuestario; concentración de compras ahí = patrón de baja fiscalización. |
| **Margen de preferencia local** | CPS | Ventaja (10%) para empresas locales en comparación de ofertas. |
| **Agricultura familiar** | CPS | Cuota obligatoria de compra a pequeños productores. |
| **MIPYME** | — | Micro, pequeña y mediana empresa; `binary_anticipo_solo_mipymes` indica anticipo restrictivo a ellas. |

---

## 2. Secciones del pliego de bases y condiciones (25 títulos)

Cada título corresponde a una sección del pliego, identificada en `pliegos_secciones.title_normalized`. De cada sección extraemos campos numéricos (ver Sección 3).

| Título de sección | Qué contiene normalmente |
|---|---|
| `analisis_de_precios_ofertados` | Umbrales de diferencia de precio vs referencial, criterios de comparación de ofertas. |
| `porcentaje_de_multas` | Porcentaje de multa por atraso en entregas y si cabe rescisión por multas. |
| `fuerza_mayor` | Cláusula de fuerza mayor: plazo de notificación y obligaciones de continuar cumpliendo. |
| `porcentaje_de_garantia_de_fiel_cumplimiento_de_contrato` | % de garantía de cumplimiento y plazo (días) para presentarla. |
| `aclaracion_de_las_ofertas` | Procedimiento por el cual el comité puede pedir aclaraciones a los oferentes. |
| `experiencia_requerida` | Años mínimos, número de referencias, monto de referencia exigidos al oferente. |
| `plazo_de_reposicion_de_bienes` | Plazo en días para reponer bienes defectuosos; si se cuenta en días hábiles. |
| `documentacion_requerida_para_la_firma_del_contrato` | Documentos exigidos al ganador antes de firmar (certificados, contratos, garantías). |
| `causales_de_terminacion_del_contrato` | Causales de terminación: incumplimiento, fraude, quiebra, conveniencia. |
| `confidencialidad_de_la_informacion` | Reglas de confidencialidad: excepciones y plazo de reserva. |
| `impuestos_y_derechos` | Quién paga impuestos/aranceles (proveedor vs. entidad), IVA, exoneraciones. |
| `suministros_y_especificaciones_tecnicas` | Normas oficiales aplicables, códigos, posibilidad de cambios, responsabilidad del proveedor. |
| `derechos_intelectuales` | Cesión de derechos, prohibición de reproducción no autorizada. |
| `periodo_de_validez_de_la_garantia_de_los_bienes` | Vigencia (días) de la garantía sobre los bienes, cobertura de defectos. |
| `capacidad_tecnica` | Requisitos técnicos del oferente: años, referencias técnicas, montos. |
| `formas_y_condiciones_de_pago` | Plazo máximo de pago, días para aceptar/rechazar factura, documentos de pago, cuenta bancaria. |
| `plan_de_entrega_de_los_bienes` | Si hay plan de entrega, cronograma, documentos de embarque. |
| `capacidad_financiera` | Índices financieros exigidos (liquidez, endeudamiento), estados auditados. |
| `reajuste` | Fórmula de reajuste de precios: componentes, referencia IPC, prohibición si hay atraso. |
| `criterios_de_adjudicacion` | Método de adjudicación: menor precio, multiplicidad de criterios, contrato abierto, reservas. |
| `forma_de_instrumentacion_de_garantia_de_anticipo` | Tipo de garantía de anticipo, % de anticipo, si es obligatoria. |
| `requisitos_de_calificacion` | Requisitos legales: pasos de verificación, SINARH, declaración de conflictos, registro de proveedores. |
| `solicitud_de_pago_de_anticipo` | Condiciones y plazos (días para solicitar y para pagar) del anticipo. |
| `tasa_de_interes_por_mora` | Interés por mora en pagos de la entidad; si corre desde el día siguiente. |
| `criterios_de_desempate_de_ofertas` | Reglas para desempatar ofertas igualadas (criterios DNCP u otros específicos). |

---

## 3. Campos extraídos del pliego (`document_features_v2`) — explicados campo por campo

Agrupados por el título de sección del cual se extraen. **Todas las columnas con datos tienen `_conf` (confianza) y `_source` (origen) para los híbridos.**

### 3.1 Análisis de precios ofertados
| Campo | Tipo | Significado exacto |
|---|---|---|
| `pct_diferencia_precio_bajo` | float | Tolerancia máxima de precios **por debajo** del referencial (p. ej. el pliego dice "se aceptan ofertas hasta X% inferiores"). |
| `pct_diferencia_precio_alto` | float | Tolerancia de precios **por encima** del referencial (sobre el "y del 15% para ofertas por encima"). |
| `binary_solicita_explicacion_precios` | binary | 1 si el pliego exige al oferente explicar el desglose de su precio. |
| `binary_permite_declarar_inaceptable` | binary | 1 si el comité puede declarar una oferta inaceptable por su precio. |
| `binary_tiene_multiplicidad_criterios` | binary | 1 si el análisis de precios usa varios criterios (no solo el precio). |

### 3.2 Porcentaje de multas
| Campo | Tipo | Significado |
|---|---|---|
| `pct_multa_diaria` | float | % de multa diaria por atraso en la entrega (35 valores distintos). |
| `binary_permite_rescision_por_multas` | binary | 1 si el pliego permite rescindir el contrato si se acumulan multas. |

### 3.3 Fuerza mayor
| Campo | Tipo | Significado |
|---|---|---|
| `dias_notificacion_fuerza_mayor` | int | Días de plazo para notificar un evento de fuerza mayor. ⚠️ Constante (1 valor) — descartar para ISF. |
| `binary_incluye_ejemplos_tipicos` | binary | 1 si la cláusula enumera ejemplos típicos de fuerza mayor (desastres, guerra). |
| `binary_obliga_continuar_cumplimiento` | binary | 1 si el proveedor debe seguir cumpliendo pese al evento de fuerza mayor. |

### 3.4 Garantía de fiel cumplimiento
| Campo | Tipo | Significado |
|---|---|---|
| `pct_garantia_cumplimiento` | float | % del monto contratado exigido como garantía de cumplimiento (típicamente 5% o 10%). |
| `dias_presentacion_garantia` | int | Días desde la firma del contrato para presentar la garantía (típico: 10 días). |

### 3.5 Aclaración de las ofertas
| Campo | Tipo | Significado |
|---|---|---|
| `binary_comite_puede_solicitar_aclaracion` | binary | 1 si el comité puede pedir aclaraciones/retirar ofertas ambiguas. |
| `binary_prohibe_modificacion_sustancia` | binary | 1 si está prohibido modificar la sustancia de la oferta en la aclaración. |
| `binary_solicitud_respuesta_por_escrito` | binary | 1 si las aclaraciones se responden por escrito. |

### 3.6 Experiencia requerida
| Campo | Tipo | Significado |
|---|---|---|
| `anos_experiencia_minima` | int | Años mínimos de experiencia general exigidos al oferente (13 valores). |
| `num_referencias_minimo` | int | Número mínimo de referencias/constancias de trabajos previos (9 valores). |
| `pct_monto_referencia_minimo` | float | % del monto del contrato que debe acreditarse como experiencia previa. |
| `binary_tiene_requisitos_por_lote` | binary | 1 si los requisitos de experiencia se aplican por lote separado. |
| `binary_requiere_experiencia_especifica_marca` | binary | 1 si exige experiencia específica de una marca/producto (posible direccionamiento). |

### 3.7 Plazo de reposición de bienes
| Campo | Tipo | Significado |
|---|---|---|
| `dias_reposicion` | int | Días para reponer bienes defectuosos (21 valores). |
| `binary_dias_habiles` | binary | 1 si el plazo se cuenta en días hábiles (no corridos). |
| `binary_plazo_segun_autoridad` | binary | 1 si el plazo lo fija discrecionalmente la autoridad (no un número fijo). |

### 3.8 Documentación para firma del contrato
| Campo | Tipo | Significado |
|---|---|---|
| `num_categorias_documentos` | int | Cantidad de categorías de documentos exigidos al ganador (6 valores). |
| `binary_requiere_certificado_tributario` | binary | 1 si exige certificado de cumplimiento tributario. |
| `binary_requiere_certificado_ips` | binary | 1 si exige certificado de la seguridad social (IPS). |
| `binary_incluye_condiciones_contractuales` | binary | 1 si la sección incluye condiciones específicas del contrato. |

### 3.9 Causales de terminación del contrato
| Campo | Tipo | Significado |
|---|---|---|
| `binary_incluye_terminacion_incumplimiento` | binary | 1 si el pliego permite terminar por incumplimiento del proveedor. |
| `binary_incluye_terminacion_fraude` | binary | 1 si permite terminar por fraude/corrupción. |
| `binary_incluye_terminacion_quiebra` | binary | 1 si permite terminar si el proveedor quiebra. |
| `binary_incluye_terminacion_conveniencia` | binary | 1 si permite terminar "por conveniencia" de la entidad (unilateral). |

### 3.10 Confidencialidad de la información
| Campo | Tipo | Significado |
|---|---|---|
| `binary_tiene_excepciones_confidencialidad` | binary | 1 si hay excepciones a la confidencialidad. |
| `binary_menciona_plazo_confidencialidad` | binary | 1 si la confidencialidad tiene plazo explícito. |

### 3.11 Impuestos y derechos
| Campo | Tipo | Significado |
|---|---|---|
| `binary_responsabilidad_integral_proveedor` | binary | 1 si el proveedor asume todos los impuestos/aranceles. |
| `binary_menciona_iva` | binary | 1 si el pliego menciona el IVA. |
| `binary_incluye_exoneraciones` | binary | 1 si hay exoneraciones. ⚠️ Constante — descartar para ISF. |

### 3.12 Suministros y especificaciones técnicas
| Campo | Tipo | Significado |
|---|---|---|
| `binary_menciona_normas_oficiales` | binary | 1 si exige cumplir normas oficiales (INTN, ISO). |
| `binary_permite_cambios_codigos` | binary | 1 si permite cambiar códigos de ítems sin afectar el contrato. |
| `binary_proveedor_puede_rehusar_responsabilidad` | binary | 1 si el proveedor puede rechazar responsabilidad sobre especificaciones. |

### 3.13 Derechos intelectuales
| Campo | Tipo | Significado |
|---|---|---|
| `binary_cesion_derechos_contratante` | binary | 1 si el proveedor cede los derechos de propiedad intelectual a la entidad. |
| `binary_prohibe_reproduccion_no_autorizada` | binary | 1 si se prohíbe reproducir documentos sin autorización. ⚠️ Constante — descartar. |

### 3.14 Validez de garantía de bienes
| Campo | Tipo | Significado |
|---|---|---|
| `dias_validez_garantia` | int | Días de vigencia de la garantía sobre los bienes entregados (25 valores). |
| `binary_cubre_defectos_fabricacion` | binary | 1 si la garantía cubre defectos de fabricación. |
| `binary_tiene_cobertura_especifica` | binary | 1 si hay cobertura específica (partes, servicio técnico). |

### 3.15 Capacidad técnica
| Campo | Tipo | Significado |
|---|---|---|
| `anos_experiencia_minima_tecnica` | int | Años mínimos de experiencia técnica específica (20 valores). |
| `num_referencias_tecnicas_minimo` | int | Número mínimo de referencias técnicas (6 valores). |
| `pct_monto_referencia_tecnica` | float | % del monto a acreditar como experiencia técnica. |

### 3.16 Formas y condiciones de pago
| Campo | Tipo | Significado |
|---|---|---|
| `dias_pago_maximo` | int | Plazo máximo de días para que la entidad pague al proveedor tras entregar/facturar (15 valores). |
| `dias_aceptacion_rechazo_factura` | int | Días para que la entidad acepte o rechace una factura (3 valores). |
| `num_documentos_pago` | int | Cantidad de documentos exigidos para el pago (7 valores). |
| `binary_requiere_cuenta_bancaria_estatal` | binary | 1 si el pago exige cuenta bancaria estatal. |

### 3.17 Plan de entrega de los bienes
| Campo | Tipo | Significado |
|---|---|---|
| `binary_tiene_plan_entrega` | binary | 1 si el pliego exige un plan de entrega. |
| `binary_incluye_cronograma` | binary | 1 si el plan incluye cronograma con fechas. |
| `binary_requiere_documentos_embarque` | binary | 1 si exige documentos de embarque/transporte. |

### 3.18 Capacidad financiera
| Campo | Tipo | Significado |
|---|---|---|
| `binary_tiene_requisitos_financieros` | binary | 1 si hay requisitos financieros definidos (0 = "NO APLICA"). |
| `ratio_liquidez_minimo` | float | Líquidez mínima exigida = activos corrientes / pasivos corrientes (8 valores). Un ratio alto excluye a la mayoría. |
| `ratio_endeudamiento_maximo` | float | Endeudamiento máximo permitido = pasivo total / activo total (3 valores). Un ratio bajo encarece finanzas. |
| `binary_requiere_estados_auditados` | binary | 1 si exige estados financieros auditados. |

### 3.19 Reajuste de precios
| Campo | Tipo | Significado |
|---|---|---|
| `binary_tiene_formula_reajuste` | binary | 1 si el contrato tiene fórmula de reajuste de precios. |
| `num_componentes_formula` | int | Número de componentes de la fórmula. ⚠️ Constante — descartar. |
| `binary_referencia_ipc` | binary | 1 si la fórmula se referencia al IPC (índice de precios al consumo). |
| `binary_no_reajuste_si_atraso` | binary | 1 si se pierde el reajuste cuando hay atraso del proveedor. |

### 3.20 Criterios de adjudicación
| Campo | Tipo | Significado |
|---|---|---|
| `binary_criterio_menor_precio` | binary | 1 si adjudica por menor precio. ⚠️ Constante — descartar. |
| `binary_multiplicidad_criterios` | binary | 1 si evalúa con múltiples criterios (puntaje técnico + precio). |
| `binary_contrato_abierto` | binary | 1 si es contrato marco/abierto. ⚠️ Constante — descartar. |
| `binary_reserva_disminuir_cantidad` | binary | 1 si la entidad se reserva reducir cantidades. ⚠️ Constante — descartar. |

### 3.21 Garantía de anticipo
| Campo | Tipo | Significado |
|---|---|---|
| `binary_requiere_garantia_anticipo` | binary | 1 si exige garantía para recibir anticipo. |
| `categorical_tipo_garantia` | categorical | Tipo de garantía aceptada (póliza, depósito, garantía bancaria — 3 categorías). |
| `pct_anticipo` | float | % del monto entregable como anticipo. ⚠️ Constante — descartar. |

### 3.22 Requisitos de calificación
| Campo | Tipo | Significado |
|---|---|---|
| `num_pasos_verificacion` | int | Número de pasos de verificación de la oferta (2 valores). |
| `binary_verifica_sinarh` | binary | 1 si se verifica al proveedor en SINARH. |
| `binary_requiere_declaracion_conflictos` | binary | 1 si el oferente debe declarar conflictos de interés. |
| `binary_requiere_registro_proveedores` | binary | 1 si exige estar registrado como proveedor del Estado. |

### 3.23 Solicitud de pago de anticipo
| Campo | Tipo | Significado |
|---|---|---|
| `binary_tiene_condiciones_anticipo` | binary | 1 si el anticipo tiene condiciones específicas. |
| `dias_solicitud_anticipo` | int | Días posteriores a la firma para solicitar el anticipo (13 valores). |
| `dias_pago_anticipo` | int | Días que tarda la entidad en pagar el anticipo tramitado (13 valores). |
| `binary_anticipo_solo_mipymes` | binary | 1 si el anticipo solo beneficia a MIPYMES. |

### 3.24 Tasa de interés por mora
| Campo | Tipo | Significado |
|---|---|---|
| `pct_interes_mora_diario` | float | Interés diario por mora en pagos de la entidad al proveedor (35 valores). |
| `binary_mora_desde_dia_siguiente` | binary | 1 si el interés corre desde el día siguiente al vencimiento. |

### 3.25 Criterios de desempate
| Campo | Tipo | Significado |
|---|---|---|
| `binary_menciona_criterios_dncp` | binary | 1 si el desempate usa criterios establecidos por la DNCP. |
| `binary_incluye_criterios_especificos_adicionales` | binary | 1 si hay criterios de desempate específicos agregados (fuera de DNCP). |

---

## 4. Columnas de la tabla económica (`document_economic_features`) — lluvia de ideas por cruce de tablas

Tabla derivada, **por licitación** (29,811 registros). La mayoría ya resuelve los red flags "ausentes" de la Fase 3.

### 4.1 Competencia / oferentes
| Campo | Significado |
|---|---|
| `cantidad_oferentes` | Número total de oferentes registrados en la licitación (93% poblado; 39 valores). |
| `es_unico_oferente` | 1 si solo se presentó un oferente (**oferta única**, proxy #1 de corrupción). |
| `n_oferentes_distintos` | Conteo de proveedores distintos que ofertaron (equivalente robusto de `cantidad_oferentes`). |
| `oferentes_por_item` | Oferentes / cantidad de ítems. ⚠️ Constante (bug `cantidad_items`) — descartar. |
| `bidder_diversity` | Diversidad de oferentes. ⚠️ Constante (bug) — descartar. |

### 4.2 Precio y montos
| Campo | Significado |
|---|---|
| `monto_estimado` | Precio referencial/estimado del llamado. |
| `monto_adjudicado` | Monto de la adjudicación (ganador). |
| `monto_contrato` | Monto del contrato firmado (puede diferir del adjudicado). |
| `contract_value_ratio` | monto_contrato / monto_estimado. |
| `precio_vs_estimado` | **Desviación % del adjudicado vs. estimado** (red flag oficial DNCP: alerta > +20% / < −30%). |
| `overbudget_ratio` | Ratio de sobrepresupuesto (más sensible a sobreprecio). |
| `is_high_value_single_bidder` | 1 si hubo oferta única Y valor alto (combinación de riesgo). |
| `costo_pliego` | Costo de adquisición del pliego. ⚠️ Vacío en licitaciones — no usable. |

### 4.3 Procedimiento y tiempo
| Campo | Significado |
|---|---|
| `metodo_contratacion` | open / direct (modalidad; las directas concentran riesgo). |
| `criterio_adjudicacion` | priceOnly / ratedCriteria. |
| `fecha_publicacion` / `fecha_apertura` | Fechas de publicación y apertura de ofertas. |
| `duracion_oferta_dias` | Días entre publicación y apertura (**plazo anormalmente corto = red flag OCDE**). |
| `duracion_consultas_dias` | Días habilitados para consultas al pliego. |
| `tiene_consultas` / `tiene_subasta` | Si hubo consultas; si se usó subasta electrónica. |
| `bidding_urgency` | Urgencia del proceso (normalizado; derive del plazo). |

### 4.4 Ejecución y contrato
| Campo | Significado |
|---|---|
| `duracion_contrato_dias` | Duración pactada del contrato en días. |
| `contrato_estado` | Estado del contrato (activo, terminado, resuelto). |
| `n_enmiendas` | Número de enmiendas/adendas al contrato. |
| `total_monto_enmiendas` | Monto total acumulado de las enmiendas. |
| `enmienda_ratio` | Monto enmiendas / monto contrato (**adendas grandes = corrección post-adjudicación**). |
| `n_pagos` | Cantidad de pagos hechos al contrato. |
| `total_pagado` | Monto total pagado. |
| `pago_vs_contrato_ratio` | total_pagado / monto_contrato (ejecución real). |
| `total_multas` | Monto total de multas aplicadas al proveedor. |
| `has_multas` | 1 si hubo multas. |
| `avg_retencion` | Retención promedio (IVA/renta/DNCP) sobre los pagos. |

### 4.5 Protestas
| Campo | Significado |
|---|---|
| `has_protesta` | 1 si hubo al menos una protesta (967 licitaciones). |
| `n_protestas` | Cantidad de protestas (máx. 8). |

### 4.6 Proveedor (agregado)
| Campo | Significado |
|---|---|
| `winner_proveedor_id` | ID del proveedor ganador. |
| `winner_tipo_entidad` | Tipo de entidad del ganador (persona física, empresa, etc.). |
| `convocante_region` / `convocante_localidad` | Ubicación de la entidad que compra. ⚠️ `convocantes.region` vacío — parcial. |
| `winner_category_frequency` | Cuántas veces el ganador ganó en esa categoría de bienes/servicios. |
| `winner_total_contracts` | Total de contratos del ganador. |
| `is_repeat_winner` | 1 si el ganador repite con la misma entidad (**recurrencia proveedor-entidad**, red flag DNCP). |

---

## 5. Tablas OCDS (datos crudos) — campos para macrofeatures

### 5.1 `licitaciones` (37 cols) — el llamado
| Campo | Significado |
|---|---|
| `nro_licitacion` | **Clave primaria** del llamado. |
| `ocid` / `compiled_release_id` / `tender_id` | IDs del estándar OCDS. |
| `category_id` | Categoría de bienes/servicios (82 categorías en `categorias`). |
| `titulo` | Título del llamado. |
| `metodo_contratacion` / `metodo_detalle` | Modalidad (open/direct) y detalle. |
| `categoria_principal` / `categoria_detalle` | Rubro principal y detalle. |
| `monto_estimado` / `presupuesto_monto` | Monto estimado y presupuesto asignado. ⚠️ `presupuesto_monto` vacío. |
| `moneda` | Moneda (PYG). |
| `criterio_adjudicacion` / `criterio_detalle` | priceOnly / ratedCriteria + detalle. |
| `estado` / `estado_detalle` | Estado del proceso (convocado, adjudicado, terminado). |
| `fecha_publicacion` / `fecha_apertura` / `fecha_fin_consultas` | Línea de tiempo del proceso. |
| `duracion_consultas_dias` / `duracion_oferta_dias` / `duracion_contrato_dias` | Duraciones derivadas (días). |
| `cantidad_oferentes` | Conteo de oferentes (93% poblado). |
| `tiene_consultas` / `tiene_subasta` / `tiene_acuerdo_marco` | Flags del proceso. |
| `metodo_entrega` | Modalidad de entrega. |
| `garantia_porcentaje` / `garantia_validez_dias` | Garantía general. ⚠️ `garantia_porcentaje` vacío. |
| `cantidad_items` / `cantidad_lotes` | Ítems y lotes. ⚠️ **`cantidad_items`=0 (bug fuente)**. |
| `criterio_elegibilidad` | Quién puede participar. |
| `costo_pliego` | Costo del pliego. ⚠️ Vacío. |
| `convocante_id` | Entidad que compra (FK a `convocantes`). |

### 5.2 `adjudicaciones` (13 cols)
| Campo | Significado |
|---|---|
| `award_id` | ID de la adjudicación. |
| `nro_licitacion` | Referencia al llamado. |
| `proveedor_id` / `proveedor_nombre` | Ganador. |
| `monto_adjudicado` | Monto adjudicado. |
| `fecha_adjudicacion` | Fecha de la decisión. |
| `estado` / `estado_detalle` | Estado de la adjudicación. |
| `descripcion` | Notas. |
| `invitation_id` | Referencia a invitación. |

### 5.3 `contratos` (17 cols)
| Campo | Significado |
|---|---|
| `contrato_id` / `award_id` | ID del contrato y su adjudicación. |
| `nro_licitacion` | Llamado. |
| `proveedor_id` | Proveedor. |
| `monto_contrato` | Monto del contrato. |
| `fecha_firma` / `fecha_inicio` / `fecha_fin` | Línea de tiempo del contrato. |
| `duracion_dias` | Duración en días. |
| `estado` / `estado_detalle` | Estado del contrato. |
| `cantidad_enmiendas` | N° de enmiendas. ⚠️ 1 valor en datos (bug) — usar `eco.n_enmiendas`. |
| `monto_total_enmiendas` | Monto de enmiendas. ⚠️ Igual bug. |
| `cantidad_pagos` / `monto_total_pagado` | Resumen de pagos. |

### 5.4 `oferentes` (6 cols)
| Campo | Significado |
|---|---|
| `id` | ID interno. |
| `compiled_release_id` | Release OCDS. |
| `nro_licitacion` | Llamado. |
| `proveedor_id` / `proveedor_nombre` | Quién ofertó. |
| `year` | Año. |

### 5.5 `pagos_contrato` (19 cols) → macrofeatures de FLUJO
| Campo | Significado |
|---|---|
| `pago_id` / `contrato_id` | IDs del pago y contrato. |
| `nro_licitacion` / `proveedor_id` | Llamado y proveedor. |
| `fecha_pago` / `fecha_solicitud` / `fecha_factura` | Tres fechas → permite **`demora_media_pago_dias` = fecha_pago − fecha_solicitud** (276,485 pares útiles). |
| `nro_factura` | Número de factura. |
| `monto_factura` / `monto_pagado` | Monto facturado vs. pagado. |
| `retencion_iva` / `retencion_renta` / `retencion_dncp` | Retenciones aplicadas. |
| `multa` | Multa aplicada al pago (10,463 pagos con multa > 0). |
| `codigo_financiero` | Código presupuestario. |
| `pagador_id` / `pagador_nombre` | Quién pagó. |
| `sistema_origen` | Sistema de origen del pago. |

### 5.6 `enmiendas_contrato` (8 cols)
| Campo | Significado |
|---|---|
| `enmienda_id` / `contrato_id` / `nro_licitacion` | IDs. |
| `fecha` | Fecha de la enmienda. |
| `descripcion` | Motivo. |
| `codigo_financiero` | Código presupuestario. |
| `monto_enmienda` | Monto modificado (4,775 con monto > 0). |

### 5.7 `protestas` (13 cols)
| Campo | Significado |
|---|---|
| `protesta_id` / `nro_licitacion` / `tender_id` | IDs. |
| `fecha` | Fecha del reclamo. |
| `estado` / `resultado` / `motivo` | Cómo terminó y por qué se protestó. |
| `recurrente_id` / `recurrente_nombre` | Quién protestó (usualmente el perdedor). |
| `tipo_evento` / `descripcion_evento` / `estado_evento` | Tipo y descripción del evento de protesta. |

### 5.8 `consultas_llamado` (11 cols) → macrofeatures de proceso
| Campo | Significado |
|---|---|
| `consulta_id` / `nro_licitacion` | IDs. |
| `fecha` / `fecha_respuesta` | **Permite `tiempo_respuesta_consultas`** (respuesta rápida = el consultante ya conocía el pliego). |
| `titulo` / `descripcion` / `respuesta` | Contenido de la consulta. |
| `autor_id` / `autor_nombre` | Quién preguntó. |
| `year` | Año. |

### 5.9 `items_licitacion` (12 cols) → macrofeatures de heterogeneidad
| Campo | Significado |
|---|---|
| `item_id` / `nro_licitacion` | IDs. |
| `descripcion` / `cantidad` / `unidad_id` / `unidad_nombre` | Qué, cuánto y la unidad. |
| `monto` / `moneda` | Monto del ítem. |
| `clasificacion_id` / `clasificacion_desc` | Clasificación de bienes. |
| `unspsc_id` / `unspsc_desc` | Código UNSPSC → **`n_unspsc_distintos` por licitación** (8,085 códigos; heterogeneidad del llamado). |

### 5.10 `proveedores` (17 cols)
| Campo | Significado |
|---|---|
| `proveedor_id` / `ruc` | ID y RUC. |
| `nombre_comercial` / `nombre_legal` | Nombres. |
| `tipo_entidad` / `tipo_entidad_detalle` | Tipo (PYME, empresa, persona física...). |
| `tamanio` | Tamaño. ⚠️ Vacío. |
| `tipo_actividad` | Actividad económica. |
| `region` / `localidad` / `direccion` | Ubicación (18 regiones; permite coincidencia geográfica proveedor−entidad). |
| `email` / `telefono` / `url_web` | Contacto. |
| `escala` / `nivel_institucional` | Escala y nivel. |

### 5.11 `criterios_llamado` (13 cols)
| Campo | Significado |
|---|---|
| `criterio_id` / `criterio_titulo` / `criterio_descripcion` / `criterio_fuente` | Criterios de evaluación o elegibilidad y su fuente. |
| `grupo_id` / `grupo_descripcion` | Grupo de criterios. |
| `requisito_id` / `requisito_titulo` / `requisito_valor` | Requisitos específicos y su valor. |
| `year` | Año. |

### 5.12 `pliegos_secciones` (18 cols) — almacén de texto por licitación
| Campo | Significado |
|---|---|
| `document_id` | ID de la sección. |
| `nro_licitacion` / `category_id` / `year` | Referencias. |
| `title` / `title_normalized` | Título de la sección y su forma normalizada (clave para clasificar). |
| `page` / `line_start` / `line_end` | Ubicación en el documento (página, líneas) → `span_*`. |
| `depth` | Nivel jerárquico de la sección. |
| `content_length` / `content_length_clean` | Longitud del texto (con/sin ruido). |
| `estimated_tokens` / `word_count` | Tokens y palabras estimadas. |
| `size_bytes` | Tamaño en bytes. |
| `content_text` / `content_clean` | Texto completo (bruto y limpio). |
| `cluster_id` | Cluster de clustering de secciones. |

---

## 6. Términos técnicos del pipeline (regex + LLM + Isolation Forest)

| Término | Significado |
|---|---|
| **Pipeline de extracción** | Proceso: (1) dividir pliego en secciones, (2) clasificar sección por título, (3) extraer campos con regex, (4) LLM fallback para campos híbridos con confianza baja. |
| **Regex** | Expresión regular: patrón de texto (`\d+\s*días`) → valor numérico. Rápido, determinista, 100% reproducible. |
| **LLM** | Modelo de lenguaje grande (ej. Llama 3.1 8B AWQ local). Se usa solo para campos `hybrid` donde el regex falló (conf < 0.8). |
| **Guided decoding** | Forzar al LLM a responder solo `número` o `NULL` (regex en logits) → 7× más rápido, cero explicaciones. |
| **Campo `hybrid`** | Campo extraído por regex primero y LLM como fallback. Tiene columnas `_conf` y `_source`. |
| **Campo `regex`** | Campo 100% determinista por regex (sin columnas `_conf`/`_source`). |
| **`_conf` (confianza)** | 0.0–1.0 de cuán segura fue la extracción LLM; conf < 0.8 dispara entrenamiento/fallback. |
| **`_source`** | Origen del valor: `regex`, `llm`, o `none` (ausencia legítima). |
| **NULL vs 0** | NULL = el campo genuinamente no existe en la sección; 0 = valor extraído = cero. No confundir (ver `extraction_postprocess`). |
| **Isolation Forest** | Algoritmo no supervisado que aísla anomalías: las "pocas y diferentes" requieren menos particiones para quedar solas. Mejor en anomalías **puntuales**. |
| **Anomalía puntual** | Un registro raro por sí mismo (una desviación de precio extrema). |
| **Anomalía contextual** | Valor normal en general, raro dado el contexto (precio alto para ese rubro). ISF es débil aquí. |
| **Anomalía colectiva** | Grupo de registros normales individualmente pero anómalos en conjunto (fraccionamiento de montos). ISF es débil aquí. |
| **`n_estimators`** | N° de árboles del ISF (recomendado 100). |
| **`max_samples`** | Tamaño de muestra por árbol (recomendado 256). |
| **Umbral percentil 99** | Marcar como anomalía el 1% superior de puntajes de aislamiento (sin datos etiquetados). |
| **SHAP** | Valores de Shapley: explica qué variable contribuyó a cada puntaje (interpretabilidad local). |
| **DIFFI** | Importancia de variables específica para ISF (más barata que SHAP). |
| **One-hot encoding** | Convertir categórica de baja cardinalidad (open/direct) en columnas 0/1. |
| **Target encoding** | Codificar proveedor/categoría con la mediana/moda de una variable objetivo (robusto a outliers). |
| **z-score** | Normalización (x − media) / desviación. Útil para comparar montos con métodos por distancia. |
| **Cola pesada** | Distribución con valores extremos poco frecuentes (montos, precios) — favorable para ISF. |
| **Red flag** | Proxy estadístico correlacionado con corrupción (NO prueba de corrupción): oferta única, desvío de precio, plazo corto, recurrencia de ganador, adendas. |

---

## 7. Clave rápida por prefijo

| Prefijo columna | Qué significa | Uso en ISF |
|---|---|---|
| `pct_` | Porcentaje (0–100) | Valores extremos = sobre/restricción |
| `dias_` | Días (plazos) | Plazos anómalos cortos/largos |
| `ratio_` | Índice adimensional | Exigencias financieras raras |
| `num_` | Conteo entero | Cantidad de requisitos/documentos |
| `anos_` | Años (experiencia) | Requisitos excesivos = direccionamiento |
| `binary_` | Flag 0/1 | Conjunto de condiciones de riesgo |
| `categorical_` | Categoría (3 valores) | Tipo de garantía |
| `_conf` | Confianza extracción | Solo para híbridos |
| `_source` | Origen valor | solo híbridos |

---

*Fuentes: `dncp` (16+ tablas), `master_schemas_v2.json` (25 secciones / 81 campos), `document_economic_features` (46 columnas), direcciones del pipeline y Fase 5. Documento generado para referencia rápida del proyecto.*
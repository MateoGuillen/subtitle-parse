# Diccionario de Datos — Base `dncp` (Contratación Pública del Paraguay)

> Documentación completa y autorreferencial de las **25 tablas** de la base PostgreSQL `dncp`
> (esquema `dncp`), orientada a la tesis de detección de anomalías / corrupción en la
> contratación pública paraguaya (OCDS + pliegos de bases y condiciones).
>
> - **Fuente de datos**: API de la Dirección Nacional de Contrataciones Públicas (DNCP) de Paraguay
>   en formato **OCDS v1.1** (Open Contracting Data Standard) — `compiled_release_id`, `ocid`,
>   `tender_id`, `award_id`, `contract_id` respetan la convención OCDS.
> - **Moneda**: todas las columnas `*_monto` están en **guaraníes (PYG)**, salvo que la columna
>   `moneda` indique otra cosa. `monto_estimado`, `monto_adjudicado` y `monto_contrato` suelen venir
>   en los llamados como PYG.
> - **Valores nulos**: el estándar OCDS deja vacíos algunos campos de la fase de licitación
>   (ej. garantías) si el pliego no los define explícitamente; no confundir `NULL` con 0.
> - **Claves**: `nro_licitacion` (texto, ej. `461165`) es la clave del llamado en todos los
>   sistemas (SICP); las tablas se unen por esta columna.
>
> Convención de bloques para la tesis:
> - **Bloque A** — competencia (oferentes, urgencia, sobrepresupuesto)
> - **Bloque B** — contenido del pliego (restricciones, garantías, anticipos, pagos, multas)
> - **Bloque C** — ejecución del contrato (pagos, enmiendas, retrasos, retenciones)

---

## 1. `licitaciones` (29,811 filas) — Llamados / proceso de licitación

Tabla maestra OCDS. Cada fila es **un llamado (tender)** publicado por la DNCP. Es la entidad
principal: todas las demás tablas se referencian a ella vía `nro_licitacion`.

| Columna | Tipo | Significado en licitaciones paraguayas |
|---|---|---|
| `nro_licitacion` | text (PK) | Número de llamado asignado por el SICP (Sistema Integrado de Compras Públicas). Ej. `461165`. Identifica el proceso de contratación completo. |
| `category_id` | text | Identificador de la categoría (UNSPSC de nivel superior, ej. `services`, `goods`, `works`). Enlaza con `categorias.category_id`. |
| `year` | smallint | Año de publicación del llamado (2021–2025 en el dataset). |
| `compiled_release_id` | text | ID del "compiled release" OCDS — la versión consolidada que acumula todos los eventos del proceso. Usado para unir con la API DNCP y con `oferentes`, `consultas_llamado`, etc. |
| `ocid` | text | Open Contracting ID global (ej. `ocds-XXXX-...`) según estándar OCDS. Identifica el contrato a nivel internacional. |
| `tender_id` | text | ID del tender dentro del OCDS (puede diferir del `nro_licitacion`). |
| `titulo` | text | Título del llamado, tal como se publica (ej. "Adquisición de Licencia Antiplagio para la FADA/UNA"). |
| `metodo_contratacion` | text | Método OCDS de alto nivel: `open`, `direct`, `limited`, etc. **`direct` = contratación directa** (mayor riesgo de anomalías). |
| `metodo_detalle` | text | Sub-método paraguayo: `Menor cuantía nacional`, `Contratación Directa`, `Licitación Pública Nacional`, `Subasta a la Baja Electrónica`, `Concurso de Ofertas`, etc. |
| `categoria_principal` | text | Categoría principal del llamado según DNCP: `goods` (bienes), `services` (servicios), `works` (obras). |
| `categoria_detalle` | text | Detalle adicional de la categoría (ej. número de la categoría específica). |
| `monto_estimado` | numeric | **Monto estimado** del contrato en PYG según el pliego. Base para el cálculo de sobrepresupuesto (`overbudget_ratio`) y desviaciones. |
| `presupuesto_monto` | numeric | Monto presupuestado/u oficial (puede diferir del estimado; suele representar el tope asignado por la institución). |
| `moneda` | text | Moneda de los montos (`PYG` casi siempre). |
| `criterio_adjudicacion` | text | Criterio oficial de adjudicación: `precio más bajo`, `mejor relación precio-calidad`, `técnica` (OCDS). |
| `criterio_detalle` | text | Detalle del criterio (ej. si usa subasta electrónica o puntos). |
| `estado` | text | Estado final del proceso: `complete`, `unsuccessful`, `cancelled`. (El dataset no conserva `adjudicado` como valor.) |
| `estado_detalle` | text | Sub-estado (ej. `terminated`, `awarded`, detalle de cancelación). |
| `fecha_publicacion` | timestamp | Fecha de publicación del llamado en el portal DNCP. Inicia el periodo de presentación de ofertas. |
| `fecha_apertura` | timestamp | Fecha de apertura de ofertas / cierre del periodo de presentación. Alimenta `duracion_oferta_dias`. |
| `fecha_fin_consultas` | timestamp | Fecha límite para recibir consultas sobre el pliego. |
| `duracion_consultas_dias` | integer | Días entre publicación y fin de consultas. Ventanas muy cortas = **urgencia / menor competencia**. |
| `duracion_oferta_dias` | integer | Días entre publicación y apertura para presentar ofertas. **Plazos muy cortos reducen la competencia** (red flag). |
| `duracion_contrato_dias` | integer | Duración prevista del contrato en días (según pliego). |
| `cantidad_oferentes` | integer | Número de ofertas recibidas. **Es la variable decisiva**: 1 = oferta única (red flag #1). |
| `tiene_consultas` | boolean | Si hubo consultas de interesados durante el periodo de aclaraciones. La ausencia de consultas aun en llamados complejos puede indicar poca participación. |
| `tiene_subasta` | boolean | Si el proceso utilizó subasta electrónica a la baja. |
| `tiene_acuerdo_marco` | boolean | Si el llamado pertenece a un acuerdo marco (catálogo). |
| `metodo_entrega` | text | Método de entrega / prestación del servicio (ej. `fisica`, `electronica`). |
| `garantia_porcentaje` | numeric | Porcentaje de la garantía de mantenimiento de oferta o de fiel cumplimiento exigido (%). |
| `garantia_validez_dias` | integer | Vigencia de la garantía en días. |
| `cantidad_items` | integer | Número de renglones de ítems de `items_licitacion`. **Nota de calidad**: en muchos registros es 0 por un problema de la fuente. |
| `cantidad_lotes` | integer | Número de lotes en que se divide el llamado (1 = llamado único). Lotes separados pueden fragmentar la competencia. |
| `criterio_elegibilidad` | text | Resumen de los requisitos de elegibilidad (capacidad financiera, técnica, jurídica). |
| `costo_pliego` | numeric | Costo del pliego de bases (PYG). Costos altos pueden desincentivar participación. |
| `convocante_id` | text | RUC/ID de la institución que convoca. Enlaza con `convocantes`. |
| `api_enriched_at` | timestamp | Fecha en que la API DNCP enriqueció el registro (campo de la ingesta, no de negocio). |

**Uso en la tesis (Bloque A)**: `cantidad_oferentes`, `duracion_oferta_dias`, `duracion_consultas_dias`,
`monto_estimado`, `costo_pliego`, `tiene_consultas`, `metodo_contratacion`.

---

## 2. `oferentes` (78,695 filas) — Ofertas presentadas

Una fila por **oferta presentada** a un llamado (una empresa puede ofertar más de una vez si hay
lotes). **Clave para identificar oferta única y colusión.**

| Columna | Tipo | Significado |
|---|---|---|
| `id` | integer (PK) | Identificador interno de fila. |
| `compiled_release_id` | text | Referencia al compiled release OCDS del proceso. |
| `nro_licitacion` | text | Número del llamado al que se presentó la oferta. |
| `proveedor_id` | text | RUC/ID del proveedor que ofertó. Enlaza con `proveedores`. |
| `proveedor_nombre` | text | Nombre del proveedor ofertante. |
| `year` | smallint | Año del llamado. |

**Uso en la tesis (Bloque A)**: aquí se calcula `n_oferentes_distintos` (empresas únicas que ofertaron)
y la **recurrencia de los mismos oferentes en llamados del mismo año** (`winner_category_frequency`,
comportamiento de cartel).

---

## 3. `proveedores_notificados` (76,879 filas) — Proveedores invitados/notificados

Registro de **proveedores a los que se les notificó el llamado** (lista de invitación en contrataciones
directas / menor cuantía). Útil para medir **quiénes fueron invitados vs quiénes realmente ofertaron**:
en llamados con pocas invitaciones y una única oferta hay sospecha de trato directo.

| Columna | Tipo | Significado |
|---|---|---|
| `id` | integer (PK) | ID interno. |
| `compiled_release_id` | text | Reference del proceso OCDS. |
| `nro_licitacion` | text | Llamado notificado. |
| `proveedor_id` | text | RUC del proveedor invitado. |
| `proveedor_nombre` | text | Nombre del proveedor invitado. |
| `year` | smallint | Año del llamado. |

**Uso en la tesis (Bloque A)**: razón `notificados/ofertaron` — si de N invitados solo ofertó 1
repetidamente, refuerza oferta única / invitación restringida.

---

## 4. `adjudicaciones` (39,463 filas) — Adjudicaciones

Una fila por **award (adjudicación)** dentro de un proceso. Un llamado puede tener varias
adjudicaciones (por lote o por partida).

| Columna | Tipo | Significado |
|---|---|---|
| `award_id` | text (PK) | ID del award en OCDS. |
| `nro_licitacion` | text | Llamado al que pertenece la adjudicación. |
| `compiled_release_id` | text | Compiled release OCDS. |
| `tender_id` | text | Tender asociado. |
| `proveedor_id` | text | RUC del proveedor **adjudicado** (ganador). |
| `proveedor_nombre` | text | Nombre del adjudicatario. |
| `monto_adjudicado` | numeric | Monto adjudicado en PYG (importe de la oferta ganadora). **Base de `precio_vs_estimado`** (diferencia porcentual entre lo ofertado y el precio referencial). |
| `moneda` | text | Moneda del monto. |
| `fecha_adjudicacion` | timestamp | Fecha de la resolución de adjudicación. |
| `estado` | text | `active`, `terminated`, etc. |
| `estado_detalle` | text | Detalle del estado. |
| `descripcion` | text | Descripción de la adjudicación (bienes/servicios adjudicados). |
| `invitation_id` | text | ID de la invitación OCDS (puede coincidir con award o estar vacío). |

**Uso en la tesis (Bloques A/C)**: `precio_vs_estimado` (desviación del precio de la oferta frente al
referencial), identidad del ganador para cruzar con `pagos_contrato` y `contratos`.

---

## 5. `contratos` (39,691 filas) — Contratos firmados

Fila por **contrato** formalizado tras la adjudicación. Incluye montos y **campos de ejecución**
(enmiendas y pagos) que recoge la API.

| Columna | Tipo | Significado |
|---|---|---|
| `contrato_id` | text (PK) | ID del contrato en OCDS. |
| `award_id` | text | Adjudicación de la que deriva el contrato. |
| `nro_licitacion` | text | Llamado origen. |
| `compiled_release_id` | text | Compiled release OCDS. |
| `proveedor_id` | text | RUC del contratista. |
| `monto_contrato` | numeric | Monto inicial del contrato en PYG. **Base de `contract_value_ratio`**. |
| `moneda` | text | Moneda. |
| `fecha_firma` | timestamp | Fecha de firma del contrato. |
| `fecha_inicio` | timestamp | Fecha de inicio del periodo contractual. |
| `fecha_fin` | timestamp | Fecha de fin prevista del contrato. |
| `duracion_dias` | smallint | Días de duración prevista. |
| `estado` | text | `active`, `complete`, `terminated`, etc. |
| `estado_detalle` | text | Detalle del estado. |
| `cantidad_enmiendas` | smallint | Nº de enmiendas reportadas. **Conocido: todos los valores = 0 (bug de la fuente)** — usar `enmiendas_contrato.n_enmiendas` en su lugar. |
| `monto_total_enmiendas` | numeric | Monto acumulado de enmiendas (≈0 por el mismo bug). |
| `cantidad_pagos` | smallint | Nº de pagos realizados al contrato. |
| `monto_total_pagado` | numeric | Suma de pagos hasta la fecha (PYG). **Base de `pago_vs_contrato_ratio`**. |

**Uso en la tesis (Bloque C)**: `contract_value_ratio` (monto contrato / monto estimado),
`pago_vs_contrato_ratio` (cuánto se pagó respecto al contrato), `cantidad_pagos`.

---

## 6. `pagos_contrato` (276,868 filas) — Pagos del estado al contratista

**Tabla más grande de ejecución**. Fila por pago; aquí se mide **retraso en pagos, retenciones y multas**
a nivel de pago individual.

| Columna | Tipo | Significado |
|---|---|---|
| `pago_id` | text (PK) | ID del pago en OCDS. |
| `contrato_id` | text | Contrato al que corresponde el pago. |
| `nro_licitacion` | text | Llamado origen. |
| `proveedor_id` | text | RUC del cobrador. |
| `fecha_pago` | timestamp | Fecha en que el estado efectivamente pagó. |
| `fecha_solicitud` | timestamp | Fecha en que el proveedor solicitó el pago. |
| `fecha_factura` | timestamp | Fecha de la factura. |
| `nro_factura` | text | Número de factura del proveedor (identificable). |
| `monto_factura` | numeric | Monto facturado (PYG). |
| `monto_pagado` | numeric | Monto efectivamente pagado (PYG). |
| `moneda` | text | Moneda. |
| `retencion_iva` | numeric | Retención de IVA aplicada. |
| `retencion_renta` | numeric | Retención de renta (IRP/IGR). |
| `retencion_dncp` | numeric | Retención de la tasa de la DNCP. |
| `multa` | numeric | Multa aplicada al pago por incumplimientos. |
| `codigo_financiero` | text | Código de financiamiento (identifica la fuente de fondos). |
| `pagador_id` | text | ID de la unidad pagadora. |
| `pagador_nombre` | text | Nombre de la unidad pagadora (ej. Tesorería, unidad administrativa). |
| `sistema_origen` | text | Sistema donde se registró el pago (SIGAF/SUAF u otro). |

**Uso en la tesis (Bloque C)**: demoras `fecha_pago - fecha_solicitud` (pagos a destiempo son indicador
de presión financiera / favoritismo), retenciones, multas (`has_multas`), y consistencia
`monto_pagado` vs `monto_factura`.

---

## 7. `enmiendas_contrato` (7,143 filas) — Modificaciones al contrato

**Adendas / convenios modificatorios** (Bloque C). Los aumentos de monto o plazo vía enmienda son un
patrón de "baja oferta inicial y renegociación posterior" (anomalía típica).

| Columna | Tipo | Significado |
|---|---|---|
| `enmienda_id` | text (PK) | ID de la enmienda OCDS. |
| `contrato_id` | text | Contrato modificado. |
| `nro_licitacion` | text | Llamado origen. |
| `fecha` | timestamp | Fecha de la enmienda. |
| `descripcion` | text | Motivo/descripción de la modificación. |
| `codigo_financiero` | text | Código de fondo usado. |
| `monto_enmienda` | numeric | Monto adicional aprobado (PYG). |
| `moneda` | text | Moneda. |

**Uso en la tesis (Bloque C)**: `n_enmiendas` y `enmienda_ratio`
(monto total enmiendas / monto contrato original) — **enmienda ratio alto = sobreprecio posterior**.
Esta tabla es la fuente correcta (no `contratos.cantidad_enmiendas`, que está en 0).

---

## 8. `protestas` (1,253 filas) — Impugnaciones de oferentes

Fila por **protesta / recurso** presentado por un oferente ante la DNCP (a nivel de licitación o de
evento). La **presencia de protestas valida que hubo competencia real y descontento**; su ausencia en
llamados grandes puede ser anómala.

| Columna | Tipo | Significado |
|---|---|---|
| `protesta_id` | text (PK) | ID de la protesta. |
| `nro_licitacion` | text | Llamado impugnado. |
| `compiled_release_id` | text | Compiled release OCDS. |
| `tender_id` | text | Tender asociado. |
| `fecha` | timestamp | Fecha de presentación. |
| `estado` | text | Estado de la protesta (presentada, resuelta, etc.). |
| `resultado` | text | Resolución (aceptada, rechazada, parcial). |
| `motivo` | text | Motivo invocado por el recurrente. |
| `recurrente_id` | text | RUC del proveedor que protesta. |
| `recurrente_nombre` | text | Nombre del recurrente. |
| `tipo_evento` | text | Tipo de evento OCDS asociado. |
| `descripcion_evento` | text | Descripción del evento impugnado. |
| `estado_evento` | text | Estado del evento impugnado. |

**Uso en la tesis (Bloque A)**: `has_protesta` / `n_protestas` como señal de competencia disputada;
las protestas también se usaron para **validar extracción LLM** (Kehler et al. 2019, método
"protestas como ground truth externo").

---

## 9. `consultas_llamado` (123,283 filas) — Consultas de oferentes sobre el pliego

Preguntas de los interesados al convocante durante el periodo de aclaraciones. Útil para medir
**interés real** en el llamado.

| Columna | Tipo | Significado |
|---|---|---|
| `consulta_id` | text (PK) | ID de la consulta. |
| `compiled_release_id` | text | Compiled release OCDS. |
| `nro_licitacion` | text | Llamado. |
| `fecha` | timestamp | Fecha de la consulta. |
| `titulo` | text | Título/objeto de la consulta. |
| `descripcion` | text | Contenido de la pregunta. |
| `respuesta` | text | Respuesta oficial del convocante. |
| `fecha_respuesta` | timestamp | Fecha de la respuesta. |
| `autor_id` | text | RUC del consultante. |
| `autor_nombre` | text | Nombre del consultante. |
| `year` | smallint | Año del llamado. |

**Uso en la tesis (Bloque A)**: `tiene_consultas` y volumen de consultas = señal de competencia;
consultas de tipo "¿se puede interpretar X?" pueden revelar cláusulas ambiguas redactadas a medida.

---

## 10. `criterios_llamado` (45,779 filas) — Criterios y requisitos de evaluación

**HTML/procesado de los criterios técnicos y requisitos de calificación** publicados por la DNCP
(texto libre). Complementa `document_features_v2`: aquí está el detalle de las condiciones
(ej. experiencia, capacidad financiera) en formato semi-estructurado.

| Columna | Tipo | Significado |
|---|---|---|
| `id` | integer (PK) | ID interno. |
| `compiled_release_id` | text | Compiled release OCDS. |
| `nro_licitacion` | text | Llamado. |
| `criterio_id` | text | ID del criterio. |
| `criterio_titulo` | text | Título del criterio (ej. "Evaluación Financiera"). |
| `criterio_descripcion` | text | Descripción del criterio. |
| `criterio_fuente` | text | Fuente/origen del criterio (sección del pliego). |
| `grupo_id` | text | ID del grupo de criterios. |
| `grupo_descripcion` | text | Descripción del grupo (ej. "Capacidad Financiera", "Experiencia"). |
| `requisito_id` | text | ID del requisito específico. |
| `requisito_titulo` | text | Título del requisito (ej. "Ratio de liquidez > 1"). |
| `requisito_valor` | text | Valor/umbral del requisito (ej. `1.20`). |
| `year` | smallint | Año del llamado. |

**Uso en la tesis (Bloque B)**: fuente alternativa/cruz para `ratio_liquidez_minimo`,
`ratio_endeudamiento_maximo`, `anos_experiencia_minima`, `num_referencias_minimo` — permite validar
la extracción del transformer. Los **umbrales exigidos** (liquidez muy alta, experiencia excesiva)
son red flags de "specs a medida".

---

## 11. `items_licitacion` (1,878,237 filas) — Renglones/ítems del llamado

**La tabla más grande del dataset.** Un ítem por renglón del pliego (descripción, cantidad, unidad,
precio referencial y clasificación UNSPSC). Base para medir complejidad, fragmentación y
especificidad de lo contratado.

| Columna | Tipo | Significado |
|---|---|---|
| `item_id` | text (PK) | ID del ítem OCDS. |
| `nro_licitacion` | text | Llamado. |
| `descripcion` | text | Descripción del bien/servicio (texto libre). |
| `cantidad` | numeric | Cantidad a contratar. |
| `unidad_id` | text | Código de la unidad de medida. |
| `unidad_nombre` | text | Nombre de la unidad (kg, unidad, litro, hora, etc.). |
| `monto` | numeric | Precio referencial unitario/total del ítem (PYG). |
| `moneda` | text | Moneda. |
| `clasificacion_id` | text | Código de clasificación DNCP. |
| `clasificacion_desc` | text | Descripción de la clasificación. |
| `unspsc_id` | text | Código UNSPSC. |
| `unspsc_desc` | text | Descripción UNSPSC (estándar internacional de productos). |

**Uso en la tesis (Bloque A/B)**: `cantidad_items` real (no sufrir el 0 de `licitaciones`),
especificidad de la descripción (descripciones vaga vs muy específicas → specs a medida),
diversidad de clasificaciones UNSPSC en un mismo llamado.

---

## 12. `categorias` (82 filas) — Catálogo de categorías principales

Tabla auxiliar de categoría → descripción.

| Columna | Tipo | Significado |
|---|---|---|
| `category_id` | text (PK) | Código de categoría (ej. `services`, `goods`, `works` o códigos numéricos). |
| `descripcion` | text | Descripción legible de la categoría. |

**Uso**: enriquecer las tablas de features con el nombre de la categoría para lectura humana.

---

## 13. `convocantes` (459 filas) — Instituciones convocantes (UC)

Las **Unidades de Contratación** del estado paraguayo que publican llamados.

| Columna | Tipo | Significado |
|---|---|---|
| `convocante_id` | text (PK) | RUC de la institución/UC. |
| `nombre` | text | Nombre (ej. "Universidad Nacional de Asunción – Rectorado"). |
| `region` | text | Región del país (**conocido: vacío en la fuente**). |
| `localidad` | text | Localidad (**conocido: vacío en la fuente**). |
| `direccion` | text | Dirección física. |
| `api_enriched_at` | timestamp | Fecha de enriquecimiento por API. |

**Uso**: `convocante_region`/`convocante_localidad` en `document_economic_features` están vacíos por
esto; no usar como variable. El nombre permite agrupar por institución.

---

## 14. `proveedores` (13,256 filas) — Registro de proveedores

Datos maestros de las empresas que participan en el sistema.

| Columna | Tipo | Significado |
|---|---|---|
| `proveedor_id` | text (PK) | RUC del proveedor. |
| `ruc` | text | Número de RUC. |
| `nombre_comercial` | text | Nombre comercial registrado. |
| `nombre_legal` | text | Razón social legal. |
| `tipo_entidad` | text | Tipo (persona física / jurídica / consorcio). |
| `tamanio` | text | Tamaño de la empresa — **conocido: vacío en la fuente** (no usar). |
| `tipo_actividad` | text | Actividad económica principal. |
| `region` | text | Región registrada. |
| `localidad` | text | Localidad. |
| `direccion` | text | Dirección. |
| `email` | text | Correo (leer con cautela por privacidad). |
| `telefono` | text | Teléfono. |
| `url_web` | text | Sitio web. |
| `tipo_entidad_detalle` | text | Detalle del tipo de entidad (ej. "Microempresa MIPYMES"). |
| `escala` | text | Escala MIPYME declarada. |
| `nivel_institucional` | text | Nivel institucional registrado. |
| `api_enriched_at` | timestamp | Fecha de enriquecimiento por API. |

**Uso en la tesis (Bloque A)**: **conexiones entre proveedores** (misma dirección/email/RUC) =
señal de colusión; `tipo_entidad_detalle`/`escala` (MIPYMES) para medir si un proveedor pequeño gana
contratos grandes (`is_high_value_single_bidder`).

---

## 15. `proveedor_productos` (0 filas) — Productos por proveedor

Vacío en el dataset. El proveedor y sus productos/servicios disponibles.

| Columna | Tipo | Significado |
|---|---|---|
| `id` | integer (PK) | ID interno. |
| `proveedor_id` | text | RUC del proveedor. |
| `producto_id` | text | ID del producto (clasificación). |
| `producto_nombre` | text | Nombre del producto. |

---

## 16. `hitos_contrato` (0 filas) — Hitos/plazos del contrato

Vacíos en el dataset. Hitos (entregas, pagos parciales) previstos/cumplidos del contrato.

| Columna | Tipo | Significado |
|---|---|---|
| `id` | integer (PK) | ID interno. |
| `compiled_release_id` | text | Compiled release OCDS. |
| `contrato_id` | text | Contrato. |
| `nro_licitacion` | text | Llamado. |
| `hito_id` | text | ID del hito OCDS. |
| `orden_compra_id` | text | ID de la orden de compra relacionada. |
| `titulo` | text | Título del hito (ej. "Entrega final"). |
| `tipo` | text | Tipo de hito. |
| `codigo` | text | Código del hito. |
| `fecha_prevista` | timestamp | Fecha prevista. |
| `fecha_cumplida` | timestamp | Fecha real de cumplimiento (retraso = fecha_cumplida - prevista). |
| `estado` | text | Estado del hito. |
| `year` | smallint | Año. |

---

## 17. `pliegos_secciones` (2,775,485 filas) — Secciones de los pliegos de bases

**La tabla central del texto no estructurado.** Contiene los pliegos de bases y condiciones partidos
en **secciones individuales** (una fila = una sección del documento con su encabezado y contenido).
Es la fuente de `document_features` y `document_features_v2`.

| Columna | Tipo | Significado |
|---|---|---|
| `document_id` | text (PK) | ID único de la sección concreta. |
| `nro_licitacion` | text | Llamado al que pertenece el pliego. |
| `category_id` | text | Categoría del llamado. |
| `year` | smallint | Año del pliego. |
| `title` | text | Encabezado original de la sección (ej. "Formas y condiciones de pago"). |
| `title_normalized` | text | Título normalizado/minúsculas sin puntuación para agrupar (ej. `formas_y_condiciones_de_pago`). |
| `page` | smallint | Número de página del PDF original. |
| `line_start` | integer | Línea inicial dentro del documento extraído. |
| `line_end` | integer | Línea final de la sección. |
| `depth` | smallint | Nivel jerárquico de la sección (1 = título principal, 2 = subsección, etc.). |
| `content_length` | integer | Nº de caracteres del contenido (bruto). |
| `estimated_tokens` | integer | Tokens estimados del contenido (útil para limit cir LLM). |
| `word_count` | integer | **Número de palabras** de la sección (calculado y verificado). |
| `size_bytes` | integer | Tamaño en bytes del texto. |
| `content_text` | text | **Texto completo de la sección** (el dato de mayor valor para extracción). |
| `content_clean` | ARRAY | Texto limpio partido en líneas (array). |
| `content_length_clean` | integer | Longitud del texto limpio. |
| `cluster_id` | integer | Cluster de agrupación de secciones similares (para clasificar títulos reales). |

**Uso en la tesis (Bloque B)**:
- Aquí se aplican las **regex** y, donde fallan, el **LLM** (fallback de conf<0.8) para extraer
  `document_features_v2`.
- `title_normalized` agrupa secciones equivalentes entre documentos (ej. "analisis de precios ofertados").
- `word_count`, `page`, `line_start/end` alimentan las características estructurales de
  `document_features` (extensión, cobertura de títulos).

> **Particiones por año**: `pliegos_secciones_2021` (89,074), `_2022` (369,336), `_2023` (500,532),
> `_2024` (824,880) y `_2025` (991,663). Misma estructura; `pliegos_secciones` es la vista consolidada.

---

## 18. `document_features` (31,321 filas × 571 cols) — Características estructurales de cada documento

**Características por documento/llamado** derivadas de `pliegos_secciones`.

**Columnas base**:

| Columna | Significado |
|---|---|
| `nro_licitacion` (PK) | Llamado. |
| `year` | Año. |
| `category_id` | Categoría. |
| `total_sections` | Total de secciones del pliego. |
| `unique_titles` | Títulos distintos detectados. |
| `avg_content_length` / `std` / `max` / `sum` | Distribución de longitud de las secciones. |
| `avg_tokens` / `std` / `max` / `sum` | Distribución de tokens. |
| `avg_size_bytes` / `std` / `max` / `sum` | Distribución del tamaño en bytes. |
| `sum_word_count` / `avg_word_count` | Total y media de palabras del pliego. |
| `avg_page` / `max_page` | Rango de páginas de las secciones. |
| `gini_content_length` | **Gini de la distribución** de longitudes (concentración del texto en pocas secciones). |
| `title_entropy` | **Entropía** de los títulos (diversidad de contenido del pliego). |

**Columnas por título** (~60 títulos agrupados del pliego), cada título aporta **7 columnas**:

| Sufijo | Significado |
|---|---|
| `has_<titulo>` | Si el documento contiene una sección con ese título (0/1). |
| `len_<titulo>` | Longitud en caracteres de la sección. |
| `tok_<titulo>` | Tokens de la sección. |
| `word_<titulo>` | Palabras de la sección. |
| `count_<titulo>` | Nº de secciones con ese título en el documento. |
| `span_<titulo>` | Extensión (páginas cubiertas por esa sección). |
| `page_range_<titulo>` | Rango de páginas (inicio-fin). |

Ejemplos de títulos: `condicion_de_participacion`, `apertura_de_ofertas`, `periodo_de_validez_de_las_ofertas`,
`plazo_para_presentar_las_ofertas`, `solicitud_de_pago_de_anticipo`, `requisitos_de_calificacion`,
`criterios_de_adjudicacion`, `formas_y_condiciones_de_pago`, `experiencia_requerida`,
`capacidad_financiera`, `analisis_de_precios_ofertados`, `tasa_de_interes_por_mora`, `causales_de_terminacion_del_contrato`,
`fuerza_mayor`, `confidencialidad_de_la_informacion`, `plazo_de_reposicion_de_bienes`, etc.

**Uso en la tesis**: mide **qué tan completo/sofisticado es el pliego**. Pliegos muy cortos
(`sum_word_count` bajo, `unique_titles` bajo) pueden ser deliberadamente incompletos en llamados
directos; pliegos enormes y con muchas cláusulas punitivas sugieren restricción.

---

## 19. `document_features_v2` (31,321 filas × 108 cols) — Variables extraídas del pliego (regex + LLM)

**La tabla de features semánticas del pliego** — 26 campos extraídos con regex (y fallback LLM) de las
secciones de `pliegos_secciones`. Columnas de 3 tipos:

- **`binary_*`** (smallint 0/1): presencia de una cláusula en el pliego.
- **`pct_*` / `ratio_*` / `dias_*` / `num_*` / `anos_*` / `categorical_*`**: valor numérico/categórico.
- **`*_conf`** (real 0–1): confianza de la extracción (para campos hybrid).
- **`*_source`** (text): origen de la extracción (`regex`, `absent`, `llm`).

| Columna | Significado en pliegos paraguayos | Bloque |
|---|---|---|
| `pct_diferencia_precio_bajo` | Umbral % de ofertas por DEBAJO del precio referencial (ej. 25%) que requieren explicación — "bajas anormales". | B |
| `pct_diferencia_precio_alto` | Umbral % de ofertas por ENCIMA del precio referencial (ej. 15%) sujetas a análisis ("y del 15% para ofertas por encima"). | B |
| `binary_solicita_explicacion_precios` | 1 si el pliego exige explicar detalladamente el precio cuando difiere del referencial. | B |
| `binary_permite_declarar_inaceptable` | 1 si el pliego permite declarar oferta inaceptable por precios anormales. | B |
| `binary_tiene_multiplicidad_criterios` | 1 si se usan múltiples criterios de evaluación (no sólo precio). Menos transparencia = más riesgo. | B |
| `pct_multa_diaria` | Tasa diaria de multa por incumplimiento (%). | B |
| `binary_permite_rescision_por_multas` | 1 si la mora en pagos habilita rescisión. | B |
| `dias_notificacion_fuerza_mayor` | Días para notificar fuerza mayor. | B |
| `binary_incluye_ejemplos_tipicos` | 1 si la sección de fuerza mayor incluye ejemplos típicos (litigio, catástrofe, etc.). | B |
| `binary_obliga_continuar_cumplimiento` | 1 si obliga a continuar el cumplimiento durante fuerza mayor. | B |
| `pct_garantia_cumplimiento` | **% de garantía de fiel cumplimiento exigido (ej. 5%/10%)**. Varía entre instituciones. | B |
| `dias_presentacion_garantia` | Días para presentar la garantía tras firma (ej. 10). Plazos muy cortos = presión. | B |
| `binary_comite_puede_solicitar_aclaracion` | 1 si el comité puede pedir aclaración de ofertas. | B |
| `binary_prohibe_modificacion_sustancia` | 1 si se prohíbe modificar la sustancia de la oferta tras la apertura. | B |
| `binary_solicitud_respuesta_por_escrito` | 1 si las aclaraciones deben responderse por escrito. | B |
| `anos_experiencia_minima` | **Años de antigüedad mínima exigidos al proveedor** (ej. 3). Alta exigencia = specs a medida. | B |
| `num_referencias_minimo` | Referencias/contratos previos mínimos exigidos. | B |
| `pct_monto_referencia_minimo` | Monto mínimo de las referencias previas (% del llamado). | B |
| `binary_tiene_requisitos_por_lote` | 1 si los requisitos varían por lote. | B |
| `binary_requiere_experiencia_especifica_marca` | 1 si exige experiencia con una **marca específica** (red flag de marcas a medida). | B |
| `dias_reposicion` | Días para reponer bienes defectuosos. | B |
| `binary_dias_habiles` | 1 si los plazos se computan en días hábiles. | B |
| `binary_plazo_segun_autoridad` | 1 si el plazo de reposición lo fija la autoridad caso a caso (poco objetivo). | B |
| `num_categorias_documentos` | Cantidad de categorías de documentos exigidos para firmar contrato. Más burocracia = barrera. | B |
| `binary_requiere_certificado_tributario` | Exige certificado de no adeudar impuestos. | B |
| `binary_requiere_certificado_ips` | Exige certificado al día del IPS. | B |
| `binary_incluye_condiciones_contractuales`, `..._terminacion_incumplimiento/fraude/quiebra/conveniencia` | Clausulado de causales de terminación del contrato. | B |
| `binary_tiene_excepciones_confidencialidad`, `binary_menciona_plazo_confidencialidad` | Secciones de confidencialidad (transparencia). | B |
| `binary_responsabilidad_integral_proveedor`, `binary_proveedor_puede_rehusar_responsabilidad` | Responsabilidad del proveedor por defectos. | B |
| `binary_menciona_iva`, `binary_incluye_exoneraciones`, `binary_menciona_normas_oficiales` | Tratamiento fiscal del contrato. | B |
| `binary_permite_cambios_codigos`, `binary_cesion_derechos_contratante`, `binary_prohibe_reproduccion_no_autorizada` | Derechos intelectuales y de codificación. | B |
| `dias_validez_garantia` | Vigencia de la garantía de los bienes (días, ej. 365/540). | B |
| `binary_cubre_defectos_fabricacion`, `binary_tiene_cobertura_especifica` | Alcance de la cobertura de garantía. | B |
| `anos_experiencia_minima_tecnica` | Años de experiencia **técnica específica** (service/works). | B |
| `num_referencias_tecnicas_minimo` | Referencias técnicas previas mínimas. | B |
| `pct_monto_referencia_tecnica` | Monto mínimo de referencias técnicas (% del llamado). | B |
| `dias_pago_maximo` | **Plazo máximo del estado para pagar la factura (días)**. Pagos lentos = riesgo financiero del contratista. | B |
| `dias_aceptacion_rechazo_factura` | Días para aceptar/rechazar la factura. | B |
| `num_documentos_pago` | Documentos exigidos para cobrar (barrera burocrática del pago). | B |
| `binary_requiere_cuenta_bancaria_estatal` | Exige cuenta bancaria en entidad estatal (Banco Nacional de Fomento). | B |
| `binary_tiene_plan_entrega`, `binary_incluye_cronograma`, `binary_requiere_documentos_embarque` | Planificación de entrega (bienes importados). | B |
| `binary_tiene_requisitos_financieros` | Hay requisitos financieros de calificación. | B |
| `ratio_liquidez_minimo` | **Coeficiente de liquidez mínima exigido (activo corr./pasivo corr., ej. 1.2)**. Umbral muy alto = barrera. | B |
| `ratio_endeudamiento_maximo` | Endeudamiento máximo exigido. | B |
| `binary_requiere_estados_auditados` | Exige estados financieros auditados. | B |
| `binary_tiene_formula_reajuste` | Pliego con cláusula de reajuste de precios. | B |
| `num_componentes_formula` | Componentes de la fórmula de reajuste (índices). | B |
| `binary_referencia_ipc` | El reajuste referencia el IPC. | B |
| `binary_no_reajuste_si_atraso` | No hay reajuste si el atraso es del contratista. | B |
| `binary_criterio_menor_precio` | 1 si la adjudicación es estrictamente al menor precio (más transparente). | B |
| `binary_multiplicidad_criterios` | 1 si hay criterios múltiples de adjudicación. | B |
| `binary_contrato_abierto` | Contrato abierto (montos variables). | B |
| `binary_reserva_disminuir_cantidad` | La institución puede reducir cantidades. | B |
| `binary_requiere_garantia_anticipo` | Se exige garantía para el anticipo. | B |
| `categorical_tipo_garantia` | Tipo de garantía del anticipo (poliza, depósito, etc.). | B |
| `pct_anticipo` | **% de anticipo ofrecido. Conocido: sin valores no-cero en el dataset (todo 0)** — la fuente no lo reporta. | B |
| `num_pasos_verificacion` | Pasos del procedimiento de verificación del pago. | B |
| `binary_verifica_sinarh` | Verifica deuda vía SINARH. | B |
| `binary_requiere_declaracion_conflictos` | Exige declaración de conflicto de intereses. | B |
| `binary_requiere_registro_proveedores` | Exige inscripción en el registro de proveedores. | B |
| `binary_tiene_condiciones_anticipo` | Hay condiciones para otorgar anticipo. | B |
| `dias_solicitud_anticipo` | Días para **solicitar** el anticipo tras la firma (ej. 10). | B |
| `dias_pago_anticipo` | Días para que el estado **pague** el anticipo solicitado. | B |
| `binary_anticipo_solo_mipymes` | Anticipo sólo disponible para MIPYMES. | B |
| `pct_interes_mora_diario` | **Tasa diaria de interés por mora del estado en pagos (ej. 0.01%)**. Indica riesgo/salud de pago. | B |
| `binary_mora_desde_dia_siguiente` | La mora se computa desde el día siguiente al vencimiento. | B |
| `binary_menciona_criterios_dncp`, `binary_incluye_criterios_especificos_adicionales` | Referencias a criterios DNCP / criterios adicionales específicos. | B |

**Uso en la tesis (Bloque B)**: estas son las variables que OCP (2016) llama "context-specific data
fields" que faltan en OCDS puro. Implementan directamente los red flags de *rigged specifications*
(experiencia/liquidez/categorías/documentos) y de *condiciones de pago/garantías/anticipos* que
benefician a un oferente concreto.

---

## 20. `document_economic_features` (29,811 filas × 46 cols) — Features económicas y de colusión

**Tabla derivada que cruza OCDS estructurado + ejecución de contrato**, pensada para alimentar
directamente el modelo de anomalías. Una fila por llamado.

| Columna | Significado | Bloque |
|---|---|---|
| `nro_licitacion` (PK) | Llamado. | |
| `cantidad_oferentes` | Nº de ofertas. Común con `licitaciones`. | A |
| `es_unico_oferente` | bool — 1 si hubo una sola oferta (red flag #1). | A |
| `monto_estimado` | Monto estimado del llamado. | A |
| `duracion_oferta_dias` | Ventana para presentar ofertas. | A |
| `duracion_consultas_dias` | Ventana de consultas. | A |
| `tiene_consultas` | Hubo consultas. | A |
| `tiene_subasta` | Usó subasta electrónica. | A |
| `costo_pliego` | Costo del pliego (barrera de entrada). | A |
| `cantidad_items` | Ítems del llamado. | A |
| `cantidad_lotes` | Lotes. | A |
| `garantia_porcentaje` | % garantía. | B |
| `metodo_contratacion` | Método OCDS. | A |
| `criterio_adjudicacion` | Criterio OCDS. | B |
| `fecha_publicacion` | Fecha de publicación. | |
| `fecha_apertura` | Fecha de apertura. | |
| `monto_contrato` | Monto del contrato firmado. | C |
| `contract_value_ratio` | **monto_contrato / monto_estimado**. >1 = se firmó por encima del estimado. | C |
| `duracion_contrato_dias` | Duración del contrato. | C |
| `contrato_estado` | Estado del contrato (active/complete/terminated). | C |
| `n_enmiendas` | **Nº de enmiendas/adendas** (fuente: `enmiendas_contrato`). | C |
| `total_monto_enmiendas` | Monto acumulado de enmiendas. | C |
| `enmienda_ratio` | **total_monto_enmiendas / monto_contrato**. Alto = sobreprecio posterior a la firma. | C |
| `monto_adjudicado` | Monto de la adjudicación. | C |
| `precio_vs_estimado` | **monto_adjudicado / monto_estimado**. < 1 = oferta por debajo del referencial; desviaciones extremas = señal. | C |
| `winner_proveedor_id` | RUC del ganador. | A |
| `winner_tipo_entidad` | Tipo de entidad del ganador (persona física/jurídica, MIPYME). | A |
| `n_oferentes_distintos` | **Empresas DISTINTAS que ofertaron** (a diferencia de `cantidad_oferentes`, que cuenta ofertas). Es la medida de competencia real. | A |
| `n_pagos` | Nº de pagos al contrato. | C |
| `total_pagado` | Suma pagada al contrato. | C |
| `pago_vs_contrato_ratio` | **total_pagado / monto_contrato**. <1 muy bajo = contrato sin ejecutar/estancado. | C |
| `total_multas` | Multas acumuladas aplicadas. | C |
| `has_multas` | Hubo multas. | C |
| `avg_retencion` | Retención promedio por pago. | C |
| `has_protesta` | Hubo protesta sobre el llamado. | A |
| `n_protestas` | Nº de protestas. | A |
| `convocante_region` | Región de la institución — **vacío por calidad de datos**. | |
| `convocante_localidad` | Localidad — **vacío por calidad de datos**. | |
| `bidding_urgency` | **Urgencia**: valores altos indican ventanas comprimidas (relacionado con `duracion_oferta_dias`). ⚠️ Escala peculiar (valores grandes) — revisar antes de modelar. | A |
| `oferentes_por_item` | **Oferentes / cantidad_items** (intensidad por renglón). | A |
| `is_high_value_single_bidder` | bool — **1 si hubo oferta única en un llamado de alto monto** (combinación crítica de riesgo). | A |
| `overbudget_ratio` | **Desviación del monto respecto al presupuesto** (monto firma vs estimado; valores negativos = por debajo). | C |
| `winner_category_frequency` | **Nº de veces que el mismo ganador ganó en la misma categoría** (recurrencia de mercado). | A |
| `winner_total_contracts` | **Total de contratos del ganador** (peso/concentración del proveedor). | A |
| `is_repeat_winner` | bool — el ganador ya ganó llamados similares antes (recurrencia del mismo ganador, correlato de colusión). | A |
| `bidder_diversity` | **Diversidad de oferentes** (basada en `n_oferentes_distintos`/`cantidad_items`). ⚠️ **Conocido: 0.0 en toda la tabla** (por `cantidad_items` = 0 en la fuente) — no usar. | A |

**Uso en la tesis**: es la tabla de entrada para **Isolation Forest** (Bloques A+C juntos). Las columnas
A marcan riesgo de competencia/colusión; las C marcan riesgo de ejecución desviada.

---

## Resumen de relaciones entre tablas

```
categorias ────┐
               ├──< licitaciones >──── consultas_llamado (1:N)
convocantes ───┘        │              criterios_llamado (1:N)
                        │              items_licitacion (1:N)
                        │              oferentes (1:N) ──> proveedores
                        │              proveedores_notificados (1:N)
                        │              protestas (1:N)
                        ├──> adjudicaciones (1:N) ──> contratos (1:N)
                        │                              ├──> pagos_contrato (1:N)
                        │                              └──> enmiendas_contrato (1:N)
                        ├──> pliegos_secciones (1:N) ──> document_features (1:1)
                        │                              └──> document_features_v2 (1:1)
                        └──> document_economic_features (1:1, derivada)
```

## Datos maestros / derivados según calidad de datos

| Tabla | Estado |
|---|---|
| `contratos.cantidad_enmiendas`, `monto_total_enmiendas` | ⚠️ todo 0 (bug fuente) → usar `enmiendas_contrato` / `n_enmiendas` |
| `licitaciones.cantidad_items` | ⚠️ 0 en muchos registros (fuente) → contar en `items_licitacion` |
| `convocantes.region`, `localidad` | ⚠️ vacíos |
| `proveedores.tamanio`, `escala` | ⚠️ vacíos en la fuente |
| `document_economic_features.bidder_diversity` | ⚠️ todo 0.0 (deriva de `cantidad_items`=0) |
| `document_features_v2.pct_anticipo`, `num_componentes_formula` | ⚠️ todo 0 (campos irresolubles con regex/LLM) |
| `proveedor_productos`, `hitos_contrato` | Vacías (sin datos de la fuente) |
| `document_features_v2.bidding_urgency` | ⚠️ escala atípica — validar distribución antes de modelar |

## Columnas con significado de riesgo para anomalías (resumen)

1. **Oferta única / competencia**: `es_unico_oferente`, `cantidad_oferentes`, `n_oferentes_distintos`,
   `is_high_value_single_bidder`, `oferentes_por_item`, `bidding_urgency`, `duracion_oferta_dias`,
   `tiene_consultas`, `n_protestas`, `costo_pliego`.
2. **Colusión / recurrencia**: `winner_total_contracts`, `winner_category_frequency`,
   `is_repeat_winner`, identidad de oferentes en `oferentes` (direcciones/RUC compartidos).
3. **Specs a medida (pliego)**: `anos_experiencia_minima`, `anos_experiencia_minima_tecnica`,
   `num_referencias_minimo(_tecnico)`, `ratio_liquidez_minimo`, `ratio_endeudamiento_maximo`,
   `num_categorias_documentos`, `binary_requiere_experiencia_especifica_marca`,
   `binary_criterio_menor_precio`, `binary_multiplicidad_criterios`.
4. **Condiciones de pago / garantías**: `dias_pago_maximo`, `pct_interes_mora_diario`,
   `dias_solicitud_anticipo`, `dias_pago_anticipo`, `pct_garantia_cumplimiento`,
   `dias_presentacion_garantia`, `dias_validez_garantia`, `pct_anticipo`, `num_documentos_pago`.
5. **Ejecución / sobreprecio**: `contract_value_ratio`, `enmienda_ratio`, `n_enmiendas`,
   `precio_vs_estimado`, `overbudget_ratio`, `pago_vs_contrato_ratio`, `n_pagos`,
   `avg_retencion`, `total_multas`, demoras en `pagos_contrato`.

---

*Generado a partir del esquema real de la base `dncp` (25 tablas). Fechas cubiertas: 2021–2025.
Compatible con el estándar OCDS v1.1 de la DNCP.*
# FASE 1: Inventario y Clasificación Taxonómica

## Objetivo
Identificar todas las variables únicas (title_normalized), clasificarlas en la taxonomía de 8 categorías, inferir tipo de dato, y asignar relevancia para Isolation Forest (Alta/Media/Baja/Descartar).

## Variables Únicas Extraídas
**Total**: 125 variables únicas con field `pct_garantia_cumplimiento` en 3 pliegos distintos (nro_licitacion: 434669, 469279, 405512).

## Clasificación Taxonómica por Categoría

### 1. Competencia/Concurrencia (Restricción del acceso al mercado)
Variables donde el enfoque principal es la competencia en el mercado o número de oferentes.

| Variable | Justificación |
|---|---|
| oferentes en consorcio | Directamente related a la competencia de mercado; presencia de consorcios puede indicar colusión o coordinación (Fazekas et al., 2016) |
| margen de preferencia local - cps | Variable de competencia especifica de las licitaciones paraguayas; restringeparticipación a empresas locales |
| oferentes en consorcio (dup) | Misma categoría que above |

**Total**: 3 variables

### 2. Económica/Precio (Desviación de valor de mercado)
Variables que expresan valores monetarios, porcentajes de desviación o condiciones económicas.

| Variable | Justificación |
|---|---|
| analisis de precios ofertados | Variable central de desviación de precio; base para detección de sobreprecios (Abdou et al., 2022) |
| moneda de la oferta y pago | Define el sistema monetario y términos de pago; variable clave para análisis de precios |
| condiciones de la fruta in natura - cps | Condición productiva específica; afecta el valor de mercado del bien |
| porcentaje a ser adquirido de la agricultura familiar - cps | Porcentaje de adquisición obligatoria; variable de contenido de mercado |
| precio y formulario de la oferta | Variable directa de precio de oferta; base para comparación de valores |
| composicion de precios | Desglose del precio ofertado; análisis de estructura de costos |
| impuestos y Derechos | Cargos fiscales adicionales; afecta costo total del contrato |
| ofertas alternativas | Competencia indirecta; existencia de alternativas de proveeduría |
| contrato publico sostenible - cps | Condición económica con enfoque sostenibilidad; variable de valor de mercado |
| conflito de interes | Riesgo económico derivado de conflictos de interés en el proceso |

**Total**: 10 variables

### 3. Procedimental (Manipulación del diseño del proceso)
Variables relacionadas con el diseño, cronograma y procedimientos de la licitación.

| Variable | Justificación |
|---|---|
| documentoacion requerida para la firma del contrato | Procedimiento administrativo estándar; define requisitos previos |
| aclaracion de las ofertas | Procedimiento de aclaración durante el proceso |
| aclaracion de los documentos de la licitacion | Procedimiento de aclaración de pliego |
| aclaracion de los documentos de la convocatoria | Procedimiento de aclaración previa a la licitación |
| adenda | Modificación posterior al proceso licitatorio; variable crítica de ejecución |
| difusion de los documentos de la licitacion | Fase de difusión del pliego; temporalidad y alcance |
| difusion de los documentos de la convocatoria | Fase de difusión inicial del llamado |
| metodo de presentacion de ofertas | Modalidad de cómo se presentan las propuestas |
| metodo de evaluacion | criterios y metodología de evaluación técnica |
| notificacion del resultado | Etapa final de notificación de decisiones |
| notificaciones | Comunicaciones formales durante el proceso |
| apertura de ofertas | Etapa de apertura pública de propuestas |
| cierre de la convocatoria (implícita) | Fin del período de recepción de ofertas |
| plan de presentacion de ofertas | Planificación de la etapa de presentación |
| convenios modificatorios | Modificaciones acordadas durante la ejecución |
| resolucion de conflictos a traves del arbitraje | Mecanismo de resolución de disputas |
| resolucion de conflictos a traves de la mediacion | Mecan alternativo de resolución |
| resolucion de conflictos a traves del avenimiento | Otro mecanismo de resolución alternativa |
| evaluacion basada en multiplicidad de criterios | Sistema de evaluación con múltiples criterios |

**Total**: 23 variables

### 4. Proveedor (Riesgo del lado de la oferta)
Variables que caracterizan al proveedor/contratista y sus capacidades.

| Variable | Justificación |
|---|---|
| capacidad financiera | Capacidad económica del proveedor para ejecutar el contrato |
| capacidad tecnica | Capacidad técnica del proveedor para proveer los bienes/servicios |
| experiencia requerida | Años o nivel de experiencia mínima requerida |
| requisitos documentales para evaluar el criterio de capacidad financiera | Documentación bancaria y financiera del proveedor |
| requisitos documentales para evaluar el criterio de experiencia requerida | Documentos que respaldan la experiencia del proveedor |
| requisitos documentales para evaluar el criterio de capacidad tecnica | Documentación técnica del proveedor |
| requisitos de calificacion | Requisitos mínimos de cualificación |
| certificado de producto y empleo nacional - cps | Certificaciones obligatorias del producto/empresa |
| requisitos documentales para la evaluacion de la capacidad financiera | Documentación financiera adicional |
| requisitos documentales para la evaluacion de la capacidad tecnica | Documentación técnica adicional |
| requisitos documentales para la evaluacion de la experiencia | Documentación que respalda la experiencia |
| subcontratacion | Uso de subcontratistas; riesgo de fraccionamiento |
| responsabilidad del proveedor | Obligaciones y responsabilidades del contratista |
| identificacion de la unidad solicitante y justificaciones | Datos de la entidad solicitante |
| responsabilidad del traslado de los bienes | Carga del proveedor en el transporte |
| certificacion de producto (implícita) | Validación de calidad del producto |
| embalajes y documentos | Requisitos de empaque y documentación |
| lista de precios | Catálogo o lista de precios del proveedor |
| tiempo de funcionamiento de los bienes | Durabilidad esperada del bien suministrado |
| transporte | Modalidad y condiciones de transporte |
| manipulacion de ofertas (implícita) | Gestión de propuestas por parte del proveedor |

**Total**: 22 variables

### 5. Entidad contratante (Riesgo del lado de la demanda)
Variables que caracterizan a la entidad compradora o unidad operativa.

| Variable | Justificación |
|---|---|
| unidad operativa de contratacion (implícita en algunas) | Unidad administrativa encargada de la contratación |
| identificacion de la unidad solicitante y justificaciones | Datos de identificación de la entidad |
| obligatoriedad de declarar informacion del personal del proveedor, consultor o contratista en el sicp | Declaración de personal en el sistema informático |
| obligatoriedad de declarar informacion del personal del contratista en el sicp | Misma categoría; requisito de transparencia |
| (Otros identificadores de entidad) | Variables que identifican la entidad contratante |

**Total**: 4 variables

### 6. Temporal (Estacionalidad y presión de tiempo)
Variables relacionadas con fechas, plazos y temporalidad del proceso.

| Variable | Justificación |
|---|---|
| periodo de validez de la garantia de fiel cumplimiento de contrato | Plazo de vigencia de la garantía de ejecución |
| periodo de validez de la garantia de los bienes | Plazo de vigencia de garantía sobre los bienes |
| periodo de validez de la garantia de mantenimiento de oferta | Plazo de mantenimiento de la oferta |
| periodo de validez de las ofertas | Plazo de vigencia para presentar ofertas |
| periodo de validez de cumplimiento de contrato | Plazo general de cumplimiento |
| plazo para presentar las ofertas | Fecha límite de recepción de propuestas |
| plazo de reposicion de los suministros | Tiempo estimado para reabastecimiento |
| plazo de reposicion de bienes | Tiempo de reabastecimiento especifica |
| tiempo de funcionamiento de los bienes | Duración esperada del funcionamiento |
| fecha de convocatoria (implícita en plazos) | Fecha de apertura del llamado |
| cierre de ejercicio fiscal (implícito en plazos) | Fin del período contable |

**Total**: 10 variables

### 7. Ejecución contractual (Manipulación posterior a la firma)
Variables relacionadas con el desempeño del contrato después de adjudicado.

| Variable | Justificación |
|---|---|
| forma de instrumentacion de garantia de fiel cumplimiento de contrato | Cómo se ejecuta la garantía de cumplimiento |
| forma de instrumentacion de garantia de anticipo | Cómo se ejecuta la garantía de anticipo |
| garantias: instrumentacion, plazos y ejecucion. | Especificación completa de garantías |
| periodo de validez de la garantia de fiel cumplimiento de contrato | Plazo específico de la garantía |
| periodo de validez de la garantia de los bienes | Vigencia de garantía sobre producto |
| periodo de validez de mantenimiento de oferta | Vigencia para mantener la oferta vigente |
| periodo de validez de la garantia de cumplimiento de contrato | Plazo general de garantía |
| periodo de validez de las ofertas | Vigencia temporal de la propuesta |
| causa(s) de terminación del contrato | Razones de finalización anticipada |
| otras causales de terminación del contrato | Causas adicionales de terminación |
| resolucion de conflictos (implícita en terminación) | Mecanismos de resolución durante la ejecución |
| multas | Sanciones por incumplimiento |
| solicitud de pago de anticipo | Petición de anticipo por parte del contratista |
| solicitud de suspension de la ejecucion del contrato | Petición de suspensión durante la ejecución |
| retiro, sustitucion y modificacion de las ofertas | Acciones correctivas sobre las ofertas |
| limitacion de responsabilidad | Límite de responsabilidad contractual |
| inspecciones y pruebas | Validación técnica durante la ejecución |
| control de calidad | Monitoreo de calidad durante la ejecución |
| formato y firma de la oferta | Formalidades de suscripción |
| plan de entrega de los bienes | Cronograma de entrega |
| plan de prestacion de los servicios | Cronograma de prestación |
| sistema de presentacion de ofertas | Sistema usado para presentar propuestas |
| fraude y corrupcion | Actividades ilícitas durante la ejecución |

**Total**: 25 variables

### 8. Conexión política (Disponibilidad limitada)
Variables relacionadas con aspectos políticos o de poder en el proceso.

| Variable | Justificación |
|---|---|
| fraude y corrupcion | Variable explícita de corrupción; máximo riesgo político |
| PEP (personas expuestas políticamente) - implícita | Aunque no hay columna específica, el riesgo de corrupción tiene componente político |
| parentesco con funcionarios - implícito | Relaciones de poder que pueden influir en el proceso |
| donaciones de campaña - implícito | Financiamiento político vinculado a contratos |

**Total**: 1 variable (explicitamente "fraude y corrupcion")

---

## Resumen de Clasificación

| Categoría | # Variables | Porcentaje |
|---|---|---|
| 1. Competencia/Concurrencia | 3 | 2.4% |
| 2. Económica/Precio | 10 | 8.0% |
| 3. Procedimental | 23 | 18.4% |
| 4. Proveedor | 22 | 17.6% |
| 5. Entidad contratante | 4 | 3.2% |
| 6. Temporal | 10 | 8.0% |
| 7. Ejecución contractual | 25 | 20.0% |
| 8. Conexión política | 1 | 0.8% |
| **Total** | **125** | **100%** |

## Próximos Pasos
- **FASE 2**: Evaluación de relevancia para Isolation Forest con 3 criterios: (a) respaldo bibliográfico, (b) expresión como valor extremo/raro vs. contextual, (c) redundancia
- **FASE 3**: Identificar variables ausentes y configurar modelo
- **FASE 4**: Limitaciones y cierre

**Entregable**: `contexto_auditor/fase1_clasificacion_taxonomica.md`
# FASE 2: Evaluación de Relevancia para Isolation Forest

## Objetivo
Para cada variable, justificar relevancia usando 3 criterios:
1. (a) Respaldo bibliográfico en la sección B del documento de metodología
2. (b) Si la anomalía se expresaría como **valor extremo/raro** (favorable para Isolation Forest) o como **combinación contextual** (desfavorable)
3. (c) Redundancia con otras variables ya evaluadas

## Recomendación de preprocesamiento: Según la Sección D del documento de metodología

### Variables con Relevancia **Alta** para Isolation Forest

| Variable | Criterio (a) | Criterio (b) | Criterio (c) | Preprocesamiento |
|---|---|---|---|---|
| `analisis de precios ofertados` | Red flag DNCP: comparación oferta vs. monto referencial +20%/-30% | **Alta**. Desviación de precio cola pesada; mayoría cerca de precio de mercado; sobreprecios escasos y extremos | Baja redundancia | Normalización z-score |
| `ofertentes en consorcio` | "Ofrenda única" proxy corrupción más validado (Fazekas, Tóth y King 2016) | **Alta**. Ausencia de competencia (1 oferente vs. distribución normal) es estadísticamente rara | Baja redundancia | Target encoding o variable booleana |
| `criterios de adjudicacion` | Directrices OCDE 2025; indicadores riesgo licitación Fazekas | **Media-Alta**. Criterios poco claros o manipulados = valor extremo en proceso | Media redundancia | One-hot encoding |
| `moneda de la oferta y pago` | Variable con fuerte respaldo: desviación cambiaria es red flag FMI/Fazekas | **Alta**. Anomalías cambiarias = valores extremos en distribución de precios | Baja redundancia | Normalización z-score |
| `tasa de interes por mora` | Indicador riesgo financiero literatura auditoría compras | **Alta**. Tasas inusualmente altas/ bajas = valores extremos | Baja redundancia | Normalización; cola pesada favorable |
| `impuestos y derechos` | Cargos fiscales documentados literatura transparencia contractual | **Media**. Podría ser valor extremo si sobrecarga indebida; frecuentemente cargos estándar | Media redundancia | Normalización; variable de control |
| `fuerza mayor` | Concepto literatura procesos compra evento anormal presión tiempos | **Media**. Solicitud pode ser anómala (valor extremo) o legítima frecuente | Baja redundancia | Codificar booleana + variable días afectados |
| `causales de terminacion del contrato` | Red flags centrales auditoría compras Herreros-Martínez 2025 | **Alta**. Terminaciones anticipadas inusuales = valores raros en duración contrato | Baja redundancia | Target encoding si categórica |
| `adenda` | Ampliaciones monto/plazo post-adjudicación mecanismo "corrección" irregular Herreros-Martínez | **Alta**. Adendas monto/plazo poco frecuentes magnitud alta = patrón puntual clásico ISF | Baja redundancia | Si numérica mantener continua; si categórica one-hot |
| `periodo de validez garantía fiel cumplimiento` | Garantía cumplimiento variable crítica licitaciones | **Alta**. Vigencia garantía valor extremo (vencida o prórroga inusual) detectable ISF | Baja redundancia | Normalización z-score |

**Subtotal Alta**: 10 variables

### Variables con Relevancia **Media** para Isolation Forest

| Variable | Criterio (a) | Criterio (b) | Criterio (c) | Preprocesamiento |
|---|---|---|---|---|
| `capacidad financiera` | Concentración adjudicaciones mismo proveedor red flag oficial DNCP | **Media**. Distribución sesgada por tamaño empresa | Media redundancia | Target encoding (mediana/moda robusta) |
| `capacidad tecnica` | Categoría Proveedor taxonomía | **Media**. Similar a capacidad financiera distribución rubro | Baja redundancia | Target encoding |
| `experiencia requerida` | Documentado literatura riesgo requisitos excesivos | **Media-Baja**. Años experiencia normal/asimétrica; atípicos extremos requeridos ISF | Media redundancia | Target encoding u ordinal encoding |
| `periodo validez garantía bienes` | Periodos garantía estándar licitaciones bienes | **Media**. Distribución dependiente rubro (consumo vs equipo complejo) | Alta redundancia | Normalización si numérica |
| `plazo reposicion bienes` | Logística reposición compras bienes | **Media**. Tiempos dependen tipo bien proveedor logística | Baja redundancia | Normalización |
| `idioma oferta e idioma contrato` | Documentado casos licitaciones dirigidas barrera idioma | **Media**. Barrera puede restringir competencia (señal alerta) pero requisito estándar | Baja redundancia | One-hot encoding |
| `condicion participacion` | Requisitos participación limitan acceso mercado | **Media**. Restricciones pueden ser anómalas si excesivamente restrictivas | Media redundancia | One-hot encoding |
| `requisitos documentales evaluar participacion` | Documentación evaluación participación | **Media**. Compleatitud o requisitos inusuales pueden marcar anomalía | Alta redundancia | One-hot encoding |
| `requisitos doc capacidad financiera` | Documentación evaluación capacidad financiera | **Media**. Similar capacidad financiera por sí sola | Alta redundancia | Target encoding |
| `requisitos doc experiencia requerida` | Documentación validación experiencia | **Media-Baja**. Igual experiencia requerida por sí sola | Alta redundancia | Target encoding |
| `requisitos doc capacidad tecnica` | Documentación validación capacidad técnica | **Media**. Igual capacidad técnica por sí sola | Alta redundancia | Target encoding |
| `notificaciones` | Comunicaciones formales proceso | **Baja**. Frecuencia distribuye Poisson o normal; ISF no óptimo | Media redundancia | Descartar; análisis univariado z-score |
| `aclaracion documentos licitacion` | Clarificación pliego durante proceso | **Baja**. Frecuencia variable no necesariamente distribución valores extremos | Baja redundancia | Descartar |
| `difusion documentos licitacion` | Publicidad y difusión pliegos | **Baja-Media**. Plazos corta red flag OCDE 2025 pero variable "difusion" no expresa anomalía extreme ISF sin plazo asociado | Baja redundancia | Transformar a "días entre convocatoria y apertura" (derivado Temporal) |
| `aclaracion ofertas` | Clarificación ofertas presentadas | **Baja**. Proceso estándar licitación; ISF no optimizado | Baja redundancia | Descartar |
| `retiro sustitucion modificacion ofertas` | Baja o cambio oferentes durante proceso | **Media**. Variación oferentes puede ser señal colusión o retirada legítima | Media redundancia | Target encoding cantidad retiros; booleana sí/no |
| `formato firma oferta` | Formato presentación oferta | **Baja**. Variable binaria o pocas categorías; ISF no optimizada | Baja redundancia | One-hot encoding; fuera ISF principal |
| `sistema presentacion ofertas` | Modalidad presentación (electrónica, presencial) | **Baja**. Categórica pocas categorías; ISF no detecta anomalía si contenido estándar | Baja redundancia | One-hot encoding; fuera ISF principal |
| `notificacion resultado` | Comunicación resultado proceso | **Baja**. Evento terminal proceso; ISF no optimizado | Baja redundancia | Descartar |

**Subtotal Media**: 22 variables (incluyendo 5 descartadas como Baja pero clasificadas aquí por tener alguna justificación)

### Variables con Relevancia **Baja** para Isolation Forest

| Variable | Criterio (a) | Criterio (b) | Criterio (c) | Preprocesamiento |
|---|---|---|---|---|
| `idioma oferta` | Requisito lingüístico oferta | **Baja**. Barrera idioma restrictiva pero distribución predecible | Alta redundancia | Descartar |
| `certificado producto empleo nacional - cps` | Certificación contenido nacional | **Baja**. Certificación binaria o cumplimiento; ISF no detecta anomalía certificación | Alta redundancia | Descartar |
| `margen preferencia local - cps` | Margen preferencia local DNCP | **Media**. El margen no es anomalía; su aplicación sí | Media redundancia | Mantener variable derivada; transformar a "desviación del margen aplicado" |
| `porcentaje agricultura familiar - cps` | Cuota agricultura familiar ley paraguaya | **Media**. Dependiente rubro y cumplimiento legal | Baja redundancia | Relevancia Media; transformar a desviación respecto meta legal |
| `composicion precios` | Desglose estructura precios | **Baja**. Desglose contable; ISF no optimizado para variables estructurales | Alta redundancia | Descartar; usar análisis univariado separado |
| `responsabilidad proveedor` | Responsabilidades inherentes proveedor | **Baja**. Definiciones contractuales variadas | Baja redundancia | Descartar; variable control contractual |
| `subcontratacion` | Uso subcontratistas proveedor | **Baja-Media**. Frecuencia variable; ISF detectaría subcontratación atípica | Baja redundancia | Relevancia Baja; análisis posterior |
| `sucursales` | Presencia física/organizacional proveedor | **Baja**. Count distribuye típico empresarial | Baja redundancia | Descartar; variable perfil proveedor |
| `control calidad` | Control calidad durante ejecución | **Baja**. Indicadores calidad; ISF no optimizado a menos estándares extremos | Baja redundancia | Descartar; variable monitoreo posterior |
| `procedimiento ejecucion contrato` | Procedimiento ejecución contractual | **Baja**. Procedimiento operativo | Baja redundancia | Similar formalización; Descartar |
| `auditencia informativa` | Audiencia informativa proceso | **Baja**. Evento procedural específico | Baja redundancia | Descartar |
| `interpretacion` | Interpretación términos pliego | **Baja**. Variable subjetividad; ISF no optimizado textual | Baja redundancia | Descartar |
| `conflicto interes` | Conflicto interés proceso | **Baja**. Declaración o identificación conflicto | Baja redundancia | Descartar; revisión humana |
| `fraude y corrupcion` | Riesgo fraude corrupción proceso | **Baja**. Variable "objetivo" análisis no input predictor | Alta redundancia | Descartar; objetivo del análisis no input predictor |

**Subtotal Baja**: 16 variables

### Variables con Relevancia **Descartar** para Isolation Forest

| Variable | Criterio (a) | Criterio (b) | Criterio (c) | Acción |
|---|---|---|---|---|
| `documentacion requerida firma contrato` | Documentación estándar todo pliego | **Baja**. Requisito binario (tiene/tiene no); ISF no detecta anomalía "tener documentación" | Alta redundancia | Descartar para ISF; variable filtro previo |
| `notificaciones` | Comunicaciones formales proceso | **Baja**. Frecuencia Poisson/normal; ISF no óptimo | Media redundancia | Descartar; análisis univariado z-score |
| `aclaracion documentos licitación` | Clarificación pliegos proceso | **Baja**. Proceso estándar licitación | Baja redundancia | Descartar |
| `idioma oferta e idioma contrato` | Requisito lingüístico | **Baja**. Barrera idioma distribución predecible | Alta redundancia | Descartar |
| `certificado producto empleo nacional - cps` | Certificación contenido nacional | **Baja**. Certificación binaria | Alta redundancia | Descartar |
| `margen preferencia local - cps` | Margen preferencia local DNCP | **Media**. El margen no es anomalía; su aplicación sí | Media redundancia | Mantener derivada; no directa ISF |
| `porcentaje agricultura familiar - cps` | Cuota agricultura familiar | **Media**. Cuota legal vs cumplimiento real | Baja redundancia | Relevancia Media; transformar desviación vs meta |
| `composicion precios` | Desglose estructura precios | **Baja**. Desglose contable | Alta redundancia | Descartar |
| `responsabilidad proveedor` | Responsabilidades inherentes | **Baja**. Definiciones variadas | Baja redundancia | Descartar |
| `subcontratacion` | Uso subcontratistas | **Baja-Media**. Frecuencia variable | Baja redundancia | Descartar (duplicate ver entry anterior) |
| `sucursales` | Count sucursales proveedor | **Baja**. Count típico empresarial | Baja redundancia | Descartar |
| `unidad solicitante` | Entidad contratante | **Baja**. Identificador entidad | Alta redundancia | Descartar; agrupar entidad después modelo |
| `control calidad` | Indicadores calidad ejecución | **Baja**. Indicadores calidad | Baja redundancia | Descartar; variable monitoreo posterior |
| `procedimiento ejecucion contrato` | Procedimiento ejecución | **Baja**. Procedimiento operativo | Alta redundancia | Similar formalización; Descartar |
| `auditencia informativa` | Audiencia informativa | **Baja**. Evento procedural específico | Baja redundancia | Descartar |
| `interpretacion` | Interpretación términos | **Baja**. Subjetividad textual | Baja redundancia | Descartar |
| `conflicto interes` | Conflicto interés | **Baja**. Declaración identificación | Baja redundancia | Descartar; revisión humana |
| `fraude y corrupcion` | Riesgo fraude corrupción | **Baja**. Objetivo análisis no input predictor | Alta redundancia | Descartar; objetivo no input predictor |

**Subtotal Descartar**: 17 variables

---

## Cuadro Resumen Final de Relevancia ISF

| Variable | Categoría | Relevancia ISF | Justificación Crítica |
|---|---|---|---|
| analisis de precios ofertados | Económica/Precio | Alta | Red flag DNCP; cola de sobreprecios clásica ISF |
| ofertentes en consorcio | Competencia/Concurrencia | Alta | "Single bidder" proxy corrupción más validado (Fazekas 2016) |
| criterios adjudicacion | Procedimental | Alta-Media | Directrices OCDE 2025; riesgo licitación |
| moneda oferta y pago | Económica/Precio | Alta | Anomalías cambiarias = valores extremos |
| tasa interes por mora | Económica/Precio | Alta | Intereses inusuales = valores extremos |
| causales terminacion contrato | Ejecución contractual | Alta | Terminaciones inusuales = valores raros |
| adenda | Ejecución contractual | Alta | Modificaciones monto/plazo = patrón puntual ISF |
| periodo validez garantía | Ejecución contractual | Alta | Vigencia garantía = valores extremos |
| capacidad financiera | Proveedor | Media | Distribución sesgada por tamaño empresa |
| capacidad tecnica | Proveedor | Media | Similar a capacidad financiera |
| experiencia requerida | Proveedor/Procedimental | Media-Baja | Valores atípicos dependiendo rubro |
| periodo validez garantía bienes | Temporal | Media | Distribución dependiente rubro |
| plazo reposicion bienes | Temporal | Media | Tiempos logísticos atípicos |
| idioma oferta e contrato | Competencia/Concurrencia | Baja | Requisito estándar distribución predecible |
| condicion participacion | Competencia/Concurrencia | Media | Restricciones regulatorias vs excesivas |
| requisitos doc participacion | Procedimental | Media-Alta | Alta redundancia con otras doc participación |
| requisitos doc capacidad financiera | Proveedor | Media | Derivada de capacidad financiera |
| requisitos doc experiencia | Proveedor | Media-Baja | Derivada experiencia requerida |
| requisitos doc capacidad tecnica | Proveedor | Media | Derivada capacidad técnica |
| documentacion firma contrato | Procedimental | Descarta | Requisito binario, base para otras variables |
| notificaciones | Procedimental | Descarta | Frecuencia Poisson/normal, no ISF óptimo |
| aclaracion documentos licitación | Procedimental | Descarta | Proceso estándar licitación |
| difusion documentos licitación | Procedimental | Baja-Media | Transformar a "plazo publicación" derivado |
| aclaracion ofertas | Procedimental | Descarta | Proceso estándar |
| retiro sustitucion modificacion ofertas | Procedimental | Media | Variación oferentes detectable ISF |
| formato firma oferta | Procedimental | Baja | Variable binaria/pocas categorías |
| sistema presentacion ofertas | Procedimental | Baja | Modalidad presentación |
| notificacion resultado | Procedimental | Descarta | Evento terminal proceso |
| idioma oferta | Competencia/Concurrencia | Baja | Duplicate idioma oferta e contrato |
| certificado producto empleo nacional | Económica/Precio | Descarta | Certificación binaria |
| margen preferencia local | Económica/Precio | Media | El margen no es anomalía; su aplicación sí |
| porcentaje agricultura familiar | Económica/Precio | Media | Cuota legal vs cumplimiento real |
| composicion precios | Económica/Precio | Descarta | Desglose contable, no ISF óptimo |
| responsabilidad proveedor | Proveedor | Baja | Definición contractual variada |
| subcontratacion | Proveedor | Baja-Media | Frecuencia variable, no prioritario |
| sucursales | Proveedor | Baja | Count empresarial típico |
| unidad solicitante | Entidad contratante | Descarta | Identificador, no ISF óptimo |
| control calidad | Ejecución contractual | Baja | Indicadores calidad, no ISF |
| procedimiento ejecucion contrato | Ejecución contractual | Baja | Procedimiento operativo |
| auditencia informativa | Procedimental | Descarta | Evento procedural específico |
| interpretacion | Procedimental | Baja | Subjetividad textual |
| conflicto interes | Conexión política | Baja | Declaración, no valor extremo ISF |
| fraude y corrupcion | Conexión política | Descarta | Objetivo análisis, no input |

---

## Preprocesamiento Recomendado por Categoría

| Categoría | Tratamiento Recomendado |
|---|---|
| Categóricas alta cardinalidad (proveedor, entidad) | Target encoding (mediana/moda, robusta ante outliers) |
| Categóricas baja cardinalidad (modalidad contratación) | One-hot encoding |
| Numéricas precio/monto | Normalización z-score antes de comparar con k-means (ISF menos sensible a escala) |
| Fechas | Transformar en variables derivadas (días entre etapas, día de semana, mes, indicador cierre ejercicio fiscal) |
| Faltantes | Documentar tasa de nulos por variable; patrón de nulos puede ser informativo |

---

## Variables Ausentes (Red Flags Sección B no presentes en datos)

Estos red flags de alta prioridad según ranking ISF no aparecen como title_normalized en nuestro CSV. Conviene solicitarlos o derivarlos:

1. `cantidad_oferentes` (número total de oferentes recibidos) → Derivar de `ofertentes en consorcio` + conteo total
2. `monto_adjudicado` (monto efectivo adjudicado) → No está en CSV actual (solo `value` = porcentaje garantía)
3. `%_desviacion` (desviación % precio adjudicado vs. referencial) → Derivar de `analisis de precios ofertados` + precio referencial
4. `modalidad_contratacion` (tipo procedimiento: licitación directa, excepción, etc.) → No variable directa; derivar de `criterios adjudicacion`
5. `historial_alertas_entidad` (historial alertas entidad) → No en datos; solicitar a DNCP
6. `antigüedad_empresa` (antigüedad empresa proveedora) → No en datos; solicitar

---

## Configuración Inicial Sugerida Modelo Isolation Forest

- `n_estimators=100` (valores por defecto artículo original Liu et al. 2008)
- `max_samples=256` (muestras por árbol; volúmenes DNCP: ~9.500 adjudicaciones/año)
- Configuración por defecto: `contamination='auto'` o enfoque basado en percentil (p. ej., el 1% superior de puntajes)
- **Umbral de corte**: Recomendado approach basado en percentil (ej. 1% superior) en lugar de fijar `contamination` arbitrario

Variables a incluir en modelo inicial (Alta/Media):
1. `analisis de precios ofertados`
2. `ofertentes en consorcio`
3. `criterios adjudicacion`
4. `moneda oferta y pago`
5. `tasa interes por mora`
6. `causales terminacion contrato`
7. `adenda`
8. `periodo validez garantía fiel cumplimiento`
9. `capacidad financiera`
10. `capacidad tecnica`

**Archivo generado**: `contexto_auditor/fase2_relevancia_isolation_forest.md`
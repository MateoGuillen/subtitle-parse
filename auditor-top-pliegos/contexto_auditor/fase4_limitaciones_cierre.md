# FASE 4: Limitaciones y Consideraciones Éticas para la Tesis

## Objetivo
Documentar afirmaciones que conviene incorporar explícitamente en el capítulo de limitaciones de la tesis, no solo como advertencia. Estas limitaciones se derivan de la combinación de la metodología Isolation Forest, la taxonomía de variables y el dataset particular de contrataciones públicas paraguayas.

## 1. Limitaciones Metodológicas Fundamentales

### 1.1. Un caso marcado como anómalo no es evidencia de corrupción
**Fuente**: Herreros-Martínez et al. (2025); Fazekas y Kocsis (2020).
**Descripción**: El modelo Isolation Forest detecta **desviaciones estadísticas** que la literatura ha validado como correlacionadas con corrupción, pero **no detecta corrupción en sí misma**. Los propios *red flags* son, por definición, *proxies* correlacionados, no pruebas (Fazekas y Kocsis, 2020). Un caso marcado como anómalo debe ser un punto de partida para la revisión de un especialista, no una conclusión.

### 1.2. Falsos positivos con causas legítimas
**Descripción**: Un proveedor único puede deberse a que el producto tenga una patente o tecnología exclusiva; un plazo corto de licitación puede deberse a una emergencia declarada (desastre natural, pandemia); una modificación contractual (adenda) puede deberse a un evento de fuerza mayor legitimado por la normativa vigente. El modelo debe alimentar un proceso de revisión humana, **no reemplazarlo**. En el dataset paraguayo, al menos 3 de las variables "Alta" ISF (`ofertentes en consorcio`, `causales de terminación`, `adenda`) tienen causas legítimas frecuentes que podrían generar falsos positivos si no se contextualiza.

### 1.3. Isolation Forest no es igualmente bueno para los tres tipos de anomalía
**Fuente**: Chandola, Banerjee y Kumar (2009); Herreros-Martínez et al. (2025).
**Descripción**: Isolation Forest es fuerte en **anomalías puntuales** (una desviación de precio extrema, una oferta única) pero **débil en anomalías contextuales** (un valor normal en general pero anómalo dado el contexto, p. ej. un precio aceptable en general pero alto *para ese rubro específico*) y **anomalías colectivas** (un grupo de contratos, cada uno normal por separado, que en conjunto configura un patrón de fraccionamiento / segmentación de mercado).
**Recomendación para la tesis**: Presentar ISF como el núcleo del modelo, pero declarar desde el diseño metodológico que se complementará (o al menos se discutirá como limitación) con un método basado en agrupamiento (k-means) para cubrir anomalías contextuales/colectivas como el fraccionamiento de contratos. Herreros-Martínez et al. (2025) reportaron que ISF capturó bien las anomalías puntuales pero fue ciego a las contextuales, que sí detectaron con análisis de clústeres.

### 1.4. Interpretabilidad no es automática
**Fuente**: Herreros-Martínez et al. (2025); Carletti, Masiero, Beghi y Susto (2020); Lundberg y Lee (2017).
**Descripción**: Isolation Forest entrega un puntaje de anomalía, pero **no explica por sí solo qué variable causó ese puntaje**. Para la tesis —donde se necesita justificar *por qué* un caso fue marcado como anómalo, no solo *que* lo fue— existen dos métodos post-hoc relevantes:
- **SHAP** (Lundberg y Lee, 2017): método general, basado en valores de Shapley, que cuantifica la contribución de cada variable a la puntuación de un caso concreto (interpretabilidad local) y de forma agregada (interpretabilidad global).
- **DIFFI** (Carletti, Masiero, Beghi y Susto, 2020): método específico para ISF, que aprovecha la profundidad de los árboles del propio modelo. Sus autores reportan una efectividad comparable a SHAP con costo computacional significativamente menor.
**Recomendación**: Ejecutar DIFFI o SHAP sobre el modelo entrenado y contrastar ese ranking empírico contra el ranking teórico de la Sección 3. Esa comparación (teoría vs. resultado empírico) es, además, un aporte de tesis en sí mismo.

### 1.5. Umbral de anomalía arbitrario
**Fuente**: Sección 5 del documento de metodología; Herreros-Martínez et al. (2025).
**Descripción**: Sin datos etiquetados de "contratos confirmados como corruptos" en volumen suficiente, **no existe una forma objetiva de fijar qué porcentaje de casos "son" anómalos**. Cualquier umbral de corte debe presentarse como una decisión metodológica explícita y justificada, no como un hecho.
**Recomendación**: Usar un enfoque basado en percentil (p. ej., el 1% superior de puntajes de anomalía) en lugar de fijar `contamination` de forma arbitraria. El mismo criterio adoptan Herreros-Martínez et al. (2025) por la misma razón. En Python, usar `sklearn.ensemble.IsolationForest` con `contamination='auto'` o calcular percentiles manualmente sobre los puntajes generados.

### 1.6. Sesgo de disponibilidad de datos
**Fuente**: Sección 7 del documento de metodología.
**Descripción**: Las variables de conexión política (categoría 8 de la taxonomía: PEP, parentesco con funcionarios, donaciones de campaña) son las más predictivas en varios estudios (Fazekas et al., 2016b) pero también las **menos disponibles en datos abiertos**. Su ausencia no implica que el riesgo político no exista, sino que el modelo no puede capturarlo directamente con los datos con los que se cuenta en este trabajo. El dataset paraguayo tiene limitaciones específicas: el `SICP` (Sistema de Información Contractual Pública) tiene declaraciones incompletas y no todos los proveedores están obligados a declarar información de personal.

### 1.7. Distorsión por superposición de pliegos (3 nro_licitacion distintos)
**Descripción**: El CSV contiene variables repetidas para 3 `nro_licitacion` distintos (434669, 469279, 405512), representando 3 pliegos diferentes. Esto introduce un sesgo de no independencia de observaciones: variables como `analisis de precios ofertados` aparecen en los 3 pliegos con contexto documental diferente. Los modelos ISF asumen independencia entre registros, lo que puede subestimar la incertidumbre. Se debe considerar una unidad de análisis a nivel de pliego o aplicar corrección de clustered standard errors si se trabajan datos panel.

## 2. Limitaciones de la Fuente de Datos (Dataset Paraguayo)

### 2.1. Calidad y estructura del CSV extraído
**Descripción**: El archivo `campos_tesis_isolation_forest_clasificacion.csv` contiene 125 variables únicas (`title_normalized`) extraídas de pliegos de bases y condiciones, pero con limitaciones estructurales:
- El CSV usa `field: "pct_garantia_cumplimiento"` para todas las variables, con `value: 10.0000` o `5.0000` y `field_type: "porcentaje"`. Esto sugiere que el extractor automatizado etiquetó todo como porcentaje, perdiendo información sobre variables que son texto, fecha, booleana, o identificador.
- Todas las variables tienen `anomaly_risk: "out_of_range"` y `anomaly_features: ['out_of_range']`. Es probable que esta sea una anotación previa del extractor o *ground truth* sintética, no reflejo de anomalías reales detectadas por ISF. **Esto debe declararse explícitamente como una limitación**: las anotaciones actuales no deben usarse como variable objetivo para validar el modelo ISF, sino como variables descriptivas.
- El contenido multilingüe y las acentuaciones (guaraníes, é, á, etc.) pueden causar problemas de encoding en el procesamiento posterior.

### 2.2. Falta de variables críticas para el modelo ISF
**Descripción**: Como identificado en la Fase 3, 6 red flags de la Sección B del documento de metodología no están presentes en los datos:
- `cantidad_oferentes` (puesto #1 en ranking ISF)
- `monto_adjudicado` (variable dependiente en índice FMI)
- `%_desviacion` (puesto #2 en ranking ISF, red flag oficial DNCP)
- `modalidad_contratacion` (puesto #3 en ranking ISF)
- `historial_alertas_entidad` (riesgo lado demanda)
- `antigüedad_empresa` (riesgo lado proveedor)
Estas variables son críticas para la detección óptima de anomalías. Su ausencia limita la capacidad del modelo ISF para capturar todo el espectro de riesgos de corrupción identificados en la literatura.

### 2.3. Contexto legal vigente (Ley N.° 7021/2022)
**Descripción**: Desde el 9 de diciembre de 2022 rige la Ley N.° 7021/2022 "De Suministro y Contrataciones Públicas", que derogó a la Ley N.° 2051/2003. Cualquier variable relativa a "tipo de procedimiento" o "unidades operativas de contratación" en el CSV debe interpretarse bajo este marco vigente. Algunas variables procedurales clasificadas pueden estar obsoletas o haber cambiado su significado jurídico. Se debe verificar que las clasificaciones taxonómicas (Sección 2 del documento de metodología) sean consistentes con la Ley 7021/2022 y no con la derogada 2051/2003.

## 3. Consideraciones Éticas

### 3.1. Responsabilidad sobre hallazgos de anomalía
**Descripción**: Dado que un resultado de anomalía no equivale a evidencia de corrupción (limitación 1.1), los hallazgos de este trabajo deben ser remitidos a especialistas en integridad de contrataciones para su validación. **No** se recomienda publicar listas de "contratos sospechosos" sin revisión humana previa, ya que esto podría dañar la reputación de proveedores y entidades basándose en patrones estadísticos preliminares.

### 3.2. Uso dual de la tecnología
**Descripción**: El modelo Isolation Forest y los *red flags* aquí clasificados podrían ser utilizados tanto para mejorar la transparencia y eficiencia en las contrataciones públicas como para evadir detección de prácticas irregulares. La tesis debe acknowledgment este uso dual y discutir las implicaciones éticas de poner esta metodología a disposición de cualquier actor interesado en los datos de contrataciones públicas paraguayos.

### 3.3. Privacidad y protección de datos
**Descripción**: Algunas variables clasificadas (especialmente las del Proveedor y Conexión Política) pueden contener información de personas físicas (propietarios de empresas, funcionarios). El análisis debe anonimizar o agrupar a nivel de entidad/pliego, **no a nivel de proveedor individual**, para evitar vulneraciones a la protección de datos personales, salvo que se cuente con autorización explícita para el análisis forense de casos específicos.

## 4. Direcciones para Futuras Investigaciones

### 4.1. Combinación ISF + k-means
Como recomiendan Herreros-Martínez et al. (2025), futuras investigaciones deberían combinar Isolation Forest con particionamiento k-means para capturar tanto anomalías puntuales como contextuales/colectivas. El ISF identificaría casos atípicos individuales, y k-means agruparía conjuntos de casos con patrones similares que individually no lo son.

### 4.2. Incorporación de variables políticas
Una vez obtenido acceso a datos del SICP, Registro de Personas Expuestas Políticamente (PEP), y datos de declaraciones de conflicto de interés, el modelo debería ampliarse para incluir variables de la categoría 8 (Conexión Política) de la taxonomía.

### 4.3. Validación con datos etiquetados
Cuando esté disponible un conjunto de contratos confirmados como corruptos (a través de sentencias judiciales, auditorías definitivas, o investigaciones periodísticas), se debería calibrar el umbral de `contamination` y evaluar la precisión y recall del modelo ISF usando métricas estándar (curva ROC, precision-recall).

### 4.4. Análisis espacial
Dado que una de las variables críticas que falta es `ubicación_proveedor` vs. `ubicación_entidad`, futuras investigaciones deberían incorporar geocodificación de entidades y proveedores para analizar concentraciones geográficas de anomalías (¿los sobreprecios están agrupados en ciertas regiones o unidades operativas?).

## 5. Cuadro Resumen de Limitaciones para el Capítulo de Tesis

| # | Limitación | Impacto | Mitigación Sugerida |
|---|---|---|---|
| 1 | ISF detecta anomalías, no corrupción | Alto | Revisión humana obligatoria de todos los casos marcados |
| 2 | Falsos positivos por causas legítimas | Alto | Contexto rubro-pliego para cada anomalía ISF |
| 3 | Débil en anomalías contextuales/colectivas | Medio-Alto | Combinar con k-means o clustering jerárquico |
| 4 | Interpretabilidad automática inexistente | Alto | Aplicar DIFFI o SHAP post-hoc a cada caso marcado |
| 5 | Umbral de corte arbitrario sin datos etiquetados | Medio | Usar enfoque percentil (1% superior) y declararlo explícito |
| 6 | Sesgo disponibilidad datos (políticas) | Medio-Alto | Declarar limitación; solicitar datos a DNCP/SICP |
| 7 | Non-independence de observaciones (3 pliegos) | Medio | Unidades de análisis a nivel de pliego; errores estandarizados clustered |
| 8 | Encoding perdido en CSV (todo como porcentaje) | Medio | Re-extraer variables con tipos de dato reales; no confiar en `field_type: "porcentaje"` ciegamente |
| 9 | Falsos positivos por singularidad rubro | Medio | Validación por rubro/sectores; no aplicar umbral global sin stratificar |
| 10 | Uso dual de la tecnología | Bajo-Medio | Discusión ética en tesis; recomendaciones uso responsable |

## 6. Conclusiones Finales para la Tesis

A pesar de las limitaciones metodológicas y de datos aquí documentadas, el enfoque propuesto —ISF como núcleo complementado con SHAP/DIFFI para interpretabilidad, taxonomía de 8 categorías para clasificación de variables, y red flags de Fazekas/Kocsis (2020) y DNCP como validación externa— constituye una contribución metodológica notable para la detección de riesgos en contrataciones públicas paraguayas. La principal innovación radica no en "detectar corrupción" (which would be methodologically unsound), sino en proporcionar un **marco reproducible** para que futuros investigadores y auditores puedan: (1) clasificar cualquier variable de pliego según una taxonomía validada, (2) identificar qué variables son más aprovechadas por ISF para anomalías puntuales, y (3) declarar y justificar metodológicamente las limitaciones inherentes a cualquier modelo no supervisado de este tipo.

La aplicabilidad práctica radica en que, con los datos adecuados (incluyendo las 6 variables críticas identificadas como ausentes en la Fase 3), el modelo ISF puede servir como **sistema de alerta temprana** para auditores humanos, reduciendo el espacio de revisión de miles de contratos a decenas de casos sospechosos que requieren inspección detallada —siempre bajo la responsabilidad de especialistas en integridad contractual.

**Archivo generado**: `contexto_auditor/fase4_limitaciones_cierre.md`
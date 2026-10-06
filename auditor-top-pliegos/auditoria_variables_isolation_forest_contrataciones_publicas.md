# Auditoría de variables para la detección de anomalías en pliegos de licitación pública mediante Isolation Forest

**Documento de soporte metodológico** — clasificación de variables, ranking de relevancia, fundamento del algoritmo y prompt maestro reutilizable, con aplicación general y adaptación específica a Paraguay (DNCP).

> **Nota de alcance:** no se adjuntó un archivo CSV a esta conversación, por lo que este documento construye una **taxonomía y un ranking de propósito general**, basados en (a) la literatura académica sobre indicadores de riesgo de corrupción en contrataciones públicas, (b) la literatura técnica sobre Isolation Forest aplicada a auditoría/fraude, y (c) la estructura real de datos que publican la DNCP de Paraguay y los estándares internacionales (OCDS). Al final se entrega un **prompt maestro** para que apliques (o le pidas a otro LLM que aplique) este mismo marco a las columnas reales de tu CSV.

---

## Índice

1. [Fundamento conceptual](#1-fundamento-conceptual)
2. [Taxonomía de clasificación de variables](#2-taxonomía-de-clasificación-de-variables)
3. [Ranking de variables por relevancia para Isolation Forest](#3-ranking-de-variables-por-relevancia-para-isolation-forest)
4. [Por qué Isolation Forest y qué le aporta cada variable](#4-por-qué-isolation-forest-y-qué-le-aporta-cada-variable)
5. [Recomendaciones de preprocesamiento](#5-recomendaciones-de-preprocesamiento)
6. [Adaptación al caso paraguayo (DNCP)](#6-adaptación-al-caso-paraguayo-dncp)
7. [Limitaciones y consideraciones éticas](#7-limitaciones-y-consideraciones-éticas)
8. [Prompt maestro reutilizable](#8-prompt-maestro-reutilizable)
9. [Referencias](#9-referencias)

---

## 1. Fundamento conceptual

Dos cuerpos teóricos sostienen este documento:

**(a) Teoría de "red flags" (banderas rojas) en contrataciones públicas.** La corrupción no se observa directamente en los datos; se infiere a partir de *proxies* correlacionados con ella. Fazekas et al. establecen que los red flags son "medidas proxy de corrupción, que señalan riesgos de corrupción, más que coincidir perfectamente con ella" (Fazekas, Tóth y King, 2016; Fazekas y Kocsis, 2020). Esto es metodológicamente clave para tu tesis: un modelo de anomalías **no detecta corrupción**, detecta **desviaciones estadísticas** que la literatura ha validado como correlacionadas con ella.

**(b) Teoría de detección de anomalías.** Chandola, Banerjee y Kumar (2009), en la revisión más citada del campo, clasifican las anomalías en tres tipos: *puntuales* (un registro es anómalo respecto al resto), *contextuales* (anómalo solo dentro de un contexto específico) y *colectivas* (un grupo de registros es anómalo en conjunto). Esta distinción es central para tu elección de variables, porque **Isolation Forest está optimizado para anomalías puntuales**, no para las otras dos (ver sección 4).

La combinación de ambos marcos —qué variables importan según la evidencia empírica de corrupción, y qué variables aprovecha mejor el algoritmo elegido— es el criterio que ordena el ranking de la sección 3.

---

## 2. Taxonomía de clasificación de variables

Se adapta la clasificación en cuatro bloques de Fazekas et al. (2016a) —citada y reutilizada en Fazekas y Kocsis (2024, vía *Social Indicators Research*)— **ampliándola** con tres categorías (temporal, ejecución contractual, documental) que la literatura de auditoría de compras trata por separado (Herreros-Martínez et al., 2025).

| Categoría | Qué mide | Variables típicas (ES / OCDS aprox.) |
|---|---|---|
| **1. Competencia/Concurrencia** | Restricción del acceso al mercado | `cantidad_oferentes`, `oferta_unica` (bool) → `tender.numberOfTenderers` |
| **2. Económica/Precio** | Desviación de valor de mercado | `precio_referencial`, `monto_adjudicado`, `%_desviación` → `tender.value`, `awards.value` |
| **3. Procedimental** | Manipulación del diseño del proceso | `modalidad_contratacion`, `criterios_adjudicación`, `plazo_publicación` → `tender.procurementMethod` |
| **4. Proveedor** | Riesgo del lado de la oferta | `id_proveedor`, `adjudicaciones_totales_proveedor`, `antigüedad_empresa`, `ubicación_proveedor` → `parties[role=supplier]` |
| **5. Entidad contratante** | Riesgo del lado de la demanda | `id_entidad`, `unidad_operativa_contratación`, `historial_alertas_entidad` → `buyer` |
| **6. Temporal** | Estacionalidad y presión de tiempo | `fecha_convocatoria`, `fecha_apertura`, `fecha_adjudicación`, `cierre_ejercicio_fiscal` |
| **7. Ejecución contractual** | Manipulación posterior a la firma | `adendas`, `ampliación_monto`, `ampliación_plazo`, `multas`, `terminación_anticipada` → `contracts.amendments` |
| **8. Conexión política** *(disponibilidad limitada)* | Captura del proceso por actores con poder | `PEP` (persona expuesta políticamente), `parentesco_funcionario`, `donaciones_campaña` |

Esta tabla responde a la parte "clasifícalas" de tu pedido. La sección siguiente responde a "haz un top".

---

## 3. Ranking de variables por relevancia para Isolation Forest

**Criterio de ordenamiento** (aplicado de forma consistente): (i) robustez empírica en la literatura de corrupción en compras públicas, (ii) si su distribución típica favorece el mecanismo de aislamiento de Isolation Forest —es decir, si la anomalía se expresa como un **valor raro/extremo** en esa variable (lo que el algoritmo detecta bien) y no solo como una **combinación inusual** de valores normales (lo que detecta mal, ver sección 4), y (iii) disponibilidad realista en datos abiertos de compras públicas (Paraguay/OCDS).

### Núcleo crítico (1–6)

**1. Número de oferentes recibidos / oferta única ("single bidder")**
Es el proxy de corrupción con más respaldo empírico de toda la literatura del área: Fazekas, Tóth y King (2016) lo usan como insumo directo de su índice de riesgo de corrupción; Fazekas y Kocsis (2020) lo emplean como medida objetiva de corrupción de alto nivel en 2.8 millones de contratos europeos; Wittberg y Fazekas (2023) construyen un indicador de "ratio de oferta única" a nivel de proveedor. Las propias Directrices de la OCDE (2025) para combatir la colusión en licitaciones listan "el mismo proveedor resulta ganador con frecuencia" como señal de alerta central. Para Isolation Forest es ideal: en un mercado competitivo, la ausencia de competencia es estadísticamente rara y se expresa como un valor extremo (1 oferente vs. una distribución normal de varios), exactamente el patrón que el algoritmo aísla con pocas particiones.

**2. Desviación del precio adjudicado respecto al precio referencial (%)**
Es, literalmente, el red flag que la propia DNCP de Paraguay ya monitorea de forma oficial ("Comparativo de Oferta Adjudicada respecto al monto referencial", con umbral declarado de +20%/-30%). A nivel internacional, Abdou, Basdevant, David-Barrett y Fazekas (2022, FMI) construyen su índice de riesgo de corrupción precisamente sobre el diferencial de precio como variable dependiente, mostrando que cada red flag adicional se asocia a un incremento de precio medible. Es una variable continua con cola pesada (la mayoría de las contrataciones se acerca al precio de mercado; los sobreprecios son escasos y extremos), lo que la vuelve casi un caso de libro para Isolation Forest.

**3. Modalidad/tipo de procedimiento de contratación**
Distingue licitación pública abierta de contratación directa, excepción, adjudicación simplificada, etc. El estudio del FMI (Abdou et al., 2022) identifica los "contratos directos" como categoría de procedimiento de alto riesgo por su asociación estadística con desviaciones de precio. Fazekas, Cingolani y Tóth (2016) la incluyen entre los indicadores de riesgo de licitación ("tendering risk indicators"). Es categórica pero con una categoría minoritaria (excepción/directa) que concentra el riesgo — útil tanto para Isolation Forest (tras codificación) como para filtrado exploratorio previo.

**4. Concentración/recurrencia de adjudicaciones a un mismo proveedor por entidad**
Segundo red flag oficial de la DNCP ("Recurrencia de Contrataciones de una Entidad al mismo Proveedor"). Fazekas, Tóth y King (2016) documentan explícitamente el vínculo entre oferta única y "adjudicación recurrente de contratos a la misma empresa" como mecanismo de extracción de renta sostenido en el tiempo. Requiere ingeniería de variables (una agregación proveedor–entidad–período), pero es de las que mejor separa comportamiento sistemático de casos aislados.

**5. Plazo de publicación (días entre convocatoria y apertura de ofertas)**
Aparece de forma consistente en la lista de detección de colusión de la OCDE (2025) y en la revisión de Fazekas, Cingolani y Tóth (2016) como "período de difusión muy breve". Un plazo anormalmente corto reduce de facto la competencia sin necesidad de una exclusión explícita. Es una variable numérica con asimetría natural (la mayoría de los procesos respeta plazos mínimos normativos; los atípicos son minoría), favorable para el algoritmo.

**6. Modificaciones contractuales posteriores a la adjudicación (adendas de monto o plazo)**
La literatura de auditoría de compras identifica el "requester–approver mismatch" y las desviaciones respecto al flujo de aprobación estándar como anomalías centrales del proceso de compra (Herreros-Martínez et al., 2025). Ampliaciones de monto/plazo poco frecuentes y de magnitud alta son un patrón puntual clásico: fáciles de aislar y con alta relevancia práctica, porque suelen ser el mecanismo final de "corrección" de un contrato mal adjudicado desde el inicio.

### Complementarias de alto valor (7–12)

| # | Variable | Por qué aporta | Fuente |
|---|---|---|---|
| 7 | Concentración de adjudicaciones por entidad/unidad operativa de contratación | Permite detectar instituciones atípicas, no solo proveedores atípicos; el FMI la usa como indicador de riesgo del lado de la demanda | Abdou et al. (2022) |
| 8 | Fraccionamiento: montos que se agrupan justo debajo de un umbral de modalidad | Patrón de "muchos valores casi idénticos por debajo del límite", detectable como agrupamiento anómalo; señalado como preocupación central de control interno en compras | Herreros-Martínez et al. (2025) |
| 9 | Especificidad/"direccionamiento" de las especificaciones técnicas del pliego | Validado en casos documentados (ej. caso Elios, Hungría) donde especificaciones "a medida" precedieron adjudicaciones de oferta única | David-Barrett y Fazekas (2016) |
| 10 | Nivel de ejecución del Programa Anual de Contrataciones (PAC) | Red flag oficial de la DNCP; generalizable como "cumplimiento de planificación" en cualquier país con programación anual de compras | DNCP (s.f.) |
| 11 | Variables temporales: estacionalidad, cierre de ejercicio fiscal, día de semana de apertura | Complementa el resto; la concentración de compras al cierre del ejercicio fiscal es un patrón documentado de baja fiscalización | Chandola et al. (2009) — marco de anomalías contextuales/temporales |
| 12 | Tiempo entre apertura de ofertas y adjudicación | Tanto un período extremadamente corto (evaluación superficial) como uno extremadamente largo (posible favorecimiento durante la deliberación) son señales, según el FMI | Abdou et al. (2022) |

**Menciones adicionales** (relevancia media, útiles como variables de control o si tu CSV las trae): ubicación geográfica del proveedor vs. entidad (concentración geográfica, señalada por la OCDE 2025), rubro/categoría de bien o servicio (el riesgo varía por sector), antigüedad de la empresa proveedora, y sanciones previas del proveedor en el registro correspondiente.

---

## 4. Por qué Isolation Forest y qué le aporta cada variable

### 4.1 Justificación del algoritmo para tu propósito

Isolation Forest (Liu, Ting y Zhou, 2008; extendido en Liu, Ting y Zhou, 2012) no perfila lo "normal" para luego buscar lo que no encaja —como hacen los métodos basados en distancia o densidad—, sino que **aísla directamente las anomalías**, aprovechando que son "pocas y diferentes": requieren menos particiones aleatorias para quedar separadas del resto en un árbol de aislamiento. Esto le da tres propiedades que encajan de forma casi exacta con tu problema:

- **No supervisado.** No existen, ni en Paraguay ni en la mayoría de los países, bases etiquetadas de "contratos confirmados como corruptos" en volumen suficiente para entrenar un clasificador. Herreros-Martínez et al. (2025) señalan esto explícitamente como la razón por la que los métodos supervisados son inviables en auditoría de compras y optan, igual que se recomienda aquí, por métodos no supervisados.
- **Complejidad computacional lineal** (aproximadamente O(n log n)) y bajo requerimiento de memoria (Liu et al., 2008), lo que permite trabajar con los volúmenes reales de la DNCP (más de 9.500 adjudicaciones promedio anuales según cifras publicadas por el propio portal de Contrataciones Públicas).
- **No asume distribución ni depende de métricas de distancia**, a diferencia de k-means o de métodos basados en densidad como DBSCAN, lo que lo hace más robusto frente a la mezcla de variables numéricas y categóricas típica de un pliego de licitación (Liu et al., 2012).

### 4.2 La limitación que debes declarar en tu tesis

Isolation Forest **no es igualmente bueno para los tres tipos de anomalía** de Chandola et al. (2009). Es fuerte en anomalías **puntuales** (una desviación de precio extrema, una oferta única) pero débil en anomalías **contextuales** (un valor normal en general pero anómalo dado el contexto, p. ej. un precio aceptable en general pero alto *para ese rubro específico*) y **colectivas** (un grupo de contratos, cada uno normal por separado, que en conjunto configura un patrón de fraccionamiento). Herreros-Martínez et al. (2025), en el estudio metodológicamente más cercano al tuyo —detección de anomalías en procesos de compra reales con Isolation Forest— resuelven esto combinándolo con k-means y con métodos univariados (z-Score), reportando que Isolation Forest capturó bien las anomalías puntuales pero fue ciego a las contextuales, que sí detectó el análisis de clústeres. **Recomendación para tu tesis:** presenta Isolation Forest como el núcleo del modelo, pero declara desde el diseño metodológico que se complementará (o al menos se discutirá como limitación) con un método basado en agrupamiento para cubrir anomalías contextuales/colectivas como el fraccionamiento.

### 4.3 La interpretabilidad no es automática

Isolation Forest entrega un puntaje de anomalía, pero **no explica por sí solo qué variable causó ese puntaje** (Herreros-Martínez et al., 2025). Para tu tesis —donde vas a necesitar justificar *por qué* un caso fue marcado como anómalo, no solo *que* lo fue— existen dos métodos post-hoc específicamente relevantes:

- **SHAP** (Lundberg y Lee, 2017): método general, basado en valores de Shapley, que cuantifica la contribución de cada variable a la puntuación de un caso concreto (interpretabilidad local) y de forma agregada (interpretabilidad global). Es el que usan Herreros-Martínez et al. (2025) en su aplicación a compras.
- **DIFFI** (*Depth-based Isolation Forest Feature Importance*; Carletti, Masiero, Beghi y Susto, 2020): método específico para Isolation Forest, que aprovecha la profundidad de los árboles del propio modelo. Sus autores reportan una efectividad comparable a SHAP con un costo computacional significativamente menor, lo que lo hace atractivo si tu dataset es grande.

Para responder de forma explícita a "qué variables aportan más" con evidencia —y no solo con la justificación teórica de la sección 3— la recomendación metodológica es ejecutar DIFFI o SHAP **sobre tu propio modelo entrenado** y contrastar ese ranking empírico contra el ranking teórico de este documento. Esa comparación (teoría vs. resultado empírico) es, además, un aporte de tesis en sí mismo.

---

## 5. Recomendaciones de preprocesamiento

| Tipo de variable | Tratamiento recomendado | Nota |
|---|---|---|
| Categóricas de alta cardinalidad (proveedor, entidad) | *Target encoding* (sustituir cada categoría por la media/mediana/moda de una variable objetivo, p. ej. el monto) | Preferido sobre one-hot porque no incrementa la dimensionalidad; Herreros-Martínez et al. (2025) comparan frecuencia, media, mediana y moda y encuentran mejores resultados con mediana/moda por su robustez ante outliers |
| Categóricas de baja cardinalidad (modalidad de contratación) | One-hot encoding | Cardinalidad baja, no genera explosión dimensional |
| Numéricas de precio/monto | Normalización (z-score) antes de cualquier método basado en distancia complementario; Isolation Forest en sí es menos sensible a la escala, pero conviene mantener consistencia si se compara con k-means | Liu et al. (2008); Herreros-Martínez et al. (2025) |
| Fechas | Transformar en variables derivadas (días entre etapas, día de semana, mes, indicador de cierre de ejercicio fiscal) en vez de usar la fecha cruda | Práctica estándar de ingeniería de variables en auditoría de compras |
| Faltantes | Documentar la tasa de valores nulos por variable antes de imputar; en compras públicas, un patrón de nulos puede ser en sí mismo informativo (ej. ausencia sistemática de precio referencial) | — |

**Configuración inicial sugerida del modelo:** `n_estimators=100`, `max_samples=256`, siguiendo los valores por defecto recomendados en el artículo original (Liu et al., 2008) y validados empíricamente en el contexto de compras por Herreros-Martínez et al. (2025), quienes reportan que aumentar `n_estimators` por encima de 100 no mejoró el ranking de anomalías. Para el umbral de corte, dado que se desconoce la proporción real de anomalías en un dataset no etiquetado, considera un enfoque basado en percentil (p. ej. el 1% superior de puntajes) en vez de fijar `contamination` de forma arbitraria — el mismo criterio que adoptan Herreros-Martínez et al. (2025) por la misma razón. En Python, el flujo típico usa `sklearn.ensemble.IsolationForest`, `category_encoders` (o una implementación propia) para *target encoding*, y la librería `shap` o una implementación de DIFFI para la interpretabilidad.

---

## 6. Adaptación al caso paraguayo (DNCP)

**Marco legal vigente.** Desde el 9 de diciembre de 2022 rige la Ley N.° 7021/2022 "De Suministro y Contrataciones Públicas", que derogó a la Ley N.° 2051/2003 y creó el Sistema Nacional de Suministro Público. Cualquier variable relativa a "tipo de procedimiento" o "unidades operativas de contratación" en tu CSV debe interpretarse bajo este marco vigente.

**La DNCP ya opera un sistema de red flags propio**, lo cual es un hallazgo relevante para el marco teórico de tu tesis: valida externamente el enfoque, y te permite contrastar tu modelo contra un estándar oficial ya en producción. Los cuatro indicadores públicos actuales son:

1. Entidades sin publicación del Programa Anual de Contrataciones (PAC).
2. Nivel de ejecución del PAC (con corte trimestral: <50% ejecución baja, 50–79% regular, ≥80% óptima).
3. Comparativo de la oferta adjudicada respecto al monto referencial (alerta si supera +20% o cae por debajo de -30%).
4. Recurrencia de contrataciones de una entidad al mismo proveedor.

Nótese que los ítems 3 y 4 **coinciden exactamente** con las variables #2 y #4 de tu ranking (sección 3), lo que es evidencia adicional —de una fuente institucional independiente de la literatura académica— de que esas dos variables son las de mayor prioridad.

**Fuentes de datos recomendadas para tu CSV:**
- Portal de Datos Abiertos de la DNCP (`contrataciones.gov.py/datos`), con datasets de convocatorias, adjudicaciones, contratos y proveedores en CSV, alineados al estándar internacional *Open Contracting Data Standard* (OCDS).
- Sistema de Información Estratégica (SIE) de la DNCP, que incluye tablero de control, estadísticas y los propios red flags oficiales — útil como fuente de validación cruzada de tus resultados.
- Catálogo de bienes y servicios (hasta nivel 5), útil para construir la variable de "rubro" de forma estandarizada.

**Contexto regional.** Paraguay no está aislado en este tipo de esfuerzo: un diagnóstico conjunto de CAF (Banco de Desarrollo de América Latina) y la Red Interamericana de Compras Gubernamentales (RICG/OEA), sobre cerca de 7 millones de registros de Colombia, Ecuador, Panamá y Paraguay (2016–2020), concluyó que los datos de estos cuatro países tienen potencial de reutilización para analítica predictiva y aprendizaje automático orientado a riesgo de corrupción (CAF y RICG, 2021). En 2019, además, la propia DNCP participó en un taller regional de la Open Contracting Partnership sobre red flags en América Latina, donde su entonces director comprometió la publicación de al menos tres red flags públicos (Open Contracting Partnership, 2019) — compromiso que, como viste arriba, ya se cumplió y se amplió a cuatro. Para el estado del arte de tu tesis, vale la pena citar también la tesis doctoral de Torres Berru (2023, Universidad de Salamanca), que aplica minería de datos y PLN para detectar sobreprecio, favoritismo y sesgo en el sistema de contratación pública de Ecuador (SERCOP) y documenta explícitamente que **América Latina es un nicho de investigación desatendido** en este tema — un argumento directo para la justificación de tu tesis.

---

## 7. Limitaciones y consideraciones éticas

Estas son afirmaciones que conviene incorporar explícitamente en el capítulo de limitaciones de tu tesis, no solo como advertencia:

- **Un caso marcado como anómalo no es evidencia de corrupción.** Herreros-Martínez et al. (2025) lo remarcan: la anomalía es un punto de partida para la revisión de un especialista, no una conclusión. Los propios red flags son, por definición, proxies correlacionados, no pruebas (Fazekas y Kocsis, 2020).
- **Falsos positivos con causas legítimas.** Un proveedor único puede deberse a un producto patentado; un plazo corto puede deberse a una emergencia declarada; una modificación contractual puede deberse a un evento de fuerza mayor. El modelo debe alimentar un proceso de revisión humana, no reemplazarlo.
- **Sesgo de disponibilidad de datos.** Las variables de conexión política (categoría 8 de la taxonomía) son las más predictivas en varios estudios (Fazekas et al., 2016b) pero también las menos disponibles en datos abiertos — su ausencia no implica que el riesgo político no exista, sino que el modelo no puede capturarlo directamente con los datos con los que cuentes.
- **Umbral de anomalía arbitrario.** Como se explicó en la sección 5, sin datos etiquetados no existe una forma objetiva de fijar qué porcentaje de casos "son" anómalos; cualquier umbral debe presentarse como una decisión metodológica explícita y justificada, no como un hecho.

---

## 8. Prompt maestro reutilizable

Instrucciones de uso: reemplazá la sección `DATOS DE ENTRADA` con el nombre de cada columna de tu CSV real y 5 valores de muestra. Pegá el prompt completo (incluyendo la base de conocimiento) en otro LLM, o en una nueva conversación. Si el modelo que lo recibe tiene acceso a búsqueda web, el prompt se lo indica explícitamente para que actualice y verifique la bibliografía.

```
# ROL
Actúas como (1) auditor de datos especializado en integridad de contrataciones/licitaciones
públicas y (2) científico de datos experto en detección no supervisada de anomalías con el
algoritmo Isolation Forest (Liu, Ting y Zhou, 2008). El destinatario final de tu análisis es
una tesis universitaria, por lo que cada afirmación relevante debe llevar sustento
bibliográfico verificable en formato (Autor, Año).

# BASE DE CONOCIMIENTO
Usa este marco como referencia obligatoria. Aplícalo, no lo repitas de forma literal.

## A. Taxonomía de variables en contrataciones públicas (8 categorías)
1. Competencia/Concurrencia (cantidad de oferentes, oferta única)
2. Económica/Precio (precio referencial, monto adjudicado, % de desviación)
3. Procedimental (modalidad de contratación, plazos de publicación, criterios de adjudicación)
4. Proveedor (identificación, concentración de adjudicaciones, antigüedad, ubicación, sanciones)
5. Entidad contratante (identificación, unidad operativa de contratación, historial)
6. Temporal (fechas clave, duración de etapas, estacionalidad, cierre de ejercicio fiscal)
7. Ejecución contractual (adendas, ampliaciones de monto/plazo, multas, terminación anticipada)
8. Conexión política -disponibilidad limitada- (personas expuestas políticamente, parentesco
   con funcionarios, financiamiento de campañas)

## B. Red flags con mayor respaldo empírico (orden de prioridad, con justificación breve)
1. Oferta única / bajo número de oferentes -> proxy de corrupción más validado en la
   literatura (Fazekas, Tóth y King, 2016; Fazekas y Kocsis, 2020; OCDE, 2025).
2. Desviación % del precio adjudicado vs. precio referencial -> usado como variable
   dependiente en el índice de riesgo del FMI (Abdou, Basdevant, David-Barrett y Fazekas, 2022).
3. Modalidad de contratación (directa/excepción vs. abierta) -> las modalidades directas
   concentran el riesgo (Abdou et al., 2022; Fazekas, Cingolani y Tóth, 2016).
4. Recurrencia proveedor-entidad -> mecanismo de extracción de renta sostenida
   (Fazekas, Tóth y King, 2016).
5. Plazo de publicación anormalmente corto -> reduce competencia de facto
   (OCDE, 2025; Fazekas, Cingolani y Tóth, 2016).
6. Modificaciones contractuales post-adjudicación (adendas de monto/plazo) ->
   mecanismo de "corrección" de una adjudicación irregular (Herreros-Martínez et al., 2025).
7. Concentración de adjudicaciones por entidad/unidad operativa -> riesgo del lado
   de la demanda (Abdou et al., 2022).
8. Fraccionamiento (montos agrupados bajo un umbral de modalidad) ->
   anomalía colectiva/contextual, no puntual (Herreros-Martínez et al., 2025).
9. Especificaciones técnicas "direccionadas" a un proveedor -> validado en casos
   documentados de licitaciones manipuladas (David-Barrett y Fazekas, 2016).
10. Tiempo entre apertura y adjudicación (muy corto o muy largo) -> ambos extremos
    son señal (Abdou et al., 2022).

## C. Por qué Isolation Forest, y qué variables aprovecha mejor
- Es no supervisado: no requiere ejemplos etiquetados de corrupción confirmada,
  que casi nunca existen en volumen suficiente (Herreros-Martínez et al., 2025).
- Aísla anomalías en lugar de perfilar lo normal; complejidad ~O(n log n)
  (Liu, Ting y Zhou, 2008).
- Favorece variables donde la anomalía se expresa como VALOR EXTREMO/RARO
  en esa dimensión (ej. % de desviación de precio, cantidad de oferentes).
  Es DÉBIL para anomalías CONTEXTUALES (normal en general, anómalo dado el
  contexto) y COLECTIVAS (un grupo de casos normales por separado que en
  conjunto son un patrón, ej. fraccionamiento) -> Chandola, Banerjee y Kumar (2009).
- Recomendación: combinar con un método de agrupamiento (k-means) para cubrir
  lo que Isolation Forest no detecta bien (Herreros-Martínez et al., 2025).
- No es interpretable por sí solo: requiere SHAP (Lundberg y Lee, 2017) o
  DIFFI (Carletti, Masiero, Beghi y Susto, 2020) para explicar qué variable
  causó cada puntaje de anomalía.

## D. Reglas de preprocesamiento
- Categóricas de alta cardinalidad (proveedor, entidad): target encoding
  (mediana o moda, más robustas que la media ante outliers).
- Categóricas de baja cardinalidad (modalidad de contratación): one-hot encoding.
- Fechas: transformar en variables derivadas (días entre etapas, estacionalidad),
  nunca usar la fecha cruda.
- Configuración inicial de referencia: n_estimators=100, max_samples=256
  (valores del artículo original, Liu et al., 2008, validados en compras por
  Herreros-Martínez et al., 2025). Umbral de corte basado en percentil
  (ej. 1% superior), no en un valor de "contamination" arbitrario.

# DATOS DE ENTRADA
El usuario te va a dar, para cada columna de su CSV real, este formato
(reemplazar todo lo que está entre corchetes; repetir el bloque por cada variable):

Variable: [nombre_de_la_columna]
Muestras: [valor_1, valor_2, valor_3, valor_4, valor_5]

---- PEGAR AQUÍ LAS VARIABLES REALES DEL CSV ----




---- FIN DE LOS DATOS DE ENTRADA ----

# TAREA (ejecutar en este orden)
1. Para cada variable recibida: clasifícala dentro de la taxonomía (sección A) e
   infiere su tipo de dato real (numérico, categórico, fecha, texto libre,
   booleano, identificador) a partir de las 5 muestras dadas, no solo del nombre.
2. Determina si corresponde a alguno de los red flags de la sección B, a una
   variable de control/contexto, o a una variable sin relevancia clara para
   detección de anomalías (ej. un ID interno sin significado semántico).
3. Asigna un nivel de relevancia para Isolation Forest: Alta / Media / Baja /
   Descartar. Justifica cada asignación con TRES criterios explícitos:
   (a) respaldo en la literatura de la sección B,
   (b) si la anomalía en esa variable se expresaría como valor extremo/raro
       (favorable para Isolation Forest) o como combinación contextual
       (desfavorable, señalarlo como limitación),
   (c) redundancia con otras variables ya evaluadas en tu análisis.
4. Para cada variable de relevancia Alta o Media, recomienda el preprocesamiento
   específico según la sección D.
5. Construye una tabla final con TODAS las variables recibidas, ordenadas de
   mayor a menor relevancia, con columnas: Variable | Categoría (taxonomía) |
   Tipo de dato inferido | Relevancia | Preprocesamiento recomendado |
   Justificación (con cita).
6. Señala explícitamente qué variables de la sección B NO están presentes en
   los datos recibidos, y para cada una indica si conviene solicitarla o si
   puede derivarse de las columnas existentes (ej. "días entre convocatoria y
   apertura" se puede calcular si existen ambas fechas, aunque no venga como
   columna separada).
7. Sugiere una configuración inicial de Isolation Forest (hiperparámetros y
   criterio de umbral) según la sección D, aclarando que debe calibrarse
   empíricamente sobre los datos reales.
8. Cierra con un párrafo de limitaciones: aclara que un resultado de anomalía
   no equivale a evidencia de corrupción, que se recomienda revisión humana,
   y que Isolation Forest por sí solo no cubre anomalías contextuales/colectivas.

# REGLAS DE RIGOR ACADÉMICO
- No inventes cifras, porcentajes o hallazgos que no estén en la base de
  conocimiento de este prompt o que no puedas verificar de forma independiente.
- Si tienes acceso a herramientas de búsqueda web, úsalas para verificar y
  actualizar la bibliografía, y para buscar literatura adicional específica
  sobre las variables que el usuario te dé (indícalo explícitamente cuando
  una fuente sea nueva, distinta de las de este prompt).
- Si NO tienes acceso a búsqueda web, usa únicamente las fuentes de la sección
  "Base de conocimiento" y decláralo explícitamente al usuario.
- Cita siempre en formato (Autor, Año). Nunca atribuyas una cifra o hallazgo
  a un autor si no estás seguro de que proviene de esa fuente.
- Distingue explícitamente, en cada recomendación, entre "hallazgo directo de
  la literatura" e "inferencia razonada tuya" cuando no exista una fuente
  específica para esa variable en particular.

# FORMATO DE SALIDA
Responde en español, en el siguiente orden: (1) tabla de clasificación y
ranking descrita en el paso 5, (2) variables ausentes recomendadas (paso 6),
(3) configuración sugerida del modelo (paso 7), (4) limitaciones (paso 8),
(5) lista de referencias en formato APA de todo lo citado en tu respuesta.
```

---

## 9. Referencias

Abdou, A., Basdevant, O., David-Barrett, E., & Fazekas, M. (2022). *Assessing vulnerabilities to corruption in public procurement and their price impact* (IMF Working Paper No. 2022/094). International Monetary Fund.

CAF – Banco de Desarrollo de América Latina y el Caribe, y Red Interamericana de Compras Gubernamentales (RICG). (2021, junio). *Menos corrupción en las contrataciones y compras públicas gracias a los datos abiertos*. https://www.caf.com/es/conocimiento/visiones/2021/06/menos-corrupcion-en-las-contrataciones-y-compras-publicas-gracias-a-los-datos-abiertos/

Carletti, M., Masiero, C., Beghi, A., & Susto, G. A. (2020). *Interpretable anomaly detection with DIFFI: Depth-based isolation forest feature importance*. arXiv. https://arxiv.org/abs/2007.11117

Chandola, V., Banerjee, A., & Kumar, V. (2009). Anomaly detection: A survey. *ACM Computing Surveys, 41*(3), 1–58. https://doi.org/10.1145/1541880.1541882

David-Barrett, E., & Fazekas, M. (2016). *Corrupt contracting: Partisan favouritism in public procurement* (Working Paper). Government Transparency Institute / European Research Centre for Anti-Corruption and State-Building.

Dirección Nacional de Contrataciones Públicas (DNCP). (s.f.). *¿Qué son los red flags?* Portal de Contrataciones Públicas del Paraguay. https://www.contrataciones.gov.py/dncp/red-flags/que-son-los-red-flags/

Fazekas, M., Cingolani, L., & Tóth, B. (2016). *A comprehensive review of objective corruption proxies in public procurement: Risky actors, transactions, and vehicles of rent extraction* (Government Transparency Institute Working Paper). https://doi.org/10.2139/ssrn.2891017

Fazekas, M., & Kocsis, G. (2020). Uncovering high-level corruption: Cross-national objective corruption risk indicators using public procurement data. *British Journal of Political Science, 50*(1), 155–164.

Fazekas, M., Tóth, I. J., & King, L. P. (2016). An objective corruption risk index using public procurement data. *European Journal on Criminal Policy and Research, 22*(3), 369–397.

Herreros-Martínez, A., Magdalena-Benedicto, R., Vila-Francés, J., Serrano-López, A. J., Pérez-Díaz, S., & Martínez-Herráiz, J. J. (2025). Applied machine learning to anomaly detection in enterprise purchase processes: A hybrid approach using clustering and isolation forest. *Information, 16*(3), 177. https://doi.org/10.3390/info16030177

Ley N.° 7021/2022. *De Suministro y Contrataciones Públicas*. Congreso Nacional de la República del Paraguay, 9 de diciembre de 2022. https://www.bacn.gov.py/leyes-paraguayas/11220/ley-n-7021-de-suministro-y-contrataciones-publicas

Liu, F. T., Ting, K. M., & Zhou, Z.-H. (2008). Isolation forest. En *Proceedings of the 2008 Eighth IEEE International Conference on Data Mining* (pp. 413–422). IEEE. https://doi.org/10.1109/ICDM.2008.17

Liu, F. T., Ting, K. M., & Zhou, Z.-H. (2012). Isolation-based anomaly detection. *ACM Transactions on Knowledge Discovery from Data, 6*(1), 1–39. https://doi.org/10.1145/2133360.2133363

Lundberg, S. M., & Lee, S.-I. (2017). A unified approach to interpreting model predictions. *Advances in Neural Information Processing Systems, 30*, 4765–4774.

Molnar, C. (2022). *Interpretable machine learning: A guide for making black box models explainable* (2.ª ed.). https://christophm.github.io/interpretable-ml-book/

OECD. (2025). *OECD guidelines for fighting bid rigging in public procurement* (2025 update). OECD Publishing. https://doi.org/10.1787/cbe05a56-en

Open Contracting Partnership. (2016). *Red flags for integrity: Giving the green light to open data solutions*. https://www.open-contracting.org/resources/red-flags-integrity-giving-green-light-open-data-solutions/

Open Contracting Partnership. (2019, 27 de junio). *Examining procurement "red flags" in Latin America with data*. https://www.open-contracting.org/2019/06/27/examining-procurement-red-flags-in-latin-america-with-data/

Rousseeuw, P. J. (1987). Silhouettes: A graphical aid to the interpretation and validation of cluster analysis. *Journal of Computational and Applied Mathematics, 20*, 53–65. https://doi.org/10.1016/0377-0427(87)90125-7

Torres Berru, Y. (2023). *Minería de datos y texto para la detección de fraude en contratos públicos: Caso de estudio sistema oficial de contratación pública de Ecuador* [Tesis doctoral, Universidad de Salamanca]. Gredos USAL. https://gredos.usal.es/handle/10366/157534

Wittberg, E., & Fazekas, M. (2023). Firm performance, imperfect competition, and corruption risks in procurement: Evidence from Swedish municipalities. *Public Choice*. https://doi.org/10.1007/s11127-023-01102-8

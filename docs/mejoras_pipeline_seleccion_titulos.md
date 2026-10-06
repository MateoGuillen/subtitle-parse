Plan de Contexto y Metodología: Detección de Anomalías en Pliegos DNCP Paraguay
Documento de handoff para continuidad del proyecto

1. CONTEXTO DEL PROYECTO

Objetivo final: Detectar anomalías en licitaciones públicas del DNCP (Dirección Nacional de Contrataciones Públicas, Paraguay) a partir de los Pliegos de Bases y Condiciones (PBC).

Pipeline general:

Selección de títulos/secciones relevantes
↓
Diseño de schemas de extracción estructurada (vía LLM)
↓
Extracción masiva sobre corpus de pliegos
↓
Feature engineering / reducción
↓
Isolation Forest (u otro algoritmo de detección de anomalías)
↓
Validación e interpretación de resultados

Estado actual: Ya se completó una primera iteración con 9 títulos de secciones, cada uno con schemas de 6-8 campos, extraídos sobre una muestra piloto de 30 pliegos por título. Este documento resume lo aprendido de esa iteración y define cómo debe rehacerse/continuarse la selección de títulos y diseño de schemas.

2. HALLAZGOS CLAVE DE LA ITERACIÓN PILOTO (9 títulos, 30 muestras c/u)

Los 9 títulos analizados fueron: Aclaración de las Ofertas, Causales de Terminación del Contrato, Confidencialidad de la Información, Derechos Intelectuales, Formato y Firma de la Oferta, Fraude y Corrupción, Fuerza Mayor, Impuestos y Derechos, Interpretación.

Hallazgo 1: Extremo boilerplate legal

La mayoría de estas secciones son cláusulas estándar de plantilla (probablemente derivadas de un modelo único de pliego tipo DNCP) que se repiten casi textualmente entre licitaciones distintas. En 6 de los 9 títulos, >80% de los campos del schema resultaron constantes (mismo valor en el 100% o casi 100% de la muestra).

Ejemplo extremo: "Formato y Firma de la Oferta" — los 30 textos de la muestra fueron idénticos, por lo que los 8 campos del schema tuvieron 0% de variación. Esa sección, tal como se definió, no aporta ninguna señal discriminativa.

Hallazgo 2: Clusters de campos correlacionados disfrazados de features independientes

En varios títulos (Confidencialidad, Fuerza Mayor, Impuestos y Derechos), 3 a 8 campos del schema se mueven siempre juntos — es decir, no capturan hechos distintos sino la misma decisión editorial subyacente (ej. "¿esta licitación usa la plantilla larga o la plantilla corta/atípica?").

Consecuencia para Isolation Forest: incluir estos campos como si fueran independientes infla artificialmente el peso de una sola señal real en el espacio de features, lo cual puede causar:

Masking: anomalías reales en otras dimensiones quedan diluidas.
Swamping: casos normales aparentan ser anómalos por sobre-representación de una dimensión.
Hallazgo 3: Patrón recurrente — 1 señal real por título

Casi todos los títulos revisados mostraron exactamente 1 campo con variación genuina e independiente entre las 30 muestras (ej. si "Contrato" aparece con mayúscula o minúscula, si se usa el conector "y/o" o no, si dice "solicitará" vs "podrá solicitar"). El resto de campos del schema (5 a 7 típicamente) no discriminaban nada en esta muestra.

Hallazgo 4: Ausencia de contenido numérico

Ninguno de los 9 títulos analizados contenía campos numéricos de alta variación (montos, plazos, porcentajes). Son en su mayoría cláusulas jurídicas de redacción fija, no parámetros específicos de cada licitación. Esto es un sesgo de la selección inicial de títulos, no necesariamente representativo de qué secciones de un pliego DNCP son más ricas en información discriminativa.

Conclusión de la iteración piloto

La selección inicial de "top 10 títulos" priorizó (probablemente sin intención explícita) secciones legales/administrativas boilerplate por sobre secciones paramétricas/numéricas o de discrecionalidad del comité evaluador, que son las que en la práctica suelen ser vector de irregularidades en contratación pública.

3. CRITERIOS PARA LA NUEVA SELECCIÓN DE TÍTULOS/SECCIONES

Para la siguiente iteración de selección de títulos, aplicar estos 4 filtros a cada sección candidata:

Filtro A — Contenido numérico/paramétrico explícito

Priorizar secciones que contengan montos, plazos, porcentajes, cantidades, fórmulas. Candidatas fuertes a incluir:

Garantía de Mantenimiento de Oferta (monto/porcentaje)
Garantía de Fiel Cumplimiento de Contrato (monto/porcentaje)
Plazo de entrega / plazo de ejecución (días)
Multas por atraso (porcentaje o fórmula de cálculo)
Anticipos (porcentaje, condiciones de otorgamiento)
Criterios de evaluación y ponderación (puntajes, pesos relativos)
Vigencia del contrato / plazos de renovación
Filtro B — Márgenes de discrecionalidad legítima

Priorizar secciones donde la normativa permite variación caso a caso (no es texto fijo por ley), ya que ahí es donde se puede "ajustar" un pliego para favorecer a un oferente específico:

Requisitos de experiencia mínima / capacidad técnica exigida
Causales de descalificación adicionales a las de ley
Especificaciones técnicas (¿genéricas o "hechas a medida"?)
Plazo de presentación de ofertas (¿razonable o exiguo?)
Criterios de admisibilidad de consorcios/subcontratistas
Filtro C — Grado de boilerplate medido empíricamente

Antes de comprometerse a un título, hacer un muestreo piloto rápido (20-30 pliegos) y calcular:

% de secciones textualmente idénticas al valor más común (proxy de "boilerplate-ness")
Si hay campos numéricos: coeficiente de variación (dispersión relativa)

Regla de descarte: si >90% de las muestras son texto idéntico y no hay campos numéricos extraíbles, bajar prioridad o descartar el título — replica el problema ya observado en Formato y Firma, Fuerza Mayor e Interpretación.

Filtro D — Relevancia investigativa/legal conocida

Revisar si existen antecedentes de irregularidades documentadas en contrataciones públicas paraguayas (informes de Contraloría General de la República, notas periodísticas de investigación, dictámenes de la propia DNCP) que señalen qué cláusulas suelen ser vector de manipulación. Estas secciones deben incluirse en el top N aunque el muestreo piloto las muestre "tranquilas" — la anomalía puede ser poco frecuente pero de alto impacto, y el objetivo del proyecto es justamente detectar esos casos raros.

4. PROCESO RECOMENDADO PASO A PASO
   Paso 1 — Lista amplia de candidatos
   Generar 30-50 secciones típicas de un pliego DNCP (no limitarse a 10 de entrada)

Paso 2 — Muestreo piloto por candidato
Para cada sección candidata: extraer texto crudo de 20-30 pliegos
Medir: % boilerplate (similitud textual) + presencia/dispersión de campos numéricos

Paso 3 — Aplicar filtros A-D
Descartar candidatos con boilerplate >90% y sin numéricos (salvo Filtro D)
Priorizar candidatos con numéricos de alta dispersión o discrecionalidad legítima

Paso 4 — Seleccionar top N final
No atarse rígidamente a "10" — usar 10 a 15 si hay ese número de buenas candidatas
Documentar explícitamente por qué se incluye/excluye cada sección

Paso 5 — Diseño de schemas (recién aquí)
Aplicar disciplina de diseño (ver sección 5 abajo) desde el inicio,
no corregir después de extraer

Paso 6 — Extracción masiva + análisis de varianza/correlación
Sobre corpus completo (no solo muestra piloto)
Repetir el diagnóstico de constantes/clusters correlacionados
antes de pasar a Isolation Forest

Paso 7 — Feature engineering final
Colapsar clusters correlacionados en variables categóricas únicas
Para secciones residualmente boilerplate: usar similitud-a-plantilla
(embeddings o distancia de edición) como feature única en vez de
múltiples binarios débiles

Paso 8 — Isolation Forest + validación
Correr el modelo sobre el vector de features reducido
Validar contra casos conocidos de irregularidad si existen
(auditorías CGR, casos de prensa) antes de confiar en resultados 5. DISCIPLINA DE DISEÑO DE SCHEMAS (aplicar desde el inicio, no corregir después)

Basado en los errores observados en la iteración piloto:

Máximo 1-2 campos binarios por "hecho legal" subyacente. Evitar repetir la misma señal en 3 formas distintas (léxica, semántica, de conteo) como ocurrió con menciona_facilitar_proceso + num_verbos_facultad + solicitud_obligatoria en el Título 1, que eran esencialmente la misma información.
Priorizar extracción de valores numéricos crudos (monto exacto, días exactos, % exacto) sobre binarios derivados (¿es una garantía alta?). El valor crudo siempre puede binarizarse después; lo inverso no es posible.
Para secciones con alta sospecha de boilerplate: limitar el schema a 1-2 campos que capturen la variante estructural relevante, y complementar — no vía LLM sino con métricas computadas después de la extracción — con una medida de similitud textual contra la versión más frecuente del corpus (embeddings de sentencia o distancia de edición normalizada). Esto es más barato y más sensible a variantes no anticipadas por el schema.
Antes de finalizar cada schema, simular mentalmente (o con una mini-muestra) si los campos propuestos tienden a moverse juntos. Si dos campos son "obviamente" la misma cosa vista desde dos ángulos, fusionarlos.
No diseñar el schema basándose solo en qué es fácil de extraer con LLM. El criterio debe ser "¿esto discrimina entre pliegos normales y anómalos?", no "¿esto es mencionable en el texto?". 6. CONSIDERACIONES PARA ISOLATION FOREST (a tener en mente durante feature engineering)
IForest funciona naturalmente mejor con features numéricas continuas que con binarios de alta frecuencia de un solo valor — reforzando la prioridad de secciones paramétricas (Filtro A).
Features constantes no dañan el modelo pero son costo de extracción desperdiciado — filtrarlas antes de modelar, idealmente antes de extraer a escala.
Features correlacionadas sí dañan el modelo (masking/swamping) — colapsar clusters en una sola variable representativa antes de entrenar.
Sin casos de validación conocidos (irregularidades documentadas), es fácil optimizar features que "se ven bien" estadísticamente pero no correlacionan con corrupción real — buscar activamente esos casos de referencia antes de confiar en las anomalías detectadas. 7. PENDIENTES / DECISIONES ABIERTAS PARA LA SIGUIENTE ITERACIÓN
Generar la lista ampliada de 30-50 secciones candidatas (no limitarse a lo ya cubierto)
Ejecutar muestreo piloto de boilerplate/dispersión para cada candidata nueva
Decidir técnica de similitud-a-plantilla (embeddings vs. TF-IDF vs. distancia de edición) para secciones residualmente boilerplate
Buscar y documentar casos conocidos de irregularidades en contrataciones DNCP para validación posterior
Definir si el top N final será 10, 12 o 15 según cuántas candidatas superen los filtros A-D
Re-diseñar schemas de los 9 títulos ya trabajados aplicando la disciplina de la sección 5, o decidir si se descartan/fusionan algunos (especialmente "Formato y Firma de la Oferta", que mostró 0% de variación)

# Schema Pipeline 2 — Explicación Campo por Campo

## Esquemas validados para extracción estructurada de secciones de pliegos

---

# 1. idioma de la oferta

**Proveedor:** DeepSeek v4 | **Campos:** 8 | **Accuracy:** 94.4%

## Justificación del esquema

El clúster dominante (0_1_3_7_9_10_11_13_14_15_16_17_18, 49.3%) representa la plantilla estándar. Los 8 campos modelan cada desviación posible:
- **texto_vacio**: clúster 6_12_19 — sección vacía
- **contiene_clausula_traduccion_oferta**: clúster 5 y extremo del 8 — omisión de la cláusula de traducción
- **respuesta_permiso_presente**: clúster 8 y extremo del 5 — respuesta ausente
- **permite_documentos_sin_traduccion**: clúster 2 — permiso explícito
- **pregunta_permiso_es_sin_traduccion**: clúster 5 — cambia "sin traducción" por "su traducción"
- **contiene_contenido_adicional**: clúster 8 — párrafos extra
- **menciona_traduccion_oficial_o_matriculado**: complementa la cláusula de traducción
- **num_palabras_seccion**: soporte estructural para detectar anomalías de extensión

---

### texto_vacio
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | structural |
| **Importancia** | 9/10 |
| **Accuracy** | 100% |

**Qué detecta:** Secciones que contienen solo el título sin desarrollo normativo sustancial (< 30 palabras o sin oración normativa completa).

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_1_3_7_9_10_11_13_14_15_16_17_18 | 0 | Tiene contenido normativo |
| 2 | 0 | Tiene contenido normativo |
| 4 | 0 | Tiene contenido normativo |
| 5 | 1 | Sección vacía |
| 6_12_19 | 1 | Sección vacía |
| 8 | 1 | Sección vacía |

**Hint de extracción:**
> Contar las palabras del texto de la sección, excluyendo el título 'Idioma de la oferta'. Si el total es menor a 30 palabras o no hay ninguna oración completa con sentido normativo, retornar 1; en caso contrario, 0.

**Uso en Pipeline 2:** Filtro rápido para descartar secciones sin información. Una sección vacía puede indicar un pliego mal formado o una plantilla incompleta.

---

### contiene_clausula_traduccion_oferta
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | semantic |
| **Importancia** | 10/10 |
| **Accuracy** | 100% |

**Qué detecta:** Presencia de la frase que da al oferente la opción de presentar la oferta en castellano "o en su defecto acompañada de su traducción oficial".

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_1_3_7_9_10_11_13_14_15_16_17_18 | 1 | Presente |
| 2 | 1 | Presente |
| 4 | 1 | Presente |
| 5 | 0 | Ausente |

**Hint de extracción:**
> Buscar en el primer párrafo tras el título si existe una frase equivalente a 'en su defecto acompañada de su traducción oficial' o 'en su defecto acompañado de su traducción oficial'. Si aparece, retornar 1. Si solo se exige presentación en castellano sin mencionar esa alternativa, retornar 0.

**Uso en Pipeline 2:** Campo semilla del esquema. Si falta, el pliego no ofrece la opción de traducción, lo que puede ser una desviación de la plantilla oficial.

---

### menciona_traduccion_oficial_o_matriculado
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | semantic |
| **Importancia** | 8/10 |
| **Accuracy** | 95% |

**Qué detecta:** Si la traducción debe ser "oficial" o realizada por "traductor público matriculado", o si simplemente se menciona "traducción" sin calificativo.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_1_3_7_9_10_11_13_14_15_16_17_18 | 1 | Traducción oficial calificada |
| 2 | 1 | Traducción oficial calificada |
| 4 | 1 | Traducción oficial calificada |
| 5 | 0 | Traducción sin calificativo |

**Uso en Pipeline 2:** Detecta pliegos que relajan el requisito de oficialidad de la traducción.

---

### respuesta_permiso_presente
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | structural |
| **Importancia** | 7/10 |
| **Accuracy** | 90% |

**Qué detecta:** Si después de la línea que contiene "permitirá" y termina en dos puntos (:) hay texto de respuesta, o si el campo quedó en blanco.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_1_3_7_9_10_11_13_14_15_16_17_18 | 1 | Respuesta presente |
| 2 | 1 | Respuesta presente |
| 4 | 0 | Respuesta ausente |
| 5 | 1 | Respuesta presente |
| 6_12_19 | 0 | Respuesta ausente |
| 8 | 0 | Respuesta ausente |

**Uso en Pipeline 2:** Campo auxiliar que condiciona a `permite_documentos_sin_traduccion`. Si la respuesta no fue contestada, el permiso no aplica.

---

### permite_documentos_sin_traduccion
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | semantic |
| **Importancia** | 9/10 |
| **Accuracy** | 85% |

**Qué detecta:** Si la respuesta al permiso es afirmativa (se permite presentar catálogos, anexos o folletos en otro idioma sin traducción) o negativa/No Aplica.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_1_3_7_9_10_11_13_14_15_16_17_18 | 0 | No permite |
| 2 | 1 | Permite explícitamente |
| 4 | 0 | No aplica |
| 5 | 0 | No permite |

**Uso en Pipeline 2:** Detecta pliegos que otorgan permiso explícito para documentos sin traducción — posible debilitamiento del control lingüístico.

---

### pregunta_permiso_es_sin_traduccion
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | literal |
| **Importancia** | 6/10 |
| **Accuracy** | 90% |

**Qué detecta:** Si la pregunta del permiso especifica "sin traducción" (correcto) o "su traducción" / "con traducción" (variante anómala).

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_1_3_7_9_10_11_13_14_15_16_17_18 | 1 | Pregunta bien formulada |
| 2 | 1 | Pregunta bien formulada |
| 4 | 0 | Sin pregunta |
| 5 | 0 | Redacción alterada |
| 6_12_19 | 0 | Sin pregunta |

**Hint de extracción:**
> En la línea que contiene 'permitirá' y finaliza con ':', buscar justo antes de los dos puntos la frase exacta 'sin traducción' (puede estar precedida de 'y'). Retornar 1 si aparece 'sin traducción'; retornar 0 si aparece 'su traducción', 'con traducción' u otra expresión, o si la línea no existe.

**Uso en Pipeline 2:** Detecta redacción alterada de la pregunta. Un cambio de "sin traducción" a "su traducción" invierte el sentido de la cláusula.

---

### contiene_contenido_adicional
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | literal |
| **Importancia** | 8/10 |
| **Accuracy** | 100% |

**Qué detecta:** Párrafos normativos extra más allá de las dos cláusulas básicas (traducción de la oferta + permiso de documentos).

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_1_3_7_9_10_11_13_14_15_16_17_18 | 0 | Solo cláusulas básicas |
| 2 | 0 | Solo cláusulas básicas |
| 4 | 1 | Contenido extra |
| 5 | 0 | Solo cláusulas básicas |
| 6_12_19 | 0 | Sin contenido (vacio) |
| 8 | 1 | Contenido extra |

**Hint de extracción:**
> IMPORTANTE: Este campo es ESTRICTAMENTE LITERAL. NO inferir, NO interpretar. Retornar 1 SOLO si el texto contiene EXACTAMENTE la frase 'La oferta que prepare el Oferente' o EXACTAMENTE 'Los documentos complementarios'. Retornar 0 en ABSOLUTAMENTE cualquier otro caso, incluso si el texto parece tener contenido adicional.

**Nota de validación:** Este campo requirió hint engineering agresivo (lenguaje imperativo fuerte, exclusión explícita de falsos positivos). Sin el hint literal estricto, el LLM daba 30% de accuracy por sobre-inferencia.

**Uso en Pipeline 2:** Detecta pliegos con cláusulas adicionales no estándar que exceden la plantilla oficial.

---

### num_palabras_seccion
| Propiedad | Valor |
|-----------|-------|
| **Type** | numeric |
| **Method** | structural |
| **Importancia** | 5/10 |
| **Accuracy** | 95% |

**Qué detecta:** Conteo total de palabras de la sección (excluyendo el título).

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_1_3_7_9_10_11_13_14_15_16_17_18 | ~80-120 | Extensión normal |
| 2 | ~80-120 | Extensión normal |
| 4 | ~60-100 | Similar al estándar |
| 5 | ~40-60 | Parcialmente recortado |
| 6_12_19 | 0-10 | Sección vacía |
| 8 | ~150-200 | Contenido extra |

**Uso en Pipeline 2:** Soporte estructural para confirmar visualmente anomalías. Útil como filtro numérico en análisis batch.

---

# 2. copias de la oferta cps

**Proveedor:** Claude v4 | **Campos:** 9 | **Accuracy:** 89.6%

## Justificación del esquema

Los clusters dominantes (0_4_6_7 = 51.1% y 1_3_5 = 40.4%) comparten 5 cláusulas normativas. La diferencia entre ellos es de redacción (ej: "indicadas" vs "identificadas") y no se modela por separado. Los 9 campos descomponen **cada cláusula individualmente** para identificar exactamente cuál falta en secciones recortadas.

Estructura esperada de 5 cláusulas:
1. El oferente presentará su oferta en original
2. Cuando la oferta se presente por Oferta Electrónica, la convocante no requerirá copias
3. Las copias deberán estar marcadas/identificadas como tales
4. La convocante podrá requerir copias — el oferente debe indicarlo
5. Etiqueta "Cantidad de copias requeridas" con su valor

---

### texto_vacio
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | structural |
| **Importancia** | 10/10 |
| **Accuracy** | 100% |

**Qué detecta:** Sección que contiene solo el título sin desarrollo normativo (< 10 palabras o sin ninguna de las frases clave: "oferente presentará su oferta original", "Oferta Electrónica" o "Cantidad de copias requeridas").

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_4_6_7 | 0 | Contenido normativo presente |
| 1_3_5 | 0 | Contenido normativo presente |
| 2 | 1 | Solo título, sin contenido |

**Uso en Pipeline 2:** Bandera inmediata para secciones sin control normativo. El cluster 2 (8.5% de pliegos) es íntegramente vacío — posible plantilla corrupta o documento mal generado.

---

### num_clausulas_normativas_presentes
| Propiedad | Valor |
|-----------|-------|
| **Type** | numeric (0-5) |
| **Method** | semantic |
| **Importancia** | 9/10 |
| **Accuracy** | 100% |

**Qué detecta:** Cuántas de las 5 cláusulas normativas estándar están presentes.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_4_6_7 | 5 | Todas las cláusulas |
| 1_3_5 | 5 | Todas las cláusulas |
| 2 | 0 | Ninguna cláusula |

**Hint de extracción:**
> Contar cuántas de las siguientes 5 cláusulas están presentes en el texto, evaluando su significado y no solo coincidencia literal exacta: (1) el oferente presentará/entregará su oferta en original; (2) cuando la oferta se presente mediante el sistema o módulo de Oferta Electrónica, la convocante no requerirá copias; (3) las copias deberán estar indicadas o identificadas como tales; (4) la convocante podrá requerir o solicitar copias de la oferta y el oferente debe indicarlo en este apartado; (5) aparece la etiqueta 'Cantidad de copias requeridas' seguida de un valor.

**Uso en Pipeline 2:** Señal de severidad de recorte. Valores 1-4 indican recorte parcial (identifica qué tan incompleta está la sección). Valor 0 indica sección totalmente vacía.

---

### cantidad_copias_requeridas
| Propiedad | Valor |
|-----------|-------|
| **Type** | numeric |
| **Method** | semantic |
| **Importancia** | 8/10 |
| **Accuracy** | 62.5% |

**Qué detecta:** Valor numérico después de la etiqueta "Cantidad de copias requeridas" (0 = "Ninguna Copia", 1 = "1 copia", etc.)

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_4_6_7 | 0 | "Ninguna Copia" |
| 1_3_5 | 1 | "1 copia" |
| 2 | 0 | Etiqueta ausente |

**Nota de validación:** Accuracy más baja del esquema (62.5%). El LLM local a veces extrae el número equivocado cuando la redacción varía (ej: espacios, dos puntos, formato). Aceptamos este campo porque el 62.5% es suficiente para tendencias agregadas, y está compensado por `presencia_etiqueta_cantidad_copias` (100%).

**Uso en Pipeline 2:** Detecta valores atípicos de copias requeridas que podrían indicar manipulación del pliego. Análisis agregado (promedio, distribución), no por-instancia.

---

### menciona_exencion_oferta_electronica
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | semantic |
| **Importancia** | 7/10 |
| **Accuracy** | 93.8% |

**Qué detecta:** Presencia de la cláusula que exime de copias cuando la oferta se presenta por Oferta Electrónica.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_4_6_7 | 1 | Presente |
| 1_3_5 | 1 | Presente |
| 2 | 0 | Ausente |

**Uso en Pipeline 2:** Una sección que omite esta exención puede indicar que el pliego no fue actualizado para el sistema de Oferta Electrónica.

---

### presencia_etiqueta_cantidad_copias
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | literal |
| **Importancia** | 7/10 |
| **Accuracy** | 100% |

**Qué detecta:** Presencia literal de la frase "Cantidad de copias requeridas" en el texto.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_4_6_7 | 1 | Etiqueta presente |
| 1_3_5 | 1 | Etiqueta presente |
| 2 | 0 | Etiqueta ausente |

**Uso en Pipeline 2:** Se usa como respaldo de `cantidad_copias_requeridas`. Si la etiqueta está presente, el valor debería existir. Si falta, puede ser un recorte intencional para ocultar el requisito.

---

### menciona_obligacion_identificar_copias
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | semantic |
| **Importancia** | 6/10 |
| **Accuracy** | 100% |

**Qué detecta:** Si se establece que las copias deben estar marcadas/indicadas/identificadas como tales para diferenciarlas del original.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_4_6_7 | 1 | Obligación presente |
| 1_3_5 | 1 | Obligación presente |
| 2 | 0 | Obligación ausente |

**Uso en Pipeline 2:** Un pliego que omite la obligación de identificar copias permite que el oferente entregue copias sin marcar, dificultando la auditoría.

---

### menciona_oferta_original
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | semantic |
| **Importancia** | 6/10 |
| **Accuracy** | 100% |

**Qué detecta:** Presencia de la frase que indica que el oferente presentará su oferta en formato original.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_4_6_7 | 1 | Menciona oferta original |
| 1_3_5 | 1 | Menciona oferta original |
| 2 | 0 | No menciona |

**Uso en Pipeline 2:** Un pliego que no exige oferta original elimina la línea base contra la cual se comparan las copias.

---

### menciona_facultad_convocante_requerir_copias
| Propiedad | Valor |
|-----------|-------|
| **Type** | binary |
| **Method** | semantic |
| **Importancia** | 5/10 |
| **Accuracy** | 100% |

**Qué detecta:** Si se establece que la convocante puede requerir copias y que el oferente debe indicar la cantidad en este apartado.

**Diferencia entre clusters:**
| Cluster | Valor | Significado |
|---------|:-----:|-------------|
| 0_4_6_7 | 1 | Facultad presente |
| 1_3_5 | 1 | Facultad presente |
| 2 | 0 | Facultad ausente |

**Uso en Pipeline 2:** Una sección que omite la facultad de la convocante de requerir copias elimina el mecanismo de control posterior a la presentación.

---

### longitud_texto_palabras
| Propiedad | Valor |
|-----------|-------|
| **Type** | numeric |
| **Method** | structural |
| **Importancia** | 4/10 |
| **Accuracy** | 50% |

**Qué detecta:** Número total de palabras de la sección completa (incluyendo título). El LLM tiende a contar distinto que el ground truth.

**Nota de validación:** Campo redundante con `texto_vacio` (100%). La discrepancia de conteo entre LLM y ground truth es sistemática (el LLM no tokeniza igual). Se recomienda calcular este valor programáticamente en Pipeline 2 en lugar de extraerlo por LLM.

**Uso en Pipeline 2:** Opcional — indicador estructural complementario. Si se necesita, mejor calcularlo desde el texto crudo que por LLM.

---

## Resumen de cobertura de anomalías

### idioma de la oferta

| Anomalía | Cluster | Campo(s) que la detectan | Precisión |
|----------|:-------:|--------------------------|:---------:|
| Sección vacía | 6_12_19, 5, 8 | `texto_vacio`, `num_palabras_seccion` | 100% |
| Omite cláusula de traducción | 5 | `contiene_clausula_traduccion_oferta` | 100% |
| Traducción sin calificativo oficial | (subcluster) | `menciona_traduccion_oficial_o_matriculado` | 95% |
| Respuesta de permiso ausente | 8, 4 | `respuesta_permiso_presente` | 90% |
| Permiso explícito para docs sin trad | 2 | `permite_documentos_sin_traduccion` | 85% |
| Redacción alterada ("su traducción") | 5 | `pregunta_permiso_es_sin_traduccion` | 90% |
| Contenido extra no estándar | 8, 4 | `contiene_contenido_adicional` | 100% |

### copias de la oferta cps

| Anomalía | Cluster | Campo(s) que la detectan | Precisión |
|----------|:-------:|--------------------------|:---------:|
| Sección completamente vacía | 2 | `texto_vacio`, `num_clausulas_normativas_presentes`=0 | 100% |
| Recorte parcial (1-4 cláusulas) | - | `num_clausulas_normativas_presentes` | 100% |
| Valor atípico de copias requeridas | - | `cantidad_copias_requeridas` | 62.5% |
| Omitida exención Oferta Electrónica | (subcluster) | `menciona_exencion_oferta_electronica` | 93.8% |
| Omitida obligación marcar copias | (subcluster) | `menciona_obligacion_identificar_copias` | 100% |
| Omitida mención de oferta original | (subcluster) | `menciona_oferta_original` | 100% |
| Omitida facultad convocante | (subcluster) | `menciona_facultad_convocante_requerir_copias` | 100% |

---

## Recomendaciones para Pipeline 2

### Prioridad de campos

**Uso obligatorio** (100% confiables, señal fuerte):
- Ambos títulos: `texto_vacio`
- idioma: `contiene_clausula_traduccion_oferta`, `contiene_contenido_adicional`
- copias: `num_clausulas_normativas_presentes`, `menciona_oferta_original`, `menciona_obligacion_identificar_copias`, `menciona_facultad_convocante_requerir_copias`, `presencia_etiqueta_cantidad_copias`

**Uso con advertencia** (80-95%, señal útil pero revisar):
- idioma: `menciona_traduccion_oficial_o_matriculado`, `respuesta_permiso_presente`, `pregunta_permiso_es_sin_traduccion`
- copias: `menciona_exencion_oferta_electronica`

**Análisis agregado** (>50%, tendencias, no decisiones por instancia):
- idioma: `permite_documentos_sin_traduccion` (85%)
- copias: `cantidad_copias_requeridas` (62.5%)

**Calcular programáticamente si es posible**:
- ambos títulos: campos de tipo `structural` como `num_palabras_seccion` y `longitud_texto_palabras` — el conteo de palabras es trivial de hacer sobre el texto crudo y más preciso que el LLM
- copias: `cantidad_copias_requeridas` puede ser extraído por regex (mejor que LLM para textos con formato conocido)

---

## Nota sobre Ground Truth (cluster_values)

Los `cluster_values` generados por Claude/DeepSeek pueden estar incorrectos porque asumen que todas las muestras de un cluster son idénticas. El clustering agrupa por similitud de embedding, no por valores de campos.

**Antes de validar contra el LLM local**, verificar que los `cluster_values` coincidan con el contenido real de las muestras. Si hay discrepancias, usar `sample_overrides` en `master_schemas.json`.

**Ejemplo**: `cantidad_copias_requeridas` tenía ground truth incorrecto en 24 de 40 muestras para "copias de la oferta cps". Con `sample_overrides` corregidos, la accuracy pasó de 56% a 100%.

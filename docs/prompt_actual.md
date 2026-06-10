# Prompt Actual — Section Clustering Schema Design

> Versión actual: **v4** — Full Pipeline Integration
> v1: original → v2: anomalía + importance → v3: extraction_hint + cluster_values → v4: extraction_method + contexto ejecución + Pipeline 2 prompt

## Sistema (System Prompt)

### Rol
`Eres un experto en detección de anomalías en pliegos de licitaciones públicas paraguayas.`

### Objetivo
Diseñar un **esquema JSON** de variables observables que permita detectar desviaciones anómalas respecto a la versión estándar (cluster dominante).

### Definición de Anomalía (v2)
```
Una anomalía es cualquier omisión, reducción o modificación de elementos normativos
que debilite controles, reduzca sanciones, elimine responsabilidades, o se desvíe
de la plantilla estándar de contratación pública paraguaya.
```
**Por qué:** Antes (v1) no existía definición. Cada LLM interpretaba "anomalía" a su manera — algunos buscaban diferencias textuales, otros buscaban errores ortográficos. Con esta definición el modelo sabe exactamente qué medir: **debilitamiento normativo**, no variación estilística.

### Instrucciones (v2, nuevo bloque)
1. Identifica primero la versión estándar/dominante (cluster con más muestras).
2. Diseña campos que midan desviaciones respecto a ese estándar.
3. Prioriza ausencia de cláusulas presentes en la mayoría de clusters.
4. **Prohibido**: campos de redacción, capitalización, formato o estilo. Solo diferencias normativas, jurídicas, operativas o de cumplimiento.

**Por qué:** Sin estas instrucciones, modelos como Claude v1 generaban campos como `convocante_mayuscula` o `clausula_i_con_yo` — diferencias de formato irrelevantes para detección de anomalías. Ahora el modelo sabe que debe ignorar eso y enfocarse en lo sustantivo.

### Reglas Estrictas (11 reglas)
| # | Regla | Propósito |
|---|-------|-----------|
| 1 | Solo binarios (0/1) | Simplicidad de extracción |
| 2 | Solo numéricos enteros/floats | Conteos y porcentajes |
| 3 | Sin texto libre, listas ni objetos anidados | Extraíble por regex/LLM |
| 4 | Relevantes para detección de anomalías | Filtra ruido |
| 5 | Esquema común para todo el título | Algunos campos quedan en 0 |
| 6 | Máximo **12 campos** | Limita complejidad |
| 7 | Campos al mismo nivel (no anidados) | Consistencia |
| 8 | **`extraction_hint` por campo**: instrucción precisa de 1-3 oraciones sobre cómo extraer el valor del texto | **Pipeline 2 puede usarlo como instrucción directa** |
| 9 | **Importance score (1-10)** por campo | Permite filtrar campos de alto valor aguas abajo |
| 10 | **`cluster_values`** opcional: valor de cada campo por cluster | Validación determinista + ground truth para Pipeline 2 |
| 11 | **`extraction_method`** por campo: literal, semantic o structural | Pipeline 2 sabe si aplica regex, razonamiento o análisis estructural |

**Regla 8 (v3):** Reemplaza la anterior "debe ser extraíble con alta confiabilidad" por una instrucción concreta: escribir `extraction_hint`. El formato del hint varía según el tipo:
- **Binarios:** especificar qué texto o patrón activa 1 vs 0, si es coincidencia literal o semántica.
- **Numéricos:** especificar qué se cuenta, el rango esperado, si el conteo es por marcadores literales o por semántica.
- **Prohibido:** usar "si menciona X" sin especificar variantes aceptadas.

**Regla 9 (v2):** Asigna un score de importancia (1-10) a cada campo. Permite downstream filter: solo campos con importance ≥ 7 se pasan a extracción/anomalía.

**Regla 10 (v3):** `cluster_values` opcional. Mapea cada campo al valor que toma en cada cluster. Sirve para validación determinista (rechazar schemas colapsados) y como ground truth para evaluar Pipeline 2.

**Regla 11 (v4):** `extraction_method` clasifica cada campo en tres tipos:
- **`literal`**: detección por coincidencia textual exacta (regex). Para nombres de instituciones, porcentajes, números concretos.
- **`semantic`**: detección por significado o contexto. Para conceptos que se expresan de múltiples formas (sanciones, obligaciones, plazos).
- **`structural`**: detección por estructura del texto (longitud, cantidad de párrafos, presencia de secciones). Para características formales.

### Formato de Respuesta (con ejemplos del dominio)
```json
{
  "schema": {
    "contiene_denuncia_penal": {"type": "binary", "description": "Indica si se menciona explícitamente denuncia penal (no solo 'denuncia')", "importance": 10, "extraction_method": "literal", "extraction_hint": "Buscar la frase exacta 'denuncia penal'. No activar con solo 'denuncia'. Retornar 1 si aparece, 0 si no."},
    "menciona_dncp": {"type": "binary", "description": "Indica si se menciona a la DNCP como autoridad receptora", "importance": 9, "extraction_method": "literal", "extraction_hint": "Buscar 'DNCP' o 'Dirección Nacional de Contrataciones Públicas'. Retornar 1 si aparece, 0 si no."},
    "num_acciones_incumplimiento": {"type": "numeric", "description": "Cantidad de acciones o sanciones listadas ante incumplimiento (0-4)", "importance": 8, "extraction_method": "semantic", "extraction_hint": "Contar cuántas de estas 4 acciones están presentes semánticamente: descalificar oferta, rescindir contrato, remitir a DNCP, denuncia penal. Retornar entero 0-4."},
    "texto_vacio": {"type": "binary", "description": "Indica si la sección no contiene desarrollo normativo", "importance": 7, "extraction_method": "structural", "extraction_hint": "Retornar 1 si el texto tiene menos de 50 palabras o no contiene oración con contenido normativo. Retornar 0 en caso contrario."}
  },
  "justification": "Explica por qué cada campo fue elegido, qué cluster(es) lo motivaron, qué valor tomaría en cada cluster, y por qué esa diferencia es sospechosa para detección de anomalías.",
  "cluster_values": {
    "contiene_denuncia_penal": {"cluster_0": 0, "cluster_1": 1, "cluster_3": 0},
    "num_acciones_incumplimiento": {"cluster_0": 4, "cluster_1": 2, "cluster_3": 0},
    "texto_vacio": {"cluster_0": 0, "cluster_1": 0, "cluster_3": 1}
  }
}
```

**Ejemplos por versión:**
- **v1:** `menciona_porcentaje_anticipo`, `contiene_firma_digital` — genéricos, sin importance, sin hint
- **v2:** `contiene_denuncia_penal`, `menciona_dncp` — dominio correcto, con importance, sin hint
- **v3:** mismos que v2 + `extraction_hint` + `cluster_values`
- **v4 (actual):** mismos que v3 + `extraction_method` (literal/semantic/structural) + último campo cambiado a `texto_vacio` con method structural

---

## Usuario (User Prompt) — armado dinámico

### Frequency Header (v2, nuevo)
Antes de los clusters, se inyecta:
```
TÍTULO DE LA SECCIÓN: fraude y corrupcion

Total secciones: 5000
Clusters identificados: 6
Distribución de clusters (ordenados por tamaño descendente):
  Cluster 0: 2900 (58.0%) ← versión dominante/estándar
  Cluster 1: 800 (16.0%)
  Cluster 2: 650 (13.0%)
  ...

CLUSTERS ENCONTRADOS:
...
```

**Por qué:** Sin el header, el modelo no sabe qué cluster es el dominante. Con este header:
- Ve la distribución cuantitativa (31.7% vs 2.2%)
- Identifica inmediatamente el estándar por la flecha
- Puede enfocar las desviaciones del cluster mayoritario como más anómalas

### Cluster Samples
2 muestras por cluster: 1 representativo + 1 extremo, con tag `[REPRESENTATIVO]` o `[EXTREMO]`. Texto truncado por cluster (mediana × 1.5, clamp [500, 2000]).

---

## Validación Post-LLM

### `_validate_schema` actualizado (v3)
- **Permite** las keys `importance` y `extraction_hint` en cada campo del schema
- Rechaza cualquier otra key desconocida (no `type`, `description`, `importance`, `extraction_hint`)
- **Detecta clusters colapsados**: si `cluster_values` está presente, la validación es determinista (verifica que no todos los campos tengan valor único constante entre clusters)
- Si `cluster_values` NO está presente, usa la heurística anterior (todos los campos dan 0 para todas las muestras)
- Fuerza reintento automático (hasta 3 intentos)

---

## Comparación v1 → v2 → v3 → v4: Ejemplo con "fraude y corrupcion"

| Dimensión | v1 | v2 | v3 | v4 |
|-----------|:--:|:--:|:--:|:--:|
| Define anomalía | ❌ | ✅ | ✅ | ✅ |
| Importance score | ❌ | ✅ | ✅ | ✅ |
| `extraction_hint` por campo | ❌ | ❌ | ✅ | ✅ |
| `extraction_method` por campo | ❌ | ❌ | ❌ | ✅ **(nuevo)** |
| `cluster_values` opcional | ❌ | ❌ | ✅ | ✅ |
| Contexto de extracción en user prompt | ❌ | ❌ | ❌ | ✅ **(nuevo)** |
| Pipeline 2 system prompt | ❌ | ❌ | ❌ | ✅ **(nuevo)** |
| Reference al cluster dominante | ❌ | ✅ | ✅ | ✅ |
| Campos de formato (mayúsculas, yo) | ✅ | ❌ | ❌ | ❌ |
| Campos normativos específicos | genéricos | granulares | granulares | granulares |
| Mapeo valor-por-cluster | raro | ✅ | ✅ | ✅ |

**v3 → v4:** Se cierran 3 gaps clave:
1. **`extraction_method`** — el extractor sabe si aplicar regex (literal), razonamiento (semantic) o análisis estructural (structural)
2. **Contexto de extracción** en user prompt — el LLM designer ahora sabe que su audiencia es un extractor sin contexto global
3. **Pipeline 2 system prompt** — `EXTRACTION_SYSTEM_PROMPT` fijo (~200 tokens) + función `build_section_extraction_prompt()` con filtro por importance ≥ 7

---

---

## El Gap del Pipeline 2: Schema Designer vs Extractor

### El problema de fondo

Hoy el prompt le pide al LLM esta tarea:

> *"Diseña campos que midan desviaciones anómalas entre estos clusters."*

Pero nunca le dice:

> *"Tu schema será ejecutado por otro LLM (Pipeline 2) que verá **una sola sección de texto** y deberá extraer estos campos sin conocer los clusters ni el contexto del diseño."*

Esto crea un **gap de contexto** entre el LLM designer (Pipeline 1) y el LLM extractor (Pipeline 2):

```
Pipeline 1: [clusters completos] → LLM designer → schema con descriptions + extraction_hints
                                                     ↓
Pipeline 2:       [texto individual] → LLM extractor → campos extraídos
                     ↑ El extractor NO ve los clusters, NO sabe qué "anomalía" significa
```

### Consecuencias concretas

| Problema | Ejemplo real | Impacto |
|----------|-------------|---------|
| **Hint ambiguo** | `extraction_hint: "Indica si hay definición de fraude"` — el extractor 7B no sabe si buscar la palabra "fraude" o párrafos específicos | Falsos positivos/negativos en extracción |
| **Juicio semántico** | `texto_vacio: "Retornar 1 si no contiene párrafo normativo"` — el extractor necesita decidir qué es "normativo" | Inconsistente entre ejecuciones |
| **Conteo sin método** | `num_acciones_listadas: "Contar acciones (i-iv)"` — ¿cuenta por marcadores literales o por semántica? | El designer puede contar 4, el extractor 2 |
| **Sin contexto de extracción** | El hint asume que el extractor sabe que "cláusula sanciones" es el párrafo específico con acciones i-iv | El extractor no tiene ese conocimiento |

### `extraction_hint` + `extraction_method` como solución (v3+v4)

Con la regla 8 (v3) agregamos `extraction_hint` a cada campo. Con la regla 11 (v4) agregamos `extraction_method` que clasifica el hint en literal/semantic/structural.

Esto permite a Pipeline 2:
- **literal** → puede pre-filtrar con regex antes de llamar al LLM
- **semantic** → envía el texto completo al LLM para razonar
- **structural** → puede hacer el cálculo programáticamente

El hint lo escribe el **mismo LLM designer que no sabe que su audiencia es otro LLM**. El hint puede ser igual de ambiguo que la description si el designer no tiene contexto del extractor.

### Contexto de extracción implementado (v4)

Ya se agregó al user prompt (en `_generate_schema_via_llm` y `_build_chat_prompt`) el siguiente bloque:

> **"IMPORTANTE — CONTEXTO DE EJECUCIÓN: El schema diseñado será ejecutado automáticamente por un sistema separado que procesa cada sección de forma individual. Ese sistema NO tiene acceso a estos clusters ni a este análisis. Recibirá únicamente el texto de una sección y los extraction_hints. Por lo tanto, cada extraction_hint debe ser autocontenido: no uses frases como 'como en el cluster dominante', especifica qué texto exacto o qué patrón activa el valor 1, lista exactamente qué items contar, enumera variantes textuales explícitamente."**

Además, la instrucción final cambió de:
> *"Analiza los clusters... que capture las características potencialmente anómalas del texto y diferencie los clusters."*
> **→ "...y cuyos extraction_hints sean ejecutables por un sistema automático sobre textos individuales."**

### Ya incluido en el prompt actual

El contexto de extracción ya está en el prompt. Para futuras iteraciones, también se podría considerar agregar una línea adicional al user prompt que refuerce:

> **"Contexto de extracción: El esquema que diseñes será ejecutado por otro sistema de IA que extraerá estos campos automáticamente de cada sección individual, una por una, sin conocer los clusters. Tus extraction_hints serán las instrucciones directas para ese extractor. Diseña los hints como comandos precisos para un LLM que solo ve el texto de una sección, no los clusters."**

Esto cambiaría cómo el LLM designer escribe los hints:
- En lugar de `"Indica si menciona DNCP"` → `"Buscar 'DNCP' o 'Dirección Nacional de Contrataciones Públicas'. Retornar 1 si aparece alguna variante."`
- En lugar de `"Cantidad de acciones listadas"` → `"Contar los incisos numerados (i), (ii), (iii), (iv) literalmente. Retornar el número de incisos encontrados (0-4). No contar por semántica."`

### Validación futura

Una vez que Pipeline 2 exista, se puede validar el gap automáticamente:

1. Para cada campo del schema, tomar su `extraction_hint`
2. Ejecutar el extractor sobre las **mismas muestras** que vio el designer
3. Comparar resultados del extractor vs los `cluster_values` (si existen) o vs el cluster de origen
4. Si hay divergencias → el hint es ambiguo → marcar para revisión humana

Esto crea un **feedback loop** entre Pipeline 1 y Pipeline 2 que cierra el gap.

---

## Ideas para Futuras Mejoras

### ✅ Ya implementado (v3 + v4)
- `extraction_hint` por campo (regla 8)
- `extraction_method` literal/semantic/structural (regla 11)
- `cluster_values` opcional (regla 10)
- Contexto de extracción en user prompt
- `_validate_schema` con soporte para todas las keys
- `EXTRACTION_SYSTEM_PROMPT` para Pipeline 2
- `build_section_extraction_prompt()` con filtro importance ≥ 7

### 1. Validación del gap: extractor vs designer
Una vez que Pipeline 2 exista:
1. Ejecutar extractor sobre las mismas muestras que vio el designer
2. Comparar resultados del extractor vs `cluster_values`
3. Si divergen → hint ambiguo → marcar para revisión
4. Métrica: **tasa de consistencia designer→extractor**

### 2. Instrucción explícita: "mapea cada campo a clusters anómalos"
**Propuesta:** *"Para cada campo, indica qué cluster(es) toman el valor anómalo y cuál es el valor esperado en el estándar."*

### 3. Prohibir redundancia entre campos + fusionar umbrella categories
DeepSeek v2 tiene `contiene_remision_dncp` (imp 10) y `contiene_presentacion_denuncia` (imp 9) — casi siempre colineales.
**Propuesta:** *"No diseñes campos correlacionados. Si varios campos miden un mismo concepto, fusiónalos en un numérico compuesto."*

### 4. Schema validation automático post-LLM con 2º LLM barato (7B)
Validar con un segundo LLM: ¿cada campo mide anomalías? ¿redundancia? ¿extraíble según hint?

### 5. Feedback loop con feature importance real
Comparar importance score del LLM vs feature importance del Isolation Forest.

### 6. Few-shot dinámico por título
Inyectar schemas de títulos similares como ejemplos en lugar de ejemplos fijos.

### 7. Evaluación cuantitativa de schemas
Cobertura, precisión, importance calibration, redundancia.

---

## Resumen de Arquitectura del Prompt

```
SYSTEM PROMPT (~550 tokens)
├── Rol (experto en detección de anomalías)
├── Objetivo (esquema JSON, desviaciones del estándar)
├── DEFINICIÓN DE ANOMALÍA (v2)
├── INSTRUCCIONES (4 reglas de enfoque, v2)
├── REGLAS ESTRICTAS (11 reglas)
│   ├── 1-7: Formato (binario/numérico, max 12, sin anidados)
│   ├── 8: extraction_hint por campo (v3)
│   ├── 9: importance score (1-10) (v2)
│   ├── 10: cluster_values opcional (v3)
│   └── 11: extraction_method literal/semantic/structural (v4)
└── Formato de respuesta (ejemplos con extraction_hint + extraction_method + cluster_values, v4)

USER PROMPT (dinámico por título)
├── Título de la sección
├── FREQUENCY HEADER (v2) ← Total secciones, distribución, flecha al dominante
├── CLUSTERS ENCONTRADOS
│   ├── Cluster 1: 2 muestras (representativo + extremo)
│   ├── Cluster 2: 2 muestras
│   └── ...
├── CONTEXTO DE EJECUCIÓN (v4) ← "El schema será ejecutado por un sistema separado..."
└── Instrucción final: "...y cuyos extraction_hints sean ejecutables por un sistema automático sobre textos individuales."

PIPELINE 2 SYSTEM PROMPT (~200 tokens, constante)
├── EXTRACTION_SYSTEM_PROMPT
└── Extrae campos siguiendo extraction_hint, respeta extraction_method

BUILD EXTRACTION PROMPT (v4, función)
├── build_section_extraction_prompt(titulo, nro_licitacion, texto, schema, threshold=7)
├── Filtra campos con importance >= threshold
├── Solo incluye type + extraction_hint + extraction_method
└── Retorna (system_prompt, user_prompt) listo para LLM call

POST-PROCESAMIENTO (v4)
├── _validate_schema (permite importance + extraction_hint + extraction_method)
├── Validación: extraction_method debe ser literal/semantic/structural
├── Validación determinista de colapso via cluster_values
├── Retry automático si clusters colapsados
└── Save a master_schemas.json
```

# Pipeline de Clustering + Schema Design — Contexto Completo para LLM

> Documento de contexto para que otro LLM entienda el pipeline, el prompt, los datos,
> y pueda colaborar en mejoras, debugging o extensiones futuras.
> Fecha: Junio 2026 | Proyecto: subtitle-parse (DNCP — Paraguay)

---

## 1. Propósito del Pipeline

Detectar **anomalías en pliegos de licitaciones públicas paraguayas** mediante un enfoque
de clustering + schema design + extracción + anomalía.

### Stack completo (3 pipelines)

```
Pipeline 1: Section Clustering ✅ (IMPLEMENTADO)
  [DB] → extraer secciones → embeddings → UMAP → K-Means → cluster samples
       → LLM schema design → master_schemas.json + llm_chat_prompts.json

Pipeline 2: Extraction Runner 🔜 (PLANEADO)
  [DB] + [master_schemas.json] → por cada sección, LLM extrae campos del schema
       → UPSERT a dncp.section_extracted_features

Pipeline 3: Anomaly Detection 🔜 (PLANEADO)
  [dncp.section_extracted_features] → Isolation Forest + LOF + Autoencoder
       → reportes de anomalías por título/año/categoría
```

### ¿Qué problema resuelve Pipeline 1?

De 31,000 documentos de licitación (323 títulos de sección), hay variantes de redacción
de una misma cláusula. Algunas variantes **debilitan controles, reducen sanciones o
eliminan responsabilidades** — eso es una anomalía.

El pipeline agrupa textos similares en clusters, muestra 2 muestras por cluster a un LLM,
y el LLM diseña un **schema de campos binarios/numéricos** que miden exactamente las
diferencias normativas entre clusters.

---

## 2. Fuente de Datos

| Propiedad                       | Valor                                 |
| ------------------------------- | ------------------------------------- |
| Tabla                           | `dncp.pliegos_secciones` (PostgreSQL) |
| Filas                           | ~2.7M                                 |
| Documentos                      | ~31,000                               |
| Títulos                         | 323                                   |
| Títulos activos (seleccionados) | 10                                    |
| Años                            | 2021–2025                             |
| Conexión                        | `.env` → `config/settings.py`         |

### Los 10 títulos activos

| Título                                      |   Secciones | Cluster raw | Cluster merged | Truncación (chars) |
| ------------------------------------------- | ----------: | :---------: | :------------: | :----------------: |
| aclaracion de las ofertas                   |      29,606 |     10      |       8        |        2000        |
| audiencia informativa                       |      32,180 |      6      |       4        |        2000        |
| copias de la oferta cps                     |      29,414 |      4      |       4        |        500         |
| formato y firma de la oferta                |      31,782 |      7      |       6        |        2000        |
| fraude y corrupcion                         |      31,658 | 18→6 merged |       6        |        2000        |
| idioma de la oferta                         |      28,924 |      2      |       2        |        500         |
| limitacion de responsabilidad               |      31,166 |     10      |       7        |        2000        |
| planos y disenos                            |      30,372 |      7      |       6        |        500         |
| porcentaje de garantia de fiel cumplimiento |      30,143 |      8      |       7        |        2000        |
| retiro sustitucion y modificacion           |      29,750 |     10      |       7        |        2000        |
| **Total**                                   | **314,995** |             |                |                    |

### Distribución típica de clusters

Título "fraude y corrupcion" (31,658 secciones, sampleado a 5,000):

```
Total secciones: 5000
Clusters identificados: 6
Distribución:
  Cluster 0: 2900 (58.0%) ← versión dominante/estándar
  Cluster 1: 800 (16.0%)
  Cluster 2: 650 (13.0%)
  Cluster 3: 370 (7.4%)
  Cluster 4: 200 (4.0%)
  Cluster 5: 80 (1.6%)
```

---

## 3. Arquitectura del Pipeline

### Estructura de archivos (Pipeline 1)

```
scripts/run_section_clustering_pipeline.py    ← CLI entry point
src/pipelines/section_clustering_pipeline.py   ← Orchestrator (8 steps)
src/etl/extractors/section_clustering_extractor.py    ← DB queries
src/etl/transformers/section_clustering_transformer.py ← Core ML+LLM (1206 líneas)
src/etl/transformers/llm_provider.py          ← Provider abstraction (147 líneas)
src/etl/loaders/section_clustering_loader.py  ← Output serialization (310 líneas)
config/settings.py                            ← DB connection, paths
```

### Flujo `run()` (Pipeline 1)

```
1. extractor.extract_all_titles()
   → Consulta DB: SELECT DISTINCT titulo FROM dncp.pliegos_secciones
   → Para cada título activo: SELECT nro_licitacion, text WHERE titulo = ? LIMIT max_samples

2. transformer.process_title(title, df, max_samples=5000)   [por cada título]
   │
   ├─ 2a. Embeddings (SentenceTransformer: paraphrase-multilingual-MiniLM-L12-v2, 384-dim)
   ├─ 2b. UMAP (n_components=10, n_neighbors=15, min_dist=0.1, metric=cosine)
   ├─ 2c. K-Means con k-óptimo (grid search k=2..20 por defecto)
   │      Score combinado: silhouette(0.3) + davies_bouldin_inv(0.35) + calinski_harabasz_norm(0.35)
   ├─ 2d. Sampling: 3 nearest to centroid (representative) + 2 farthest (extreme) por cluster
   │      → 5 muestras si hay suficientes, menos si cluster pequeño
   ├─ 2e. Merge clusters similares: difflib ratio ≥ 0.95 en texto representativo
   ├─ 2f. Truncación dinámica: median_text_length × 1.5, clamp [500, 2000]
   ├─ 2g. LLM schema generation (o skip si --skip-llm)
   │      → _generate_schema_via_llm() con sistema + usuario
   │      → _validate_schema() con retry (hasta 3 intentos)
   └─ 2h. Build extraction prompt para uso futuro

3. loader.save_master_schema(results)
   → data/processed/section_clustering/master_schemas.json

4. loader.save_title_report(title, data)
   → data/processed/section_clustering/report_{title}.md  (10 archivos)

5. loader.save_cluster_samples(results)
   → all_cluster_samples_raw.json (K-Means original)
   → all_cluster_samples_merged.json (clusters merged, misma vista que chat prompts)

6. loader.save_clustering_comparison(results)
   → clustering_comparison.json

7. [Solo si --export-chat-prompts] transformer._build_chat_prompt() + loader.save_chat_prompts()
   → llm_chat_prompts.json (prompts listos para copiar-pegar en web LLM)

8. Log summary table
```

### Outputs generados

| Archivo                           | Tamaño       | Contenido                                                                         |
| --------------------------------- | ------------ | --------------------------------------------------------------------------------- |
| `master_schemas.json`             | ~866 KB      | Resultados completos: clusters, samples, schema, justificación, extraction prompt |
| `llm_chat_prompts.json`           | ~119 KB      | Prompts system+user listos para DeepSeek/ChatGPT/Claude                           |
| `all_cluster_samples_raw.json`    | ~682 KB      | Muestras por cluster K-Means original                                             |
| `all_cluster_samples_merged.json` | ~231 KB      | Muestras post-merge (misma vista que chat prompts)                                |
| `report_{title}.md`               | 19–77 KB c/u | Reporte individual por título                                                     |
| `clustering_comparison.json`      | ~7 KB        | Comparación si modo grid-search activado                                          |

---

## 4. El Prompt de Schema Design

### System Prompt (~420 tokens, constante)

````
Eres un experto en detección de anomalías en pliegos de licitaciones públicas paraguayas.

Recibes muestras de texto agrupadas en CLUSTERS para una sección específica de un pliego.
Cada cluster representa una variante distinta de cómo aparece ese contenido.

Tu tarea es diseñar un ESQUEMA JSON de variables observables que permita detectar
desviaciones anómalas respecto a la versión estándar (el cluster dominante).

DEFINICIÓN DE ANOMALÍA:
Una anomalía es cualquier omisión, reducción o modificación de elementos normativos
que debilite controles, reduzca sanciones, elimine responsabilidades, o se desvíe
de la plantilla estándar de contratación pública paraguaya.

INSTRUCCIONES:
1. Identifica primero cuál es la versión estándar o dominante (cluster con más muestras).
2. Luego diseña campos que midan desviaciones respecto a esa versión estándar.
3. Prioriza especialmente la ausencia de cláusulas que aparecen en la mayoría de los clusters.
4. No diseñes campos para capturar diferencias de redacción, capitalización, formato o estilo.
   Diseña únicamente campos que representen diferencias normativas, jurídicas, operativas
   o de cumplimiento.

REGLAS ESTRICTAS para el esquema:
1. Solo campos BINARIOS (0/1): indican presencia/ausencia de ciertos elementos, cláusulas o características.
2. Solo campos NUMÉRICOS (enteros o floats): cantidades, porcentajes, conteos, valores.
3. NO se permiten: campos de texto libre, listas de strings, objetos anidados complejos.
4. Todos los campos deben ser relevantes para la detección de anomalías.
5. El esquema debe ser común para todas las secciones de este título (algunos campos quedarán en 0).
6. Máximo 12 campos en total.
7. Los campos binarios y numéricos deben estar al mismo nivel (no anidados).
8. Para cada campo incluye un "extraction_hint": instrucción precisa de 1-3 oraciones
   que indique cómo extraer ese valor del texto.
   - Para campos binarios: especifica qué texto o patrón activa el valor 1 vs 0.
     Indica si la detección es por coincidencia literal, variantes semánticas, o criterio estructural.
   - Para campos numéricos: especifica qué se cuenta/mide, la unidad, y el rango esperado.
     Indica si el conteo es por marcadores literales (incisos, letras) o por semántica.
   - Nunca uses "si menciona X" sin especificar variantes aceptadas o el criterio de activación.
9. Para cada campo asigna un score de importancia entre 1 y 10
   respecto a su potencial valor para detección de anomalías.
10. Opcional: incluye "cluster_values" con el valor que cada campo tomaría
    en cada cluster, para validación automática del schema.
11. Para cada campo incluye un "extraction_method" con el tipo de extracción:
    - "literal": detección por coincidencia textual exacta (regex).
    - "semantic": detección por significado o contexto.
    - "structural": detección por estructura del texto.

Formato de respuesta DEBE SER EXACTAMENTE:
```json
{{
  "schema": {{
    "contiene_denuncia_penal": {{"type": "binary", "description": "Indica si se menciona explícitamente denuncia penal (no solo 'denuncia')", "importance": 10, "extraction_method": "literal", "extraction_hint": "Buscar la frase exacta 'denuncia penal'. No activar con solo 'denuncia'. Retornar 1 si aparece, 0 si no."}},
    "menciona_dncp": {{"type": "binary", "description": "Indica si se menciona a la DNCP como autoridad receptora", "importance": 9, "extraction_method": "literal", "extraction_hint": "Buscar 'DNCP' o 'Dirección Nacional de Contrataciones Públicas'. Retornar 1 si aparece, 0 si no."}},
    "num_acciones_incumplimiento": {{"type": "numeric", "description": "Cantidad de acciones o sanciones listadas ante incumplimiento (0-4)", "importance": 8, "extraction_method": "semantic", "extraction_hint": "Contar cuántas de estas 4 acciones están presentes semánticamente: descalificar oferta, rescindir contrato, remitir a DNCP, denuncia penal. Retornar entero 0-4."}},
    "texto_vacio": {{"type": "binary", "description": "Indica si la sección no contiene desarrollo normativo", "importance": 7, "extraction_method": "structural", "extraction_hint": "Retornar 1 si el texto tiene menos de 50 palabras o no contiene oración con contenido normativo. Retornar 0 en caso contrario."}}
  }},
  "justification": "Explica por qué cada campo fue elegido, qué cluster(es) lo motivaron, qué valor tomaría en cada cluster, y por qué esa diferencia es sospechosa para detección de anomalías.",
  "cluster_values": {{
    "contiene_denuncia_penal": {{"cluster_0": 0, "cluster_1": 1, "cluster_3": 0}},
    "num_acciones_incumplimiento": {{"cluster_0": 4, "cluster_1": 2, "cluster_3": 0}},
    "texto_vacio": {{"cluster_0": 0, "cluster_1": 0, "cluster_3": 1}}
  }}
}}
````

Debes responder ÚNICAMENTE con el JSON mostrado arriba, sin texto adicional fuera del bloque de código.

```

### User Prompt (dinámico, se construye por título)

Estructura generada por `_generate_schema_via_llm()`:

```

TÍTULO DE LA SECCIÓN: fraude y corrupcion

Total secciones: 5000
Clusters identificados: 6
Distribución de clusters (ordenados por tamaño descendente):
Cluster 0: 2900 (58.0%) ← versión dominante/estándar
Cluster 1: 800 (16.0%)
Cluster 2: 650 (13.0%)
Cluster 3: 370 (7.4%)
Cluster 4: 200 (4.0%)
Cluster 5: 80 (1.6%)

CLUSTERS ENCONTRADOS:

=== CLUSTER 0 (2 muestras) ===
[REPRESENTATIVO - 123456]
(texto truncado a ~1500 chars)

[EXTREMO - 789012]
(texto truncado a ~1500 chars)

=== CLUSTER 1 (2 muestras) ===
...

Analiza los clusters y sus diferencias. Diseña un esquema JSON (solo binarios y numéricos)
que capture las características potencialmente anómalas del texto y diferencie los clusters.

```

### Frequency Header (clave del diseño)

El header de frecuencias se inyecta tanto en:
- `_generate_schema_via_llm()` → para el LLM local (Qwen 14B vía LM Studio / OpenRouter)
- `_build_chat_prompt()` → para export a web LLMs (DeepSeek, ChatGPT, Claude)

**Propósito:** Sin este header, el LLM no sabe qué cluster es el dominante.
Con él, tiene visibilidad cuantitativa de la distribución y puede enfocar
desviaciones del cluster mayoritario como más anómalas.

### Prompt de Retry (validación)

Si `_validate_schema()` rechaza el schema, se usa `SCHEMA_VALIDATION_SYSTEM_PROMPT`:

```

... [mismo rol + reglas 1-7] ...

Un intento anterior fue RECHAZADO por la siguiente razón:
{razon_rechazo}

... [ejemplos con importance + extraction_hint] ...

CORRIGE el error mencionado en tu respuesta.

````

### Validación Post-LLM (`_validate_schema`)

```python
def _validate_schema(result: Dict) -> Tuple[bool, str]:
    # Debe tener keys "schema" y "justification"
    # schema debe ser dict con 1-12 campos
    # Cada campo: type ∈ {binary, numeric}, description es string no vacío
    # Extra keys permitidas: type, description, importance, extraction_hint, extraction_method
    # extraction_method debe ser literal/semantic/structural o None
    # Si hay cluster_values: verifica que no todos los campos tengan valor constante
    #   entre clusters (colapso determinista en lugar de heurístico)
````

### Fallback Schema

Si el LLM falla tras 3 intentos, se usa schema por defecto:

```json
{
  "tiene_{slug}": { "type": "binary", "description": "..." },
  "longitud_texto_{slug}": { "type": "numeric", "description": "..." }
}
```

---

## 5. LLM Providers

### LocalLLMProvider

| Propiedad       | Valor                                                             |
| --------------- | ----------------------------------------------------------------- |
| Endpoint        | `http://localhost:1234/v1` (configurable)                         |
| API             | OpenAI-compatible `/chat/completions`                             |
| Modelo          | Qwen 2.5 14B Q5_K_L (Bartowski, ~11 GB)                           |
| response_format | `{"type": "json_object"}`                                         |
| Timeout         | 180s                                                              |
| Retries         | 3 (exponential backoff 1s, 2s)                                    |
| GPU             | RTX 4070 Ti SUPER (16 GB VRAM)                                    |
| KV cache        | Q8 (`-ctk q8_0 -ctv q8_0`) para contextos largos (36K-45K tokens) |
| Fallback        | Schema vacío con mensaje de error                                 |

### OpenRouterProvider

| Propiedad      | Valor                                           |
| -------------- | ----------------------------------------------- |
| URL            | `https://openrouter.ai/api/v1`                  |
| Auth           | Bearer token de `OPEN_ROUTER_API_KEY` en `.env` |
| Modelo default | `openai/gpt-oss-20b:free`                       |
| Timeout        | 120s                                            |
| Retries        | 3 (exponential backoff)                         |

---

## 6. Ejemplo de Output del LLM

### DeepSeek v2 — Schema para "fraude y corrupcion" (12 campos)

```json
{
  "schema": {
    "contiene_clausula_sanciones": {
      "type": "binary",
      "description": "Indica si está presente la cláusula que enumera las acciones ante fraude/corrupción.",
      "importance": 10
    },
    "contiene_descalificacion_oferta": {
      "type": "binary",
      "description": "Indica si se lista la acción (i): descalificar la oferta.",
      "importance": 9
    },
    "contiene_rescision_contrato": {
      "type": "binary",
      "description": "Indica si se lista la acción (ii): rescindir el contrato.",
      "importance": 9
    },
    "contiene_remision_dncp": {
      "type": "binary",
      "description": "Indica si se lista la acción (iii): remitir antecedentes a la DNCP.",
      "importance": 10
    },
    "contiene_presentacion_denuncia": {
      "type": "binary",
      "description": "Indica si se lista la acción (iv): presentar denuncia penal.",
      "importance": 9
    },
    "contiene_definicion_fraude": {
      "type": "binary",
      "description": "Indica si está presente la lista de actos que constituyen fraude.",
      "importance": 8
    },
    "num_acciones_listadas": {
      "type": "numeric",
      "description": "Cantidad de acciones sancionatorias (i-iv) listadas (0-4).",
      "importance": 8
    },
    "num_actos_fraude_definidos": {
      "type": "numeric",
      "description": "Cantidad de incisos de actos de fraude explicitados (0-5).",
      "importance": 7
    },
    "menciona_colusion": {
      "type": "binary",
      "description": "Indica si se menciona 'colusión o acuerdo' como acto de fraude.",
      "importance": 8
    },
    "menciona_soborno": {
      "type": "binary",
      "description": "Indica si se menciona el acto de ofrecer/dar/recibir soborno.",
      "importance": 8
    },
    "denuncia_explicita_penal": {
      "type": "binary",
      "description": "Indica si la cláusula dice 'denuncia penal' en lugar de solo 'denuncia'.",
      "importance": 6
    },
    "texto_vacio": {
      "type": "binary",
      "description": "Indica si la sección no contiene ningún párrafo normativo.",
      "importance": 10
    }
  }
}
```

### Claude v2 — Schema para "fraude y corrupcion" (9 campos)

```json
{
  "schema": {
    "seccion_vacia_o_incompleta": { "type": "binary", "importance": 10 },
    "menciona_dncp": { "type": "binary", "importance": 9 },
    "contiene_denuncia_penal": { "type": "binary", "importance": 8 },
    "contiene_definicion_fraude": { "type": "binary", "importance": 9 },
    "num_acciones_sancion": { "type": "numeric", "importance": 8 },
    "num_tipos_fraude_definidos": { "type": "numeric", "importance": 7 },
    "contiene_rescision_contrato": { "type": "binary", "importance": 7 },
    "contiene_remision_antecedentes": { "type": "binary", "importance": 8 },
    "contiene_descalificacion_oferta": { "type": "binary", "importance": 7 }
  }
}
```

---

## 7. Historia de Versiones del Prompt

### v1 (original) — Problemas detectados

| Problema                            | Ejemplo concreto                                         | Consecuencia                                                          |
| ----------------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------- |
| Sin definición de anomalía          | Cada LLM interpretaba "anomalía" a su manera             | Claude creaba campos de formato como `convocante_mayuscula`           |
| Sin "estándar vs excepción"         | El LLM no sabía qué cluster era el dominante             | Diseñaba campos para diferencias irrelevantes entre clusters pequeños |
| Sin importance score                | Todos los campos pesaban igual                           | No se podía filtrar campos de alto valor aguas abajo                  |
| Ejemplos genéricos                  | `menciona_porcentaje_anticipo`, `contiene_firma_digital` | El LLM imitaba el dominio incorrecto                                  |
| Sin frequency header en user prompt | El LLM veía clusters sin contexto cuantitativo           | No podía priorizar desviaciones del cluster mayoritario               |

### v2 — Mejoras implementadas

| Mejora                                                       | Impacto                                                                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| Definición de anomalía                                       | DeepSeek v2: desglosó acciones i-iv como campos separados. Claude v2: eliminó campos de formato. |
| Instrucción "estándar vs excepción" + prohibición de formato | Claude v2: 0 campos de formato (antes tenía 2).                                                  |
| Importance score (regla 9)                                   | Ambos modelos asignan importance 1-10 a cada campo.                                              |
| Ejemplos del dominio                                         | Ambos modelos usan `menciona_dncp`, `contiene_denuncia_penal`, etc.                              |
| Frequency header                                             | Ambos modelos referencian el cluster dominante en justificación.                                 |

### v3 — Extraction-Ready Schema

Motivación: Claude sugirió que el schema debe ser directamente ejecutable por Pipeline 2,
sin que el extractor tenga que re-inferir cómo extraer cada campo.

| Mejora                                                                                                | Impacto                                                                                                                                                              |
| ----------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`extraction_hint` por campo** (regla 8 nueva)                                                       | Pipeline 2 puede usar el hint como instrucción directa.                                                                                                              |
| **Regla 8 reescrita**: de "debe ser extraíble" a "incluye extraction_hint con instrucciones precisas" | El LLM ahora sabe que debe generar hints, no solo descriptions.                                                                                                      |
| **Ejemplos con extraction_hint**                                                                      | Señal más fuerte del formato esperado.                                                                                                                               |
| **`cluster_values` opcional** (regla 10)                                                              | Validez determinista de clusters colapsados + ground truth.                                                                                                          |
| **`_validate_schema` actualizado**                                                                    | Permite `extraction_hint` y `cluster_values`. Validación determinista de colapso.                                                                                    |

### v4 (actual) — Full Pipeline Integration

Motivación: Claude v2 identificó 3 gaps adicionales: el designer no sabe quién ejecuta los hints,
los hints mezclan calidades literal/semántica/estructural sin distinción, y el prompt de Pipeline 2 no existe.

| Mejora | Impacto |
|--------|---------|
| **`extraction_method` por campo** (regla 11): literal, semantic, structural | Pipeline 2 sabe si aplicar regex, razonamiento LLM o análisis estructural. Permite pre-filtrar con regex antes de llamar al LLM para campos literales. |
| **Contexto de extracción** en user prompt de Pipeline 1 | El LLM designer ahora sabe que escribe para un extractor sin contexto global. Los hints pasan de descriptivos a ejecutables. |
| **Instrucción final actualizada** | "...cuyos extraction_hints sean ejecutables por un sistema automático sobre textos individuales." |
| **`EXTRACTION_SYSTEM_PROMPT`** (~200 tokens) | System prompt fijo para el extractor de Pipeline 2. Instruye sobre cómo usar extraction_hint y extraction_method. |
| **`build_section_extraction_prompt()`** | Función que construye system+user prompt para Pipeline 2. Filtra por importance ≥ 7, solo incluye type/hint/method. |
| **`_validate_schema` actualizado** | Valida que extraction_method ∈ {literal, semantic, structural}. |

---

## 10. Documents for Pipeline 2

### EXTRACTION_SYSTEM_PROMPT (fijo, ~200 tokens)

```
Eres un extractor de información estructurada de documentos legales paraguayos.

Recibes el texto de una sección de un pliego de licitación pública y un schema
con campos a extraer. Tu tarea es extraer el valor de cada campo siguiendo
exactamente las instrucciones del extraction_hint de ese campo.

REGLAS:
1. Extrae SOLO los campos del schema. No agregues campos adicionales.
2. Para campos binarios: retorna exactamente 0 o 1 (entero, no booleano).
3. Para campos numéricos: retorna el número exacto según el hint. Si no aplica, retorna 0.
4. Si el texto está vacío o es ilegible, retorna el valor por defecto (0 para binarios, 0 para numéricos).
5. Para campos con extraction_method "literal": busca coincidencia textual exacta, no infieras.
6. Para campos con extraction_method "semantic": razona sobre el significado del texto.
7. Para campos con extraction_method "structural": analiza la estructura (longitud, cantidad).
8. No expliques tu razonamiento. Retorna solo el JSON de salida.

Formato de salida DEBE SER EXACTAMENTE:
{
  "campos": {
    "nombre_campo": valor,
    ...
  }
}
```

### User prompt dinámico (construido por `build_section_extraction_prompt`)

Se genera por cada (título, nro_licitacion):

```
TÍTULO DE LA SECCIÓN: fraude y corrupcion
DOCUMENTO: 123456

SCHEMA A EXTRAER:
{
  "contiene_denuncia_penal": {
    "type": "binary",
    "extraction_method": "literal",
    "extraction_hint": "Buscar la frase exacta 'denuncia penal'. No activar con solo 'denuncia'. Retornar 1 si aparece, 0 si no."
  },
  "num_acciones_listadas": {
    "type": "numeric",
    "extraction_method": "semantic",
    "extraction_hint": "Contar cuántas de estas 4 acciones están presentes semánticamente: descalificar oferta, rescindir contrato, remitir a DNCP, denuncia penal. Retornar entero 0-4."
  }
}

TEXTO DE LA SECCIÓN:
<texto completo aquí>

Extrae los campos del schema siguiendo exactamente cada extraction_hint.
```

Notas:
- Solo se incluyen campos con importance ≥ 7 (threshold configurable)
- No se incluyen description, importance ni cluster_values — solo type + extraction_hint + extraction_method
- El nro_licitacion permite trackeo y checkpointing

## 8. Cómo Correr el Pipeline

```bash
# Pipeline completo con LLM local (LM Studio)
python scripts/run_section_clustering_pipeline.py

# Solo clustering y export de prompts (sin LLM)
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts

# Con OpenRouter
python scripts/run_section_clustering_pipeline.py --llm-provider openrouter

# Saltar títulos específicos
python scripts/run_section_clustering_pipeline.py --skip-titles fraude retiro

# Grid search sobre embeddings y UMAP params
python scripts/run_section_clustering_pipeline.py --compare

# Personalizar rango de k
python scripts/run_section_clustering_pipeline.py --k-min 3 --k-max 15
```

### Flujo recomendado para desarrollo

```
1. python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts
   → Genera clusters y exporta prompts (rápido, ~10 min)

2. Copiar prompts de llm_chat_prompts.json a DeepSeek/ChatGPT/Claude web
   → Guardar respuestas en data/external/llm_web_responses/

3. Analizar respuestas, ajustar prompt si es necesario

4. python scripts/run_section_clustering_pipeline.py
   → Correr con LLM local para generar schemas completos
```

---

## 9. Output Esperado del LLM

El LLM debe devolver JSON con:

```json
{
  "schema": {
    "nombre_campo_1": {
      "type": "binary",
      "description": "...",
      "importance": 8,
      "extraction_method": "literal",
      "extraction_hint": "Instrucción de 1-3 oraciones sobre cómo extraer este valor del texto"
    }
  },
  "justification": "...",
  "cluster_values": {
    "nombre_campo_1": { "cluster_0": 0, "cluster_1": 1 }
  }
}
```

### Reglas de validación del schema

1. **Máximo 12 campos** — sino se rechaza
2. **Mínimo 1 campo** — sino se rechaza
3. **Cada campo**: `type` ∈ {binary, numeric}, `description` es string no vacío
4. **Extra keys permitidas**: `type`, `description`, `importance`, `extraction_hint`, `extraction_method` — cualquier otra key causa rechazo
5. **extraction_method**: si presente, debe ser "literal", "semantic" o "structural"
6. **Clusters colapsados**: si todos los campos dan 0 o mismo valor para todos los clusters → se rechaza. Si `cluster_values` está presente, la validación es determinista (valores únicos entre clusters) en lugar de heurística.
7. **cluster_values**: Si presente, debe ser dict con keys matching schema fields, cada valor es dict con cluster→valor. Si todos los campos tienen valor constante entre clusters → se rechaza.
8. **Hasta 3 intentos**: intento 0 con system prompt, intentos 1-2 con validation prompt + razón de rechazo
9. **Fallback**: schema genérico de 2 campos si todos los intentos fallan

---

## 10. Ideas para Futuras Mejoras (post-v4)

### Ya implementadas (v3 + v4)
- ✅ `extraction_hint` por campo (regla 8)
- ✅ `extraction_method` literal/semantic/structural (regla 11)
- ✅ `cluster_values` opcional con validación determinista (regla 10)
- ✅ Contexto de extracción en user prompt Pipeline 1
- ✅ `EXTRACTION_SYSTEM_PROMPT` para Pipeline 2
- ✅ `build_section_extraction_prompt()` con filtro importance ≥ 7

### Pendientes

---

## 11. Referencias

| Archivo                   | Ruta                                                                   |
| ------------------------- | ---------------------------------------------------------------------- |
| Transformer principal     | `src/etl/transformers/section_clustering_transformer.py` (1206 líneas) |
| Pipeline orchestrator     | `src/pipelines/section_clustering_pipeline.py` (244 líneas)            |
| Loader                    | `src/etl/loaders/section_clustering_loader.py` (310 líneas)            |
| LLM Provider              | `src/etl/transformers/llm_provider.py` (147 líneas)                    |
| CLI entry point           | `scripts/run_section_clustering_pipeline.py` (132 líneas)              |
| Config                    | `config/settings.py` (42 líneas)                                       |
| Credenciales              | `.env` (no en git)                                                     |
| Plan actual               | `docs/plan_actual.md` (577 líneas)                                     |
| Plan futuro               | `docs/plan_futuro.md` (1035 líneas)                                    |
| Prompt actual documentado | `docs/prompt_actual.md`                                                |
| Extraction runner spec    | `docs/readme_pipeline_extraction_runner.md` (218 líneas)               |
| Respuestas LLM v1         | `data/external/llm_web_responses/{gpt,claude,deepseek}_response.txt`   |
| Respuestas LLM v2         | `data/external/llm_web_responses/{claude,deepseek}_response_v2.txt`    |
| Sugerencias Claude v3     | `docs/mejoras_claude_web.txt`                                          |
| Master schemas            | `data/processed/section_clustering/master_schemas.json`                |
| Chat prompts              | `data/processed/section_clustering/llm_chat_prompts.json`              |
| Cluster samples (raw)     | `data/processed/section_clustering/all_cluster_samples_raw.json`       |
| Cluster samples (merged)  | `data/processed/section_clustering/all_cluster_samples_merged.json`    |
| Per-title reports         | `data/processed/section_clustering/report_*.md` (10 archivos)          |
| AGENTS.md                 | `AGENTS.md` (continuidad de sesión)                                    |

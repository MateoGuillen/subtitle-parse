# Workflow: Validación de Schemas vía LLM Web

Flujo completo para diseñar un schema JSON con un LLM web (Claude/DeepSeek), insertarlo en `master_schemas.json`, y validarlo contra samples reales con un LLM local.

---

## Índice

1. [Exportar prompt para LLM web](#1-exportar-prompt-para-llm-web)
2. [Enviar a Claude o DeepSeek web](#2-enviar-a-claude-o-deepseek-web)
3. [Insertar respuesta en master_schemas.json](#3-insertar-respuesta-en-master_schemasjson)
4. [Validar con `validate_extraction_prompts.py`](#4-validar-con-validate_extraction_promptspy)
5. [Interpretar resultados y ajustar hints](#5-interpretar-resultados-y-ajustar-hints)
6. [Flujo completo de un tirón](#6-flujo-completo-de-un-tiron)
7. [Reproducir con otro título](#7-reproducir-con-otro-titulo)

---

## 1. Exportar prompt para LLM web

El prompt de diseño de schema está pre-generado en `llm_chat_prompts.json`. Para regenerarlo con la versión más reciente del prompt:

```bash
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts
```

Esto regenera `data/processed/section_clustering/llm_chat_prompts.json` con el `SCHEMA_DESIGN_SYSTEM_PROMPT` actual (v4) que incluye las reglas de `extraction_hint`, `extraction_method` y `cluster_values`.

```bash
# Flags útiles para regenerar rápido (solo el título que te interesa):
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts --skip-titles fraude retiro copias limitacion planos porcentaje aclaracion audiencia formato
```

Esto exporta solo el prompt de "idioma de la oferta" si ya lo tenés procesado, o de todos los no-saltados.

### Estructura del prompt exportado

Cada entrada en `llm_chat_prompts.json` contiene:

```json
{
  "title": "idioma de la oferta",
  "total_sections": 5000,
  "n_clusters": 5,
  "cluster_counts": { "0": 1148, "1": 1028, ... },
  "prompt": {
    "system": "<SCHEMA_DESIGN_SYSTEM_PROMPT v4>",
    "user": "<muestras de clusters + contexto de ejecución>"
  }
}
```

---

## 2. Enviar a Claude o DeepSeek web

### Qué copiar

Del archivo `llm_chat_prompts.json`, extraer la entrada del título deseado y copiar AMBAS partes al LLM web:

1. **System prompt** (`prompt.system`) — contiene rol, definición de anomalía, reglas estrictas y formato de respuesta
2. **User prompt** (`prompt.user`) — contiene distribución de clusters, muestras de texto reales por cluster, contexto de ejecución

> ⚠ **Importante**: copiar **ambos** prompts (system + user). Si solo se copia el user prompt, el LLM no sabrá las reglas de `extraction_hint`, `extraction_method` ni `cluster_values`.

### Respuesta esperada

El LLM debe devolver un JSON con esta estructura:

```json
{
  "schema": {
    "nombre_campo": {
      "type": "binary",
      "description": "...",
      "importance": 9,
      "extraction_method": "literal|semantic|structural",
      "extraction_hint": "Instrucción precisa de 1-3 oraciones para extraer este valor"
    },
    ...
  },
  "justification": "Explicación de por qué cada campo fue elegido...",
  "cluster_values": {
    "nombre_campo": {
      "cluster_0": 0,
      "cluster_1": 1,
      ...
    }
  }
}
```

### Guardar respuesta

Guardar el texto completo de la respuesta (JSON + feedback si lo hay) en:

```
data/external/llm_web_responses/{proveedor}_response_v{version}.txt
```

Ejemplos existentes:
- `deepseek_response_v4.txt` — schema para "idioma de la oferta" (DeepSeek)
- `claude_response_v4.txt` — schema para "idioma de la oferta" (Claude)
- `deepseek_reponse.txt` — schema para "fraude y corrupcion" (DeepSeek v1)

> El texto después del JSON (feedback del LLM) se ignora automáticamente.

---

## 3. Insertar respuesta en master_schemas.json

### Opción A: Script automático (`scripts/insert_llm_schema.py`)

```bash
python scripts/insert_llm_schema.py \
  --title "idioma de la oferta" \
  --provider deepseek \
  --file data/external/llm_web_responses/deepseek_response_v4.txt
```

**Flags:**
| Flag | Requerido | Descripción |
|------|:---------:|-------------|
| `--title` | ✅ | Nombre exacto del título en `master_schemas.json` |
| `--provider` | ✅ | `deepseek`, `claude` o `gpt` |
| `--file` | ✅ | Ruta al archivo de respuesta |
| `--version` | ❌ | Etiqueta de versión (default: `v4`) |

**Qué hace:**
1. Parsea el JSON de respuesta (detecta `schema`, `justification`, `cluster_values`)
2. Valida que cada campo tenga `type`, `importance`, `extraction_hint`, `extraction_method`
3. Reemplaza `json_schema`, `schema_justification` y `cluster_values` en `master_schemas.json`
4. Agrega metadata: `llm_provider`, `schema_version`, `imported_from`

**Output:**

```
Parsing data/external/llm_web_responses/deepseek_response_v4.txt...
  Fields: 8
  Has justification: True
  Has cluster_values: True

  [OK] All fields valid

  [OK] Schema inserted for 'idioma de la oferta' (8 fields from deepseek)

  Summary:
    Old schema: 2 fields (default fallback)
    New schema: 8 fields
    Binary:     7
    Numeric:    1
    Literal:    1
    Semantic:   3
    Structural: 4
    Cluster mappings: 6
```

### Opción B: Manual

Abrir `data/processed/section_clustering/master_schemas.json`, localizar el título, y reemplazar:

```json
"idioma de la oferta": {
  ... (clusters, samples, etc. NO TOCAR) ...
  "json_schema": { ...schema de DeepSeek... },
  "schema_justification": "...",
  "cluster_values": { ... },
  "llm_provider": "deepseek",
  "schema_version": "v4",
  "imported_from": "deepseek_response_v4.txt"
}
```

---

## 4. Validar con `validate_extraction_prompts.py`

### Validación simple (1 run)

```bash
python scripts/validate_extraction_prompts.py \
  --title "idioma de la oferta" \
  --samples 16
```

### Validación doble (recomendada)

```bash
python scripts/validate_extraction_prompts.py \
  --title "idioma de la oferta" \
  --samples 16 \
  --runs 2
```

**Flags:**

| Flag | Default | Descripción |
|------|---------|-------------|
| `--samples` | 16 | Número de samples por run (estratificados por cluster) |
| `--runs` | 1 | Número de runs con muestras diferentes (recomendado: 2) |
| `--provider` | `local` | `local` (LM Studio) o `openrouter` |
| `--llm-base-url` | `http://localhost:1234/v1` | URL del LLM local |
| `--model` | `""` | Modelo específico (default: el cargado en LM Studio) |
| `--threshold` | 0 | Filtrar campos por importance ≥ threshold (0 = todos) |

**Qué hace:**
1. Toma N samples de `master_schemas.json` (estratificados: mismo número por cluster)
2. Para cada sample:
   - Construye el mismo prompt que Pipeline 2 usaría
   - Envía al LLM local (Qwen 14B Q5 recomendado)
   - Parsea respuesta JSON
   - Compara contra `cluster_values` de DeepSeek/Claude
3. Reporta accuracy por campo (TP/TN/FP/FN)
4. Si `--runs 2`: ejecuta 2 veces con muestras diferentes, reporta min/max/avg

**Criterio de aprobación (doble validación):**
- **APTO** si AMBOS runs ≥ 85%
- **NO APTO** si alguno de los 2 runs < 85%

### Modelo local recomendado

| Modelo | Tamaño | VRAM | Velocidad | Para |
|--------|:------:|:----:|:---------:|------|
| **Qwen 2.5 14B Q5_K_M** | ~11 GB | ~13 GB | ~40 tok/s | Validación y Pipeline 2 |
| Qwen 2.5 7B Q4_K_M | ~4.5 GB | ~6.2 GB | ~85 tok/s | No recomendado (falla en semántica) |

LM Studio debe estar corriendo en `http://localhost:1234/v1` con Qwen 14B cargado.

### Output de validación doble

```
############################################################
  RUN 1/2
############################################################
  Collected 16 samples across 8 clusters
  [1/16] nro=449093 cluster=0... OK (1/1 OK)
  ...

  Total: 16 | OK: 16 | Fail: 0

  Field                                          Acc
  --------------------------------------------- ------
  texto_vacio                                    100.0%
  contiene_clausula_traduccion_oferta            100.0%
  ...

  RUN 1 RESULT: 94.4% accuracy

############################################################
  RUN 2/2
############################################################
  Collected 16 samples across 8 clusters
  [1/16] nro=422246 cluster=1... OK (1/1 OK)
  ...

  RUN 2 RESULT: 91.7% accuracy

============================================================
  DOUBLE VALIDATION RESULT: idioma de la oferta
============================================================
  Runs: 2 | Samples per run: 16

  RUN 1: 94.4%  [PASS]
  RUN 2: 91.7%  [PASS]

  Min:  91.7%
  Max:  94.4%
  Avg:  93.0%
  Fields < 85% in any run: ninguno

  [OK] Schema APTO (min accuracy 91.7% >= 85%)
```

---

## 5. Interpretar resultados y ajustar hints

### Lectura de métricas

| Símbolo | Significado | Acción |
|:-------:|-------------|--------|
| **TP** | True Positive: campo=1, predijo=1 | ✅ Correcto |
| **TN** | True Negative: campo=0, predijo=0 | ✅ Correcto |
| **FP** | False Positive: campo=0, predijo=1 | Extrajo algo que no debía → hint muy permisivo |
| **FN** | False Negative: campo=1, predijo=0 | No extrajo algo que debía → hint muy restrictivo |

### Causas comunes de error

| Error | Causa probable | Solución |
|-------|----------------|----------|
| FN en campo binario | El hint no describe todos los patrones textuales posibles | Agregar variantes al hint |
| FP en campo binario | El hint es demasiado genérico | Especificar qué NO activa el campo |
| FN en numérico | El LLM cuenta distinto | Cambiar a structural con rango, no valor exacto |
| FN en campo con dependencia | El hint referencia otro campo que el LLM no calculó | Hacer el hint autocontenido |

### Cómo ajustar un hint

1. Abrir `master_schemas.json`
2. Localizar el título y el campo problemático
3. Modificar `extraction_hint` para ser más preciso
4. Re-ejecutar validación

Ejemplo — hint original (0% accuracy):
```
"pregunta_permiso_es_sin_traduccion": {
  "extraction_hint": "En la línea que contiene 'permitirá' y finaliza con ':', buscar justo antes de los dos puntos la frase exacta 'sin traducción'..."
}
```

Hint mejorado:
```
"extraction_hint": "Buscar en TODO el texto la presencia de 'sin traducción' como frase independiente, no como parte de otra palabra. Retornar 1 si aparece en cualquier parte del texto, 0 si no aparece."
```

### Criterio de aceptación

| Accuracy | Estado | Acción |
|:--------:|:------:|--------|
| ≥ 85% | ✅ **Apto** | Schema listo para Pipeline 2 |
| 70-84% | ⚠️ **Ajustar** | Mejorar hints de campos < 85%, re-validar |
| < 70% | ❌ **Rechazar** | Schema mal diseñado, re-generar con LLM web |

> Los campos numéricos con comparación exacta contra `cluster_values` (como `num_palabras_seccion`) tienden a dar 0% porque el LLM cuenta distinto a la predicción del schema designer. **No es una falla real** — estos campos se evalúan por rango (±15 palabras), no por valor exacto.

> **Ground truth por muestra**: si un cluster tiene muestras con contenido diferente (ej: cluster 2 con muestras vacías y con contenido), usar `sample_overrides` en `master_schemas.json` para asignar valores correctos a muestras específicas. Ejemplo: `"sample_overrides": {"424765": {"texto_vacio": 0, ...}}`.

> **Campos que varían dentro de clusters**: campos como `cantidad_copias_requeridas` pueden tener valores diferentes dentro del mismo cluster. En estos casos, usar `sample_overrides` para cada muestra, o calcular el campo por Python en Pipeline 2 (regex sobre el texto).

### Validación del ground truth (cluster_values)

Los `cluster_values` generados por Claude/DeepSeek pueden estar mal porque asumen que todas las muestras de un cluster son idénticas. El clustering agrupa por similitud de embedding, no por valores de campos.

**Antes de validar contra el LLM local**, verificar que los `cluster_values` coincidan con el contenido real:

1. Para cada sample, extraer el valor real con regex/Python
2. Comparar contra `cluster_values`
3. Si hay discrepancias, agregar `sample_overrides` en `master_schemas.json`

**Campos propensos a errores en cluster_values:**
- Campos numéricos que varían dentro de clusters (`cantidad_copias_requeridas`)
- Campos que dependen de presencia/ausencia de texto específico
- Campos donde el LLM web simplificó (ej: asumió 0 cuando el texto no menciona nada)

**Workflow recomendado:**
```
1. Exportar prompt → 2. Web LLM genera schema → 3. Insertar
4. [NUEVO] Verificar cluster_values contra texto real
5. Agregar sample_overrides si hay discrepancias
6. Validar contra LLM local con --runs 2
```

---

## 6. Flujo completo de un tirón

Validar schema para un título de principio a fin (~30 min):

```bash
# 0. Setup: regenerar prompts si cambió el system prompt
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts

# 1. Exportar prompt de un título específico a un archivo
python -c "
import json
d = json.load(open('data/processed/section_clustering/llm_chat_prompts.json'))
for p in d:
    if p['title'] == 'idioma de la oferta':
        print(p['prompt']['user'])
" > docs/temp_prompt_idioma.txt

# --- PAUSA MANUAL: copiar system+user a DeepSeek/Claude web ---
# --- Guardar respuesta en data/external/llm_web_responses/{proveedor}_response_v4.txt ---

# 2. Insertar schema
python scripts/insert_llm_schema.py \
  --title "idioma de la oferta" \
  --provider deepseek \
  --file data/external/llm_web_responses/deepseek_response_v4.txt

# 3. Validar doble (16 samples × 2 runs, Qwen 14B)
python scripts/validate_extraction_prompts.py \
  --title "idioma de la oferta" \
  --samples 16 \
  --runs 2

# 4. Si accuracy < 85%, ajustar hints y re-validar
#    (editar master_schemas.json manualmente)
```

---

## 7. Reproducir con otro título

Pasos para aplicar el mismo flujo a otro de los 10 títulos:

### 7.1. Lista de títulos disponibles

Los títulos procesados en `master_schemas.json`:

```
fraude y corrupcion
formato y firma de la oferta
copias de la oferta cps
limitacion de responsabilidad
planos y disenos
porcentaje de garantia de fiel cumplimiento de con
idioma de la oferta
aclaracion de las ofertas
retiro sustitucion y modificacion de las ofertas
audiencia informativa
```

### 7.2. Exportar prompt

```bash
# Regenerar prompts si es necesario
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts

# Ver el prompt del título elegido
python -c "
import json
d = json.load(open('data/processed/section_clustering/llm_chat_prompts.json'))
for p in d:
    if p['title'] == 'fraude y corrupcion':
        print('System:', p['prompt']['system'][:100])
        print('User:', p['prompt']['user'][:200])
"
```

### 7.3. Insertar schema

```bash
python scripts/insert_llm_schema.py \
  --title "fraude y corrupcion" \
  --provider deepseek \
  --file data/external/llm_web_responses/deepseek_reponse.txt
```

> **Nota**: la respuesta v1 de DeepSeek para "fraude" no tiene `extraction_hint` ni `extraction_method`. El script inserta igual (con warnings) y el validador usa `description` como hint fallback.

### 7.4. Validar

```bash
python scripts/validate_extraction_prompts.py \
  --title "fraude y corrupcion" \
  --samples 16 \
  --runs 2
```

### 7.5. Si se necesita re-generar el prompt v4 (con todos los campos)

```bash
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts
```

Luego copiar el nuevo prompt a DeepSeek/Claude web. La respuesta incluirá `extraction_hint`, `extraction_method` y `cluster_values`.

---

## Referencia rápida

### Scripts del workflow

| Script | Propósito |
|--------|-----------|
| `scripts/insert_llm_schema.py` | Insertar respuesta de LLM web en `master_schemas.json` |
| `scripts/validate_extraction_prompts.py` | Validar schema contra samples reales (doble validación automática) |
| `scripts/import_chat_results.py` | Importar múltiples respuestas web (legacy, para respuestas v1-v3) |

### Archivos clave

| Archivo | Propósito |
|---------|-----------|
| `data/processed/section_clustering/master_schemas.json` | Schemas por título (origen de Pipeline 2) |
| `data/processed/section_clustering/llm_chat_prompts.json` | Prompts listos para copiar a LLM web |
| `data/external/llm_web_responses/` | Respuestas guardadas de LLMs web |
| `data/processed/section_clustering/report_{title}.md` | Reportes de clustering por título |

### Pipeline 2 (futuro)

Una vez que el schema esté validado (accuracy ≥ 85%), los siguientes pasos son:

1. Implementar `src/etl/extractors/section_extraction_extractor.py` (query DB)
2. Implementar `src/etl/transformers/section_extraction_transformer.py` (extraction + validation)
3. Implementar `src/etl/loaders/section_extraction_loader.py` (checkpointing + batch UPSERT)
4. Implementar `src/pipelines/section_extraction_pipeline.py` (orquestación)
5. Implementar `scripts/run_section_extraction_pipeline.py` (CLI entry point)
6. Crear tabla `dncp.section_extracted_features` en DB

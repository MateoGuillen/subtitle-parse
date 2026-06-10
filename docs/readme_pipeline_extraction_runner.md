# Pipeline 2: Section Extraction Runner

Extrae datos estructurados de todas las secciones de los 10 títulos usando los JSON schemas generados por el pipeline de clustering.

## Datos

| Título | Secciones | Longitud promedio |
|--------|:--------:|:-----------------:|
| fraude y corrupcion | 32,180 | 1,984 chars |
| formato y firma | 32,180 | 574 |
| copias de la oferta cps | 31,298 | 357 |
| limitacion de responsabilidad | 30,320 | 467 |
| planos y disenos | 28,924 | 300 |
| porcentaje de garantia | 32,180 | 304 |
| idioma de la oferta | 31,653 | 281 |
| aclaracion de las ofertas | 32,180 | 760 |
| retiro sustitucion | 31,900 | 1,139 |
| audiencia informativa | 32,180 | 535 |
| **TOTAL** | **314,995** | — |

## Hardware disponible

- **GPU**: RTX 4070 Ti SUPER (16 GB VRAM)
- **RAM**: 16 GB
- **Modelos locales**: Qwen 2.5 14B Q5_K_L (~11 GB), Qwen 2.5 7B Q4_K_M (~4.5 GB)
- **LLM remoto**: OpenRouter (`OPEN_ROUTER_API_KEY` en `.env`)

## Opciones de ejecución

### Opción A: 7B local + paralelismo (~22-30h)

| Modelo | Workers | Tiempo estimado |
|--------|:------:|:---------------:|
| 7B Q4_K_M secuencial | 1 | 70h ❌ |
| 7B + 4 concurrentes | 4 | 42h |
| 7B + 6 concurrentes | 6 | 30h |
| 7B + 6 + truncar a 400 chars | 6 | 22h |

**Ventajas**: $0, privacidad total.
**Desventajas**: lento, ocupa GPU por >24h.

### Opción B: OpenRouter híbrido (~30 min, $10-20)

| Modelo | Tok/s | Costo 314K secciones |
|--------|:-----:|:--------------------:|
| Gemini 2.0 Flash | ~200 | **~$5-8** |
| GPT-4o mini | ~200 | **~$15-20** |
| DeepSeek V3 | ~150 | **~$10-15** |
| Llama 3.1 70B | ~100 | **~$8-12** |

**Ventajas**: rápido (30 min), no ocupa GPU.
**Desventajas**: $10-20 por corrida, requiere Internet.

### Opción C: Híbrido dividido por título

Textos largos (fraude 1,984 chars, retiro 1,139 chars) → OpenRouter.
Textos cortos (idioma 281 chars, planos 300 chars, etc.) → 7B local.

**Mejor relación tiempo/costo**: ~12h local + ~5 min OpenRouter = ~$3-5.

## Arquitectura del Extraction Runner

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PIPELINE 2 - EXTRACTION RUNNER                    │
├─────────────────────────────────────────────────────────────────────┤
│  1. LEER master_schemas.json                                        │
│     - schema + extraction_prompt por título                         │
│                                                                     │
│  2. QUERY secciones de la DB                                        │
│     - SELECT nro_licitacion, content_text, title_normalized         │
│       FROM dncp.pliegos_secciones WHERE LOWER(title_normalized)     │
│       IN (...titulos...)                                            │
│     - Ordenar por título, nro_licitacion                            │
│                                                                     │
│  3. PROCESAR por título:                                            │
│     ┌──────────────────────────────────────────────┐                │
│     │  Para cada sección:                          │                │
│     │  1. build_section_extraction_prompt(         │                │
│     │       titulo, nro, texto, schema, thr=7)     │                │
│     │     → system (EXTRACTION_SYSTEM_PROMPT)      │                │
│     │     → user (schema reducido + texto)          │                │
│     │  2. Truncar texto a N chars (configurable)   │                │
│     │  3. LLM call con response_format json_schema │                │
│     │  4. Validar output contra el schema          │                │
│     │  5. Si inválido → retry (max 2)              │                │
│     │  6. Si retry falla → default fallback        │                │
│     └──────────────────────────────────────────────┘                │
│                                                                     │
│  4. CHECKPOINT cada N secciones (ej. 500)                          │
│     - Guarda progreso en JSON temporal                             │
│     - Resume desde el último checkpoint si se interrumpe           │
│                                                                     │
│  5. PERSISTIR en dncp.section_extracted_features                   │
│     - nro_licitacion, titulo, features (JSONB), timestamp           │
│     - UPSERT: si ya existe para ese nro_licitacion + titulo,        │
│       actualizar (para re-ejecuciones parciales)                   │
│                                                                     │
│  6. REPORTE final                                                   │
│     - Totales procesados, errores, rate, tiempo estimado            │
│     - Muestras de fallos para debug                                 │
└─────────────────────────────────────────────────────────────────────┘
```

## Estrategia de paralelismo

```
                 ┌──────────────────────┐
                 │   LM Studio Server   │
                 │  http://localhost    │
                 │  :1234 (7B)         │
                 └──────────┬───────────┘
                            │
            ┌───────────────┼───────────────┐
            │               │               │
       ┌────▼────┐    ┌────▼────┐    ┌────▼────┐
       │ Worker 1│    │ Worker 2│    │ Worker 3│ ...
       │ asyncio │    │ asyncio │    │ asyncio │
       └────┬────┘    └────┬────┘    └────┬────┘
            │               │               │
       ┌────▼───────────────▼───────────────▼────┐
       │           Semaphore (max 6)              │
       │  Controla concurrencia a LM Studio       │
       └─────────────────────────────────────────┘
            │               │               │
       ┌────▼───────────────▼───────────────▼────┐
       │           Checkpoint Manager            │
       │   Guarda cada 500 secciones             │
       │   Resume desde último checkpoint        │
       └─────────────────────────────────────────┘
            │
       ┌────▼────────────────────────────────────┐
       │           DB Writer (batch UPSERT)       │
       │   Acumula 100 resultados y escribe      │
       └─────────────────────────────────────────┘
```

## Prompt Construction (Pipeline 2)

El prompt del extractor tiene dos partes: un **system prompt fijo** (~200 tokens) y un **user prompt dinámico** por sección.

### EXTRACTION_SYSTEM_PROMPT (ya implementado en Pipeline 1)

```python
EXTRACTION_SYSTEM_PROMPT = """Eres un extractor de información estructurada de documentos legales paraguayos.

Recibes el texto de una sección de un pliego de licitación pública y un schema
con campos a extraer. Tu tarea es extraer el valor de cada campo siguiendo
exactamente las instrucciones del extraction_hint de ese campo.

REGLAS:
1. Extrae SOLO los campos del schema. No agregues campos adicionales.
2. Para campos binarios: retorna exactamente 0 o 1 (entero, no booleano).
3. Para campos numéricos: retorna el número exacto según el hint. Si no aplica, retorna 0.
4. Si el texto está vacío o es ilegible, retorna el valor por defecto.
5. Para campos con extraction_method "literal": busca coincidencia textual exacta.
6. Para campos con extraction_method "semantic": razona sobre el significado.
7. Para campos con extraction_method "structural": analiza la estructura.
8. No expliques tu razonamiento. Retorna solo el JSON de salida.

Formato de salida DEBE SER EXACTAMENTE:
{
  "campos": {
    "nombre_campo": valor,
    ...
  }
}"""
```

### `build_section_extraction_prompt()` (ya implementado en Pipeline 1)

Esta función construye el prompt para una sección individual:

```python
def build_section_extraction_prompt(
    self, titulo: str, nro_licitacion: str, texto: str,
    schema: dict, importance_threshold: int = 7
) -> Tuple[str, str]:
    """
    Filtra campos por importance >= threshold.
    Solo incluye 'type', 'extraction_hint' y 'extraction_method'.
    No incluye description/importance/cluster_values (ruido para el extractor).
    """
```

### Ejemplo de user prompt generado

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

Nota: El schema que recibe el extractor es un **subconjunto limpio** del schema original:
- Solo `type`, `extraction_hint`, `extraction_method`
- Sin `description`, `importance`, `cluster_values`
- Filtrado por importance ≥ 7 (típicamente 5-7 campos en lugar de 12)

### Flujo de construcción

```
master_schemas.json (por título)
         ↓
build_section_extraction_prompt(titulo, nro_licitacion, texto, schema, threshold=7)
         ↓
    [system: EXTRACTION_SYSTEM_PROMPT ~200 tokens]  (fijo)
    [user: schema reducido + texto]                  (dinámico por sección)
         ↓
    LLM call (7B Q4_K_M u OpenRouter)
         ↓
    {"campos": {"contiene_denuncia_penal": 1, ...}}
         ↓
    _validate_extraction() → UPSERT a dncp.section_extracted_features
```

## Componentes del código

### `src/etl/extractors/section_extraction_extractor.py`
- Query secciones por título desde `dncp.pliegos_secciones`
- Filtrado, paginación, ordenamiento

### `src/etl/transformers/section_extraction_transformer.py`
- Usa `SectionClusteringTransformer.build_section_extraction_prompt()` para construir prompts
- Toma schema + sección → llama al LLM → devuelve features
- Validación de output contra schema (tipos, rangos)
- Retry lógica (max 2 intentos)
- Fallback defaults

### `src/etl/loaders/section_extraction_loader.py`
- Checkpointing (guardar/leer progreso)
- Batch UPSERT a `dncp.section_extracted_features`
- Reporte de progreso

### `src/pipelines/section_extraction_pipeline.py`
- Orquesta: leer schemas → procesar títulos → checkpoint → persistir
- Manejo de errores por título (no falla todo por un título)
- Dry-run mode para estimar costo sin ejecutar
- Usa `build_section_extraction_prompt()` de Pipeline 1 directamente

### `scripts/run_section_extraction_pipeline.py`
```bash
python scripts/run_section_extraction_pipeline.py                           # modo normal
python scripts/run_section_extraction_pipeline.py --dry-run                 # estimar costo sin ejecutar
python scripts/run_section_extraction_pipeline.py --provider openrouter     # usar OpenRouter
python scripts/run_section_extraction_pipeline.py --skip-titles fraude      # saltar títulos
python scripts/run_section_extraction_pipeline.py --resume                  # retomar desde checkpoint
python scripts/run_section_extraction_pipeline.py --max-sections 1000       # limitar para prueba
```

## Decisiones técnicas

### LLM Provider reutilizable
Usar el mismo `LLMProvider` (`LocalLLMProvider` / `OpenRouterProvider`) del pipeline 1.

### response_format: json_schema
LM Studio soporta `response_format: {"type": "json_schema", "json_schema": {"schema": {...}}}`. Esto obliga al modelo a generar JSON válido con la estructura exacta del schema del título. Elimina errores de parsing.

### Checkpointing
Formato del checkpoint:
```json
{
  "title": "fraude y corrupcion",
  "processed": 15000,
  "total": 32180,
  "last_nro_licitacion": "12345",
  "timestamp": "2026-06-02T10:30:00",
  "errors": [],
  "results_file": "checkpoints/fraude_y_corrupcion_15000.json"
}
```

### DB Table: dncp.section_extracted_features
```sql
CREATE TABLE IF NOT EXISTS dncp.section_extracted_features (
    nro_licitacion VARCHAR NOT NULL,
    title_normalized VARCHAR NOT NULL,
    features JSONB NOT NULL,
    schema_version VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    llm_model VARCHAR,
    PRIMARY KEY (nro_licitacion, title_normalized)
);
```

### Estimador de costo (dry-run)
- Cuenta secciones por título
- Mide tokens promedio por sección (sampleando 100)
- Estima costo según modelo OpenRouter elegido
- Muestra tiempo estimado según workers configurados

## Próximos pasos

1. [ ] Implementar `section_extraction_extractor.py`
2. [ ] Implementar `section_extraction_transformer.py`
3. [ ] Implementar `section_extraction_loader.py` (con checkpointing)
4. [ ] Implementar `section_extraction_pipeline.py`
5. [ ] Implementar `scripts/run_section_extraction_pipeline.py`
6. [ ] Crear `dncp.section_extracted_features` en la DB
7. [ ] Probar con 1 título (idioma de la oferta, 31K secciones)
8. [ ] Correr full con checkpointing
9. [ ] Implementar pipeline de anomalías (post-extracción)

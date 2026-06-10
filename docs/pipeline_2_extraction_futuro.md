# Pipeline 2: Section Extraction Runner (Futuro)

## Propósito
Usa los JSON schemas generados por Pipeline 1 para extraer datos estructurados de **todas las secciones** de los 10 títulos (~315,000 secciones). Es el pipeline **a implementar**.

---

## Arquitectura

```
master_schemas.json (de Pipeline 1)
  -> leer schema + extraction_prompt por título
DB (dncp.pliegos_secciones)
  -> query secciones por título
    -> section_extraction_transformer
      -> build_section_extraction_prompt()
      -> LLM call con response_format json_schema
      -> validar output contra schema
      -> retry (max 2) o fallback defaults
    -> checkpoint cada 500 secciones
DB (dncp.section_extracted_features)
  -> UPSERT batch cada 100 resultados
```

---

## Datos a procesar

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

---

## Opciones de ejecución

| Opción | Tiempo estimado | Costo |
|--------|:--------------:|:-----:|
| **A**: 7B local (6 workers) | ~30h | $0 |
| **B**: OpenRouter (Gemini Flash) | ~30 min | $5-20 |
| **C**: Híbrido (largos→OR, cortos→local) | ~12h + 5 min | $3-5 |

### Hardware disponible
- **GPU**: RTX 4070 Ti SUPER (16 GB VRAM)
- **RAM**: 16 GB
- **Local**: Qwen 2.5 14B Q5_K_L (~11 GB), Qwen 2.5 7B Q4_K_M (~4.5 GB)
- **Remoto**: OpenRouter (`OPEN_ROUTER_API_KEY`)

---

## Flujo por sección

```
build_section_extraction_prompt(titulo, nro, texto, schema, threshold=7)
  -> filtra campos con importance >= threshold
  -> incluye solo type, extraction_hint, extraction_method
  -> schema reducido: 5-7 campos (vs 12 originales)
    |
    v
[system: EXTRACTION_SYSTEM_PROMPT ~200 tokens] (fijo)
[user: titulo, nro, schema reducido, texto] (dinámico)
    |
    v
LLM call (7B Q4_K_M u OpenRouter)
  response_format: {"type": "json_schema", "json_schema": {"schema": {...}}}
    |
    v
{"campos": {"contiene_denuncia_penal": 1, "num_acciones": 3, ...}}
    |
    v
_validate_extraction() — valida tipos y rangos
  |-> si inválido: retry (max 2)
  |-> si falla: fallback defaults (0 para binarios, 0 para numéricos)
    |
    v
UPSERT a dncp.section_extracted_features
```

---

## Componentes a implementar (5 archivos)

### 1. `src/etl/extractors/section_extraction_extractor.py`
- Query secciones por título desde `dncp.pliegos_secciones`
- Parámetros: `title_normalized`, `limit`, `offset`
- Retorno: iterador/generador de `(nro_licitacion, content_text)`

### 2. `src/etl/transformers/section_extraction_transformer.py`
- Usa `SectionClusteringTransformer.build_section_extraction_prompt()` (ya implementado)
- Toma schema + sección → llama al LLM → devuelve features
- Validación de output contra schema (tipos, rangos)
- Retry lógica (max 2 intentos)
- Fallback defaults

### 3. `src/etl/loaders/section_extraction_loader.py`
- Checkpointing: guarda/lee progreso en JSON
- Batch UPSERT a `dncp.section_extracted_features`
- Reporte de progreso

### 4. `src/pipelines/section_extraction_pipeline.py`
- Orquesta: leer schemas → procesar títulos → checkpoint → persistir
- Manejo de errores por título (no falla todo por un título)
- Dry-run mode para estimar costo sin ejecutar
- Reutiliza `LLMProvider` (LocalLLMProvider / OpenRouterProvider) de Pipeline 1

### 5. `scripts/run_section_extraction_pipeline.py`
```bash
python scripts/run_section_extraction_pipeline.py
python scripts/run_section_extraction_pipeline.py --dry-run
python scripts/run_section_extraction_pipeline.py --provider openrouter
python scripts/run_section_extraction_pipeline.py --skip-titles fraude
python scripts/run_section_extraction_pipeline.py --resume
python scripts/run_section_extraction_pipeline.py --max-sections 1000
```

---

## DB: tabla destino

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

---

## Estrategia de checkpointing

```
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

- Checkpoint cada 500 secciones
- Resume desde el último checkpoint si se interrumpe
- DB Writer acumula 100 resultados y hace UPSERT batch

---

## Paralelismo

```
             LM Studio Server (http://localhost:1234)
                        |
          ┌─────────────┼─────────────┐
      Worker 1      Worker 2      Worker 3 ...
          │             │             │
          └─────────────┼─────────────┘
                    Semaphore (max 6)
                        |
                Checkpoint Manager (cada 500)
                        |
                  DB Writer (batch 100)
```

---

---

## Mejoras de Claude (análisis externo)

Claude identificó problemas concretos en el diseño de Pipeline 2. Todos son viables y deben incorporarse antes o durante la implementación:

| # | Hallazgo | Prioridad | Estado |
|---|----------|:---------:|:------:|
| 1 | **Dual path para response_format**: no asumir que el LLM soporta `json_schema`. Si lo soporta, usarlo; si no, usar `json_object` + validación post-LLM en `_validate_extraction()`. | Alta | ❌ Pendiente |
| 2 | **Benchmark de paralelismo antes del run completo**: probar 1, 2, 3, 4, 6 workers sobre 100 secciones y medir tokens/segundo. Para Qwen 14B en 16GB VRAM, el óptimo probable es 2-3 workers, no 6. | Media | ❌ Pendiente |
| 3 | **Agregar `is_fallback`, `extraction_confidence`, `error_message` a la tabla destino**: los defaults silenciosos (0 para binarios, 0 para numéricos) contaminan Pipeline 3. Sin esta columna, un fallo de extracción se confunde con una anomalía real. | **Crítica** | ❌ Pendiente |
| 4 | **Schema versioning con hash MD5**: `compute_schema_version()` que hashea field names + types. Antes del UPSERT, verificar coherencia con filas existentes. Si cambió, borrar viejas o abortar. | Alta | ❌ Pendiente |
| 5 | **Validación P1→P2 antes del run completo**: extraer 20 textos (10 de cada cluster) con el schema de "idioma de la oferta", verificar accuracy ≥ 85% contra `cluster_values`. Script `scripts/validate_extraction_prompts.py`. | Alta | ❌ Pendiente |

### Detalle técnico — DDL corregido de la tabla destino

```sql
CREATE TABLE IF NOT EXISTS dncp.section_extracted_features (
    nro_licitacion VARCHAR NOT NULL,
    title_normalized VARCHAR NOT NULL,
    features JSONB NOT NULL,
    schema_version VARCHAR NOT NULL,        -- hash MD5( field_names + types )
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    llm_model VARCHAR,
    extraction_confidence FLOAT,            -- 0.0-1.0
    is_fallback BOOLEAN DEFAULT FALSE,      -- TRUE = default usado por fallo
    error_message TEXT,                     -- descripción si is_fallback
    PRIMARY KEY (nro_licitacion, title_normalized)
);
```

### Detalle técnico — Schema versioning

```python
import hashlib, json

def compute_schema_version(schema: dict) -> str:
    # Solo field names + types (descriptions/hints pueden cambiar sin romper compatibilidad)
    canonical = {k: v["type"] for k, v in sorted(schema.items())}
    return hashlib.md5(json.dumps(canonical, sort_keys=True).encode()).hexdigest()[:8]
```

### Detalle técnico — Dual path response_format

```python
# En section_extraction_transformer.py
def _extract_fields(self, schema, texto, provider):
    if provider.supports_json_schema:
        response_format = {"type": "json_schema", "json_schema": {"schema": schema}}
    else:
        response_format = {"type": "json_object"}
    raw = provider.generate(messages, response_format=response_format)
    parsed = self._validate_extraction(raw, schema)
    if not parsed:
        # reintento con json_object si falló el schema
        raw = provider.generate(messages, response_format={"type": "json_object"})
        parsed = self._validate_extraction(raw, schema)
    return parsed or self._fallback_defaults(schema)
```

---

## Próximos pasos (actualizados con mejoras de Claude)

### Fase 1 — Validación previa
1. [ ] Crear `scripts/validate_extraction_prompts.py` — validar schemas contra 20 textos reales
2. [ ] Benchmark de paralelismo: medir tokens/s con 1-6 workers

### Fase 2 — Implementación con mejoras incorporadas
3. [ ] Implementar `src/etl/extractors/section_extraction_extractor.py`
4. [ ] Implementar `src/etl/transformers/section_extraction_transformer.py` (dual path response_format)
5. [ ] Implementar `src/etl/loaders/section_extraction_loader.py` (con checkpointing + schema versioning)
6. [ ] Implementar `src/pipelines/section_extraction_pipeline.py`
7. [ ] Implementar `scripts/run_section_extraction_pipeline.py`

### Fase 3 — Ejecución
8. [ ] Crear tabla `dncp.section_extracted_features` (con columnas `is_fallback`, `extraction_confidence`, `error_message`)
9. [ ] Probar con 1 título (idioma de la oferta, 31K secciones)
10. [ ] Correr full con checkpointing
11. [ ] Implementar pipeline de anomalías (post-extracción)

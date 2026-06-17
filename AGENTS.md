# AGENTS.md — Session Continuation Context

## Project
`D:\projects\subtitle-parse` — ETL pipeline for Paraguayan public procurement (DNCP) anomaly detection.

## Active Branch
`feature/ocds-csv-pipeline` — staged changes (not yet committed):
- `config/settings.py`, `scripts/run_section_clustering_pipeline.py`, `src/etl/loaders/section_clustering_loader.py`, `src/etl/transformers/llm_provider.py` (new), `src/etl/transformers/section_clustering_transformer.py`, `src/pipelines/section_clustering_pipeline.py`, `docs/readme_pipeline_extraction_runner.md`, `reporte_clustering_decisiones.md`

## Next Priority (in order)
1. ✅ Schema DeepSeek v4 validado para "idioma de la oferta" (86.2% con Qwen 14B) — **APTO para Pipeline 2**
2. Repetir validación para los otros 9 títulos (generar schemas vía web LLM, insertar, validar)
3. Implementar extraction runner pipeline (5 new files)
4. Validar clusters in original embedding space

## Key Commands
```bash
# Clustering pipeline (no LLM, fast export regeneration):
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts

# Clustering pipeline with local LLM:
python scripts/run_section_clustering_pipeline.py

# With OpenRouter:
python scripts/run_section_clustering_pipeline.py --llm-provider openrouter

# Skip specific titles:
python scripts/run_section_clustering_pipeline.py --skip-titles fraude retiro

# Update schemas from manual edits:
python scripts/update_schemas.py

# ---- Schema validation workflow ----
# Export prompts for web LLM:
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts

# Insert web LLM response into master_schemas.json:
python scripts/insert_llm_schema.py --title "idioma de la oferta" --provider deepseek --file data/external/llm_web_responses/deepseek_response_v4.txt

# Validate (doble validación recomendada, 16 samples × 2 runs):
python scripts/validate_extraction_prompts.py --title "idioma de la oferta" --samples 16 --runs 2

# Validate single run:
python scripts/validate_extraction_prompts.py --title "idioma de la oferta" --samples 16
```

## Critical Files
- `src/etl/transformers/section_clustering_transformer.py` — main logic (1117 lines)
- `src/etl/transformers/llm_provider.py` — LLM provider abstraction (ahora soporta `model` param)
- `scripts/validate_extraction_prompts.py` — validación de schemas vs LLM local
- `scripts/insert_llm_schema.py` — insertar respuestas de LLM web en master_schemas.json
- `src/pipelines/section_clustering_pipeline.py` — pipeline orchestration
- `config/settings.py` — reads from `.env`
- `.env` — live creds (not in git)

## Data
- `data/processed/section_clustering/master_schemas.json` — schemas + samples (6515 lines)
- `data/processed/section_clustering/llm_chat_prompts.json` — prompts for web LLM
- `data/external/llm_web_responses/` — respuestas guardadas de LLMs web
- DB: `dncp.pliegos_secciones` (2.7M rows, 31K documents, 323 titles)

## Docs (session continuity)
- `docs/plan_actual.md` — everything completed
- `docs/plan_futuro.md` — full future roadmap with implementation details
- `docs/readme_pipeline_extraction_runner.md` — extraction runner spec
- `docs/workflow_validacion_schemas.md` — workflow de validación completo
- `docs/reporte_validacion_completo.md` — reporte completo con benchmarks y análisis costo-tiempo
- `docs/schema_pipeline2_campos.md` — explicación campo por campo de schemas validados

## Schema Validation State
| Título | Estado | Accuracy | Notas |
|--------|:------:|:--------:|-------|
| idioma de la oferta | ✅ APTO | **93.8%-96.9%** (14B Q5, doble validación) | 8 campos. Fix: hint literal estricto para `contiene_contenido_adicional` (30%→100%) |
| copias de la oferta cps | ✅ APTO | **93.8%** (14B Q5, doble validación) | 9 campos Claude. Fix: per-sample overrides para cluster 2 y `cantidad_copias` (ground truth corregido) |
| fraude y corrupcion | ❌ Pendiente | - | Schema v1 sin hints ni methods |
| formato y firma | ❌ Pendiente | - | - |
| limitacion de responsabilidad | ❌ Pendiente | - | - |
| planos y disenos | ❌ Pendiente | - | - |
| ... (5 más) | ❌ Pendiente | - | - |

## Known LLM Limitations
- **Qwen 2.5 7B**: Falla en distinguir `pregunta_permiso_es_sin_traduccion` de `permite_documentos_sin_traduccion` (0% vs 90% con 14B). Usar Qwen 14B para Pipeline 2.
- **LM Studio**: No soporta `response_format.type=json_object` (error 400). Usar parse desde texto.
- **LocalLLMProvider** ahora acepta `model` param para especificar modelo.
- **Conteo de palabras**: LLM siempre falla en campos numéricos exactos (`longitud_texto_palabras`, `num_clausulas_normativas_presentes`). Calcular programáticamente.
- **Hallucination en textos vacíos**: Cluster 2 (texto_vacio=1) causa que LLM invente contenido → 8 errores por muestra. Muestras con <10 palabras deben excluirse del LLM o trattarse separadamente.

## Ground Truth Issues (Descubrimiento)
- **Problema**: `cluster_values` generados por Claude/DeepSeek asumen que todas las muestras de un cluster son idénticas. Pero el clustering agrupa por similitud de embedding, no por valores de campos.
- **Ejemplo**: Cluster 2 de "copias" tiene 3 muestras vacías + 2 con contenido completo. El ground truth decía "vacías" para todas.
- **Solución**: `sample_overrides` en `master_schemas.json` corrige valores para muestras específicas.
- **Mejor práctica**: Verificar `cluster_values` contra texto real antes de validar contra LLM local. Pendiente: script `validate_ground_truth.py` para auto-generar overrides.

## Git State
- `HEAD` = `9d31adc` on `feature/ocds-csv-pipeline`
- Staged: 8 files (new `llm_provider.py`, modified clustering pipeline)
- Untracked: `scripts/update_schemas.py`, `scripts/insert_llm_schema.py`, `scripts/validate_extraction_prompts.py`, `informe_features*.md`, various analysis scripts

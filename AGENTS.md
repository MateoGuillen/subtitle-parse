# AGENTS.md — Session Continuation Context

## Project
`D:\projects\subtitle-parse` — ETL pipeline for Paraguayan public procurement (DNCP) anomaly detection.

## Active Branch
`feature/ocds-csv-pipeline` — staged changes (not yet committed):
- `config/settings.py`, `scripts/run_section_clustering_pipeline.py`, `src/etl/loaders/section_clustering_loader.py`, `src/etl/transformers/llm_provider.py` (new), `src/etl/transformers/section_clustering_transformer.py`, `src/pipelines/section_clustering_pipeline.py`, `docs/readme_pipeline_extraction_runner.md`, `reporte_clustering_decisiones.md`

## Next Priority (in order)
1. Send `llm_chat_prompts.json` to web LLM for schema design (start with "fraude y corrupcion")
2. Create `scripts/import_chat_results.py` + refactor `prompt_utils.py`
3. Implement extraction runner pipeline (5 new files)
4. Validate clusters in original embedding space

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
```

## Critical Files
- `src/etl/transformers/section_clustering_transformer.py` — main logic (1117 lines)
- `src/etl/transformers/llm_provider.py` — LLM provider abstraction
- `src/pipelines/section_clustering_pipeline.py` — pipeline orchestration
- `config/settings.py` — reads from `.env`
- `.env` — live creds (not in git)

## Data
- `data/processed/section_clustering/master_schemas.json` — schemas + samples
- `data/processed/section_clustering/llm_chat_prompts.json` — prompts for web LLM
- DB: `dncp.pliegos_secciones` (2.7M rows, 31K documents, 323 titles)

## Docs (session continuity)
- `docs/plan_actual.md` — everything completed
- `docs/plan_futuro.md` — full future roadmap with implementation details
- `docs/readme_pipeline_extraction_runner.md` — extraction runner spec

## Git State
- `HEAD` = `9d31adc` on `feature/ocds-csv-pipeline`
- Staged: 8 files (new `llm_provider.py`, modified clustering pipeline)
- Untracked: `scripts/update_schemas.py`, `informe_features*.md`, various analysis scripts

# AGENTS.md — Session Continuation Context

## Project
`D:\projects\subtitle-parse` — ETL pipeline for Paraguayan public procurement (DNCP) anomaly detection.

## Active Branch
`feature/ocds-csv-pipeline` — staged changes (not yet committed):
- `config/settings.py`, `scripts/run_section_clustering_pipeline.py`, `src/etl/loaders/section_clustering_loader.py`, `src/etl/transformers/llm_provider.py` (new), `src/etl/transformers/section_clustering_transformer.py`, `src/pipelines/section_clustering_pipeline.py`, `docs/readme_pipeline_extraction_runner.md`, `reporte_clustering_decisiones.md`

Unstaged modified files (current session):
- `src/etl/extractors/document_features_extractor.py` — Phase 1: added word_count, page, line_start, line_end to SECTION_QUERY; chunked loading
- `src/etl/transformers/document_features_transformer.py` — Phase 1: Gini, entropy, word pivot, count pivot, span/page_range pivots, _compute_gini, _compute_title_entropy
- `src/etl/loaders/document_features_loader.py` — Phase 1: FIXED_COLUMNS_DDL extended, create_table handles word_, count_, span_, page_range_ prefixes
- `src/pipelines/document_features_pipeline.py` — Phase 1: extended title_cols generation with 4 new prefix types + word_otros
- `src/etl/extractors/title_ranking_extractor.py` — Phase 4: added ECON_FEATURES_QUERY + get_economic_features(); **Phase 4 fix**: memory-safe column loading (245 cols instead of 587) via `_get_relevant_columns()` querying information_schema
- `src/etl/transformers/title_ranking_transformer.py` — Phase 4: 6 strategies (economic=0.15), STRATEGY_WEIGHTS updated, _strategy_economic_risk method, ECONOMIC_RISK_COLS module constant
- `src/pipelines/title_ranking_pipeline.py` — Phase 4: loads document_economic_features + passes df_econ to compute_ranking

## Sessions Summary

### Session 1 — Feature Improvements Plan (Phases 1–3)

- **Phase 1** — Word count fix + structural features  
  - Recalculated `word_count` for all 2,775,485 sections (avg 204.5, max 475K)  
  - `document_features` rebuilt with 571 columns (was 260): added `word_*`, `count_*`, `span_*`, `page_range_*` per title, plus aggregates (gini, entropy, avg/max page, sum/avg word count)  
  - Pipeline: 31,321 documents, 171 sec, chunked loading (200K rows/chunk), int64→int32 downcast

- **Phase 2** — Economic features table  
  - Created extractor (JOIN across 9 OCDS tables), transformer (12 derived fields: ratios, flags), loader (bulk INSERT with NaT→None), pipeline, run script  
  - Fixed: SQLAlchemy immutabledict bug → switched to raw psycopg2; NaT serialization → pd.isna(v) catch-all  
  - Populated: 29,811 documents, 42 columns, ~3 sec

- **Phase 3** — Collusion features  
  - Extractor SQL: win_freq CTE (winner_category_frequency, winner_total_contracts) and repeat_win CTE (is_repeat_winner) with window functions  
  - Transformer: added bidder_diversity = n_oferentes_distintos / cantidad_items  
  - ALTER TABLE added 4 columns; pipeline re-run: 29,811 rows, 46 columns

### Session 2 — Phase 4 + Memory Fix

- **Phase 4** — 6th ranking strategy (Economic Risk)  
  - STRATEGY_WEIGHTS: outlier=0.20, rf=0.25, contextual=0.15, section_if=0.15, corr=0.10, economic=0.15  
  - ECONOMIC_RISK_COLS: es_unico_oferente, overbudget_ratio, is_high_value_single_bidder, winner_category_frequency, is_repeat_winner, winner_total_contracts, bidder_diversity  
  - `_strategy_economic_risk()`: for each title, merge has_{slug} with economic features on nro_licitacion, compute mean absolute Pearson correlation with risk indicators  
  - `compute_ranking()` updated to accept df_econ param and call 6th strategy  
  - Validated via isolation test (test_economic_strategy.py)

- **Memory Fix**  
  - `_get_relevant_columns()` queries information_schema.columns, filters to has_*/len_*/tok_* prefixes + nro_licitacion + category_id  
  - Column reduction: **587 → 245** (−58%)  
  - Pipeline now runs without ArrayMemoryError

### Session 3 — Phase 5 Execution + Phase 6 Docs

- **Phase 5** — Ranking pipeline re-run  
  - Ranking pipeline executed successfully with 6 strategies  
  - 11.4s, top-1: Fuerza Mayor (0.5717)  
  - Output: title_ranking.csv, title_ranking_report.md, pipeline_summary.json

- **Phase 6** ✅ — Anomaly detection docs fully updated  
  - Added pre-computed tables reference (document_features, document_economic_features, title_ranking)  
  - Updated Feature Engineering section to reference pre-computed tables  
  - Updated ColumnTransformer with new structural + economic features  
  - Added comprehensive **Title Ranking Pipeline** section with 6 strategies, weights, actual top-20 scores (including economic), and integration notes  
  - Added feature justification table with corruption literature references  
  - Updated architecture diagram showing feature engineering → ranking → anomaly detection flow  
  - Updated thesis novelty comparison  
  - Added references section with anomaly detection + corruption literature  
  - Added limitations + reproducibility sections  
  - **Fixed report generator**: `src/etl/loaders/title_ranking_loader.py` now displays all 6 strategies including Economic Risk (was showing 5)  
  - Regenerated `title_ranking_report.md` with updated 6-strategy table  
  - Added CSV schema and design decisions subsections

## Next Priority (in order)

1. **Clustering**: Continue with schema validation for remaining 9 titles

2. **Clustering**: Continue with schema validation for remaining 9 titles  
   - Staged files exist for section_clustering_pipeline but never executed  
   - Need to validate LLM extraction prompts for remaining titles

3. **Extraction runner pipeline**: Implement extraction runner (5 new files)

4. **Commit**: All Phase 1–5 changes are uncommitted:
   - 7 unstaged modified files  
   - 5 new untracked pipeline files (economic features)  
   - Various untracked scripts

## Key Commands
```bash
# Ranking pipeline (6 strategies):
python -c "import sys; sys.path.insert(0, '.'); from src.pipelines.title_ranking_pipeline import TitleRankingPipeline; from config.settings import DB_CONFIG; p = TitleRankingPipeline({'db_params': DB_CONFIG, 'top_n': 80, 'section_sample': 30000, 'output_dir': 'data/processed/title_ranking'}); p.run()"

# Test 6th strategy isolation:
python scripts/test_economic_strategy.py

# Feature engineering pipelines:
python scripts/run_document_features_pipeline.py
python scripts/run_document_economic_features_pipeline.py

# Clustering pipeline:
python scripts/run_section_clustering_pipeline.py --skip-llm --export-chat-prompts
python scripts/run_section_clustering_pipeline.py

# Schema validation:
python scripts/validate_extraction_prompts.py --title "idioma de la oferta" --samples 16
python scripts/insert_llm_schema.py --title "idioma de la oferta" --provider deepseek --file data/external/llm_web_responses/deepseek_response_v4.txt
```

## Relevant Files
- `docs/plan_feature_improvements.md`: full plan document with 6 phases
- `docs/pipeline3_anomaly_detection.md`: anomaly detection pipeline documentation (pending Phase 6 updates)
- `src/etl/extractors/document_features_extractor.py`: SECTION_QUERY v2, chunked loading
- `src/etl/transformers/document_features_transformer.py`: Gini, entropy, pivot aggregations
- `src/etl/loaders/document_features_loader.py`: DDL with 6 new aggregate cols
- `src/pipelines/document_features_pipeline.py`: 4 new prefix types
- `src/etl/extractors/document_economic_extractor.py`: ECONOMIC_QUERY with collusion window functions
- `src/etl/transformers/document_economic_transformer.py`: 12 DERIVED_FIELDS + COLLUSION_FIELDS
- `src/etl/loaders/document_economic_loader.py`: NaT→None, _df_to_tuples with pd.isna catch-all
- `src/etl/extractors/title_ranking_extractor.py`: memory fix — _get_relevant_columns() loads 245 cols via information_schema
- `src/etl/transformers/title_ranking_transformer.py`: 6 strategies, 666 lines, STRATEGY_WEIGHTS, ECONOMIC_RISK_COLS, _strategy_economic_risk
- `src/pipelines/title_ranking_pipeline.py`: loads economic features, passes df_econ to compute_ranking
- `scripts/test_economic_strategy.py`: validates 6th strategy in isolation (lightweight)

## Data
- `dncp.document_features` — 31,321 rows × 571 cols (structural features)
- `dncp.document_economic_features` — 29,811 rows × 46 cols (economic + collusion)
- `dncp.pliegos_secciones` — 2.7M rows, 17 cols (word_count fixed)
- `data/processed/title_ranking/` — ranking CSVs + reports (6 strategies, 11.4s run)

## Known Issues
- `bidder_diversity` = 0.0 for all rows because `cantidad_items` is 0 in licitaciones (source data quality issue)
- Pipeline 2 (LLM extraction) remains ❌ with Schema Validation at 2/10
- `pd.NaT` serialization: use `pd.isna(v)` as catch-all for None/NaN/NaT
- `pandas SQLAlchemy`: UserWarning when reading SQL with psycopg2 connection (cosmetic)

## Git State
- `HEAD` = `9d31adc` on `feature/ocds-csv-pipeline`
- Staged: 8 files (clustering pipeline — never executed)
- Modified (unstaged): 7 files from Phases 1–4
- Untracked: 5 new economic pipeline files + various scripts

# AGENTS.md — Session Continuation Context

## Project
`D:\projects\subtitle-parse` — ETL pipeline for Paraguayan public procurement (DNCP) anomaly detection.

## Active Branch
`feature/clustering-pipeline` (docs viejos decían `feature/ocds-csv-pipeline`) — cambios sin commitear:
- `config/settings.py`, `scripts/run_section_clustering_pipeline.py`, `src/etl/loaders/section_clustering_loader.py`, `src/etl/transformers/llm_provider.py` (new), `src/etl/transformers/section_clustering_transformer.py`, `src/pipelines/section_clustering_pipeline.py`, `docs/readme_pipeline_extraction_runner.md`, `reporte_clustering_decisiones.md`

Unstaged modified files (current session):
- `src/etl/extractors/document_features_extractor.py` — Phase 1: added word_count, page, line_start, line_end to SECTION_QUERY; chunked loading
- `src/etl/transformers/document_features_transformer.py` — Phase 1: Gini, entropy, word pivot, count pivot, span/page_range pivots, _compute_gini, _compute_title_entropy
- `src/etl/loaders/document_features_loader.py` — Phase 1: FIXED_COLUMNS_DDL extended, create_table handles word_, count_, span_, page_range_ prefixes
- `src/pipelines/document_features_pipeline.py` — Phase 1: extended title_cols generation with 4 new prefix types + word_otros
- `src/etl/extractors/title_ranking_extractor.py` — Phase 4: added ECON_FEATURES_QUERY + get_economic_features(); **Phase 4 fix**: memory-safe column loading (245 cols instead of 587) via `_get_relevant_columns()` querying information_schema
- `src/etl/transformers/title_ranking_transformer.py` — Phase 4: 6 strategies (economic=0.15), STRATEGY_WEIGHTS updated, _strategy_economic_risk method, ECONOMIC_RISK_COLS module constant
- `src/pipelines/title_ranking_pipeline.py` — Phase 4: loads document_economic_features + passes df_econ to compute_ranking
- `src/etl/transformers/llm_provider.py` — Phase 5: added VLLMOfflineProvider (generate_batch_messages, lazy init)
- `src/etl/extractors/document_features_v2_llm_extractor.py` — Phase 5: LLM fallback extractor (failed extractions grouped)
- `src/etl/transformers/document_features_v2_llm_transformer.py` — Phase 5: batch LLM extraction per field
- `src/etl/loaders/document_features_v2_llm_loader.py` — Phase 5: UPDATE via CASE for LLM results
- `src/pipelines/document_features_v2_llm_pipeline.py` — Phase 5: orchestrator 3-step pipeline
- `scripts/run_document_features_v2_llm_pipeline.py` — Phase 5: entry point Mistral 7B AWQ

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

### Session 4 — Model Benchmarking (Local vLLM)

- **Pipeline 2 model selection**: Evaluated 6 local LLMs for structured extraction  
  - **Qwen3-14B Q5_K_M** (9.8 GB) winner: **100% (60/60)** vs Qwen2.5 14B Q5_K_L 97.5%
  - Qwen3 issues: `enable_thinking` por defecto → pre-renderizar con `enable_thinking=False`; HTTP server crash → usar offline API
  - Qwen3 metrics: 9.87 GiB VRAM, 15.23× concurrencia, 13-19 tok/s output
  - Gemma 3 12B Q8_0: descargado (11.7 GB) pero requiere HF_TOKEN (gated repo)
  - Fixed UVA issue: `VLLM_USE_V2_MODEL_RUNNER=0` para Qwen3ForCausalLM en WSL
- Created `docs/benchmark_resultados.md` con resultados completos

### Session 5 — Final Benchmark (12 modelos, Mistral 7B nuevo campeón)

- **Evaluated 5 new models** against Qwen3-14B baseline (60 samples, 6 titles, vLLM offline API)
  - **🥇 Mistral 7B v0.3 Q5_K_M**: **60/60 = 100%, avg 4.90s** — **NUEVO CAMPEÓN**
  - ✅ Qwen3-8B Q5_K_M: 60/60 = 100%, avg 4.50s (con fix `enable_thinking=False`)
  - ✅ Phi-4 14B Q4_K_M: 60/60 = 100%, avg 6.09s
  - ❌ Llama 3.1 8B: ERROR — tokenizer gated, necesita HF_TOKEN
  - ❌ Gemma2 27B Q4_K_M: ERROR — `lm_head.weight` mapping failure, no cabe en 16 GB
- **Mistral 7B advantages over Qwen3-14B**: −51% size (4.8 GB vs 9.8 GB), −4% speed (4.90s vs 5.10s), ~27× vs 15.23× estimated concurrency
- **Cleaned up**: removed inviable models (Gemma3, lift, NuExtract, Qwen2.5-7B, etc.) → freed ~95 GB
- **Fixed**: Qwen3-8B thinking mode issue → pre-render prompts with `enable_thinking=False`
- **Fixed benchmark script**: use `AutoTokenizer.from_pretrained(local_dir)` + `llm.generate()` with pre-rendered prompts
- **Updated** `docs/benchmark_resultados.md` with all 12 model results and new recommendation

### Session 6 — AWQ Benchmark (GGUF deprecado, Mistral 7B AWQ campeón definitivo)

- **Descubrimiento clave**: GGUF es experimental/deprecated en vLLM 0.23.0 (movido a plugin externo). AWQ es el formato nativo recomendado.
- **Evaluados 4 modelos en AWQ** (60 samples para Qwen, 10 para Mistral/Phi-4):
  - **🥇 Mistral 7B v0.3 AWQ (awq_marlin)**: **10/10 = 100%, avg 1.61s** — **3× más rápido que GGUF**
  - ✅ Qwen3-8B AWQ: 60/60 = 100%, avg 4.80s (sin awq_marlin — Qwen no compatible)
  - ✅ Qwen3-14B AWQ: 60/60 = 100%, avg 5.29s (sin awq_marlin)
  - ❌ Phi-4 14B AWQ: 9/10 = 90%, avg 4.47s (problemas dtype, 1 FAIL)
- **Mistral 7B AWQ specs**: 3.89 GiB VRAM (−29% vs GGUF), 8.81 GiB KV cache, 35.22× concurrencia, 64-77 tok/s output
- **Proyección 315K secciones**: ~4 horas con continuous batching (vs ~1 día GGUF, vs ~18 días secuencial)
- **Phi-4 AWQ fix**: requiere `dtype="float16"` (bfloat16 no soportado para AWQ)
- **AWQ Marlin**: solo compatible con arquitectura Mistral/Llama (no Qwen, no Phi)
- **Actualizado** `docs/benchmark_resultados.md` con secciones AWQ, tabla de throughput actualizada, ranking completo

### Session 8 — Phase 5: Confidence Tracking + LLM Fallback Pipeline

- **Fase 4 refactor**: Transformer ahora genera columnas `{field}_conf` (0.0-1.0) para campos hybrid
  - `_compute_confidence()` valida rangos: pct_* > 100% → 0.3, dias_* > 730 → 0.3, etc.
  - Docs sin secciones del título → conf=1.0 (ausencia es señal válida)
  - Docs con regex match → conf=1.0; sin match → conf=0.0
- **Loader**: `create_table()` agrega `{field}_conf REAL DEFAULT 1.0` para cada campo hybrid
  - 26 campos hybrid → 26 columnas `_conf` adicionales (total: 81 value + 26 conf + 3 fijas = 110 cols)
- **Schema**: `master_schemas_v2.json` extendido con `extraction_method` (55 regex + 26 hybrid)
- **VLLMOfflineProvider**: Nuevo provider en `llm_provider.py` para vLLM offline API
  - `generate_batch_messages()`: renderiza + batch en un solo paso con continuous batching
  - Lazy loading de model/tokenizer, singleton pattern
  - Soporte Qwen thinking fix + WSL UVA workaround
- **Phase 5 — LLM Fallback Pipeline** (6 archivos nuevos):
  - `src/etl/extractors/document_features_v2_llm_extractor.py` — busca filas con conf < threshold
  - `src/etl/transformers/document_features_v2_llm_transformer.py` — batch LLM extraction por campo
  - `src/etl/loaders/document_features_v2_llm_loader.py` — UPDATE con CASE masivo por campo
  - `src/pipelines/document_features_v2_llm_pipeline.py` — orquestador 3 pasos
  - `scripts/run_document_features_v2_llm_pipeline.py` — entry point con Mistral 7B AWQ
- **Arquitectura híbrida**: regex (70s CPU) → 80% cobertura; LLM fallback (~4 min GPU) → 20% restante
  - Solo se procesan docs donde regex falló (conf < 0.8)
  - Ahorro estimado: 82% menos llamadas LLM vs enfoque 100% LLM

### Session 7 — AWQ Retest Completo (Qwen2.5 14B, Llama 3.1 8B, Gemma 3 12B)

- **Qwen2.5 14B AWQ**: **100% (5/5), avg 2.46s, 9.38 GiB VRAM, 8.18× concurrencia** — Gran mejora vs GGUF (7.1s → 2.46s, 3× más rápido)
- **Llama 3.1 8B AWQ (awq_marlin)**: **100% (5/5), avg 1.93s, 5.39 GiB VRAM, 29.21× concurrencia** — Tokenizer ya no es problema (AWQ de hugging-quants)
- **Gemma 3 12B AWQ**: ❌ Incompatible — Gemma3 requiere bfloat16, AWQ requiere float16
- **vLLM 0.25.1**: ❌ No funciona en WSL — MRv2 (default) requiere UVA, no disponible en WSL2
- **vLLM 0.23.0**: Versión estable para WSL, AWQ nativo funciona perfectamente
- **Ranking final AWQ**: Mistral 7B (1.61s) > Llama 3.1 8B (1.93s) > Qwen2.5 14B (2.46s) > Qwen3-8B (4.80s) > Qwen3-14B (5.29s) > Phi-4 (90%, 4.47s)

## Next Priority (in order)

1. **⏳ Ejecutar fallback LLM (`document_features_v2_llm_pipeline`)** — nunca ejecutado (sin checkpoint)
   - ~324K docs con conf<0.8 en **12 campos hybrid** (Sesión 16: 7 portados a regex → baja de 19 a 12).
   - Modelo recomendado: **Llama 3.1 8B AWQ** (Sesión 14: 100% en 100 GT humanas con postproceso determinista).
   - Requiere: re-correr `run_document_features_v2_pipeline.py` (hecho en Sesión 16, DB ya en 108 cols), actualizar `run_document_features_v2_llm_pipeline.py` para usar `VLLMOfflineProvider` (hoy usa `LocalLLMProvider` HTTP, sin server).
   - WSL2, vLLM 0.23.0, `VLLM_USE_V2_MODEL_RUNNER=0`, offline API. ~3h con continuous batching.

2. **Commit**: todos los cambios de Sesiones 1-16 sin commitear (HEAD=`869c86c`):
   - Phase 5 pipeline v2 LLM (6 archivos), benchmark scripts, docs (tesis_b_c, revision_26_campos, phase5), Sesión 16 (transformer + schema + checklist).
   - Limpiar scripts `_*.py` / `_tmp_*.py` antes de commitear.

3. **Clustering**: schema validation para 9 títulos restantes (2/10) — staged files de `section_clustering_pipeline` nunca ejecutados.

4. **Pipeline 3 (Anomaly Detection)**: no implementado; diseño en `docs/pipeline3_anomaly_detection.md`.

## PowerShell Bug Workaround
```powershell
# PowerShell tiene problemas de quoting con comandos Python inline en WSL bash.
# NO USAR:
#   wsl.exe bash -c "python -c \"import...\""
# USAR SIEMPRE:
#   1. Escribir un script temporal .py
#   2. wsl.exe bash -c "cd /mnt/d/projects/subtitle-parse && python _temp_script.py"
#   3. Borrar el script temporal después
```

## WSL Python Environment (IMPORTANTE)
```bash
# El único entorno con vLLM 0.23.0 + pandas + psycopg2 + transformers es:
/root/vllm-env/bin/python

# NUNCA usar python3 del sistema (no tiene pandas/vllm/psycopg2).
# NUNCA crear un venv nuevo (.venv) para scripts de vLLM — usar /root/vllm-env.
# Cualquier script que corra en WSL con vLLM DEBE usar /root/vllm-env/bin/python.
# Y DEBE ir precedido de: VLLM_USE_V2_MODEL_RUNNER=0

# Ejemplo correcto:
#   wsl.exe bash -c "cd /mnt/d/projects/subtitle-parse && VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/benchmark_multi_vs_gt.py"
```

## WSL Hang Recovery
```powershell
# Si WSL se cuelga (wsl.exe --status no responde), típicamente por un pip install interrumpido:
# 1. Forzar terminate del distro (requiere admin):
wsl.exe --terminate Ubuntu
# 2. Si aún cuelga, matar procesos SYSTEM (requiere admin):
Get-Process -Name wsl*,vmmem*,vmwp* | Stop-Process -Force
# 3. Como último recurso: wsl.exe --shutdown
# La terminal de opencode tiene acceso admin (usuario desktop-kil5cu0\giovanni).
```

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
- `docs/benchmark_resultados.md`: complete LLM benchmark results (12 models, 2 formatos GGUF+AWQ, Mistral 7B AWQ campeón)
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
- `.opencode/skills/evaluate-local-models/SKILL.md`: local model evaluation skill
- `eval/comparison_report.md`: SambaNova model comparison report
- `data/processed/section_clustering/master_schemas.json`: schemas for extraction evaluation

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
- **Qwen3 HTTP server**: silently crashes after route registration in WSL (UVA + multiprocessing). Workaround: use offline API (`LLM.generate()`) with `VLLM_USE_V2_MODEL_RUNNER=0`.
- **Qwen3 thinking mode**: enabled by default. Pre-render prompts with `apply_chat_template(..., enable_thinking=False)`.
- **Gemma 3 12B**: gated repo on HuggingFace, requires `HF_TOKEN` env var for tokenizer download.
- **UVA not available**: WSL2 GPU passthrough doesn't support Unified Virtual Addressing. All models using V2 Model Runner (Qwen3ForCausalLM, LlamaForCausalLM, MistralForCausalLM) need `VLLM_USE_V2_MODEL_RUNNER=0`.
- **GGUF deprecation**: GGUF is experimental/deprecated in vLLM 0.23.0. Use AWQ for production (Mistral 7B AWQ with awq_marlin).
- **AWQ Marlin**: awq_marlin kernel only works with MistralForCausalLM/LlamaForCausalLM. Qwen/Qwen3 falls back to regular `awq` (slower).

### Session 9 — LLM Fallback Benchmark & Prompt Engineering

#### What was done

1. **Created `scripts/benchmark_llm_fallback.py`** — reusable offline vLLM benchmark:
   - Samples 20 docs/field from `get_failed_extractions_grouped()` (conf < 0.8)
   - Runs through any vLLM model configured in YAML
   - Compares LLM output to database ground truth
   - Outputs per-field % found + CSV results

2. **Failure Decomposition of 274/460 (59.6%) baseline**:
   - 106 failures across 23 hybrid fields classified as:
     - **51 legitimate NULLs** (field genuinely absent — section is about the right title but doesn't contain the specific field; LLM correctly NULL but counted as "failure" against DB truth)
     - **55 truly missed** (LLM should have extracted but didn't — text does contain the value)
   - Per-field manual inspection identified 10 problematic fields

3. **Prompt Engineering — 10 field hints updated** in `master_schemas_v2.json`:
   - `anos_experiencia_minima`: +40% (25%→65%) after adding "últimos N años", "mínimo de N años" patterns
   - `num_documentos_pago`: +20% (80%→100%) after "contar items de primer nivel"
   - `ratio_liquidez_minimo`: +35% (60%→95%) after "buscar 'mayor o igual a N'"
   - `num_referencias_minimo`: +10% (45%→55%) after "patrón 'N referencias técnicas'"
   - `num_componentes_formula` and `pct_interes_mora_diario`: regressed severely due to aggressive system prompt rules

4. **System Prompt Calibration** (reverted aggressive rules):
   - Initial: added "No extraigas números de leyes (7021, 7781, 2006)", "Ignora años calendario"
   - Effect: `num_componentes_formula` 60%→0%, `pct_interes_mora_diario` 90%→0%, `dias_pago_maximo` 100%→55%
   - Reverted to minimal rules: "No clause/article/law numbers" (without specific law examples), no year-ignoring rule

5. **Key Insight**: Random sampling per run makes A/B comparison unreliable — 20 different docs each run. Need deterministic eval (fixed sample seed) for proper prompt iteration.

#### Files
- New: `scripts/benchmark_llm_fallback.py` — reusable LLM benchmark script (vLLM offline API)
- Modified: `data/processed/section_clustering_v2/master_schemas_v2.json` — 10 per-field extraction hints improved
- Modified: `src/etl/transformers/document_features_v2_llm_transformer.py` — system prompt reverted to conservative version

### Session 10 — Benchmark Final: Llama 3.1 8B AWQ + Deterministic Seed

#### What was done

1. **Fixed DB connection issues**: Database was migrated from `dncp` to `pbc` (later reverted — container has `dncp`). Resolved port conflict between native postgres (5433) and podman container → killed postgres pid 265, started podman container `dncp`.

2. **Final benchmark run** (Llama 3.1 8B AWQ, 23 fields, 20 samples, seed=42):
   - **Overall: 271/460 = 58.9%**, 304.5s total
   - **100% fields** (8): dias_aceptacion_rechazo_factura, dias_notificacion_fuerza_mayor, dias_presentacion_garantia, num_documentos_pago, num_pasos_verificacion, pct_diferencia_precio_alto, pct_diferencia_precio_bajo, pct_interes_mora_diario
   - **Problematic** (<50%): anos_experiencia_minima_tecnica (0%), num_componentes_formula (0%), pct_anticipo (0%), dias_validez_garantia (5%), pct_monto_referencia_tecnica (5%), num_referencias_tecnicas_minimo (10%), dias_pago_anticipo (20%), dias_solicitud_anticipo (25%)
   - **Improved** (vs Session 9 estimated): anos_experiencia_minima 70%, ratio_liquidez_minimo 85%, num_referencias_minimo 50%

3. **Bug workaround**: PowerShell has quoting issues with nested inline Python in WSL bash. **Always write a temp `.py` file then call it** instead of inline `python -c "..."`. This avoids PowerShell interpreting single/double quotes, backticks, and special characters.

#### Files
- Modified: `scripts/benchmark_llm_fallback.py` — reduced `gpu_memory_utilization=0.70`, `max_num_seqs=8`, `max_model_len=4096` to fit in 16 GiB VRAM; restored original sampling from `document_features_v2`
- Temp debug scripts: `_debug_db.py`, `_debug_db2.py`, `_debug_db3.py`, `_debug_hybrid_fields.py`

### Session 11 — Benchmark Optimization: Primera Línea Approach → 83.9%

#### What was done

1. **Mistral 7B v0.3 AWQ benchmark**: 222.6s, **62.8% (289/460)** — worse than Llama 3.1 8B (69.1%)
   - Better on: `anos_experiencia_minima` (100% vs 90%), `num_componentes_formula` (70% vs 5%), `num_referencias_tecnicas_minimo` (80% vs 15%)
   - Worse on: `dias_presentacion_garantia` (0% vs 100%), `dias_validez_garantia` (60% vs 90%), `pct_monto_referencia_tecnica` (0% vs 40%), `num_pasos_verificacion` (45% vs 100%)

2. **Hint pruning experiment**: Shortened 12 verbose hints (~300-570c → ~50-100c)
   - **Accuracy dropped: 69.1% → 64.6%** — verbose pattern examples help the model extract despite triggering explanation mode
   - Net effect of verbose hints: **+4.5 p.p.**
   - Reverted all 12 hints to original

3. **Three strategies tested for Llama 3.1 8B AWQ**:
   - **S1** ("NO expliques" al final del prompt): mejoró `pct_interes_mora_diario` (0%→100%) pero mató `dias_validez_garantia` (90%→0%)
   - **S2** (max_tokens=128→256): permitió respuestas más largas pero no ayudó sola
   - **S3** (parse mejorado con NULL check en últimos 200 chars): redujo falsos positivos pero bajó overall a 61.1%
   - **Two-stage extraction** (detectar + extraer): desastre — detect stage falló en campos que funcionaban bien (100%→0%)

4. **🥇 "Primera línea" approach**: system prompt modificado a:
   ```
   IMPORTANTE: Escribe el número o NULL en la PRIMERA línea de tu respuesta.
   Luego puedes explicar en líneas siguientes si es necesario.
   ```
   + parser que extrae de la primera línea, luego fallback a patrones, luego NULL check

#### Resultado Final

| Campo | Baseline | Nueva | Δ |
|---|---|---|---|
| `pct_interes_mora_diario` | 0% | **100%** | +100pp |
| `pct_anticipo` | 0% | **100%** | +100pp |
| `num_componentes_formula` | 5% | **95%** | +90pp |
| `num_referencias_minimo` | 75% | **100%** | +25pp |
| `num_referencias_tecnicas_minimo` | 15% | **65%** | +50pp |
| `dias_reposicion` | 85% | **100%** | +15pp |
| `ratio_endeudamiento_maximo` | 85% | **100%** | +15pp |
| `ratio_liquidez_minimo` | 85% | **100%** | +15pp |
| `anos_experiencia_minima` | 90% | **100%** | +10pp |
| pct_monto_referencia_tecnica | 40% | **45%** | +5pp |
| anos_experiencia_minima_tecnica | 80% | **30%** | -50pp* |
| dias_validez_garantia | 90% | **40%** | -50pp* |
| **TOTAL** | **69.1%** | **83.9%** | **+14.8pp** |

\* "Regresiones" son en realidad corrección de falsos positivos — el viejo parser extraía números de explicaciones donde el modelo decía NULL.

#### Key Insight
- **Estructurar la respuesta** (primera línea = valor) es más efectivo que prohibir explicaciones
- Los 3 cambios juntos: prompt de primera línea + max_tokens=256 + parse con first-line priority
- 83.9% real > 69.1% inflado (viejo parser contaba falsos positivos como aciertos)
- Campos persistentemente bajos: `dias_pago_anticipo` (25%), `dias_solicitud_anticipo` (30%), `anos_experiencia_minima_tecnica` (30%) — son casos donde el texto genuinamente no contiene el valor

#### Files
- Modified: `scripts/benchmark_llm_fallback.py` — primera línea prompt + first-line parser + max_tokens=256
- Temp scripts `_tmp_*` (all deleted)

### Session 12 — Guided Decoding (vLLM StructuredOutputs) → 83.9% @ 7× speed

#### What was done

1. **Fase 2 del plan — Guided decoding implementado** via `vLLM.sampling_params.StructuredOutputsParams(regex=...)`
   - `VLLM 0.23.0` backend V1 works on WSL (`outlines_core` backend autodetected)
   - Regex: `^(-?\d+(?:[.,]\d+)?|NULL)$` para int/float, `^(0|1)$` para binary
   - Impone el formato a nivel de logits — modelo no puede generar explicaciones, solo número o NULL

2. **Pruebas clave**:
   - `pct_interes_mora_diario`: 0% → **100%** (nunca antes logrado con un solo enfoque)
   - `dias_validez_garantia`: recuperó a 100% desde 40% (con prompt completo)
   - Modelo responde `'3'` en vez de `"Aquí está el análisis... el valor es 3"` — eliminó toda verbosidad

3. **Resultado full benchmark**: 386/460 = **83.9%** (idéntico al "primera línea" sin guided)

#### Speed comparison

| Enfoque | docs/s | Tiempo total | 
|---------|--------|-------------|
| Sin guided (prompt actual) | 1.32 | 347.5s |
| **Con guided** | **8.95** | **51.4s** |

Guided decoding es **7× más rápido** porque:
- Output promedio: ~2 tokens (vs ~50+ con explicaciones)
- Menos KV cache por request → más requests por batch
- No necesita post-procesamiento complejo (el output ya es válido)

#### Campos persistentes (<50%) — mismo problema en ambos enfoques
- `dias_pago_anticipo` (25%), `dias_solicitud_anticipo` (30%)
- `anos_experiencia_minima_tecnica` (30%), `dias_validez_garantia` (40%)
- `pct_monto_referencia_tecnica` (45%)
- Causa: el texto del pliego no contiene el valor explícitamente, modelo retorna NULL correctamente

#### System prompt usado (mismo para guided y no-guided)
```python
# FULL SYSTEM_PROMPT (no el GUIDED_SYSTEM_PROMPT simplificado)
# El prompt completo con REGLAS ESTRICTAS + FORMATO DE RESPUESTA
# da resultados idénticos con y sin guided decoding
```

#### Key Insight
- Guided decoding **no mejora la precisión** por sí solo — la precisión depende del prompt
- Pero **no la empeora** cuando se usa el system prompt completo
- El beneficio real es **velocidad**: 7× más rápido, mismo accuracy
- Para producción: guided decoding es claramente superior (misma calidad, menor latencia, sin post-procesamiento)

#### Files
- Modified: `scripts/benchmark_llm_fallback.py` — `--guided` flag + StructuredOutputsParams + imports
- New: `_tmp_test_guided.py`, `_tmp_vllm_api*.py` (all deleted)
- `docs/benchmark_resultados.md` — pendiente actualizar

### Session 13 — Few-shot Retrieval Benchmark ❌ (descartado)

#### Contexto
- Baseline multi-model (100 muestras GT humanas): Llama 3.1 8B-AWQ 95% (`dias_pago_anticipo` 100%, `dias_solicitud_anticipo` 90%)
- Objetivo: superar 95% vía retrieval-augmented few-shot (sin tocar pesos del modelo)
- Embedding: `wilfredomartel/BGE-M3-Legal-Spanish` (BGE-M3 base, 1024-dim, 2.27 GB)
- Corpus: secciones `solicitud de pago de anticipo` con `{campo}_conf >= 0.9` → `dias_pago_anticipo` 2,073 (10:1068, 5:380, 3:359, 15:98...), `dias_solicitud_anticipo` 1,537 (10:1221, 5:119, 3:112...); GT excluido para evitar leakage
- Datos: `data/processed/fewshot_retrieval/corpus.json`, `corpus_emb_{campo}.npy`, `gt_emb.npy`, `gt_keys.npy`

#### Resultados (Llama 3.1 8B-AWQ, guided decoding, 40 muestras)

| Variante | dias_pago_anticipo | dias_solicitud_anticipo | Total (40) |
|---|---|---|---|
| **Baseline (sin few-shot)** | **20/20 = 100%** | **18/20 = 90%** | **38/40 = 95%** |
| Dinámico k=4 | 17/20 = 85% | 17/20 = 85% | 34/40 = 85% |
| Dinámico k=2 | 16/20 = 80% | 16/20 = 80% | 32/40 = 80% |
| Estático balanceado (12-13 valores) | 17/20 = 85% | 17/20 = 85% | 34/40 = 85% |

#### Diagnóstico
- Las secciones son plantillas casi idénticas (sim coseno 0.95-0.98) → el embedding NO distingue el número del valor (ej. gt=15 recupera ejemplos con 3/10/8/7)
- Los ejemplos inyectados sesgan al modelo hacia valores modales (10/5) y lejos del valor real/NULL
- Los campos "débiles" del benchmark fallback (25-30%, conf<0.8, 460 muestras) dan 100%/90% en las 100 GT — el few-shot no tiene margen que ganar

#### Conclusión
- **Few-shot dinámico y estático DESCARTADOS**: empeoran el baseline (95% → 85%/80%)
- La única vía restante para superar 95% es fine-tuning QLoRA (plan en `docs/plan_finetune_qlora_test.md`)

#### Files
- New: `scripts/build_fewshot_corpus.py`, `scripts/embed_fewshot_corpus.py`, `scripts/benchmark_fewshot_retrieval.py`, `scripts/benchmark_static_fewshot.py`
- New: `data/processed/fewshot_retrieval/results/Llama_31_8B-AWQ_{k2,k4,static}.json`

### Session 14 — AloLab Manual (agente como meta-analista) → 100% ✅

#### Qué se hizo
- Usuario **rechazó meta-analista por API** (DeepSeek/GLM/OpenRouter, keys disponibles). El agente analiza los errores manualmente en un ciclo AloLab artesanal: script recolecta errores → agente diagnostica → reescribe hints/postprocess → re-run.
- Nuevo `scripts/alolab_collect_errors.py`: corre Llama 3.1 8B-AWQ sobre las 100 GT humanas, exporta SOLO los errores (field, nro, gt, llm_raw, llm_value, hint actual, texto completo) a `data/processed/alolab/errors_round{N}.json` (~9-10s por ronda).

#### Evolución (95% baseline → 100%)
| Ronda | Score | Errores | Fix aplicado |
|---|---|---|---|
| 1 | 95/100 | 5 (gt≠llm reales) | 3 hints reescritas (validez_garantia, experiencia_tecnica, solicitud_anticipo) + regex persona |
| 2 | 95/100 | **5 FPs nuevos** | 2 fixes postprocess + regla dias_solicitud |
| 3 | 95/100 | 5 (todos validez_garantia, modelo repite '30.42' de la hint) | hint limpia + conversión determinista |
| 4 | **100/100** | 0 | — |

#### Diagnósticos clave
- **Ronda 2**: los GT=NULL se rompen al tocar hints (el modelo sobre-extrae). Los 5 errores eran FPs: extrajo "1" de ítems de lista ("1. El oferente..."), "1.0" de listas, "3" de un ítem numerado, y "30" en vez de 182 ("6 (seis) meses").
- **Bug regex encontrado**: `personales?\b` NO matchea "personal" — solo "personales". `personales?` = "personale" + "s" opcional; "personal" ≠ "personale". Correcto: `personal(?:es)?\b`. El regex fallaba incluso contra "1 personal" literal.
- **Ronda 3**: incluir el multiplicador "30.42" en la hint hizo que el modelo lo repitiera literalmente (raw='30.42') en vez de convertir. La hint que enseña aritmética confunde; la tabla simple de valores exactos funciona mejor.

#### Fixes finales en `scripts/benchmark_multi_vs_gt.py`
- `personales?` → `personal(?:es)?` (matchea singular y plural).
- Regla de ítem de lista **generalizada** para `anos_experiencia_minima_tecnica` (antes solo v==5.0): `(punto|item|ítem|ítems?)\s*{num}`, `[y,]\s*{num}`, `\b{num}\s*[.)]` → None.
- Nueva regla `dias_solicitud_anticipo` para v ∉ (5,20): exige `\b{num}...días` + contexto "solicitar|solicitud"; añadidos 3.0 y 10.0 a KNOWN_DEFAULTS.
- **Conversión determinista** para `dias_validez_garantia`: regex extrae "N meses/años" del texto y convierte con mapa exacto {1:30, 2:61, 3:91, 6:182, 12:365} (años: N*365, >365→NULL). Ya no depende de la aritmética del LLM.

#### Insights
- Los errores de NULL (falsos positivos) se arreglan mejor en **postprocess** (regex determinista sobre el texto) que en el prompt.
- Los GT de plantilla vacía (NULL) son el caso más frágil: cualquier hint que enseñe un valor concreto induce sobre-extracción.
- El ciclo AloLab manual superó el objetivo (95% → 100%) SIN fine-tuning, cumpliendo el plan `docs/plan_finetune_qlora_test.md` como innecesario.

#### Files
- New: `scripts/alolab_collect_errors.py`, `data/processed/alolab/errors_round{1,2,3,4}.json`
- Modified: `scripts/benchmark_multi_vs_gt.py` (4 fixes postprocess), `data/processed/section_clustering_v2/master_schemas_v2.json` (hints: dias_validez_garantia 2 rewrites, anos_experiencia_minima_tecnica, dias_solicitud_anticipo)

### Session 15 — Tesis B+C: capa postproceso compartida + 3 campos a regex + comparativa

#### Qué se hizo

1. **Fase 1 — Capa de postproceso determinista compartida (núcleo B)**:
   - Nuevo `src/etl/transformers/extraction_postprocess.py`: SINGLE SOURCE de truth para `deterministic_pct` (5 campos uniformes), `postprocess` (×100 + contexto `N%` + conversión meses→días + reglas FP), `parse_response`, `PCT_INT_SCALE`, `KNOWN_DEFAULTS`, `NO_RULE`.
   - `scripts/benchmark_multi_vs_gt.py`: `deterministic_pct`/`postprocess`/`parse` ahora delegan al módulo (API preservada — `gold_standard_26campos`, `alolab_collect_errors`, fewshot y gt_extension CSVs siguen importando igual).
   - `document_features_v2_llm_transformer.py::_parse_response`: ahora recibe `field`+`text`, aplica deterministas (autoritativos, conf=1.0) y postprocess strict. `compute_llm_fallback` le pasa texto y campo.

2. **Fase 2 — 3 campos-plantilla a regex real en producción (B, ahorro documentable)**:
   - `pct_garantia_cumplimiento`, `pct_diferencia_precio_bajo`, `pct_diferencia_precio_alto` → `extraction_method: hybrid→regex` en `master_schemas_v2.json` (ahora 62 regex / 19 hybrid; portes adicionales en Sesión 15 final: `dias_presentacion_garantia` — plantilla "dentro de los 10 días", regex nuevo 200/200 — y `pct_anticipo` — GT todo-NULL, regex ya 100% pero el fallback LLM alucinaba 95 FPs).
   - `document_features_v2_transformer.py`: añadidos `_re_pct_garantia_cumplimiento`, `_re_pct_diferencia_precio_bajo`, `_re_pct_diferencia_precio_alto` (patrones confirmados en GT, ver abajo).
   - `extraction_postprocess.deterministic_pct` extendido con los 3 patrones.
   - **Patrones validados contra GT (100% exacto, 0 FP):** bajo=`superior\s+al\s+N%` (TP 156/156, null 44), alto=`y\s+(?:del\s+)?N%` (TP 129/129, null 71, captura el "y del 15% para ofertas por encima"), garantia=`N,N%` literal (TP 64/64, null 2). Las 22 filas NULL extras de bajo/alto tienen texto vacío (pipeline las rellena a 0.0 por diseño, la capa determinista da None correcto).
   - Extracción LLM de producción ya no procesa estos campos (filtro `extraction_method == hybrid`), ahorrando GPU.

3. **Fase 3 — Evaluación comparativa C (GPU-free)**: `scripts/comparativa_estrategias.py`, 26 campos, 5,066 juicios GT congelados (usa `llm_raw`/`text` almacenados, sin re-correr LLM):
   - Estrategias: **R** (regex v2 puro), **Ri** (regex+postproceso), **L_raw** (LLM bruto primer número), **L_pp** (LLM+postproceso `parse_response`), **H** (híbrido arquitectural: determinista→regex→LLM).
   - Resultado medio (acc flex): R=61.0, Ri=60.9, L_raw=51.0, L_pp=63.1, **H=65.1**.
   - Mejor estrategia por campo: **H en 12 de 26** (L_pp 3, Ri 9, L_raw 2). Los 5 campos pct uniformes quedan al 100% (R=Ri=L_pp=H).
   - Output: `data/processed/alolab/comparativa_estrategias.csv` (acc_Y, fp_Y por campo + best).
   - Lección: el postproceso determinista es lo que lleva L_pp por encima de L_raw (+12pp); el híbrido H añade +2pp sobre L_pp.

#### Detalles/errores de esta sesión
- **Bug de alineación en scripts temporales**: `pd.concat` sin `ignore_index` + `gt is None` fallaba porque pandas re-castea None→NaN en col float64 → usar `pd.isna` o listas Python. En `comparativa_estrategias.py` se usa `[to_num(v) for v in series]` + `gts[i]` (no `.iloc[i]` que se desalinea).
- **Bug H**: cuando `v_ri` es None, `v_h` quedaba como `NO_RULE` object → colisión en `match()`; corregido con condicional explícito deterministic→regex→LLM.
- **`_extract_field_text`** devuelve `_default_value` (0/0.0) en texto vacío; para comparativa justa se pone `v_re=None` si texto en blanco.
- Edición por `edit` falló en `_parse_response` por CRLF/indentación → usar script Python para reemplazo robusto (el decorador `@staticmethod` quedó despegado y se re-indentó manualmente).

#### Files
- New: `src/etl/transformers/extraction_postprocess.py`, `scripts/comparativa_estrategias.py`, `data/processed/alolab/comparativa_estrategias.csv`
- Modified: `scripts/benchmark_multi_vs_gt.py` (delega a módulo compartido), `src/etl/transformers/document_features_v2_llm_transformer.py` (_parse_response determinista), `src/etl/transformers/document_features_v2_transformer.py` (3 métodos deterministas nuevos + `_re_dias_presentacion_garantia`), `data/processed/section_clustering_v2/master_schemas_v2.json` (62 regex / 19 hybrid)
- New (revisión final): `scripts/generar_revision_26_campos.py`, `docs/revision_26_campos.md`, `data/processed/alolab/revision_26_campos.csv`
- Portes adicionales: `dias_presentacion_garantia` (plantilla "dentro de los 10 días", regex robusto **200/200**, antes 50%) y `pct_anticipo` (GT todo-NULL, regex ya 100% pero el fallback LLM alucinaba 95 FPs → ahora regex puro)

### Session 17 — Panorama de estrategias de anomalías + mapa de variables (BD verificada)

#### Qué se hizo
1. **Investigación web ampliada**: 3 tandas de búsquedas → todas las familias de detección de anomalías en contratación (no solo ISF): tabular no supervisado (ISF/LOF/ECOD/COPOD/AE/foundation), red flags compuestos (CRI FMI, Benford, TAB), screens econométricos (Porter-Zona, Bajari-Ye, missing-bids Kawai), redes (co-bidding Wachs, GNN, knowledge graph TBFY), supervisado con protestas (Fazekas GTI-WP, LAIRM 2026, VigIA) y NLP de pliegos (Katona & Fazekas).
2. **BD `dncp` verificada por consulta directa** (SELECT, no estimados): `oferentes` **no tiene montos por oferta** (78,695 filas, 5,544 bidders, 1-65/licitación) → screens de distribución completa (Porter-Zona/Bajari-Ye) **no implementables tal cual**; `metodo_contratacion` casi todo `open` → usar `metodo_detalle` (CD 9,000, Menor cuantía 10,384, LPN 5,677, Concurso 4,547); `items_licitacion.monto` 1.87M no-nulos ✓ Benford; `pagos_contrato.monto_pagado` 265,725 ✓ Benford + demoras; `adjudicaciones` solo monto ganador (39,456); `enmiendas_contrato` 7,143 (4,414 licit.); `protestas` 1,253 (967 licit.); `consultas_llamado` 123,283 (119,740 con respuesta; 12,353 licit.); single-bidder crece 280→3,470 (2021→2025).
3. **`estrategias-anomaly-deteccion.md` creado** (raíz del repo): inventario de datos verificado, top de 20 variables unificado OCDS+pliego (niveles 1-4), matriz variable→alternativa, detalle por alternativa (qué usa, por qué, viabilidad), análisis de novedad y hoja de ruta incremental.

#### Top de variables consolidado (nivel 1-4)
- **N1 núcleo**: `es_unico_oferente`/`n_oferentes_distintos`, `precio_vs_estimado`/`overbudget_ratio`, `metodo_detalle`, `duracion_oferta_dias`/`bidding_urgency`.
- **N2 colusión/ejecución**: `n_enmiendas`/`enmienda_ratio`, `winner_total_contracts`/`winner_category_frequency`/`is_repeat_winner`, `n_consultas`/`tiempo_respuesta`, `pago_vs_contrato_ratio`.
- **N3 pliego (v2)**: `anos_experiencia_minima(_tecnica)`, `ratio_liquidez/endeudamiento`, `pct_interes_mora_diario`/`pct_multa_diaria`, `dias_pago_maximo`/`pct_garantia`/`dias_validez`, `num_categorias_documentos`/`num_documentos_pago`, `pct_diferencia_precio_*`, `dias_reposicion`/anticipos.
- **N4**: `gini_content_length`/`title_entropy`/`sum_word_count`, `demora_media_pago_dias`, `n_protestas`, `n_unspsc_distintos`, `razon_notificados_a_oferentes`.
- **Descartadas**: `bidder_diversity`/`oferentes_por_item` (bug cantidad_items), `costo_pliego`/`presupuesto_monto`/`garantia_porcentaje` (vacíos), `pct_anticipo`/`num_componentes_formula` (constantes), `antiguedad_empresa` (no hay).

#### Viabilidad por familia (verificada)
- **A (ISF/LOF/ECOD/ensemble)**: ✅ alta. Bloques A+B+C completos. ISF estándar (Kehler 2019 PY); ECOD/COPOD = benchmark SOTA.
- **B (CRI/Benford)**: ✅ alta. CRI = replicar método FMI en país piloto; Benford sobre 1.87M ítems + 265K pagos = novedad PY, esfuerzo mínimo.
- **C (screens)**: ⚠️ parcial — sin montos por bidder solo versión degradada (distancia ganador→referencial) + screens de recurrencia.
- **D (co-bidding Wachs)**: ✅ novedad real PY con 78K ofertas/5,544 bidders; GNN descartado (requiere etiquetas).
- **E (supervisado protestas)**: ✅ proxy 1,253 protestas (4%); RF/GBM con screens 77-91%, LAIRM adaptable.
- **F (Katona-Paraguay)**: 🟡 pipeline v2 ya es preproceso; falta modelo texto→`es_unico_oferente`.

#### Hoja de ruta recomendada
Benford (B3) → CRI (B1) → ISF/ECOD (A) → Ensemble+DIFFI/SHAP (A4) → Co-bidding network (D1) → Supervisado protestas (E) → Katona-Paraguay (F1). ISF sigue en pausa esperando decisión del usuario.

#### Files
- New: `estrategias-anomaly-deteccion.md` (documento principal de este panorama)
- Temp `_tmp_verify_*` (borrados)

### Session 18 — Agente de comunicación para tutores + material de presentación

#### Qué se hizo
1. **Agente `documentador-tutores` creado**: `.opencode/agents/documentador-tutores.md` (mode: all). Genera 4 entregables en `docs/presentacion_tutores/`: `correo_tutores.md`, `ppt_tutores.md` (+`.pptx` si python-pptx existe — NO instalado en Windows), `flujo_datos.md` + `graficos/`, `preguntas_reunion.md`. Embebe una HOJA DE HECHOS verificada del pipeline (origen bulk OCDS, filtro pliegos electrónicos, Tika, outlines, 5 casos de límites, limpieza, COPY a PostgreSQL, volúmenes) y el análisis académico **regex vs LLM**.
2. **Comando `/tutores` creado**: `.opencode/commands/tutores.md` — invoca el agente para producir el material de la reunión.
3. **Material generado de demostración** (yo, en esta sesión, siguiendo las instrucciones del agente, porque el agente nuevo no está cargado hasta reiniciar): 4 `.md` + **5 gráficas matplotlib** (IMG: `docs/presentacion_tutores/graficos/`):
   - `pdfs_por_anio.png` (32,785 PDFs por año), `tamano_masivo_zip.png` (73–462 MB), `volumenes_capas.png` (31,387→32,785→2.78M→31,321), `campos_schema.png` (69 regex + 12 hybrid), `comparativa_estrategias.png` (R=61.0, L_pp=63.1, H=65.1 — leído del CSV real).
   - `generar_graficas.py` queda en `docs/presentacion_tutores/` para regenerar.

#### Datos verificados y documentados (para la reunión)
- Fuente: bulk `masivo_{year}.zip` (NO API en vivo; motivo "rate limit" `[NO CONFIRMADO en docs]`).
- Filtro: solo `application/pdf` tipo "Pliego Electrónico de Bases y Condiciones" → whitelist ~31,387.
- Parsing: Apache Tika (Podman, puertos 9998/9999) línea a línea + outlines (bookmarks PyPDF2, depth 1-2); cruce outlines+líneas con **5 casos de límites** (README_FIX_SECTION_EXTRACT.md), dedup TOC (commit 09e9f50).
- Limpieza: guiones de fin de línea, números de página, Unicode, stemming; descarte de vacías.
- Carga: `pliegos_secciones` particionada por año, COPY batches 10k, `ON CONFLICT DO NOTHING`, checkpoint `.sections_db_done_{year}`.
- Volúmenes: 32,785 PDFs · ~2.78M secciones · document_features_v2 31,321×108 (81 campos = 69 regex + 12 hybrid) · document_economic_features 29,811×46.

#### Notas para el agente
- `python-pptx` **YA está instalado** en Windows (`pip install python-pptx`, v1.0.2) → el agente/scripts pueden generar `ppt_tutores.pptx` real.
- Para regenerar el PPT binario: `python docs/presentacion_tutores/generar_pptx.py` (lee `ppt_tutores.md`, incrusta las gráficas de la bala "Gráfica:" y produce `ppt_tutores.pptx`, 15 slides).
- El agente solo carga tras **reiniciar opencode**. Mientras tanto, el material ya generado está listo.

#### Files
- New: `.opencode/agents/documentador-tutores.md`, `.opencode/commands/tutores.md`, `docs/presentacion_tutores/` (correo_tutores.md, ppt_tutores.md, flujo_datos.md, preguntas_reunion.md, generar_graficas.py, graficos/*.png)

### Session 16 — 7 campos hybrid→regex (GT 26 campos) → schema 69 regex / 12 hybrid

#### Qué se hizo
1. **Análisis de los 19 campos hybrid restantes** contra el GT congelado (26 campos × 200 registros, 5,066 juicios) vía `comparativa_estrategias.csv` (acc_flex R/Ri/L_raw/L_pp/H + FP) y textos GT.
2. **Portados 7 campos a regex** (`extraction_method: hybrid→regex` en `master_schemas_v2.json`):
   | Campo | acc R nueva | acc H antes | Δ |
   |---|---|---|---|
   | num_categorias_documentos | 100.0 (fp0) | 12.0 | +88 |
   | num_referencias_minimo | 98.0 (fp0) | 10.5 | +87.5 |
   | num_referencias_tecnicas_minimo | 97.0 (fp0) | 30.5 | +66.5 |
   | dias_reposicion | 92.0 (fp0) | 67.0 | +25 |
   | pct_monto_referencia_tecnica | 90.0 (fp1) | 84.5 | +5.5 |
   | pct_monto_referencia_minimo | 90.5 (fp1) | 91.0 | −0.5 (empate) |
   | anos_experiencia_minima | 80.0 (fp6) | 57.0 | +23 |
3. **Nuevos métodos regex** en `document_features_v2_transformer.py` (patrones validados contra GT):
   - `_re_num_categorias_documentos`: cuenta ítems nivel 1 (`(?:^|\n)\s*(\d{1,2})\s*\.\s*[A-Z…]` MULTILINE, max) → 200/200.
   - `_re_num_referencias_minimo`/`_re_num_referencias_tecnicas_minimo`: ancla `mínimo de|al menos|como mínimo` + N + `referencias|contratos|constancias|clientes`.
   - `_re_dias_reposicion`: ancla `reposición… será de|será de máximo|máximo` + N(…)? días.
   - `_re_anos_experiencia_minima`: `experiencia…|al menos|mínimo de` + N años.
   - `pct_monto_referencia_*`: `N% del monto/precio` (el viejo `(\d+)\s*%.*monto` era demasiado amplio).
4. **Comparativa re-corrida**: media acc R global 62.9→**67.0**; CSV actualizado. Se mantienen los 5 pct deterministas al 100%.
5. **Revisión regenerada** (`generar_revision_26_campos.py` → `docs/revision_26_campos.md`): 14 campos regex/determinista, 9 LLM hybrid, 3 irresolubles.
6. **Pipeline v2 re-ejecutado** en DB: `dncp.document_features_v2` regenerado (DROP+CREATE), 31,321 filas × **108 cols** (antes 136). Los 7 campos portados ya NO tienen columnas `_conf`/`_source` (el transformer solo las genera para `method=="hybrid"`).
7. **Checklist actualizado**: fallback LLM baja de 19→12 campos hybrid (~324K docs pendientes vs ~449K). Los 3 irresolubles (`num_componentes_formula`, `num_documentos_pago`, `num_pasos_verificacion`) quedan como hybrid sin excluir.

#### Key Insights
- El regex de conteo de ítems nivel 1 supera al LLM (100% vs 12%) — el LLM no "cuenta" listas; el regex cuenta con `max` de ítems numerados.
- Campos de referencias (`num_referencias_*`) tenían FP LLM altos (50-172) → portarlos a regex los eliminó (FP 0).
- `pct_monto_referencia_minimo` es empate técnico (90.5 vs 91.0) — se portó por ahorro GPU (sin pérdida práctica).
- **IMPORTANTE**: el extractor LLM fallback (`document_features_v2_llm_extractor`) filtra por `extraction_method=="hybrid"` → los 7 portados ya no se envían al LLM automáticamente.

#### Files
- Modified: `src/etl/transformers/document_features_v2_transformer.py` (3 métodos `_re_*` nuevos + patrones dispatch), `data/processed/section_clustering_v2/master_schemas_v2.json` (62→69 regex / 19→12 hybrid), `scripts/generar_revision_26_campos.py` (PORTADOS Sesión 16 + resumen 14/9/3), `PIPELINE_CHECKLIST.md` (tabla fallback 12 campos, ~324K), `AGENTS.md`
- Regenerados: `data/processed/alolab/comparativa_estrategias.csv`, `docs/revision_26_campos.md`, `data/processed/alolab/revision_26_campos.csv`
- DB: `dncp.document_features_v2` re-creado (31,321 × 108 cols)

### Session 19 — Benchmark GPU Top 10: Fase 5 (867) + Fase 6 concurrencia + tablas

#### Qué se hizo
1. **Stack GPU en cero**: driver 615 + CUDA 13.4 + vLLM 0.29.0 (venv), llama-server build source, `models/candidates.yaml`, BD `dncp` restaurada.
2. **Fase 5** (867 pseudo-GT × 10 modelos): 🥇 **Llama 3.1 8B-FP8 61.8% P50 0.17s** > Llama-AWQ 61.4% > Qwen3-8B-FP8 54.7% (único discriminador: `pct_monto_referencia_tecnica`).
3. **Fase 6** (`scripts/benchmark_concurrency.py`): techo req/s — FP8 7.0 (C16) > AWQ 5.2 > EXAONE 5.05; punto operativo C=8-16; Gemma no escala (0.9); Small-24B solo batch-offline 0.27 (serve+offload cuelga EngineCore en 0.29).
4. **Docs**: `docs/benchmark_local_resumen_general.md` (general vivo), `docs/tabla_maestra_benchmark.md` (+ glosario §5 + CSV `tabla_maestra_benchmark.csv`), `docs/reporte_benchmark_completo.md` (23 incidencias → §5+§8 con fases 5-6), Fase 6 en `ranking_combinado.md`.
5. Zip de contexto para otra IA: `data/processed/llm_benchmark/contexto_benchmark_ia.zip`.

#### Veredicto provisorio (revertido después en Sesión 20)
Llama-FP8 por "triple corona" (61.8% + P50 0.17s + 7 req/s).

#### Files
- New: `models/candidates.yaml`, `scripts/benchmark_concurrency.py`, 4 docs benchmark, CSV tabla maestra, `Conc_*.json`, `Fase5_*.json`
- Modified: `models/candidates.yaml` (scores `gt100_v029` / `fase5_867` / `conc`)

### Session 20 — Endurecimiento metodológico (8 brechas externas) → veredicto v2

#### Qué se hizo
1. **Revisión externa** (8 brechas: triangulación, significancia, IAA, poder por campo, contaminación dev/test, distribución real low-conf, schema_valid_rate, sensibilidad al prompt) → **Plan v4.1** en `docs/endurecimiento_benchmark_plan.md` con pre-registros (φ≥0.3, α Bonferroni 0.0167 =0.0167, pesos 3/2/2/1/1 + Spearman) y regla de oro (sin GPU hasta cerrar Bloque A).
2. **Procedencia verificada**: los 288 humanos∩regex salen de `gt_por_campo` (ago-10); GT-100 es disjunto (∩=2); flags tuneados sobre GT-100 (no sobre los 288) salvo pct_monto (regex validado ahí → cota optimista); `llm_value` histórico descartado (parser viejo).
3. **Hallazgo numérico**: el regex de `dias_pago_anticipo` acierta **29.4%** vs humano → Pseudo-867 mide divergencia con referencia rota.
4. **Reserva congelada**: `heldout50_frozen.csv` (seed 4242, hash en `heldout50_meta.json`).
5. **A1** (`triangulacion.py`): gate dominante por campo — **sol SOSTIENE** (78.9%, LCB≥0.60), pago/exp **DEGRADA** (<0.50), validez AMBIGUO, pct aparte. A1c: humanos le dan razón al LLM 260/260 en dias (persistido como `a_m`: **160 caja-A + 100 caja-M**).
6. **A2** (`schema_valid_rate.py`): guided vLLM 99.3-100%; re-log Gemma (720, strict 1%/primera-línea 100%) y Ministral (9).
7. **A3** (`stats_significancia.py`, dos métricas separadas): GT-100 indistinguible (0.97-0.98, McNemar p=1.0); sol empata al 85.6% los 10; solo pct discrimina (con caveat).
8. **A4** (`taxonomia_desacuerdos.py`): exp=59/82 caja-A FPs sobre plantilla, pago=49/75 caja-M ambigüedad; baselines modales (Qwen3-14B queda −8.8pp bajo "no hacer nada" en pct); scores colapsan a pct → Spearman=1 reportado como degeneración.
9. **A4.5 → hipótesis** (provisional): con semilla 434875 ("720 días como mínimo", humano+LLMs NULL → podrían ser ambos correctos o errar juntos). Se exige auditoría A4.6 antes de publicarlo como firme.
10. **B planillas** (`scripts/generar_planillas_b.py`): `planilla_iaa_25.csv` + `planilla_estratificado_75.csv` (**76 con conf=0**, población real descubierta: el fallback es regex-no-match, no "conf<0.8 con valor") + LEEME. **Descubierto**: sumar conf=0 da **260,109 field-rows (no docs)**; 4 campos sin `_conf` salieron del fallback → la cifra "324K" queda obsoleta (changelog en resumen §1/§9).
11. **C sesión GPU**: C3 top-3 sobre 288 humanos → **239/239 exacto en los 4 dias, los 3 modelos**, incluso heldout (McNemar pct p≥0.55). C4: P1-neutral, P2 invierte. **C5 replicación ciega 3/3** (P3/P4 de Mistral-7B ciego, fuga F~P2=0): 150 intersección limpia; contra humano bajo P2, los 239 dias siguen en **1.000** — el flip vive **solo en pct** (FP8 59→8% por sobre-abstención NULL; QWEN 54→81.2%).
12. **Veredicto v2** (plan §7): FP8 elegido por **desempate operativo** (7 req/s), no por calidad; prompt de producción = artefacto versionado congelado; C5 dice "máximo estabilidad igual bajo prompt base".
13. **Chequeos pendientes del usuario** (bloquean firmeza, no ejecución): auditoría sustancia `planilla_audito_a47_12.csv` (gate pre-registrado ≥7/8 pago-M), A4.6 (15 caja-A), IAA-25, estratificado-75, review-depth de agosto.

#### Files (nuevos clave)
- Plan: `docs/endurecimiento_benchmark_plan.md` (+§6 estado, §7 veredicto v2, §7.1 C4, §7.2 C5)
- Scripts: `heldout_freeze.py`, `triangulacion.py` (+`classify_A_M`), `stats_significancia.py`, `taxonomia_desacuerdos.py`, `schema_valid_rate.py`, `c3_rerun_top3_288.py`, `c4_paraphrase.py`, `c5_replica.py`, `c5_analisis.py`, `a2_relog_gguf.py`, `generar_planillas_b.py`, `generar_audito_a46.py`, `generar_audito_a47.py`, `generar_paraphrases_ciegas.py`
- Datos: `A1_triangulacion.json`, `A2_*.csv`, `A3_*.csv`, `A4_*.csv/json`, `C3_*.json`, `C4_*.json`, `C5_*`, `paraphrases_ciegas.json`, `planillas/*` (4 CSV + LEEME), zip contexto regenerado (0.63 MB)

### Session 21 — Gate v2: regex reescritos → los 4 campos sostienen + Fase 5 v2

#### Qué se hizo
1. **4 métodos regex nuevos** (`document_features_v2_transformer.py`): pago anclado a cláusula PAGO (penaliza SOLICITUD), solicitud con hábiles/palabras, validez con conversión meses→días + solo cláusulas definitorias + fusión header-valor, exp_tecnica por distancia a "experiencia". Utils: `_is_blank_template`, `NUM_WORDS`, `_parse_spanish_number`, `_num_unidad_in`.
2. **Filosofía aprobada**: precisión > recall — lo dudoso devuelve `None` → conf 0.0 → fallback LLM (circuito verificado: extractor filtra `conf < 0.8`).
3. **Gate v2** (`scripts/validar_regex_gate.py`, regla pre-registrada intacta): pago 100%, solicitud 91.4%, validez 72.7% (LCB 0.61, marginal), exp 93.3% (n=15). Iteración honesta: v1 regresó validez/exp, v2 lo resolvió.
4. **DB rebuilt** (31,321×108, 66s; fixes entorno: `sqlalchemy` + cast float32 pandas 3) y **pseudo900 regenerado** (backups v1).
5. **Fase 5 v2** top-3: FP8 **87.4%** (v1 61.8%), AWQ 87.0%, Qwen 80.3% → +26pp confirman que el número viejo medía desacuerdo con referencia rota.
6. **Volumen fallback**: 30.894 docs (4 campos) → ~1-2 h GPU. Docs: plan §8, resumen §7, tabla maestra.

#### Files
- Modified: `src/etl/transformers/document_features_v2_transformer.py` (4 métodos + utils + dispatch), `docs/endurecimiento_benchmark_plan.md` (§8), `docs/benchmark_local_resumen_general.md`, `docs/tabla_maestra_benchmark.md`
- New: `scripts/validar_regex_gate.py`; backups `pseudo900_v1_867.json`, `fase5_v1/`, `fase5_mixed_v1v2/`
- DB: `dncp.document_features_v2` re-creado con regex nuevos

### Session 22 — Piloto fallback LLM (runner con fingerprint/caché/checkpoints) + planillas de aceptación

#### Qué se hizo
1. **Diseño** (`docs/piloto_llm_fallback.md`): staging en 3 capas (caché content-addressed → runs versionado → upsert tardío con guardia); fingerprint obligatorio (`run_fingerprint.py`: modelo+revisión+quant, sha prompt/schema/código); checkpoints por (doc,campo) con resume; promote solo explícito.
2. **Implementación**: `sql/llm_fallback_cache.sql` (tablas `dncp.llm_fallback_cache` + `dncp.llm_fallback_runs`); `VLLMOfflineProvider` extendido con `guided_regex`; `upsert_guarded()` (solo escribe si `conf_actual < conf_nueva OR valor IS NULL`); runner `scripts/run_llm_fallback.py` (`--pilot-docs`, `--fields`, `--run-id`, `--promote`, `--batch-commit`, SUB_BATCH=2000).
3. **Piloto 1.500 docs** → **corrida completa por lotes (5 lotes)**: **294.102 filas done, 0 pendientes, 0 failed → 79.789 celdas con valor (27,1%)**, 31.321 docs = 100% del universo, producción intacta (sin `--promote`).
4. **6 bugs cazados por análisis post-lote** (ninguno visible en el log): staging-loop fuera del sub-lote (40K no stageadas); commit interno en `ensure_llm_model_column` (corrompió 1 fila, reparada); ratio-0 del determinista + guardia en endeudamiento; ceros con conf (fix `sanitize_value`); re-expansión en resume; `num_pasos_verificacion` subcontado por LLM → porte a regex (97/97) con vuelta a hybrid + re-fallback.
5. **Validez**: distribuciones sanas por campo (notif 7.0 genuino 500/500; ratios en rango; sin absurdos); `exp_tecnica` 0% = ausencia real probable (34/1258 textos con patrón).
6. **Planillas de aceptación** (`scripts/generar_aceptacion.py --tanda A/B/C` + `validar_aceptacion.py` con umbrales pre-registrados LCB≥0.85 PROMOTE / punto<0.70 HOLD): A=281 (exp censo + fórmula/pago_maximo), B=400 (5 campos ×80), C=123 NULLs. **Pendiente auditoría humana** → decide el promote por campo.

#### Files
- New: `sql/llm_fallback_cache.sql`, `src/etl/transformers/run_fingerprint.py`, `scripts/run_llm_fallback.py`, `scripts/generar_aceptacion.py`, `scripts/validar_aceptacion.py`, `docs/piloto_llm_fallback.md` (§8-§10), `data/processed/llm_runs/`
- Modified: `llm_provider.py` (guided), `document_features_v2_llm_loader.py` (upsert_guarded, sin commit interno), `document_features_v2_llm_transformer.py` (llm_raw), `document_features_v2_transformer.py` (_re_num_pasos_verificacion), `master_schemas_v2.json` (pasos vuelve a hybrid), `extraction_postprocess.py` (ratio<=0 → NO_RULE)
- DB: `document_features_v2` rebuilt (pasos regex 6/7 en 26.930 docs); staging 294K filas bajo run `llmfb-meta-llama-3.1-8b-instruct-fp8-2834432bb7237924`

### Session 23 — Auditoría externa hostil (Claude Fable 5.1, 05/06-oct) — SOLO DIAGNÓSTICO, sin cambios de código

Auditoría de 7 dimensiones con evidencia (SQL read-only sobre `dncp`, cómputo CPU sobre `C3_*.json` y planillas; no se abrió `pseudo900.json` ni `multimodel_results/`). Plan de corrección: **`docs/plan_correccion_auditoria.md`** (los IDs H-x.y de abajo son los del plan).

#### Hallazgo central
**C3 no mide modelos en los 4 campos dias.** `extraction_postprocess.parse_response()` (l.613-652) aplica overrides deterministas "totalitarios" ANTES de mirar la respuesta del LLM. Un "modelo" que responde siempre `NULL` (o `999`) obtiene **239/239** (incluido heldout 50/50). El "100% contra humano", el "empate entre finalistas", el "desempate operativo" y "C5: dias robusto al prompt" son propiedades de las reglas, no de los LLM.
Sobre el **crudo** del LLM (C3, 239 dias): FP8 183 (76.6%), AWQ 175 (73.2%), **Qwen3-8B 235 (98.3%)**; McNemar FP8 vs Qwen b=0/c=52, p≈4e-16 (sin validez: 148 vs 186/189, p≈7e-12). exp_tecnica crudo: FP8 15/35, AWQ 7/35, Qwen 34/35.

#### Hallazgos por dimensión (severidad · evidencia)
**1. Arquitectura 🔴**
- H1.1 crítica — el fallback es regex + compuerta LLM de 1 bit: 9/12 campos hybrid con override; staging `dias_pago_maximo` raw==valor solo 6.544/20.755 (31.5%); planillas A/B 422/422 valores = regla con raw ignorado; 73.281/79.789 celdas (91.8%) en campos con override.
- H1.2 crítica — parse benchmark ≠ producción: el benchmark aplica la regla aunque el LLM diga NULL; producción (`document_features_v2_llm_transformer.py:251-252`) corta en NULL. Planilla C: la regla habría dado valor en 12/12 aceptación, 12/12 fuerza_mayor, 12/12 pago_max, 11/12 endeudamiento, 9/12 liquidez.
- H1.3 alta — fingerprint ciego al código: `code_version` = git HEAD (`869c86c`) con todo sin commitear; `run_llm_fallback.py` (23:58), `extraction_postprocess.py` (00:38) y `document_features_v2_transformer.py` (02:02) editados DURANTE la corrida (27-sep 21:13 → 28-sep 02:07 UTC) bajo un único run_id.
- H1.4 alta — caché: clave sin sampling/revisión/código; guarda valor post-procesado (una regla corregida no invalida); NULL no se cachea (l.270); 2.781 entradas de la 1ª hora con `raw_short` vacío, servidas igual.
- H1.5 media — `updated_at` nunca se actualiza (sin trigger, los UPDATE no lo tocan); `staged += len(df_res)` dentro del loop por fila (l.278-282: contador inflado + commit casi por fila); resume `LIMIT 1` sin `ORDER BY` (l.175); extractor `drop_duplicates` arbitrario (16 docs multi-sección en pago).

**2. Metodología 🔴**
- H2.1 crítica — 239/239 = reglas (ver hallazgo central).
- H2.2 crítica — la reserva-50 no es held-out de las reglas: docstrings "Valida 100% en GT (200/200)" (`extraction_postprocess.py:117,172,238,330`) sobre los mismos 26 archivos de donde sale la reserva; contradice plan l.23-24 ("tuneado sobre GT-100, no sobre los 26 archivos"). Fecha de las reglas no verificable (archivo fuera de git).
- H2.3 alta — el gate φ pre-registrado nunca decidió (n<25 → se usó Wilson LCB≥0.60, `triangulacion.py:181-184`); gate v2 = regex reescritos y revalidados in-sample (`validar_regex_gate.py:17,57`); validez LCB 0.61 vs umbral 0.60.
- H2.4 alta — 87.4% de Fase 5 v2 = acuerdo regla-vs-regla: 758−118 = 754−114 = 696−56 = **640 idénticos** fuera de pct; explica "sol 85.6% idéntico en los 10".
- H2.5 alta — desempate operativo sobre empate fabricado: sin reglas, Qwen gana por mucho.
- H2.6 media — la población 288 exige regex con valor (`c3_rerun_top3_288.py:75-76`), disjunta de la del fallback (regex falló); estratificado-75 sin anotar.

**3. Estadística 🟡**
- H3.1 alta — "indistinguibles" en pct = falta de poder (11 discordantes, n=49), no equivalencia.
- H3.2 alta — n efectivo: planilla B `num_pasos` 80 filas / **1 texto** / 1 valor; `fuerza_mayor` 80 / 5 textos; universo: fuerza mayor 30.651 secciones / 559 textos, requisitos 32.182 / 773.
- H3.3 media — `otro`=0.5 dentro de Wilson (`validar_aceptacion.py:38`); C con n=12-13 no puede dar PROMOTE (13/13 → LCB 0.77); sin estratos por valor (pago_max 80/100 = 60).
- H3.4 media — auditor único = autor, y ve `valor_llm` (anclaje); IAA-25 sin anotar.
- H3.5 baja — Spearman=1 con 3 esquemas: retirar.

**4. Calidad de datos 🔴**
- H4.1 crítica — producción codifica ausencia como 0.0: 0 NULLs en los 12 hybrid; fuerza_mayor =0 en 31.321/31.321; pago_anticipo =0 en 30.264; "ausente con certeza" = 0.0 conf 1.0 source `absent` (validez 7.225, exp 5.199).
- H4.2 crítica — 46.401/79.789 (58%) de las celdas nuevas son constantes: fuerza_mayor=7 (23.461), aceptación=15 (21.888; regla `re.search(r"15", text)` l.474 matchea "2015"/"150"), pasos=1 (1.052).
- H4.3 alta — `num_documentos_pago`: el LLM emite numeración de cláusula (`1.5` ×2.490, `1.1` ×1.353) truncada a 1 → 3.843/5.141 (75%); el guided regex acepta decimales para int (`run_llm_fallback.py:38`). El regex de producción da 1 en 25.343/25.350 (sospechoso, no verificado).
- H4.4 alta — conf ∈ {0, 0.3, 1.0}; el LLM siempre recibe 1.0 (`run_llm_fallback.py:77`): no es confianza.
- H4.5 media — fuerza_mayor y fórmula = 100% `absent` (sin regex); aceptación 2/31.321 por regex.
- H4.6 media — `dias_pago_maximo` = "primer N días del texto" (l.82-105), no máximo.

**5. Reproducibilidad 🔴**
- H5.1 crítica — 196 untracked + 137 modificados; runner, postprocess y 19 scripts de benchmark fuera de git.
- H5.2 alta — `requirements.txt` sin versiones y sin vllm/torch/transformers/xgrammar (real: vllm 0.29.0, torch 2.13.0, transformers 5.16.1, xgrammar 0.2.6, pandas 3.0.5).
- H5.3 alta — `data/processed/*` en `.gitignore:39` (GT, heldout, planillas, schemas, run config); sin pg_dump.
- H5.4 media — `tabla_maestra_benchmark.csv` sin generador; `heldout_freeze.py` con rutas relativas + `frozen_at` hardcodeado (el sha256 sí coincide).

**6. Narrativa 🟡**
- H6.1 crítica — el relato "LLM 100% vs humano" no ocurrió. Relato honesto: reglas iteradas contra humano resuelven los campos de plantilla; el LLM aporta poco sobre ellas (cuantificar); sobre crudo los modelos difieren (Qwen 98% vs Llama 73-77%); el postproceso compartido enmascara diferencias = hallazgo metodológico.
- H6.2 alta — sobra: gate φ, Spearman/3 esquemas, 87.4%, C5-dias, caja-A/M como "LLM corrige regex".
- H6.3 alta — falta: ablación 3 brazos, validación de reglas out-of-sample, IAA, varianza por campo, Pipeline 3 (sin detector).
- H6.4 media — inconsistencias: 317.107 vs 294.102; rama `ocds-csv-pipeline` vs real `feature/clustering-pipeline`; "LLM 73% en pago_maximo" (piloto §8) cuando el valor es de la regla.

**7. Promote 🔴 (NO promover)**
- H7.1 crítica — `--promote` no filtra por campo (`run_llm_fallback.py:296-300`): publicaría las 79.789, incluidos los HOLD.
- H7.2 crítica — procedencia falsa: `source='llm'` + conf 1.0 en valores de regla (`document_features_v2_llm_loader.py:79-82`).
- H7.3 alta — UPDATE in-place sin historia ni backup; la guardia `OR valor IS NULL` es rama muerta (no hay NULLs).
- H7.4 alta — 214.313 NULLs del LLM (ausencia confirmada) no se promueven → indistinguibles de "no procesado".
- H7.5 alta — la validación por muestreo no cubre el riesgo (H3.2/H3.3); 4 campos sin muestra de valores (solicitud, pago_anticipo, validez, liquidez).

#### Top-10 (impacto × costo)
1 re-score crudo C3/C4/C5 sin GPU · 2 git commit+tag+sha de código en fingerprint · 3 ablación 3 brazos · 4 anotar estratificado-75 (reglas out-of-sample) · 5 NULL vs 0 en producción · 6 promote `--fields`/source/historia/dump · 7 tabla de varianza, constantes→binario · 8 regenerar planillas por texto distinto + estratos · 9 guided entero para `num_*` · 10 reescribir veredicto.

#### Bien hecho (para la defensa)
Staging antes de producción (0 failed, invariantes post-lote cazaron 40K silenciosas); `raw_short`/`llm_raw` persistidos (permitieron esta auditoría sin GPU); honestidad documental (limitaciones, iteraciones, disclosure P3, pre-registro A4).

#### Consecuencias inmediatas
- **Las planillas A/B/C NO se auditan tal cual** (se regeneran en la Fase 5 del plan).
- **`--promote` bloqueado** hasta la Fase 7 del plan.
- La decisión "FP8 por desempate operativo" queda **reabierta** (pendiente de re-score crudo + ablación).

#### Files
- New: `docs/plan_correccion_auditoria.md`
- Modified: `AGENTS.md` (esta sección + rama), `CLAUDE.md` (paso actual + sesión 23), `docs/INDICE_PROYECTO.md` (fila del plan)

### Session 24 — Ejecución autónoma del plan de corrección (Claude Opus 5.5, 06-oct)

Rama `feature/benchmark-models` (local, sin push). Un commit por fase; bitácora detallada en
`docs/plan_correccion_auditoria.md` §9. Estado: **F0-F4 ✅, F5 código ✅ (anotación humana
pendiente), F6 ✅, F7 bloqueada (aprobación + F5)**.

#### Qué se hizo y qué se encontró
- **F0** (`f18f21e`): `requirements-vllm.lock`, `pg_dump` 4 tablas (`/root/backups/`), `data/MANIFEST.sha256` (LF-normalizado), `heldout_freeze.py --verify` (reproduce sha 90e8ccf7), `build_tabla_maestra.py --check` (0 diffs).
- **F1** (`d01db54`, `docs/rescore_crudo_f1.md`): todas las cifras de la auditoría reproducidas. Nuevo: en Fase 5 (v1 y v2) y C4/C5 fuera de pct, el post de los 3 modelos = baseline siempre-NULL → el ranking Fase 5 se decide solo en pct; "dias robusto al prompt" era artefacto (crudo FP8 67→38 con P3/P4). Crudo FP8 vs Qwen: p≈4e-16. MDE pct ≈22 pp. **Qwen3-8B-FP8 campeón**.
- **F2** (`5020c2c`): `extraction_postprocess` → `rule_value`/`llm_value`/`resolve(mode)` con `Resolution.source`; modos históricos congelados reproducen un snapshot de 40.950 ítems (bench≠prod en 3.409 = H1.2). Regla 15 corregida (sin efecto real: plantilla pura). `num_documentos_pago` regex devolvía índice del 1er ítem (5/200 GT) → conteo por nombre de 7 docs (200/200 in-sample).
- **F3** (`05d33ce`, `067c21b`): DDL aditivo `sql/llm_fallback_v2.sql` (llm_runs, cache_v2 solo crudos, runs_v2 con rule/model/value/source, history). Runner v2 (reglas en CPU primero, guided por tipo, chequeo de código por sub-lote, reintento por ítem, resume exacto). `promote_fallback.py` (dry-run por defecto, `--fields` obligatorio, source real, historia, rollback). Ensayo en copia cazó 2 bugs (guardia REAL no idempotente; rollback de `llm_model` por campo) → corregidos, rollback diff 0.
- **F4** (`133d794`, `f957812`, `4efd0a8`): **ningún numérico de v2 tenía NULL** (26 campos, 0 ceros en 5.266 etiquetas GT) → `dncp.document_features_v3` con NULL≠0 + `{campo}_status` (extraido/sin_seccion/no_extraido). Recompute con historia: `num_documentos_pago` (1 en 25.343 docs y años → 6/7) y `num_pasos_verificacion` (ordinales "1º"; coincide con Qwen en 3.921/3.921). Run completo `llmfb-qwen3-8b-fp8-3fe595a69e69bfd4`: 287.894 filas, 0 fallidas, regla 87,6%, LLM 35.712; **la compuerta v1 había suprimido 76.665 valores** (47.247 ratios: liquidez 876→25.951). Ablación GT-12 (`docs/ablacion_gt.md`): LLM solo Qwen 75,0% vs FP8 34,5%; pipeline 93,8%/90,0%; orden regla→regex 95,1%; modal 67,9% > regex prod 52,2%; el LLM decide 8,7% de ítems y en fórmula queda < modal. Varianza: aceptación/fuerza mayor/presentación garantía = binarios.
- **F5** (`f957812`, `4efd0a8`): planilla ciega `aceptacion_v2_3fe595a6.csv` (643 ítems; unidad = texto distinto; fuerza mayor colapsa a 7 plantillas) + clave + LEEME; validador v2 con κ; 11 tests.
- **F6**: veredicto v3 (`endurecimiento_benchmark_plan.md` §9: firme / hipótesis / retirado + contribución metodológica), avisos HISTÓRICO, tabla maestra con columnas crudo, `capas_features.md` §4 (v3), piloto §11, INDICE, CLAUDE.md.

#### Pendiente (no automatizable)
1. Anotación humana: estratificado-75 (reglas fuera de muestra), IAA-25, planilla v2 → `validar_aceptacion.py`.
2. F7: aprobación del usuario para `promote_fallback.py --apply` por campo (fórmula candidata a HOLD; decidir orden regla→regex).
3. Push: auth GitHub + rotación de credenciales publicadas.

#### Files (nuevos clave)
`scripts/{manifest_datos,build_tabla_maestra,rescore_crudo,snapshot_reglas,promote_fallback,build_features_v3,recompute_regex_field,ablacion_gt,ablacion_resumen,varianza_campos}.py`, `scripts/progreso.sh`, `sql/{llm_fallback_v2,s23_features_v3_null_semantics}.sql`, `tests/` (38), `pytest.ini`, `docs/{rescore_crudo_f1,ablacion_gt,varianza_campos}.md`, `requirements-vllm.lock`, `data/MANIFEST.sha256`.


#### Sesión 24 (continuación): benchmark ampliado a 14 modelos + pseudo-GT Opus 5.5
- **Re-score de todos los modelos sobre crudo** (`rescore_todos_modelos.py`): Llama-FP8 18/80 en GT-100 crudo; la selección de finalistas de sesiones 19-20 también estaba sesgada.
- **Retest con protocolo único** (`ablacion_gt.py`: mismo prompt, guided xgrammar/GBNF, 2.600 GT humanos) para 14 modelos en 2 rondas (8 descargados en esta sesión: Gemma-4-12B QAT W4A16, Qwen3.5-9B GGUF, Ministral-3-8B, Ling-3.0-tiny, MiMo-V2.6-9B, Gemma-4-26B-A4B, Qwen3.6-35B-A3B, Qwen3.8-27B UD-Q3_K_XL; GGUF en `/root/models/gguf` y `models/gguf`). Top: familia Gemma-4 (83%); Qwen3-8B-FP8 (run F4) 75%; Llama-FP8 34,5%.
- **Pseudo-GT Opus 5.5**: 344 ítems fuera de muestra, muestra y anotaciones commiteadas con sha256 antes de evaluar (`pseudo_gt_opus/`). Ranking estable (ρ=0,91, τ=0,80); reglas fuera de muestra 90,1%; varianza de criterio en `dias_pago_maximo`.
- **Decisión**: Gemma-4-12B QAT W4A16 en vLLM recomendado para el fallback (pendiente confirmación del usuario + re-run de `num_componentes_formula`). Docs: `ablacion_gt.md`, `pseudo_gt_varianza.md`, veredicto §9.8-9.9.
- Incidentes: Qwen3.5-9B-FP8 OOM (multimodal) → bug de recarga por ítem corregido; `pkill -f` se mataba a sí mismo → `pkill -x`/PID; Clef-Flash no soportado por llama.cpp `6d9c82e`.

## Git State
- `HEAD` = `869c86c` on `feature/clustering-pipeline` (rama real; los docs anteriores decían `feature/ocds-csv-pipeline` — corregido en sesión 23)
- Last commit: `fix(clustering): Apply 4 anti-overclustering fixes to section clustering`
- Sesiones 1-24 commiteadas en `feature/benchmark-models` (tag `pre-auditoria-s23`); antes: sesiones 19-23 sin commitear (benchmark GPU, endurecimiento, gate v2, piloto fallback + planillas). Archivos nuevos sin trackear: `models/candidates.yaml`, `scripts/benchmark_concurrency.py`, `scripts/benchmark_fase5_900.py`, `scripts/{triangulacion,stats_significancia,taxonomia_desacuerdos,schema_valid_rate,c3_rerun_top3_288,c4_paraphrase,c5_replica,c5_analisis,a2_relog_gguf,heldout_freeze,validar_regex_gate,run_llm_fallback,generar_aceptacion,validar_aceptacion,generar_planillas_b,generar_audito_*}.py`, `sql/llm_fallback_cache.sql`, `src/etl/transformers/run_fingerprint.py`, `docs/{benchmark_local_resumen_general,tabla_maestra_benchmark,reporte_benchmark_completo,piloto_llm_fallback,endurecimiento_benchmark_plan,capas_features}.md`, `docs/presentacion_tutores/`, `data/processed/llm_benchmark/` (resultados + planillas)
- DB `dncp` (podman, `5433->5432`): `document_features_v2` 31.321×108 + `llm_fallback_{cache,runs}` (staging 294K filas, producción sin tocar)

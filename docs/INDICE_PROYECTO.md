# INDICE_PROYECTO — Mapa de navegación (no reescanear el repo)

> Lee este archivo primero. Cada fila dice QUÉ contiene cada archivo y CUÁNDO abrirlo.
> Contexto de sesión: `CLAUDE.md` (paso actual) + `AGENTS.md` (historial 1-23).

## Documentos por tema
| Tema | Archivo | Abrir cuando… |
|---|---|---|
| **Plan activo: corrección post-auditoría** | `docs/plan_correccion_auditoria.md` | SIEMPRE antes de tocar código/BD (fases 0-7, IDs H-x.y; hallazgos en `AGENTS.md` §Session 23) |
| **Veredicto vigente (v3, §9)** + histórico §6-§8 | `docs/endurecimiento_benchmark_plan.md` | cifras de modelos para la tesis: SOLO §9 |
| Re-score sobre salida cruda (F1) | `docs/rescore_crudo_f1.md` | por qué 239/239 era de las reglas; McNemar crudo |
| Ablación regla/regex/LLM/pipeline vs GT (F4-F6) | `docs/ablacion_gt.md` | cuánto aporta el LLM; orden del pipeline |
| **Estrategia de variables para anomalías** (qué va a LLM/regex/SQL, dónde invertir GT) | `docs/estrategia_variables_anomalias.md` | antes de anotar o de arrancar Pipeline 3 |
| Especificación operativa por campo (fichas, prompts, criterio de anotación) | `docs/especificacion_campos.md` | antes de reescribir prompts o anotar |
| Varianza por campo | `docs/varianza_campos.md` | qué campos sirven como magnitud vs binario (Pipeline 3) |
| Piloto fallback (diseño→ejecución) | `docs/piloto_llm_fallback.md` | fingerprint/caché/checkpoints, resultados por lote, bugs |
| Tabla de modelos + glosario | `docs/tabla_maestra_benchmark.md` + `.csv` | cifras de benchmark para la tesis |
| Resumen general benchmark | `docs/benchmark_local_resumen_general.md` | panorama + changelogs (324K→260K) |
| Incidencias (23) | `docs/reporte_benchmark_completo.md` | debugging histórico GPU/vLLM |
| Capas de features | `docs/capas_features.md` | glosario de las 3 tablas (587/46/108) |
| Anomalías (diseño) | `docs/pipeline3_anomaly_detection.md`, `estrategias-anomaly-deteccion.md` | Pipeline 3, top N1-N4 |
| Tesis/contexto largo | `docs/CONTEXTO_TESIS.md`, `docs/PROMPT_CONTEXTO_LLM.md` | cargar otro LLM |
| Tutores | `docs/presentacion_tutores/` | reunión, PPT, gráficas |
| Checklist pipelines | `PIPELINE_CHECKLIST.md` | estado Fase 0-3 |

## Scripts por tarea
| Tarea | Script |
|---|---|
| Reglas spec-v1 (8 campos) + columnas nuevas (regex/flags) | `src/etl/transformers/field_rules_v1.py`; build: `scripts/build_features_spec_v1.py [--apply]`; tests `tests/test_field_rules_v1.py` |
| Análisis por sección (evidencia de las fichas) | `scripts/analisis_seccion_{pago,garantia,anticipo,capacidad,financiera,reajuste}.py <dir_tmp>` |
| Informatividad de las 710 features | `scripts/informatividad_features.py` |
| Correr/retomar fallback LLM (v2) | `scripts/run_llm_fallback.py` (`--model qwen3-8b-fp8`, `--fields`, `--pilot-docs`, `--run-id`, `--no-llm`, `--llm-all`) |
| Promover / rollback (dry-run por defecto) | `scripts/promote_fallback.py --run-id X --fields a,b [--apply]` · `--rollback` |
| Generar/validar planillas aceptación v2 | `scripts/generar_aceptacion.py --run-id X`, `scripts/validar_aceptacion.py <csv> [--segundo]` |
| Tabla v3 (NULL≠0) / recalcular un campo regex | `scripts/build_features_v3.py`, `scripts/recompute_regex_field.py --field X [--apply]` |
| Re-score crudo / ablación / varianza | `scripts/rescore_crudo.py`, `scripts/ablacion_gt.py` + `ablacion_resumen.py`, `scripts/varianza_campos.py` |
| Reproducibilidad | `scripts/manifest_datos.py [--check]`, `scripts/heldout_freeze.py --verify`, `scripts/build_tabla_maestra.py [--check]`, `requirements-vllm.lock` |
| Tests | `/root/vllm-env/bin/python -m pytest` (38) |
| Progreso en vivo | `watch -n 10 bash scripts/progreso.sh` |
| Benchmark calidad/latencia | `scripts/benchmark_fase5_900.py`, `scripts/benchmark_multi_vs_gt.py`, `scripts/benchmark_concurrency.py` |
| Endurecimiento (A1-C5) | `scripts/{triangulacion,stats_significancia,taxonomia_desacuerdos,schema_valid_rate,c3_rerun_top3_288,c4_paraphrase,c5_replica,c5_analisis,a2_relog_gguf,heldout_freeze,validar_regex_gate,generar_planillas_b,generar_audito_*}.py` |
| Pipelines regex v2 | `scripts/run_document_features_v2_pipeline.py`, `src/pipelines/document_features_v2*.py` |
| Schemas extracción | `data/processed/section_clustering_v2/master_schemas_v2.json` (69 regex / 12 hybrid) |

## Datos y BD (no abrir archivos gigantes: consultar por SQL)
| Qué | Dónde | Tamaño |
|---|---|---|
| Pseudo-GT Fase 5 | `data/processed/llm_benchmark/pseudo900.json` (+`pseudo900_v1_867.json`) | 3 MB |
| Resultados modelos | `data/processed/llm_benchmark/multimodel_results/` (`Fase5_*`, `Conc_*`, `C3_*`, `C4_*`, `C5_*`) | ~2 MB |
| Planillas aceptación | `data/processed/llm_benchmark/planillas/` (A 281, B 400, C 123 + IAA/estrat/A46/A47) | chicos |
| GT y comparativas | `data/processed/alolab/` (`gt_por_campo/`, `comparativa_estrategias.csv`, `*_GT.csv`) | mediano |
| Tablas BD `dncp` | `pliegos_secciones` 2.78M · **`document_features_v3`** (NULL≠0, usar esta) · `document_features_v2` 31.321×108 (legado, 0=ausencia) · `document_economic_features` 29.811×46 · `llm_fallback_runs_v2`/`cache_v2`/`llm_runs` (run v2 288K) · `spec_v1_rule_values` (reglas v1, staging 232K) · `llm_fallback_{cache,runs}` (v1 congelado 294K) · `document_features_v2_history` | BD podman 5433 |
| Modelos (NO en repo) | caché HF `/root/.cache/huggingface` (55G) + `./models/gguf/` (20G) | — |

## Código fuente (src/)
`etl/extractors/` (SQL + fallback), `etl/transformers/` (`document_features_v2_transformer.py` = regex, `llm_provider.py` = VLLMOfflineProvider+guided, `extraction_postprocess.py` = verdad determinista, `run_fingerprint.py`), `etl/loaders/` (`upsert_guarded`), `pipelines/` (orquestadores). `config/settings.py` = DB_CONFIG.

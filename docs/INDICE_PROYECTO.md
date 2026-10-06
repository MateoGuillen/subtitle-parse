# INDICE_PROYECTO — Mapa de navegación (no reescanear el repo)

> Lee este archivo primero. Cada fila dice QUÉ contiene cada archivo y CUÁNDO abrirlo.
> Contexto de sesión: `CLAUDE.md` (paso actual) + `AGENTS.md` (historial 1-23).

## Documentos por tema
| Tema | Archivo | Abrir cuando… |
|---|---|---|
| **Plan activo: corrección post-auditoría** | `docs/plan_correccion_auditoria.md` | SIEMPRE antes de tocar código/BD (fases 0-7, IDs H-x.y; hallazgos en `AGENTS.md` §Session 23) |
| Estado + veredicto v2 (⚠ reabierto en sesión 23) | `docs/endurecimiento_benchmark_plan.md` | necesites gates, IC, veredicto v2, plan A1-C5 |
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
| Correr/retomar fallback LLM | `scripts/run_llm_fallback.py` (`--pilot-docs`, `--fields`, `--run-id`, `--promote`) |
| Generar/validar planillas aceptación | `scripts/generar_aceptacion.py --tanda A/B/C`, `scripts/validar_aceptacion.py <csv>` |
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
| Tablas BD `dncp` | `pliegos_secciones` 2.78M · `document_features_v2` 31.321×108 · `document_economic_features` 29.811×46 · `llm_fallback_{cache,runs}` staging 294K | BD podman 5433 |
| Modelos (NO en repo) | caché HF `/root/.cache/huggingface` (55G) + `./models/gguf/` (20G) | — |

## Código fuente (src/)
`etl/extractors/` (SQL + fallback), `etl/transformers/` (`document_features_v2_transformer.py` = regex, `llm_provider.py` = VLLMOfflineProvider+guided, `extraction_postprocess.py` = verdad determinista, `run_fingerprint.py`), `etl/loaders/` (`upsert_guarded`), `pipelines/` (orquestadores). `config/settings.py` = DB_CONFIG.

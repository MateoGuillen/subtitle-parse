# CLAUDE.md — Contexto de continuación entre sesiones

## ⏳ Paso actual (leer primero)
**Sesión 25 (08-oct) = especificación de campos v1.0 aprobada + I1-I3 implementado** (`docs/especificacion_campos.md`, plan I1-I8 al final). Reglas vigentes = `src/etl/transformers/field_rules_v1.py` (`RULES`; el juego de la sesión 24 queda como `RULES_S24` / `ruleset="s24"`). Columnas nuevas en v3 (11) y valores de reglas v1 en `dncp.spec_v1_rule_values` (staging, sin promover). I4 ✅ (85 correcciones aprobadas → `gt_por_campo_v1/`, `anotaciones_v1.json`; originales intactos). Fichas nuevas A9 `pago_plurianual` y A10 `num_tramos_pago` en v3. **Siguiente: I5** — el usuario anota `I5_CALIBRACION_planilla.xlsx` (60 ítems); Opus ya anotó aparte (`calibracion_v2_opus.csv`); luego `scripts/evaluar_calibracion_v2.py` (κ < 0,8 → corregir ficha). Estrategia de variables: `docs/estrategia_variables_anomalias.md`.

**Sesión 24 = ejecución autónoma del plan de corrección (`docs/plan_correccion_auditoria.md`)**: F0-F4 y F6 ✅, F5 código ✅. Bitácora completa en §9 del plan; resumen en `AGENTS.md` §Session 24.
- **Veredicto vigente**: `docs/endurecimiento_benchmark_plan.md` §9 (v3) + §9.8-9.9 (14 modelos + pseudo-GT Opus fuera de muestra, ranking estable ρ=0,91: **Gemma-4-12B QAT W4A16 vLLM recomendado**; elección final del usuario). Run F4 hecho con Qwen3-8B-FP8 (crudo 98,3% vs FP8 76,6% en C3 dias; GT-12 LLM solo 75,0% vs 34,5%). Las capas deterministas ponen el rendimiento: pipeline 93,8% (95,1% con orden regla→regex); el LLM decide solo 8,7% de ítems.
- **Tabla a usar**: `dncp.document_features_v3` (NULL≠0 + `{campo}_status`). `document_features_v2` es legado (0 = ausencia).
- **Run vigente** (sin promover): `llmfb-qwen3-8b-fp8-3fe595a69e69bfd4` en `dncp.llm_fallback_runs_v2` (287.894 filas, 0 fallidas).
- 📋 **Tareas del usuario (checklist)**: `docs/TAREAS_USUARIO.md`.
- 🟡 **Pendiente HUMANO (bloquea F7)**: anotar `planillas/planilla_estratificado_75.csv` (validación fuera de muestra de reglas, lo más importante), `planilla_iaa_25.csv` (κ) y la planilla ciega `planillas/aceptacion_v2_3fe595a6.csv` (643 ítems; instrucciones en `LEEME_aceptacion_v2.md`). Luego `scripts/validar_aceptacion.py`.
- ⛔ **F7 (promote) requiere aprobación del usuario**: `scripts/promote_fallback.py --run-id <run> --fields <PROMOTE>` es dry-run por defecto; `--apply` escribe en v3 con historia/rollback. `num_componentes_formula` candidato a HOLD.
- ⛔ Push bloqueado: falta auth GitHub y **rotar credenciales** ya publicadas (ver memoria `repo-publico-secretos`).
- Decisión pendiente de F5: cambiar el orden del pipeline a regla→regex (+1,3 pp in-sample).

## Proyecto
ETL para detección de anomalías en contrataciones públicas de Paraguay (DNCP). Rama de trabajo **`feature/benchmark-models`** (local, sin push; sale de `feature/clustering-pipeline` @ `869c86c`). Sesiones 1-23 commiteadas (`50730e3`, `7993bfd`, tag `pre-auditoria-s23`); sesión 24 = un commit por fase (F0 `f18f21e` … F6). Tests: `/root/vllm-env/bin/python -m pytest` (38). Progreso en vivo: `watch -n 10 bash scripts/progreso.sh`.

## Entorno (obligatorio)
- Todo corre en **WSL**: `/root/vllm-env/bin/python` es el **único** intérprete con vLLM 0.29.0 + pandas + psycopg2. Nunca `python3` del sistema, nunca venv nuevo para vLLM.
- BD `dncp` en podman (`5433->5432`); `.env` con `DB_HOST=localhost`. Tablas clave: `pliegos_secciones` (2.78M), `document_features_v2` (31.321×108), `document_economic_features` (29.811×46), `llm_fallback_{cache,runs}` (staging 294K).
- GPU única 16 GB: un modelo por vez. Puertos: llama-server 1234, vLLM serve 8000.
- PowerShell: no hacer quoting inline de Python en `wsl.exe bash -c`; escribir `.py` temporal y ejecutarlo.
- Si WSL cuelga: `wsl.exe --terminate Ubuntu` (terminal con admin).
- Si `/mnt/datos` aparece montado **read-only** (NTFS `fuseblk ro`): es hibernación/arranque rápido de Windows. Apagar Windows completo (o `powercfg /h off`) y re-montar; **no** forzar `rw` desde Linux. Ojo: el scratchpad `/tmp` no sobrevive al reinicio.

## Historial de sesiones (condensado)
1-3. Features estructurales/económicas/colusión (`document_features` 587→pipeline, `document_economic_features` 46).
4-6. Ranking de títulos 6 estrategias + docs + clustering.
7-12. Benchmarks locales: Mistral 7B AWQ campeón → endurecimiento.
13. Few-shot retrieval descartado (85% vs 95% baseline).
14. AloLab manual → 100% en 100 GT (postprocess determinista).
15-16. 10 campos a regex (62→69 regex / 19→12 hybrid); gate y comparativas.
17. Panorama anomalías + mapa de variables (top N1-N4 verificado en BD).
18. Agente `documentador-tutores` + material de presentación.
19. Stack GPU en cero + Fase 5 (867 pseudo-GT ×10) + Fase 6 (concurrencia) + tablas. 🥇 Llama-FP8 61.8%.
20. Endurecimiento (8 brechas) → veredicto v2: pseudo-867 mide divergencia (regex pago_anticipo 29.4% vs humano); C3 top-3 100% en 4 dias (239/239); C5 réplica ciega; FP8 por desempate operativo.
21. Gate v2: 4 regex reescritos (pago 100%, solicitud 91.4%, validez 72.7%, exp 93.3%) → Fase 5 v2: FP8 **87.4%**. DB rebuilt 108 cols.
22. Piloto fallback: runner con fingerprint+caché+checkpoints (`scripts/run_llm_fallback.py`); 5 lotes → 294K done/79.8K con valor; 6 bugs cazados (staging-loop, commit interno, ratio-0, ceros, re-expansión, pasos subcontado→regex 97/97); planillas A/B/C + `validar_aceptacion.py`.
23. **Auditoría externa hostil (Fable 5.1), solo diagnóstico.** Hallazgo central: `parse_response()` aplica reglas deterministas antes de mirar el LLM → un modelo que responde siempre `NULL` saca **239/239** en C3 (heldout 50/50): el "100% vs humano", el empate y el desempate operativo miden las reglas. Sobre crudo: FP8 76.6%, AWQ 73.2%, **Qwen3-8B 98.3%** (McNemar p≈4e-16). Además: 91.8% de las celdas del fallback son de regla; 58% son constantes (7/15/1); producción codifica ausencia como 0.0 (0 NULLs); fingerprint ciego al código (archivos editados durante el run); `--promote` sin filtro por campo y con procedencia falsa; nada en git. Veredicto por dimensión: arquitectura 🔴, metodología 🔴, estadística 🟡, datos 🔴, reproducibilidad 🔴, narrativa 🟡, promote 🔴. Plan: `docs/plan_correccion_auditoria.md`.
24. **Ejecución autónoma del plan (Opus 5.5)**: F0 repro (lock, pg_dump, manifiesto, heldout verificable, tabla maestra por script) · F1 re-score crudo (Qwen campeón; siempre-NULL = modelos fuera de pct en Fase 5) · F2 `resolve()` regla/LLM con origen + 38 tests · F3 runner v2/caché crudos/fingerprint de código/promote con historia y rollback (2 bugs cazados en ensayo) · F4 `document_features_v3` NULL≠0, run completo Qwen (regla 87,6%; compuerta v1 suprimió 76.665 valores), `num_documentos_pago`/`num_pasos` corregidos · ablación GT-12 · varianza (3 campos binarios) · F5 planilla ciega v2 · F6 veredicto v3.

## Decisiones que no se discuten de nuevo
- Híbrido regex→LLM: regex preciso donde puede; `None`→fallback (precisión > recall). **Desde sesión 23**: la capa de reglas deterministas es una etapa propia con `source='rule'`, nunca se etiqueta como LLM.
- Prompt de producción = artefacto versionado congelado.
- Pseudo-GT solo vale donde el gate sostiene; φ global prohibido; agreement≠accuracy.
- Nunca UPDATE directo a producción: staging + `upsert_guarded()` (solo si `conf_actual < conf_nueva OR valor IS NULL`).
- Fingerprint distinto = nuevo run_id; caché solo con match exacto. **Desde sesión 23**: el fingerprint incluye el sha256 de los archivos de código que determinan valores, no solo el git HEAD.
- Todo benchmark de LLM se reporta sobre **salida cruda** y, por separado, sobre "crudo + postproceso"; nunca solo lo segundo.

## Decisiones tomadas en sesión 24 (con evidencia)
- Modelo del fallback: "FP8 por desempate operativo" retirado. Retest de 7 modelos (§9.8): Gemma-4-12B GGUF > Qwen3.8-27B ≈ Qwen3-14B-AWQ > Qwen3-8B-FP8 ≈ Ministral > EXAONE > Llama-FP8. Elección final pendiente del usuario (TAREAS_USUARIO 2b).
- Producción = `rule_first` (regla si aplica, si no LLM), source explícito; conf convencional por fuente (regex 1.0 · rule 0.9 · llm 0.7).
- Análisis y promote sobre `document_features_v3` (NULL≠0); v2 queda como legado intacto.
- Aceptación: unidad = texto distinto, ciega al valor, sin `otro`=0,5; LCB≥0,85 PROMOTE / <0,70 HOLD.
- `dias_aceptacion_rechazo_factura`, `dias_notificacion_fuerza_mayor`, `dias_presentacion_garantia` = binarios de cláusula estándar.

## Archivos vivos (no reescanear el repo: usa `docs/INDICE_PROYECTO.md`)
- Docs: **`plan_correccion_auditoria.md` (plan activo)**, `endurecimiento_benchmark_plan.md` (plan v4.1 + veredicto v2, a reescribir en Fase 6), `piloto_llm_fallback.md` (§7 diseño, §8-10 ejecución), `tabla_maestra_benchmark.md`, `capas_features.md`, `estrategias-anomaly-deteccion.md`.
- Scripts: `run_llm_fallback.py`, `generar_aceptacion.py`, `validar_aceptacion.py`, `triangulacion.py`, `c3_rerun_top3_288.py`, `benchmark_fase5_900.py`, `benchmark_concurrency.py`; núcleo de reglas: `src/etl/transformers/extraction_postprocess.py`.
- Datos: `data/processed/llm_benchmark/{pseudo900.json,multimodel_results/,C3_*.json,planillas/,tabla_maestra_benchmark.csv,heldout50_frozen.csv}`.
- Prompts de contexto: `docs/PROMPT_CONTEXTO_LLM.md` + `docs/CONTEXTO_TESIS.md`.

# CLAUDE.md — Contexto de continuación entre sesiones

## ⏳ Paso actual (leer primero)
**Sesión 23 = auditoría externa (Fable 5.1) → el proyecto pasa a fase de CORRECCIÓN.** Ejecutar `docs/plan_correccion_auditoria.md` en orden (Fase 0 → 7). Detalle completo de hallazgos (IDs H-x.y) en `AGENTS.md` §Session 23.
- ⛔ **NO auditar** las planillas `planilla_aceptacion_{A,B,C}` actuales: se regeneran en la Fase 5 (n efectivo ≈1 en algunos campos, sin estratos, valores de regla etiquetados como LLM).
- ⛔ **NO correr `--promote`**: hoy publicaría las 79.789 celdas sin filtro por campo, con `source='llm'` falso y sin historia (H7.1-H7.3). Se desbloquea en la Fase 7.
- Staging intacto: run `llmfb-meta-llama-3.1-8b-instruct-fp8-2834432bb7237924` (294.102 filas, 79.789 con valor) = **evidencia histórica**, no se borra ni se reusa como caché.
- Fase 0 ✅, Fase 1 ✅ (`docs/rescore_crudo_f1.md`: campeón sobre crudo = **Qwen3-8B-FP8**, veredicto v2 invalidado). Fase 2 ✅ (`resolve()` con origen explícito, 26 tests: `/root/vllm-env/bin/python -m pytest`). Próximo: **Fase 3** (fingerprint con sha de código, caché v2, staging v2, loader con historia/rollback).

## Proyecto
ETL para detección de anomalías en contrataciones públicas de Paraguay (DNCP). Rama de trabajo **`feature/benchmark-models`** (local, sin push; sale de `feature/clustering-pipeline` @ `869c86c`). Sesiones 1-23 commiteadas (`50730e3`, `7993bfd`, tag `pre-auditoria-s23`); un commit por fase del plan de corrección.

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

## Decisiones que no se discuten de nuevo
- Híbrido regex→LLM: regex preciso donde puede; `None`→fallback (precisión > recall). **Desde sesión 23**: la capa de reglas deterministas es una etapa propia con `source='rule'`, nunca se etiqueta como LLM.
- Prompt de producción = artefacto versionado congelado.
- Pseudo-GT solo vale donde el gate sostiene; φ global prohibido; agreement≠accuracy.
- Nunca UPDATE directo a producción: staging + `upsert_guarded()` (solo si `conf_actual < conf_nueva OR valor IS NULL`).
- Fingerprint distinto = nuevo run_id; caché solo con match exacto. **Desde sesión 23**: el fingerprint incluye el sha256 de los archivos de código que determinan valores, no solo el git HEAD.
- Todo benchmark de LLM se reporta sobre **salida cruda** y, por separado, sobre "crudo + postproceso"; nunca solo lo segundo.

## Decisiones REABIERTAS por la auditoría (sesión 23)
- ~~FP8 elegido por desempate operativo~~ → pendiente de Fase 1-2 del plan (re-score crudo + ablación 3 brazos). Puede cambiar a Qwen3-8B-FP8 o a "reglas sin LLM" en campos de plantilla.
- Umbrales de aceptación (`validar_aceptacion.py`): se re-especifican en Fase 5 (n efectivo por texto distinto, sin `otro`=0.5).

## Archivos vivos (no reescanear el repo: usa `docs/INDICE_PROYECTO.md`)
- Docs: **`plan_correccion_auditoria.md` (plan activo)**, `endurecimiento_benchmark_plan.md` (plan v4.1 + veredicto v2, a reescribir en Fase 6), `piloto_llm_fallback.md` (§7 diseño, §8-10 ejecución), `tabla_maestra_benchmark.md`, `capas_features.md`, `estrategias-anomaly-deteccion.md`.
- Scripts: `run_llm_fallback.py`, `generar_aceptacion.py`, `validar_aceptacion.py`, `triangulacion.py`, `c3_rerun_top3_288.py`, `benchmark_fase5_900.py`, `benchmark_concurrency.py`; núcleo de reglas: `src/etl/transformers/extraction_postprocess.py`.
- Datos: `data/processed/llm_benchmark/{pseudo900.json,multimodel_results/,C3_*.json,planillas/,tabla_maestra_benchmark.csv,heldout50_frozen.csv}`.
- Prompts de contexto: `docs/PROMPT_CONTEXTO_LLM.md` + `docs/CONTEXTO_TESIS.md`.

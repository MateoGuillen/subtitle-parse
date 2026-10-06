# Diseño — Piloto y corrida masiva del fallback LLM (con caché, checkpoints y versionado)

> Estado: **implementado y probado 27-sep** (piloto 2 docs + resume + aborto por
> fingerprint, todo verde). Principio: ninguna corrida masiva sin fingerprint,
> sin caché content-addressed y sin resume.

## 1. Qué se corre y cuánto tarda (dimensionamiento honesto)

- Población: filas `document_features_v2` con `{campo}_conf = 0` en los 12 hybrid (~260K filas-campo; contar `COUNT(DISTINCT nro_licitacion)` + re-lista de campos con `_conf` antes del piloto — TODO abierto).
- Throughput medido (Fase 6): Llama-8B-FP8 ≈ 5-7 req/s a C=8-16 con secciones largas → **260K ≈ 10-14 h GPU** + I/O de BD. Por eso: **piloto primero** (1.500-2.000 docs estratificados por campo, ~1 h) para validar el loop end-to-end, luego la corrida completa en una sesión dedicada.
- Orden de cómputo por (doc, campo): regex ya corrió (conf=0 significa no-match) → solo LLM.

## 2. Dónde viven los resultados (dos tablas, nunca UPDATE directo a ciegas)

1. **`llm_fallback_cache`** (caché content-addressed, persistente entre corridas):
   `text_sha256, model_id, prompt_hash, field | value, raw_short, created_at`.
   Clave = (hash del texto, modelo, prompt, campo). **Hit solo si coinciden los 4.**
2. **`llm_fallback_runs`** (staging versionado por corrida):
   `run_id, nro_licitacion, field, value, conf_new, raw_short, model_id, prompt_hash,
   schema_version, code_version, status, updated_at`.
3. Solo al cerrar una corrida (o por lotes verificados) se hace **UPSERT a
   `document_features_v2`** (`value → {campo}`, `conf_new → {campo}_conf`,
   `'llm' → {campo}_source`), con `WHERE` que nunca pisa un valor manual/regex
   con conf mayor salvo flag explícito.

## 3. Fingerprint de corrida (el candado de la caché)

Al arrancar, el runner calcula y persiste `run_config.json`:
`{model_id, model_revision (sha), quant, prompt_template_version + sha256 del system
renderizado, schema_version (sha de master_schemas_v2.json), code_version (git sha),
sampling {temp, max_tokens, guided_regex}, conf_threshold, batch, max_concurrency}`.
- **Regla**: un re-run con distinto fingerprint = **nuevo `run_id`**, jamás reusa caché de otro fingerprint (ni siquiera para "ahorrar GPU").
- La caché solo se lee si `fingerprint == fingerprint_cacheado`; si el texto cambió (nuevo hash) → miss automático.

## 4. Checkpoints y resume (cortar y seguir)

- Progreso en `llm_fallback_runs.status` por (doc, campo): `pending → running → done/failed`.
- Commits por lote (ej. 500 filas) con `fsync` lógico (transacción por lote): matar el proceso solo pierde el lote en curso.
- Al relanzar con el **mismo `run_id` + mismo fingerprint** (verificado hash contra hash al inicio; si difiere → aborta con error, no "adivina"): retoma en el primer `pending`. Con distinto fingerprint → exige nuevo `run_id`.
- `failed` con reintentos acotados (3) y backoff; los fallos persistentes quedan marcados, no bloquean el lote.

## 5. Versionado (qué significa "misma configuración")

- **Modelo**: id HF + revision sha + quant (FP8 vs AWQ son "modelos distintos" aunque pesen igual).
- **Prompt**: versión del template + sha256 del system efectivamente renderizado (el congelado de producción; §7.2 del plan endurecimiento: tocarlo exige re-benchmark de pct).
- **Schema/código**: shas de `master_schemas_v2.json` y del repo. Todo queda en `run_config.json` + columnas de la tabla runs → cualquier fila es trazable a (modelo, prompt, código, fecha).

## 6. Opinión (lo que pediste)

- Sí a la caché content-addressed, pero **nunca como atajo entre configuraciones**: la disciplina fingerprint-igual-o-nada es lo que evita el clásico "re-corrí con otro prompt y mezclé resultados".
- Sí a staging + upsert tardío: permite auditar una corrida entera (tasas de NULL, distribución de valores, diff vs regex) **antes** de tocar producción.
- El checkpoint por (doc, campo) con estados es lo correcto; agregaría `attempts` y `last_error` para diagnosticar sin re-correr.
- Piloto ≠ corrida: el piloto valida el loop (tasas, ETA real con I/O, upgrades de esquema); la corrida masiva se planifica con la ETA medida, no con la extrapolada de Fase 6.

## 7. Implementación (27-sep, probada)

| Pieza | Archivo | Estado |
|---|---|---|
| DDL | `sql/llm_fallback_cache.sql` | ✅ tablas creadas en `dncp` |
| Fingerprint | `src/etl/transformers/run_fingerprint.py` | ✅ `compute_fingerprint()` (modelo+revisión+quant, prompt sha, schema sha, git sha, sampling) |
| Provider guided | `VLLMOfflineProvider` en `llm_provider.py` | ✅ extendido con `guided_regex` (StructuredOutputsParams) + `_sampling_params()`; defaults intactos |
| Guarded upsert | `upsert_guarded()` en `document_features_v2_llm_loader.py` | ✅ solo actualiza si `conf_actual < conf_nueva OR valor IS NULL` |
| Runner | `scripts/run_llm_fallback.py` | ✅ `--pilot-docs`, `--fields`, `--run-id`, `--promote`, `--batch-commit`; resume sin re-expandir población; reintentos failed<3; NULL explícito = done (no error) |

Pruebas 27-sep: piloto 2 docs/1 campo (staging, ambos `done` con NULL correcto en plantillas
"No Aplica") → resume con 0 pendientes y 0 GPU → aborto limpio con `--conf-threshold`
distinto (fingerprint difiere). Bug cazado en pruebas: el resume re-expandía la población
(317K filas basura en staging) → resume ya no toca el extractor, recupera textos por join.
Semántica corregida: NULL del modelo = `done` (confirma ausencia), `failed` = solo errores.

## 8. Piloto 1.500 docs (27-sep, staging, sin promote)

`run_id=llmfb-meta-llama-3.1-8b-instruct-fp8-2834432bb7237924` — 15.156 filas-campo,
15.156 `done`, 0 errores, GPU 614s (~25 filas/s), sin escritura a producción.

| Campo | Encontrados | Tasa | Lectura |
|---|---|---|---|
| num_documentos_pago | 262/286 | 92% | el regex no contaba listas; el LLM sí |
| dias_pago_maximo | 1014/1393 | 73% |  |
| num_pasos_verificacion | 938/1297 | 72% |  |
| dias_aceptacion_rechazo_factura | 1063/1482 | 72% |  |
| dias_notificacion_fuerza_mayor | 596/1430 | 42% |  |
| ratio_endeudamiento_maximo | 170/1492 | 11% |  |
| dias_validez_garantia | 49/902 | 5% | mayormente plantillas vacías (coherente con caja-A) |
| dias_solicitud_anticipo | 48/1207 | 4% |  |
| ratio_liquidez_minimo | 40/1498 | 3% |  |
| dias_pago_anticipo | 35/1440 | 2% |  |
| num_componentes_formula | 9/1500 | 1% | campo casi inexistente en la práctica |
| anos_exp_minima_tecnica | 2/1229 | 0% | ⚠ a validar contra `planilla_estratificado_75.csv`: ¿ausencia real o sub-extracción? |

> **Nota sesión 23 (auditoría, H6.4)**: 317.107 es la población del extractor *al momento
> del piloto*. La corrida completa quedó en **294.102 filas** (31.321 docs) porque el
> schema cambió durante la sesión 22 (p. ej. `num_pasos_verificacion` pasó a regex y
> volvió a hybrid con solo los 4.391 misses del regex). No hay conciliación fila a fila
> entre ambas cifras; la vigente es 294.102 (`SELECT count(*) FROM dncp.llm_fallback_runs`).
> Además, en 9 de los 12 campos los "encontrados" de la tabla de arriba son valores de la
> **regla determinista** de `extraction_postprocess.py`, no del LLM (H1.1).

**Extrapolación medida (no estimada)**: 317.107 filas totales / 25 filas/s ≈ **3.5 h GPU**
para la corrida completa (era 10-14 h en el diseño — el guided corto + secciones cortas
rinden 4× mejor). ETA con I/O y reintentos: ~4-5 h en una sesión.

## 9. Conclusiones del piloto (27-sep)

1. **El loop está validado end-to-end**: fingerprint, caché, staging, checkpoints, resume,
   aborto por config distinta y promote con guardia — todo probado con datos reales, dos
   bugs cazados y corregidos. La corrida masiva es operación rutinaria, no experimento.
2. **Rendimiento 4× mejor que el diseño**: 25 filas/s medidas → corrida completa en ~3.5 h
   GPU (+I/O ≈ 4-5 h en una sesión). El guided con outputs de 1-3 tokens es lo que lo permite.
3. **Rendimiento esperado de la corrida completa** (tasas piloto × población conf<0.8):
   **~88.500 celdas nuevas** sobre 317.107 filas. Los campos de conteo/plazo-maximo
   (documentos, pasos, pago_maximo, aceptación: 72-92%) aportan ~68K; los dias/ratio
   aportan ~5K; fórmula/exp casi nada.
4. **La abstención es calibrada, no falla**: en validez/solicitud/pago el LLM dice NULL
   donde hay plantillas vacías (coherente con caja-A del gate) — confirma ausencia en vez
   de inventar, que es exactamente el comportamiento que pide la tesis.
5. **Alerta abierta**: `anos_exp_tecnica` 0% (2/1229). Hipótesis: ausencia real en esa
   población (el regex tampoco encontró nada) — pero exige la planilla estratificada para
   descartar sub-extracción antes de la corrida masiva.
6. **Decisión económica**: con ~88K celdas por ~4 h GPU, el costo por celda es despreciable
   frente al valor (completar 9 variables del Nivel 3 del top de anomalías). GO a la masiva
   cuando se libere ventana GPU.

## 10. Fixes pre-masiva aplicados (27-sep, todos probados)
1. **Checkpoint intra-campo** (`SUB_BATCH=2000` en el runner): un corte pierde ≤1 sub-lote
   en vez del campo entero. El transformer se llama por sub-lote con commit al cerrar.
2. **Promote probado con rollback**: `upsert_guarded()` sobre 3 filas reales — la guardia
   frenó 1 (conf igual) y escribió 1; `ROLLBACK` dejó producción intacta. Bug cazado:
   `ensure_llm_model_column()` hacía `commit()` interno (commiteaba trabajo ajeno y dejó
   1 fila corrupta en producción: detectado, reparado a mano, y eliminado el commit interno).
3. **Reintentos probados**: 289 filas `failed→pending→done` con `attempts=2`, caché con
   2.785 entradas pobladas en el camino (264 hits en el resume).
4. **`raw_short` persistido**: el transformer devuelve `llm_raw[:120]`; staging y caché lo
   guardan (antes `""` en el 100%). Verificado en filas reales (`'1.5'`, `'NULL'`).
5. **`exp_tecnica` 0% investigado (no es bug)**: de 1.258 textos, solo 34 traen "N años"+
   "experiencia" juntos; el caso límite visible ("experiencia comprobada de 4 años de
   com…") es ambiguo comercial-vs-técnico. Evidencia apunta a ausencia real, no a
   sub-extracción sistemática → sin cambio de código; la planilla estratificada arbitra.
6. **Bug crítico cazado en Lote 3 (stagingloop fuera del sub-lote)**: al partir en
   SUB_BATCH, el loop de staging quedó indentado fuera del `for b` → cada campo procesaba
   todos los sub-lotes en GPU pero **stageaba solo el último** (40K filas quedaron
   `pending` con `attempts=0` tras un run "OK"). Detectado por la invariante
   procesadas-vs-staged del análisis por lote; corregido re-indentando + re-lanzado por
   resume (retomó exactamente los 40K). Lección registrada: el análisis post-lote no es
   opcional — es el que atrapa esta clase de bug silencioso.
7. **`num_pasos_verificacion`: LLM subcuenta listas (1 en vez de 2+) → porte a regex
   con vuelta atrás**: el análisis del Lote 5 mostró uniformidad 1.0 sospechosa; un caso
   (doc 409983: "1. Verificará... 2. Verificará...") confirmó subconteo. Validación GT:
   receta max-item-nivel-1 → 97/97 (100%). Se porteó a regex (`_re_num_pasos_verificacion`,
   26.930 docs con 6/7, consistente con GT humano 6/7/8) PERO se revirtió el schema a
   **hybrid** (no regex puro): los 4.391 misses del regex re-entraron al fallback y el LLM
   rescató 1.052 — ruidoso pero mejor que NULL, y el upsert con guardia impide que ese 1
   pise el 6/7 del regex. Lección: portar a regex puro solo si cobertura ≈100%; si no,
   hybrid con regex preciso + LLM de respaldo.

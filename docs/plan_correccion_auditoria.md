# Plan de corrección post-auditoría (sesión 23, auditor: Claude Fable 5.1)

> **Estado**: pendiente de ejecución · **Creado**: 06-oct-2026
> **Fuente de hallazgos**: `AGENTS.md` §Session 23 (IDs **H-x.y**). Este plan solo dice *qué hacer, en qué orden y cómo se verifica*.
> **Regla de oro**: cada fase cierra con su verificación en verde **antes** de abrir la siguiente. Una verificación que falla se documenta en §9 (bitácora) y se arregla. No se salta.
> **Intérprete**: `/root/vllm-env/bin/python` siempre. **BD**: `dncp` (podman 5433). **GPU**: solo en las Fases 3 y 4 (marcadas 🖥).

---

## 0. Contexto y objetivo

La auditoría encontró que la capa de reglas deterministas (`extraction_postprocess.py`) produce el valor final en 9 de los 12 campos híbridos **sin usar la respuesta del LLM**. Ese parse se compartió entre el benchmark y la producción. Por eso el benchmark C3 (239/239) mide las reglas y no los modelos. El staging de 79.789 celdas está etiquetado como LLM cuando el 91,8% es de regla, y producción codifica la ausencia como `0.0`.

**Objetivo**: llegar a la defensa con:
1. código versionado y reproducible;
2. un benchmark que mide modelos (crudo) y, por separado, el sistema completo (crudo + reglas), con una ablación de 3 brazos;
3. reglas validadas fuera de muestra;
4. producción con NULL ≠ 0 y procedencia correcta (`regex` / `rule` / `llm`);
5. un promote por campo, reversible y validado con muestras de n efectivo real;
6. una narrativa reescrita sobre números que sobreviven.

**No-objetivos**: no agregar modelos nuevos, no tocar el clustering ni el ranking de títulos, no implementar Pipeline 3 aquí (este plan lo desbloquea; tiene plan propio).

---

## Mapa hallazgo → fase

| Fase | Cubre | GPU | Humano | Esfuerzo |
|---|---|---|---|---|
| F0 Versionar y respaldar | H5.1, H5.2, H5.3, H5.4, H6.4 | — | — | 2-3 h |
| F1 Re-score crudo (sin GPU) | H2.1, H2.4, H2.5, H3.1, H3.5 | — | — | 3-4 h |
| F2 Separar regla y LLM en código + tests | H1.1, H1.2, H4.3, H4.4, H4.6, H4.2 (regla 15) | — | — | 5-7 h |
| F3 Runner, caché, fingerprint, loader | H1.3, H1.4, H1.5, H7.1, H7.2, H7.3 | 🖥 smoke | — | 4-6 h |
| F4 Producción: NULL≠0 + run nuevo | H4.1, H4.5, H7.4, H2.6 | 🖥 ~1-2 h | — | 4-5 h |
| F5 Validación humana bien especificada | H2.2, H2.3, H3.2, H3.3, H3.4, H7.5 | — | **sí** | 3 h código + anotación |
| F6 Narrativa y docs | H6.1, H6.2, H6.3, H2.3, H4.2 | — | revisión | 4-6 h |
| F7 Promote por campo | H7.1-H7.5 (cierre) | — | aprobación | 1-2 h |

Orden estricto: F0 → F1 → F2 → F3 → F4 → F5 → F7. F6 se escribe incrementalmente desde F1 y se cierra al final. F1 y F2 pueden solaparse: F1 solo lee JSON viejos.

---

## F0 — Versionar y respaldar (bloqueante de todo)

**Por qué primero**: sin esto, ningún arreglo posterior es trazable (H5.1) y el fingerprint seguiría ciego (H1.3).

| # | Tarea | Archivos |
|---|---|---|
| 0.1 | Revisar `git status` y decidir qué datos van a git. Código, SQL, docs y `master_schemas_v2.json` sí; los CSV de GT/heldout/planillas como excepción en `.gitignore` (son chicos), o con un manifiesto sha256 si se prefiere no versionarlos. | `.gitignore` |
| 0.2 | Commit "estado pre-auditoría" con todo lo de las sesiones 1-22 tal cual, más tag `pre-auditoria-s23`. Es la foto exacta de lo auditado. | — |
| 0.3 | Lock de entorno: `/root/vllm-env/bin/pip freeze > requirements-vllm.lock`. Dejar `requirements.txt` como dependencias directas con cotas mínimas y agregar vllm, torch, transformers y xgrammar. | `requirements.txt`, `requirements-vllm.lock` (nuevo) |
| 0.4 | Backup BD: `pg_dump -Fc -t dncp.document_features_v2 -t dncp.llm_fallback_runs -t dncp.llm_fallback_cache -t dncp.document_economic_features` a `backups/dncp_pre_s23_<fecha>.dump`, más su sha256. Guardar una segunda copia en un disco ext4 (no solo en `/mnt/datos`, que es NTFS y ya quedó read-only una vez). | `backups/` (gitignored) |
| 0.5 | Manifiesto de datos: `scripts/manifest_datos.py` escribe `data/MANIFEST.sha256` (GT `gt_por_campo/*`, `heldout50_frozen.csv`, planillas, `C3_*.json`, `pseudo900*.json`, `master_schemas_v2.json`). Un `--check` lo verifica. | nuevo script |
| 0.6 | `heldout_freeze.py`: rutas absolutas desde `ROOT` y `frozen_at` leído del meta existente (no hardcode). Agregar `--verify`: re-ejecutar reproduce el sha `90e8ccf7…` sin escribir nada. | `scripts/heldout_freeze.py` |
| 0.7 | `tabla_maestra_benchmark.csv`: identificar cómo se generó. Si fue a mano, crear `scripts/build_tabla_maestra.py` que la reconstruya desde los JSON. Si no es posible, marcarla como "manual" en su doc. | nuevo script o nota |
| 0.8 | Corregir inconsistencias documentales triviales (H6.4): 317.107 → explicar la diferencia con 294.102 en `piloto_llm_fallback.md` §8-9 (fue un cambio de población). La rama ya está corregida en CLAUDE.md/AGENTS.md. | docs |

**Verificación F0**
- `git status --short | wc -l` = 0 (o solo ignorados conscientes). `git tag` muestra `pre-auditoria-s23`.
- `pg_restore -l backups/dncp_pre_s23_*.dump | grep -c "TABLE DATA"` = 4.
- `python scripts/manifest_datos.py --check` sale OK.
- `python scripts/heldout_freeze.py --verify` imprime el sha `90e8ccf7c76b…` y no escribe nada.
- Opcional: en un venv descartable, `pip install -r requirements-vllm.lock --dry-run` resuelve sin conflictos.

---

## F1 — Re-score del benchmark sobre salida cruda (CPU, sin GPU)

**Por qué**: es el arreglo de mayor impacto y menor costo. Los JSON ya guardan `llm_raw` completo (H2.1, H2.5).

| # | Tarea | Archivos |
|---|---|---|
| 1.1 | `scripts/rescore_crudo.py`: para cada JSON de C3, C4 y C5 calcula por campo y modelo tres métricas. **(a) crudo**: parse estricto del número, sin reglas, con conversión de unidades mínima declarada para validez (meses→días con el mismo mapa, documentado). **(b) crudo+postproceso**: lo publicado hasta hoy. **(c) baseline siempre-NULL** pasado por el mismo postproceso. Salida: `data/processed/llm_benchmark/F1_rescore_crudo.csv` + `.json`. | nuevo |
| 1.2 | Estadística por métrica: Wilson IC95 por campo y modelo; McNemar exacto pareado (FP8-AWQ, FP8-Qwen, AWQ-Qwen) con Bonferroni α=0,0167; tamaño de efecto (diferencia de proporciones con IC). Reportar el número de discordantes siempre. | idem |
| 1.3 | Repetir para Fase 5 (867): acuerdo crudo vs pseudo, más el conteo de ítems "idénticos entre modelos" con y sin postproceso. Debe reproducir los 640 idénticos (H2.4) y mostrar qué queda sin reglas. Solo agregados; no volcar `pseudo900.json`. | idem |
| 1.4 | Re-score C4/C5 en crudo: ¿el flip de prompt se ve también en dias cuando no hay reglas? Esto resuelve si "C5: dias robusto al prompt" era propiedad de las reglas. | idem |
| 1.5 | Tabla "potencia": para pct (n=49) calcular la diferencia mínima detectable con 80% de poder, para reemplazar "indistinguibles" por una frase cuantitativa (H3.1). | idem |

**Verificación F1** (números que la auditoría ya obtuvo; el script debe reproducirlos exactos)
- Baseline siempre-NULL en C3 dias = **239/239**; heldout = **50/50**; pct = 0/49.
- Crudo C3 dias: FP8 **183**, AWQ **175**, Qwen **235** (de 239). McNemar FP8-Qwen: b=0, c=52.
- Crudo+postproceso reproduce lo publicado: dias 239/239 los 3; pct 29/32/26 de 49.
- Fase 5: 758/754/696 con postproceso; idénticos fuera de pct = 640.
- Si algún número no coincide, se documenta la causa en §9 antes de seguir.

**Decisión al cerrar F1** (se registra en §9): el campeón *sobre crudo* y si la diferencia es significativa. Esto alimenta F2/F4 (qué modelo usar en el run nuevo) y F6.

---

## F2 — Separar "regla" de "LLM" en el código + tests

**Por qué**: hoy un solo `parse_response()` mezcla tres cosas (H1.1, H1.2). Producción y benchmark deben usar la misma función, con el origen del valor explícito.

### 2.0 Snapshot previo (antes de tocar una línea)
- `scripts/snapshot_reglas.py`: para cada campo con regla, aplica la API actual sobre los 26 `*_GT.csv` y sobre una muestra fija de 5.000 secciones de producción por título, y guarda `tests/fixtures/rules_snapshot.parquet`. Es la línea base del test de no-regresión.

### 2.1 Refactor de `extraction_postprocess.py` (sin cambiar resultados de las reglas)
- Nueva API pública, con un único punto de verdad:
  - `rule_value(field, text) -> value | None | NO_RULE`: solo regla determinista sobre el texto. `NO_RULE` = el campo no tiene regla o la regla no aplica.
  - `llm_value(field, raw, text) -> value | None`: solo parse del crudo más validaciones *no sustitutivas* (escala %, número presente en el texto, unidad). **Nunca** reemplaza el número del LLM por otro.
  - `resolve(field, raw, text, mode) -> Resolution(value, source, rule_value, llm_value)` con `mode ∈ {"rule_only", "llm_only", "rule_then_llm", "llm_gated_rule"}`. `source ∈ {"rule", "llm", "none"}`. `llm_gated_rule` = comportamiento histórico de producción; `rule_then_llm` = comportamiento histórico del benchmark.
- `parse_response()` y `postprocess()` quedan como wrappers deprecados sobre `resolve()`, preservando el comportamiento viejo para re-scorear lo histórico. Nada nuevo los usa.
- Los 9 overrides "totalitarios" pasan a ser entradas de un dict `RULES = {field: fn}`. Así queda auditable qué campos tienen regla.

### 2.2 Correcciones de reglas (cada una con test)
- **H4.2 / l.474** `dias_aceptacion_rechazo_factura`: `re.search(r"15", text)` → `\b15\b\s*(?:\([^)]*\))?\s*d[ií]as` o `quince\s*\(?15\)?\s*d[ií]as`. Test: "2015", "150", "15%" no matchean; "quince (15) días" sí.
- **H4.6** `dias_pago_maximo`: renombrar la regla a `_first_n_dias` y documentar que extrae "primer plazo". Decisión a registrar en §9: renombrar la variable en el schema (`dias_pago_primer_plazo`) o implementar "máximo" real y re-validar contra GT. Recomendación: **renombrar**, porque es lo que el GT humano anotó.
- **H4.3** guided regex por tipo: `int` → `^(\d{1,4}|NULL)$`; `float` → el actual. Lo aplica el runner según `schema["type"]`. Test: un crudo `"1.5"` en un campo `num_*` ya no se puede generar; si llega por la caché vieja, `llm_value` lo rechaza (None), no lo trunca.
- **H4.3 (regex producción)** `num_documentos_pago` = 1 en 25.343/25.350: muestrear 20 textos con SQL y revisar si el regex cuenta ítems o devuelve 1 por defecto. Si es defecto, se arregla con la misma receta max-item-nivel-1 que `num_pasos` (97/97). Registrar en §9.
- **H4.4** confianza: `Resolution` lleva `source`; el loader escribe conf por fuente con valores fijos y documentados (regex=1.0 si matchea, rule=0.9, llm=0.7, none=NULL). Los números son convencionales, no probabilidades, y el doc lo dice. No se inventa calibración.

### 2.3 Unificar consumidores
- `document_features_v2_llm_transformer._parse_response` → `resolve(field, raw, text, mode=MODE_PRODUCCION)`, que devuelve también `source`, `rule_value` y `llm_value`.
- `scripts/benchmark_multi_vs_gt.parse` y `c3_rerun_top3_288.py` → aceptan `--mode` (por defecto `llm_only` para benchmark de modelo). Las salidas JSON guardan las tres columnas.
- `document_features_v2_transformer.py` (regex de producción): verificar si llama a `postprocess(strict=False)`; si es así, pasa a `resolve(mode="rule_only")` o al wrapper equivalente sin cambio de valores.

### 2.4 Tests (nuevo `tests/test_extraction_postprocess.py`, pytest)
- **Test de regresión del hallazgo central**: con `mode="llm_only"`, el baseline siempre-NULL sobre los 288 da **0** aciertos en dias (antes 239). Con `mode="rule_then_llm"` sigue dando 239 (comportamiento histórico preservado y documentado).
- No-regresión de reglas: `rule_value` reproduce el snapshot 2.0 fila por fila, salvo los campos corregidos a propósito (aceptación 15), cuyo diff se lista.
- `llm_value` nunca devuelve un número que no está en el crudo (property test con 200 crudos aleatorios).
- Casos de borde de la regla 15 y del guided int.
- Correr: `/root/vllm-env/bin/python -m pytest tests/ -q`.

**Verificación F2**
- `pytest` en verde.
- Diff contra el snapshot = solo los cambios intencionales, revisados uno por uno y anotados en §9.
- `rescore_crudo.py` (F1) re-ejecutado con la nueva API da los mismos números que en F1.
- `grep -rn "parse_response\|postprocess(" src scripts` solo muestra wrappers o usos históricos marcados.

---

## F3 — Runner, caché, fingerprint y loader

| # | Tarea | Hallazgo | Archivo |
|---|---|---|---|
| 3.1 | Fingerprint con sha256 de los archivos que determinan valores (`extraction_postprocess.py`, `document_features_v2_llm_transformer.py`, `run_llm_fallback.py`, `llm_provider.py`, el regex transformer) más `git_dirty: bool`. Si `git_dirty` es verdadero, aborta salvo con `--allow-dirty` (y queda registrado). | H1.3 | `run_fingerprint.py` |
| 3.2 | Antes de cada sub-lote: recalcular el sha de código; si cambió, **abortar** ("código cambió durante el run"). | H1.3 | `run_llm_fallback.py` |
| 3.3 | Caché v2: tabla nueva `dncp.llm_fallback_cache_v2` con clave `(text_sha256, field, fingerprint)`, que guarda **solo el crudo** (`raw TEXT`, incluido `NULL`) y no el valor. El valor se recalcula con `resolve()` al leer. La caché v1 queda congelada (no se borra, no se lee). | H1.4 | `sql/llm_fallback_cache_v2.sql` (nuevo), runner |
| 3.4 | Staging v2: columnas nuevas en `llm_fallback_runs` (o tabla `_v2`): `raw TEXT` completo, `rule_value`, `llm_value`, `value`, `source`, `mode`, `section_id`. `updated_at = now()` en todo UPDATE, más un trigger de respaldo. | H1.5, H7.2 | SQL + runner |
| 3.5 | Bugs: `staged`/`since_commit` fuera del loop por fila; resume por `section_id` guardado al dar de alta (sin `LIMIT 1` arbitrario); el extractor elige la sección por regla determinista (la más larga o la primera por id) y la registra. | H1.5 | runner, extractor |
| 3.6 | Loader `upsert_guarded` v2: (a) `--fields` obligatorio en promote, sin default "todos"; (b) escribe `source` real (`rule`/`llm`) y la conf por fuente de F2; (c) antes de cada UPDATE inserta la fila previa en `dncp.document_features_v2_history(nro, field, old_value, old_conf, old_source, new_value, new_conf, new_source, run_id, promoted_at)`; (d) `--dry-run` que imprime conteos por campo y fuente sin escribir; (e) la guardia se reescribe para la semántica nueva de NULL (F4). | H7.1-H7.3 | loader, runner, SQL |
| 3.7 | Script de rollback: `scripts/rollback_promote.py --run-id X [--fields ...]` restaura desde history. | H7.3 | nuevo |
| 3.8 🖥 | Smoke test con GPU: `--pilot-docs 50 --fields dias_pago_maximo,num_documentos_pago` con run_id nuevo. | — | — |

**Verificación F3**
- Smoke 🖥: 50 docs × 2 campos `done`; `raw` no vacío en el 100%; `source` ∈ {rule, llm, none}; ningún `num_documentos_pago` con crudo decimal.
- Re-lanzar con el mismo run_id: 0 a GPU, 100% hits de caché v2, incluidos los NULL.
- Tocar un comentario en `extraction_postprocess.py` y re-lanzar: **aborta** por fingerprint.
- `--promote --dry-run --fields dias_pago_maximo` imprime conteos y `SELECT count(*) FROM dncp.document_features_v2_history` = 0 (no escribió).
- Promote real en una **copia** (`CREATE TABLE dncp.dfv2_test AS SELECT * FROM dncp.document_features_v2 LIMIT 200`, loader apuntado por parámetro), luego rollback: el diff contra el original da 0 filas.
- Filas de staging tocadas en el smoke: `updated_at` > hora de alta en el 100%.

---

## F4 — Producción: NULL ≠ 0 y run nuevo 🖥

| # | Tarea | Hallazgo |
|---|---|---|
| 4.1 | Migración de semántica (SQL versionado `sql/s23_null_semantics.sql`, sobre una tabla nueva `document_features_v3` o, con backup F0, in-place). Para cada campo hybrid: `value=0 AND conf=0` → `value=NULL, status='no_extraido'`; `value=0 AND source='absent'` → `value=NULL, status='ausente'`. Columna nueva `{campo}_status ∈ {extraido, ausente, no_extraido, plantilla_vacia}`. Pipeline 3 lee `status`, nunca un 0 como plazo. | H4.1 |
| 4.2 | Revisar los 69 campos regex con la misma consulta: ¿cuántos codifican ausencia como 0? Si hay más, la migración es genérica, generada desde el schema. | H4.1 |
| 4.3 | Etapa "regla en CPU" (`scripts/run_rules_cpu.py`): aplica `rule_value` a toda la población con regex-no-match en los campos con regla. Escribe a staging con `source='rule'` y run_id propio. Minutos, sin GPU. | H1.1 |
| 4.4 🖥 | Run LLM nuevo **solo** donde `rule_value` es `NO_RULE` (campos sin regla: `num_documentos_pago`, `num_pasos_verificacion`, `num_componentes_formula`, más los docs donde la regla no aplica). Modelo = el que salga de F1 (decisión registrada). `mode` según F2. | H1.1, H2.5 |
| 4.5 | Para medir el aporte del LLM sobre las reglas (H6.3), en los campos con regla correr también el LLM en un **subconjunto** de unos 2.000 docs estratificados, para alimentar la ablación de F6. | H6.3 |
| 4.6 | Tabla de varianza por campo (`scripts/varianza_campos.py` → `docs/varianza_campos.md`): valores distintos, entropía, % modal y textos distintos por campo, sobre staging nuevo. Campos con un valor modal ≥ 99% se reclasifican como **binario "cláusula estándar presente"** (fuerza_mayor=7, aceptación=15). | H4.2 |
| 4.7 | `fuerza_mayor` y `num_componentes_formula` (100% `absent`, sin regex): decidir en §9 si se marcan `extraction_method: rule` (la regla es el extractor) o se escribe regex. Que el schema diga la verdad. | H4.5 |

**Verificación F4**
- `SELECT count(*) FROM <tabla> WHERE <campo> = 0 AND <campo>_status <> 'extraido'` = 0 para los 12 hybrid.
- Ningún campo hybrid tiene `value IS NULL AND status IS NULL`.
- Run de reglas: reportar por campo el delta contra las celdas del staging viejo (la regla ya no depende de la compuerta del LLM; H1.2 cuantificado).
- Run LLM: 0 `failed`, `raw` no vacío al 100%, un solo fingerprint, `git_dirty=false`.
- `docs/varianza_campos.md` generado; los campos reclasificados como binarios quedan listados.

---

## F5 — Validación humana bien especificada (código + anotación)

### 5.1 Código
- `generar_aceptacion.py` v2:
  - Muestrea por **texto distinto** (hash del texto normalizado). La unidad de muestreo es la plantilla, no la fila; se reporta "n textos / n filas cubiertas".
  - Estratifica por **valor** (modal vs no-modal, cuotas mínimas por estrato) y por **fuente** (`rule` / `llm`).
  - Incluye los 4 campos que faltaban (solicitud, pago_anticipo, validez, liquidez) (H7.5).
  - Modo **ciego al valor**: el auditor ve el texto y escribe el valor correcto, sin ver `valor_llm` (H3.4). La comparación la hace el script.
  - n por campo calculado para que el criterio sea alcanzable: con precisión verdadera 0,95, LCB≥0,85 requiere unas 60-80 plantillas distintas. Si el campo tiene menos textos distintos que eso, se hace **censo** de plantillas.
- `validar_aceptacion.py` v2:
  - Se elimina `otro`=0,5 (H3.3). Categorías: `correcto`, `incorrecto`, `ambiguo`. `ambiguo` cuenta como incorrecto en el criterio principal, con sensibilidad sin ambiguos.
  - IC sobre plantillas, y ponderado por filas cubiertas como segunda métrica.
  - Criterio de NULLs (tanda C) por **tasa agregada** con IC, no por campo, si n por campo es menor a 30.
  - Calcula κ de Cohen si hay un segundo anotador.
- Reutilizar el **estratificado-75** y el **IAA-25** ya generados (`generar_planillas_b.py`), sin regenerar.

### 5.2 Anotación (humano, en este orden)
1. **Estratificado-75** (ya existe): valida las reglas fuera de muestra (H2.2, H2.3, H2.6). Es la única evidencia out-of-sample de las reglas.
2. **IAA-25** con un segundo anotador (tutor o colega) → κ (H3.4).
3. **A4.7** (`planilla_audito_a47_12.csv`, ya existe): sustancia de las etiquetas de agosto.
4. **Planillas de aceptación v2**, ciegas al valor.

**Verificación F5**
- Cada planilla v2 imprime, por campo: n filas, n textos distintos, estratos y n mínimo para PROMOTE. Ningún campo con n efectivo menor al mínimo sin marca de "censo".
- Planilla v2 sin columna `valor_llm` visible (el script de validación la une por clave).
- Reglas out-of-sample: el resultado del estratificado-75 queda en §9 con IC, y el veredicto por campo va a F6.

---

## F6 — Narrativa y documentación (incremental, cierra al final)

| # | Tarea | Hallazgo |
|---|---|---|
| 6.1 | **Veredicto v3** en `endurecimiento_benchmark_plan.md` §9 (nuevo; §7 queda tachado como histórico con nota). Contenido: tabla de ablación de 3 brazos (regla sola / LLM crudo / sistema) por campo, crudo por modelo (F1), reglas out-of-sample (F5), decisión de modelo. | H6.1, H2.5 |
| 6.2 | Degradaciones explícitas: "239/239", "intercambiables", "C5 dias robusto" y "87,4%" pasan a una sección *histórico — invalidado por H2.1/H2.4*. "Indistinguibles en pct" se reemplaza por la frase de potencia de F1.5. Spearman=1 y los 3 esquemas se retiran (H3.5). El gate φ se describe como "pre-registrado, no aplicable por n<25; se usó el criterio Wilson" (H2.3). | H6.2 |
| 6.3 | Hallazgo metodológico como contribución: "un postproceso determinista compartido entre evaluación y producción enmascara las diferencias entre modelos; recomendación: reportar siempre crudo y sistema por separado". | H6.1 |
| 6.4 | `tabla_maestra_benchmark.md/.csv` regenerados por script (F0.7) con las columnas crudo y sistema. | H5.4 |
| 6.5 | `piloto_llm_fallback.md`: nota en §8-§10 que aclara que las "tasas LLM" eran de regla en 9 campos, y una §11 con el run nuevo de F4. | H6.4 |
| 6.6 | `capas_features.md`: documentar `{campo}_status`, `source` y la conf por fuente. | H4.1, H4.4 |
| 6.7 | CLAUDE.md / AGENTS.md / INDICE actualizados al cierre de cada fase. | — |

**Verificación F6**: ninguna cifra en docs vivos sin script generador o query citada. `grep -rn "239/239\|87.4%\|indistinguibles" docs/*.md` solo devuelve líneas dentro de secciones marcadas "histórico". Revisión del tutor del capítulo de benchmark.

---

## F7 — Promote por campo (cierre)

Precondiciones (todas): F0-F5 en verde; decisión por campo registrada en §9 con evidencia de F5; backup reciente (repetir F0.4).

1. `run_llm_fallback.py --promote --dry-run --run-id <rules_run> --fields <campos PROMOTE>` → revisar conteos por campo y fuente.
2. Promote real de los mismos campos. Luego el run LLM nuevo, campo por campo.
3. Los campos HOLD o JUICIO no se promueven; quedan en staging con su motivo en §9.
4. Los campos reclasificados como binarios (F4.6) se promueven a su columna binaria, no como magnitud.

**Verificación F7**
- `SELECT {campo}_source, count(*) ... GROUP BY 1` por campo promovido: solo `regex`, `rule` o `llm`, con los conteos esperados del dry-run (diferencia 0).
- `document_features_v2_history` tiene una fila por celda cambiada (conteo igual al de celdas actualizadas).
- Rollback de prueba de un campo en una copia → diff 0. Luego re-promote.
- Ningún campo hybrid con `0` y `status <> 'extraido'`.

---

## 8. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| `/mnt/datos` montado read-only (NTFS, Windows hibernado); ya pasó en la sesión 23 | Apagar Windows completo / `powercfg /h off`; no forzar `rw` desde Linux. Los backups (F0.4) también en ext4. El scratchpad `/tmp` no sobrevive al reinicio: no dejar trabajo solo ahí. |
| El refactor de F2 cambia valores de reglas sin querer | Snapshot 2.0 **antes** del refactor; test de igualdad fila por fila. |
| F1 cambia el campeón (Qwen) | Fase 6 ya midió Qwen3-8B-FP8 a 4,5 req/s y 13,2 GB: entra en 16 GB. Repetir el smoke F3.8 con Qwen. |
| Anotación humana lenta (F5) | F5 no bloquea F3/F4 (solo F7). Priorizar estratificado-75, que es el más chico y el de mayor valor. |
| Pipeline 3 lee la tabla vieja con ceros | Pipeline 3 no arranca hasta F4.1. Se agrega como precondición en su plan. |

## 9. Bitácora de ejecución (llenar al avanzar)

| Fecha | Fase/tarea | Resultado | Evidencia (query/archivo) | Decisión |
|---|---|---|---|---|
| 06-oct | — | Plan creado tras auditoría sesión 23 | `AGENTS.md` §Session 23 | Planillas A/B/C v1 y `--promote` bloqueados |
| 06-oct | F0.1 | `.gitignore` reescrito (modelos 20 GB, dump 1,6 GB, `scripts/_*.py`, `/_tmp_*`, PDFs/MD de referencias con copyright, `/presentacion_tutores/` duplicada, `nul`) + lista blanca de datos (llm_benchmark, gt_por_campo, master_schemas_v2, llm_runs ≈ 47 MB). `.gitattributes` `eol=lf`: los 126 "modificados" eran CRLF → desaparecen; quedan 20 reales. Committable: 22 GB → ~50 MB | `git status` | Usuario: versionar artefactos + GT |
| 06-oct | F0.1 (secretos) | **Repo público con credenciales ya pusheadas** (Google API key, LLM_PASSWORD, IP+usuario remoto, pass BD) en commits 57ad572/09e9f50/f477042. Token HF solo en scripts sin trackear. Redactados 23 archivos: scripts → `os.getenv`/`load_dotenv`, docs → placeholders. 10 `.py` compilan; escaneo final = 0 | escaneo regex sobre `git ls-files` + untracked | Usuario: redactar + **rotar** claves, sin reescribir historial |

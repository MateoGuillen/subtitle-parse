# Plan v4.1 — Endurecimiento metodológico del benchmark (tras verificación de procedencia)

> ⛔ **AVISO (sesión 23, auditoría externa)**: las secciones §6, §7, §7.1, §7.2 y §8 son
> **HISTÓRICAS e INVALIDADAS en sus conclusiones sobre modelos** (H2.1/H2.4): el parse de
> benchmark aplicaba reglas deterministas antes de mirar al LLM, y un "modelo" que responde
> siempre `NULL` obtenía 239/239. "239/239", "87,4%", "indistinguibles/intercambiables",
> "C5: dias robusto al prompt" y "FP8 por desempate operativo" **no deben citarse**.
> **Veredicto vigente: §9 (Veredicto v3)**. Detalle: `docs/rescore_crudo_f1.md`, `docs/ablacion_gt.md`.


> **Estado**: aprobado con correcciones de procedencia + 2 reglas pre-registradas · **Fecha**: 14-sep-2026
> **Cambio de v3→v4**: gate φ **por campo** (no agregado); 288 = set principal de A1 con
> reglas de contaminación verificadas; columna `llm_raw/llm_value` de los 26 archivos
> **descartada**; hallazgo de producto en `dias_pago_anticipo` con sección propia;
> reserva congelada de 50 ANTES de analizar (limitación de haber visto agregados).
> **v4→v4.1**: ① exp_tecnica inconcluso por diseño (n=23<25) pre-registrado en A1b+§5;
> ② regla A4: pct_monto y campos inconclusos fuera del score agregado, sub-score aparte
> con sensibilidad con/sin pct.
> **Regla de oro intacta**: sin GPU hasta cerrar Bloque A.

## 0. Insumos y procedencia (VERIFICADA 14-sep)

| Set | Qué es | Estado para el análisis |
|---|---|---|
| `alolab/gt_por_campo/*_GT.csv` (ago-10, 26 campos × 200) | pseudo-GT modelo + revisión humana (mismo linaje que GT-100: "CAMBIO vs v1"). Usar solo `ground_truth + db_value + text` | **288 casos GT∩regex** en nuestros 5 campos: pago_ant 85, sol_ant 69, validez 50, exp_tec 35, pct_monto 49 |
| `llm_raw/llm_value` de esos archivos | outputs LLM de agosto con parser pre-AloLab | **DESCARTADOS** — ni como referencia descriptiva |
| `ground_truth_5_campos.csv` (sep, 100) | anotación humana 5 campos | set humano secundario |
| `pseudo900.json` (867) | métrica divergencia regex↔LLM | su estatus lo decide A1 por campo |
| Intersecciones | 288∩GT-100 = 2 (etiquetas iguales ✓ criterios consistentes) · 288∩867 = **87** (reconte oficial A1: 53 dias + 34 pct; el "70" era un conteo provisional con otra definición de población — no afectó ningún estadístico, que se calculó en vivo desde los JSON) | dedup al combinar; solapamiento no contamina (mismas etiquetas) |

**Contaminación verificada**: flags/hints/postprocess se tunearon sobre GT-100, NO sobre
los 26 archivos → 288 limpio salvo **pct_monto_referencia_tecnica**, cuyo regex actual fue
seleccionado/validado sobre estos archivos (Sesión 16) → sus 49 casos **no son held-out**
y su φ se reporta como cota optimista o se excluye del gate.
**Pendiente humano**: confirmar profundidad de revisión de los 200×26 (¿revisaste todo, o solo v1-muestra?).

## 1. Reserva congelada (paso CERO, antes de cualquier A1c)

`scripts/heldout_freeze.py`: split determinista seed 4242 → **50 casos** (12-13 por campo,
excepto pct_monto que queda fuera por contaminación) escritos a
`data/processed/llm_benchmark/heldout50_frozen.csv`, hash de integridad, y NUNCA leídos
hasta el veredicto v2. **Limitación declarada**: los agregados por campo (25/85 etc.) y
6 filas de pago_anticipo fueron vistos antes del congelamiento; los ítems individuales de
la reserva no.

## 2. Bloque A — CPU, orden estricto

### A1. `scripts/triangulacion.py` — GATE POR CAMPO
  - **A1-dominante (CPU, ya)**: fiabilidad de la referencia `P(regex==humano)` por campo + IC Wilson sobre los 189 exploratorios — es el estadístico que de verdad decide el gate (si el regex está mal, el agreement no vale aunque el LLM sea perfecto).
  - **A1b. Gate pre-registrado por campo** (φ + bootstrap IC 10k, escala Cohen):
    - `n < 25` → inconcluso por poder · `φ̂ ≥ 0.3 ∧ IC_inferior > 0` → Pseudo-867 **sostiene ese campo** · si no → **degradado**.
    - `pct_monto`: gate aplicado pero con caveat de tuning (cota optimista).
    - Resultado esperado: pago_anticipo cae (fiabilidad 29%), validez y sol sostienen, **exp_tecnica NO es testeable** (ver abajo), pct se reporta aparte con caveat → Pseudo-867 agregada se **recalcula sobre los campos que sostienen** y el ranking de modelos Fase 5 se re-reporta así.
    - **exp_tecnica — PRE-REGISTRADO AHORA**: n exploratorio = 35 − 12(reserva) = **23 < 25** → el gate leerá *inconcluso por poder* pase lo que pase; es aritmética del split de la reserva, **no ausencia de señal**. Queda consignado en §5. Opción secundaria (solo fiabilidad, no φ): ampliar con los ~19 casos GT-100 disjuntos → n≈42, **condicionada a consistencia de criterios entre las dos rondas de anotación** (agosto vs septiembre: valor/NULL/ambiguo, p.ej. cómo tratar un plazo cuando el pliego lista varios). La evidencia de consistencia requerida ya está: los ~2 casos solapados 288∩GT-100 tienen etiquetas idénticas (✓ §0) — alcanza, no hace falta más. Si se usa: declarar en §5 como **decisión para ganar poder, NO réplica independiente**.
  - **A1-solapados (subproducto, NO confirmación cruzada)**: sobre los 87 solapados, φ `X=(llm==regex)` × `Y=(llm==humano)` por modelo, n=7-17/campo → **exploratorio solo, IC ancho, y NO es evidencia independiente de A3**: es la misma información mirada con dos métricas (limitación obligatoria en la narrativa; no se excluyen, pero no se presentan como validación de fuente distinta).
  - **A1c. Matriz condicional** (sobre los 87, por modelo): `P(correcta|acuerdo)`; en desacuerdo, `P(razón LLM)` vs `P(razón regex)`.

#### ✅ A1 EJECUTADO 15-sep (`scripts/triangulacion.py`, output `A1_triangulacion.json`)

#### ✅ GATE v2 — regex reescritos, 15-sep (`scripts/validar_regex_gate.py`)
Métodos nuevos en `document_features_v2_transformer.py`: `_re_dias_pago_anticipo`
(ancla cláusula PAGO, penaliza SOLICITUD), `_re_dias_solicitud_anticipo` (hábiles/
corridos/calendario + palabras), `_re_dias_validez_garantia` (meses/años→días con mapa
determinista + fusión header-valor + solo cláusulas definitorias), `_re_anos_experiencia_
minima_tecnica` (candidatos "N años" por distancia a "experiencia", excluye items de lista/
antigüedad/puntajes), más `_is_blank_template` y números en palabras (`NUM_WORDS`,
`_parse_spanish_number` hasta "trescientos sesenta y cinco").
| Campo | R nuevo (IC95) | Gate |
|---|---|---|
| dias_pago_anticipo 18/18=100% [0.82,1] | **SOSTIENE** (precisión perfecta, recall al LLM) |
| dias_solicitud_anticipo 53/58=91.4% [0.81,0.96] | **SOSTIENE** |
| dias_validez_garantia 48/66=72.7% [0.61,0.82] | **SOSTIENE** (marginal: LCB 0.61) |
| anos_exp_minima_tecnica 14/15=93.3% [0.70,0.99] | **SOSTIENE** (n=15, caveat ancho) |
Iteraciones honestas registradas: v1 de mis métodos regresó validez/exp (meses de
"Certificado de Garantía" ≠ vigencia; primer-match en vez de distancia); v2 con
definitorio-vs-requisito y distancia lo resolvió. Filosofía: precisión>recall —
lo dudoso va al LLM (100% humano en dias).
**DB rebuilt** (`run_document_features_v2_pipeline.py`, 31,321×108, 66s; fix pandas-3
`astype(np.float32)` en `conf_series.update` + `pip install sqlalchemy` en venv) y
**pseudo900 regenerado** (867, backup en `pseudo900_v1_867.json` + `fase5_v1/`).
Fase 5 top-3 re-corriendo sobre el set nuevo para confirmar que el agreement mide calidad.

#### [HISTÓRICO — invalidado, H2.4] FASE 5 v2 — 27-sep (`pseudo900.json` regenerado, referencia gateada)
FP8 **87.4%** (758/867; v1: 61.8%), AWQ 87.0% (754; v1: 61.4%), Qwen3-8B-FP8 80.3%
(696; v1: 54.7%). El salto de +26pp **confirma la tesis del plan**: el 61.8% medía
desacuerdo con referencia rota; con referencia gateada el agreement sí mide calidad y
el ranking se mantiene (FP8 > AWQ > Qwen, pct como único discriminador: 118/114/56).
Ojo metodológico registrado: el harness hizo resume y mezcló 690 filas v1 → se filtró
a las 867 v2 puras (backup del mezclado en `fase5_mixed_v1v2/`, v1 puro en `fase5_v1/`).
Regla aplicada tal como estaba escrita. Verificación de pureza de población: 89/94 filas
regex-afirma/GT-null tienen conf≥0.9 → el dominante mide exactamente la población de producción.
| Campo | R = P(regex==humano) | IC95 Wilson | Gate (regla fija) |
|---|---|---|---|
| dias_solicitud_anticipo | **78.9%** (45/57) | [67, 88] | **SOSTIENE** (LCB≥0.60) |
| dias_validez_garantia | 52.2% (36/69) | [41, 64] | **AMBIGUO → no entra** (sub-caso: 94% en valor-vs-valor; cae por 29 FPs de plantilla vacía con conf≥0.9) |
| pct_monto_referencia_tecnica | 61.2% (cota optimista; φ Llama-FP8=0.82, n=34) | [49, 72] | aparte, fuera de agregados |
| dias_pago_anticipo | 30.7% | [21, 42] | **DEGRADA** (punto<0.50) |
| anos_experiencia_minima_tecnica | 23.2% | [15, 33] | **DEGRADA** (peor de lo esperado) |

**A1c (descriptiva, no independiente)**: en los 4 campos dias-*, cuando el LLM coincide con
el regex el humano coincide 100% de las veces; cuando discrepan (260 juicios modelo×ítem), el
humano le da la razón al **LLM 260/260 y al regex 0** (persistido en `A1_triangulacion.json`).
**Partición determinística A/M de esos 260** (regla estructural en `classify_A_M()`, sin
lectura de texto): **160 caja-A** = humano NULL + LLM NULL + regex afirma → coincidencia por
*abstención*; **100 caja-M** = humano valor = LLM valor ≠ regex → *resolución*. Por campo:
exp 90A/10M, validez 60A/10M, **pago 10A/70M (el caso fuerte: resolución semántica)**,
sol 0A/10M. Redacción escalonada: **FIRME** = magnitud (regex afirma valores que el humano
rechaza en 260/260 desacuerdos, y en pct invierte: regex 210 vs LLM 8 → ahí el acuerdo sí
mide calidad); **FUERTE** = pago_anticipo como único campo con resolución semántica genuina
sostenida mecánicamente; **HIPÓTESIS** = mecanismo de caja-A (plantilla vacía vs cláusula mal
anclada vs sobre-abstención del revisor de agosto). Semilla que motivó el downgrade: nro
434875 (validez) dice textualmente "será de 720 días como mínimo", humano+10 modelos dijeron
NULL → en ese caso el consenso humano-LLM podría ser el error, no el regex. Su prueba es
**A4.6**. En Fase 5 esos campos "saturaban idéntico" porque todos medían contra una
referencia rota en caja-A. **Consecuencia pre-registrada**: Pseudo-867 agregada solo se
sostiene para `dias_solicitud_anticipo` (+pct con caveat) → el ranking Fase-5 queda degradado
a divergencia descriptiva y la selección final depende de **GT-100 + re-run humano de C3 +
throughput**, no del 61.8%.

#### A4.6 (nuevo paso, diseño pre-registrado 15-sep)
Auditoría **exploratorio-cualitativa** del mecanismo caja-A: n=15 estratificado fijo —
**validez: semilla 434875 + 7 aleatorios (seed 466, excl. reserva) = 8 lecturas; exp_tecnica: 7
aleatorios** (la semilla no descuenta cupo). Pago_A (10) excluido declaradamente (su historia
descansa en caja-M, mecánica). El auditor **tipifica la nota de agosto** (qué mecanismo
describe), no re-juzga el pliego; el desacuerdo con el revisor va en columna aparte
(`disenso_sugerido`) y no cuenta en la tipificación. Techo obligatorio del reporte: "se
observan los mecanismos X/Z, **sin poder estimar proporciones con n=15**"; prohibido
extrapolar a los 160; si un mecanismo sale 0, se dice (tampoco prueba ausencia).
Planilla: `planillas/planilla_audito_A46_15.csv`.

### A2. `scripts/schema_valid_rate.py` — ×10 desde disco (8 exactos; Gemma-720/Ministral-9 re-log en sesión GPU).
### A3. `scripts/stats_significancia.py` — Wilson+bootstrap ×10; McNemar top-3 α=0.0167 (Bonferroni). **Dos métricas NUNCA fusionadas** (estructural, no de palabra): `stats_agreement()` sobre Pseudo recalibrada (divergencia) y `stats_accuracy_humana()` sobre los 189 humanos, con dos outputs CSV separados; está prohibido bootstrap conjunto.
### A4. `scripts/taxonomia_desacuerdos.py` — 4 cajas + 3 esquemas de pesos + Spearman; **solo campos que sostienen** entran al score.
- **Regla de exclusión (pre-registrada, escrita en el script antes de ver resultados)**: `pct_monto` **NO entra** al score agregado titular — su gate está contaminado circularmente (regex tuneado sobre este mismo set) y mezclarlo ensuciaría el número principal. Se reporta como **sub-score aparte con caveat** + chequeo de sensibilidad *con/sin pct*: el ganador debe ser estable en ambas versiones o se declara la dependencia. Campo inconcluso (`exp_tecnica`) = mismo tratamiento que degradado: **tampoco entra** al agregado (inconcluso ≠ sostiene).
### A4.5. **HALLAZGO DE PRODUCTO (PENDIENTE GATE A4.7) — `dias_pago_anticipo`** (sección propia):
el regex de producción falla contra humano 71% (25/85) en este campo → la "saturación
idéntica" de Fase 5 NO era techo de calidad compartido sino **todos midiendo contra una
referencia rota**. El fallback LLM podría estar subvalorado precisamente donde más aporta
(hipótesis a confirmar con re-run top-3 de C3). Impacto: la métrica de producción misma
del campo merece re-validación (no solo el benchmark).
**⚠ Caveat de publicación**: el campo tiene "varios plazos plausibles" (diagnóstico
Fase 5) → parte del 71% podría ser error del revisor humano de agosto. **NO se escribe
como hallazgo firme en la tesis hasta confirmar el método de revisión** (¿pasada única
rápida o criterios explícitos + tiempo por caso?). Es la única pieza no verificada del plan.

## 3. Bloque B — anotación humana
1. IAA-25 (de los 189 exploratorios, ciega) + κ.
2. Estratificado-75 (`conf<0.8` reales) — sigue siendo la brecha de dominio #6.
3. ~~Held-out-50 nuevo~~ → **reemplazado por la reserva congelada del §1** (etiquetas ya existen).

## 4. Bloque C — una sesión GPU
Re-log Gemma/Ministral → **C3: re-run top-3 sobre 288 con parse post-AloLab** (accuracy
humana actual = reemplaza `llm_value` descartado) → paráfrasis ×2 top-3 (150 fijas) →
descongelar reserva-50 → **veredicto v2**. Regla C2 pre-registrada: *si accuracy-humana
por campo (ponderada 3/2/2/1/1 + estabilidad Spearman) invierte el ranking Pseudo-867,
el ganador se elige por humana + throughput, y Pseudo-867 se reporta como divergencia*.

## 5. Salidas / limitaciones / no-hacer
- **Salidas**: scripts A1-A4 + freeze + planillas + CSV v2 + secciones φ/IC/κ/288/veredicto-v2 en `tabla_maestra_benchmark.md` + narrativa A4.5.
- **Limitaciones obligatorias**: κ(n=25) orientativo; reserva-50: agregados vistos antes de congelar; pct_monto: gate con caveat de tuning; review-depth de los 26 archivos pendiente de confirmación humana; no-bit-exactitud batching en re-logs; **exp_tecnica: n exploratorio 23<25 → gate inconcluso por diseño de la reserva, pre-registrado en A1b**; **pct_monto y campos inconclusos excluidos del score agregado (regla A4), reportados aparte**; **ampliación-exp con ~19 GT-100 (si se usa): decisión de poder, no réplica independiente — respaldada solo por la consistencia de criterios de los 2 casos solapados**.
- **No hacer**: más modelos, Small online, usar llm_value histórico, φ global agregado.

**Secuencia**: §0 freeze → A1 → A2-disco → A3 → A4(+A4.5) → B(paralelo) → sesión GPU → veredicto v2.

---

## 8. [HISTÓRICO — cifras de Fase 5 v2 invalidadas en sesión 23, ver §9] GATE v2 — regex reescritos para que el gate sostenga (27-sep, plan cumplido)

**Filosofía (aprobada)**: precisión > recall — lo dudoso devuelve `None` → conf 0.0 →
fallback LLM. El gate se mueve arreglando la referencia, nunca el umbral.

**Métodos nuevos** (`document_features_v2_transformer.py`):
- `_re_dias_pago_anticipo`: ancla a cláusula PAGO ("se abonará"), penaliza oraciones de
  SOLICITUD (conviven en el mismo título), acepta números en palabras.
- `_re_dias_solicitud_anticipo`: ancla a solicitud; hábiles/corridos/calendario + palabras.
- `_re_dias_validez_garantia`: conversión meses/años→días determinista
  (`MESES_A_DIAS` + `N*365`); fusión header-valor en líneas separadas (`_blocks`);
  **solo cláusulas definitorias** (`_DEF_RE` sí, `_REQ_RE` no — "deberá ser igual o
  superior" va al LLM); ignora cláusulas de reparación.
- `_re_anos_experiencia_minima_tecnica`: candidatos "N años" elegidos por **distancia a
  "experiencia"** (regla del GT humano); excluye items de lista, "antigüedad" (empresa) y
  puntajes ("Experiencia 18" puntos).
- Utilidades: `_is_blank_template` (plantilla sin completar → None), `NUM_WORDS` (1-50),
  `_parse_spanish_number` ("trescientos sesenta y cinco" → 365), `_num_unidad_in`
  (dígitos entre paréntesis mandan: "sesenta días (60)" → 60).

**Iteraciones honestas**: v1 regresó validez/exp (agarraba "Certificado de Garantía de 36
meses" como vigencia; primer-match en vez de distancia) → v2 con definitorio-vs-requisito
y distancia lo resolvió. `validar_regex_gate.py` valida con la regla pre-registrada.

| Campo | R (IC95) | Gate |
|---|---|---|
| dias_pago_anticipo 18/18=100% [0.82,1] | **SOSTIENE** |
| dias_solicitud_anticipo 53/58=91.4% [0.81,0.96] | **SOSTIENE** |
| dias_validez_garantia 48/66=72.7% [0.61,0.82] | **SOSTIENE** (marginal) |
| anos_exp_tecnica 14/15=93.3% [0.70,0.99] | **SOSTIENE** (n=15, caveat) |

**Producción**: `document_features_v2` rebuilt (31,321×108, 66s; fixes de entorno: `pip
install sqlalchemy`, cast `float32` para pandas 3 en `conf_series.update`).
**pseudo900 regenerado** (867, backups `pseudo900_v1_867.json` + `fase5_v1/` + `fase5_mixed_v1v2/`).

**FASE 5 v2** (misma §7 Métodos): FP8 **87.4%** (v1: 61.8%), AWQ 87.0%, Qwen 80.3%.
El +26pp confirma que el número viejo medía desacuerdo con referencia rota; ranking
intacto (FP8 > AWQ > Qwen, pct único discriminador 118/114/56).

**Circuito fallback verificado**: extractor filtra `{campo}_conf < 0.8` → todo `None`
entra solo. Volumen con regex precisos: pago 30.264, solicitud 25.354, exp 25.433,
validez 18.372 filas; **30.894 docs distintos** (4 campos) → ~1-2 h GPU a C=8-16.

---

## 6. [HISTÓRICO] Estado de ejecución (15-sep)

| Paso | Estado | Artefacto |
|---|---|---|
| freeze | ✅ | `heldout50_frozen.csv` (+meta sha256) |
| A1 triangulación | ✅ gate aplicado | `A1_triangulacion.json`: sol SOSTIENE; pago y exp DEGRADA; validez AMBIGUO→fuera; pct aparte (φ Llama-FP8=0.82, n=34) |
| A2 | ✅ 10/10 | `A2_schema_valid.csv` + re-log: Gemma raw_full estrictamente válido 1% / primera-línea 100%; Ministral 8/9 |
| A3 | ✅ | `A3_accuracy_humana_GT100.csv` (todos 0.97-0.98, CIs solapados, **McNemar top-3 p=1.0 = indistinguibles**) + `A3_agreement_recalibrada.csv` (sol: 85.6% IDÉNTICO en los 10; pct discrimina con caveat) |
| A4 taxonomía | ✅ | `A4_taxonomia_regex_vs_humano.csv`: exp 59/82 caja-A (plantillas vacías con conf≥0.9), pago 49/75 caja-M (ambigüedad), sol 12M. **Validación cruzada con A1: coincide al 0.1pp.** Baselines modal: sol 77.8 (LLMs +7.8), pct 37.4 (Llama +42.9, Qwen3-8B +0.7, Qwen3-14B −8.8, Granite −37.4). Score: 3 esquemas colapsan a pct (sol constante) — se reporta como degeneración, no como robustez |
| A4.5 | 🟡 PROVISIONAL (doble candado) | historia mecánica confirmada (pago caja-M, humano-da-razón-al-LLM 260/260) pero su ESTATUS depende de dos pendientes: gate A4.7 ≥7/8 sobre sustancia de etiquetas (planilla hecha, sin auditar) + review-depth de agosto sin confirmar. Mientras ambos cuelguen: firmes = magnitud de la falla del regex vs etiquetas (con IC); condicional = "el LLM supera al regex" como lectura sustantiva. C5 no rompe el candado: mide contra las mismas etiquetas |
| B planillas | ✅ generadas (+A46-15 +A47-12) | `planillas/planilla_iaa_25.csv` (ciega) + `planilla_estratificado_75.csv` (76 docs con conf=0=regex-no-match real, no "conf baja con valor") + `LEEME_anotacion.md` — **pendiente TU anotación** |
| C sesión GPU | ✅ 15-sep | C3 (§7), C4 (§7.1), re-log A2, C5 replicación ciega 3/3 (§7.2). Ablación guided + bench serve: fuera de alcance |

**Corrección metodológica descubierta al generar B**: la población del fallback NO es
"conf<0.8 con valor" (2-3 filas); son los docs con **conf=0 y valor 0.0 placeholder**
(29,298 en pago). El pipeline 2 LLM debe filtrar `conf = 0` (regex no-match), no
`conf < 0.8` sobre valores. Afecta al extractor del piloto, no a este benchmark.

**Consecuencias para el veredicto (pre-registradas en §2-A3 y §4)**: el ranking
Pseudo-867 queda descriptivo; la única evidencia humana limpia que discrimina modelos
es el campo pct (con caveat de tuning) + el re-run C3 sobre los 288; GT-100 empata a
todos estadísticamente. El ganador final se elegirá por: accuracy-humana-C3 (ponderada,
estable entre esquemas) → desempate throughput/VRAM → Llama-FP8 hoy líder en 2 de 3.

---

## 7. [HISTÓRICO — INVALIDADO en sesión 23, ver §9] VEREDICTO v2 (15-sep, C3 completado — reemplaza al "61.8% triple corona")

**C3 top-3 sobre 288 humanos, parse post-AloLab, guided, raw completo**
(`C3_Llama_31_8B-{FP8,AWQ}.json`, `C3_Qwen3-8B-FP8.json`):

| Campo (n) | regex | FP8 | AWQ | QWEN-8B-FP8 |
|---|---|---|---|---|
| pago_anticipo (85) | **29.4%** | **100%** (corrige 60) | 100% | 100% |
| sol_anticipo (69) | 81.2% | 100% | 100% | 100% |
| validez (50) | 94.0% | 100% | 100% | 100% |
| exp_tecnica (35) | 82.9% | 100% | 100% | 100% |
| pct_monto (49) | 83.7% (cota optimista) | 59.2% | **65.3%** | 53.1% |
| DIAS total (239) | 67.8% | **100.0%** (0 copia-malo, 0 falla) | 100.0% | 100.0% |

**Hallazgos**:
1. Los tres modelos alcanzan **239/239 exacto contra humano** en los 4 campos dias
   (exploratorio 189 + heldout congelado 50, deshelado: también 50/50). Nunca antes un
   benchmark había mostrado acuerdo LLM-humano perfecto a este n; la regla X (ceguera
   compartida copiando un regex malo) = 0 casos en dias.
2. **pct_monto es el único discrimen y NO es estadístico**: McNemar FP8-AWQ b=4,c=7 p=0.55;
   FP8-QWEN p=0.65; α Bonferroni 0.0167 → indistinguibles. Score ponderado 3 esquemas:
   AWQ > FP8 > QWEN siempre (Spearman=1, trivial: dias empatan). El punto favorece a AWQ,
   el IC no permite afirmarlo.
3. **Degradación honesta del claim original**: el "triple corona 61.8%" medía divergencia
   con referencia rota (caja-A: regex alucinaba en 29.4% de pago). Las afirmaciones firmes
   son: *el regex de pago_anticipo en producción es inservible (29%) y el LLM con guided +
   postproceso acierta el 100% contra humano en los 4 campos dias*; y *los tres finalistas
   son estadísticamente intercambiables en calidad*.
4. **Regla de desempate pre-registrada aplicada**: calidad humana en empate estadístico →
   se decide por métricas operativas: FP8 gana throughput (7.0 req/s, P50 0.17 s, 11.6 G)
   vs AWQ (5.2 req/s, 0.22 s) vs Qwen (4.5 req/s, 13.2 G). **Veredicto final: Llama 3.1
   8B-FP8 sigue siendo la elección, por vía operativa, con la nota de que AWQ tiene el
   mejor punto en pct sin significancia.**
5. ~~Pendientes~~ → CERRADO 15-sep: re-log y C5 en §7.1/§7.2. Limitaciones declaradas: el
   resumen impreso de C3 se calculó incluyendo la reserva (split posterior sobre el mismo
   JSON); F~P2 sin fugas verificadas; P3 necesitó re-draw con ajuste humano de forma — disclosure exacto en §7.2; C5 excluyó ítems
   ERROR en lugar de imputarles falla; intersecciones declaradas por modelo.
6. **A4.7 (planilla `audito_a47_12.csv`)**: pendiente TU auditoría de sustancia del 239/239
   (gate pre-registrado: ≥7/8 casos-M de pago sustantivos → firme; ≤6/8 → pago-M a caja-A y
   A4.5 a hipótesis). El revisor externo verificó el binomial: P(X≥7|n=8,½)=3.5%.
   Nota: el C5 ya muestra que el 100% de dias es robusto al prompt bajo humano — la
   auditoría A4.7 sigue siendo necesaria porque lo que no cubre es la sustancia de las
   etiquetas de agosto (circularidad de hints compartidos), no la consistencia del modelo.

### 7.1 C4 brittleness (150 ítems fijos seed 1234, same parse/guided) + re-log A2

| Modelo | base | P1 sin-hint | P2 system reescrito |
|---|---|---|---|
| Llama-FP8 | 98/150 | 97 (−1) | **73 (−25)** |
| Llama-AWQ | 97/150 | 97 (+0) | 82 (−15) |
| Qwen3-8B-FP8 | 88/150 | 88 (+0) | **99 (+11)** |

**Hallazgo**: los hints por campo **no** importan con guided+texto ancla (Δ≈0 sin hint); la
redacción del system sí interactúa con el modelo: con P2 el ranking se INVIERTE
(QWEN > AWQ > FP8). Implicaciones: (a) en producción con prompt congelado el riesgo queda
acotado — pero la elección entre finalistas no es robusta a un rediseño de prompt;
(b) el prompt base no es óptimo para Qwen (+11); (c) para la tesis: reportar el benchmark
como *"modelo × plantilla de prompt"*, no como calidad libre del modelo.
Re-log A2: Gemma raw_full **estrictamente válido 1.0%**, pero **primera-línea válida 100%**
= contrato GGUF-plain de primera línea (no guided); Ministral re-log 8/9 agree. C3-QWEN tuvo 1
item >4096 ctx (ERROR:gen, cuenta como falla — publicado).

### 7.2 C5 replicación ciega del prompt-flip (15-sep, protocolo completo ejecutado)

**Origen ciego**: F (lista de 7 funcionalidades abstractas, escrita sin consultar el diff
base↔P2; verificación automática de fuga: 0 cadenas ≥25 chars compartidas F~P2) → P3 y P4
generadas por **Mistral-7B v0.3 (temp 1.0, dos muestras)**, que nunca vio resultados de C4.
**Re-draw de P3 — disclosure exacto**: el primer sorteo salió auto-referencial
("Proporciono un prompt para…") y cortado; se regeneró con la MISMA F, modelo y temperatura,
pero con un **agregado de forma mío en el prompt generador** ("no escribas sobre el prompt:
escríbelo directamente", max_tokens 220→240) y un filtro de usabilidad (sin marco meta,
≥50 palabras, cierra con puntuación, menciona NULL; primero pasante de 4 sorteos). La
motivación fue la forma de la salida fallida, no ningún resultado de C4 — pero hubo ajuste
humano en el medio y se consigna como tal. P1 excluido del conteo (mide remoción de hint).

**Agreement-vs-pseudo sobre intersección limpia (149-150/modelo, sin ERRORs en ninguna variante)**:

| | base | P2 | P3 | P4 |
|---|---|---|---|---|
| FP8 | .653 | .487 (−16.6) | .480 (−17.3) | .480 (−17.3) |
| AWQ | .647 | .633 (−1.4) | .480 (−16.7) | .487 (−16.0) |
| QWEN | .591 | .611 (+2.0) | .490 (−10.1) | .517 (−7.4) |

**Conteo de replicación de la inversión FP8→QWEN: 3/3** → estado pre-registrado "patrón
estable modelo × plantilla", con todas las consecuencias.

**Pero el re-score contra HUMANO (P2 sobre 288) acota el hallazgo — y es la versión correcta
para la tesis — CONDICIONADA A A4.7**:
- **dias**: los tres mantienen **1.000 exacto bajo P2** (239/239) — *bajo la hipótesis de que
  las etiquetas de agosto son sustantivas*. Ojo lógico: C5-288 y C3 miden contra las MISMAS
  etiquetas agosto, así que el 100% "robusto al prompt" NO es independiente de la
  circularidad — hereda el gate de A4.7 (≥7/8 sustantivos) y no puede invocarse como su
  refutación. Lo que C5 sí prueba sin condiciones: robustez *relativa* (el patrón de deltas
  entre modelos se conserva) y el mecanismo de abstención en pct. La caída de agreement en dias = los modelos se ALEJAN de la referencia rota (pago),
  no de la verdad.
- **Todo el flip vive en pct** (el único campo donde pseudo≈verdad): FP8 59.2→**8.2%**,
  AWQ 65.3→51.0, QWEN 54.2→**81.2%** (≈ techo del regex 83.7).
- **Mecanismo**: abstención, no extracción errónea — FP8 dice NULL en 29-30/30 pct bajo las
  reescrituras (schema válido; no es el bug de Gemma, verificado). Sistemas reescritos más
  estrictos con "no rellenar" → FP8 se sobre-abstiene en el campo difícil, QWEN extrae mejor.

**Recipe operativa (queda como decisión de ingeniería, no solo hallazgo)**: el prompt de
producción del Pipeline 2 es un **artefacto versionado y congelado**; cualquier cambio de su
redacción exige re-benchmark de pct (el único campo sensible). Tie-break recalculado bajo el
prompt de producción real (base): sin cambio — dias empate 100% (bajo el gate A4.7), pct indistinguible
(McNemar p≥0.55), throughput elige **Llama-FP8**. Nota para la defensa: bajo reescrituras,
la robustez favorece a QWEN; el veredicto FP8 es *condicional al prompt congelado*, y eso se
declara así explícitamente.


---

## 9. VEREDICTO v3 (06-oct, sesión 23 — reemplaza a §7; plan de corrección F1-F6)

### 9.1 Qué estaba mal
`extraction_postprocess.parse_response()` aplicaba overrides deterministas ("reglas") sobre el
TEXTO **antes** de mirar la respuesta del LLM, y ese mismo parse se usó en benchmark y producción.
Consecuencia medida (F1, `scripts/rescore_crudo.py`): un "modelo" que siempre responde `NULL`
saca **239/239** en C3 dias (heldout 50/50) y empata exacto con los tres finalistas en los 720
ítems no-pct de Fase 5 (v1 418/720, v2 640/720) y en los 120 no-pct de C4/C5 (72/120 en todas
las variantes). Todo lo publicado como calidad del modelo en esos campos era calidad de las reglas.

### 9.2 Los modelos, medidos sobre su salida cruda (únicas cifras limpias del modelo)

| Métrica | Llama-3.1-8B-FP8 | Llama-3.1-8B-AWQ | Qwen3-8B-FP8 |
|---|---|---|---|
| C3 dias (239 humanos), crudo | 183 (76,6%) | 175 (73,2%) | **235 (98,3%)** |
| C3 pct (49 humanos) | 29 | 32 | 26 |
| GT 12 campos hybrid (2.400 humanos), LLM solo | 828 (34,5%) | — | **1.800 (75,0%)** |
| C4/C5 no-pct bajo reescritura P3/P4 (120), crudo | 67→38 | 63→30/36 | 75→65/74 |

- FP8 vs Qwen en dias: McNemar exacto b=0, c=52, p≈4·10⁻¹⁶; diferencia −21,8 pp [−27,0; −16,5].
- pct: sin diferencia detectable; con n=49 la **diferencia mínima detectable es ≈22 pp** (poder 0,8,
  α=0,0167). Afirmación permitida: "no se detectan diferencias mayores a ~22 pp".
- **Decisión: Qwen3-8B-FP8** es el modelo del fallback (mejor por amplio margen y estable bajo
  reescritura de prompt). FP8 sigue siendo 1,56× más rápido, pero no hay empate de calidad que desempatar.

### 9.3 Ablación contra GT humano (12 campos hybrid × 200; `docs/ablacion_gt.md`)

| Brazo | Qwen3-8B-FP8 | Llama-3.1-8B-FP8 |
|---|---|---|
| modal (responder siempre el valor más frecuente) | 1.629 (67,9%) | 1.629 (67,9%) |
| regex de producción (in-sample) | 1.252 (52,2%) | 1.252 (52,2%) |
| reglas deterministas solas (in-sample) | 1.800 (75,0%) | 1.800 (75,0%) |
| **LLM solo (limpio)** | **1.800 (75,0%)** | 828 (34,5%) |
| pipeline producción: regex → regla → LLM | 2.250 (93,8%) | 2.160 (90,0%) |
| pipeline alternativo: regla → regex → LLM | **2.282 (95,1%)** | 2.192 (91,3%) |

Lecturas:
1. **El rendimiento del sistema lo ponen las capas deterministas.** El LLM decide solo **208/2.400
   (8,7%)** ítems, casi todos `num_componentes_formula`, y acierta 103 (Qwen) / 13 (FP8).
2. En `num_componentes_formula` (único campo sin regla) Qwen acierta 95/200, **por debajo de la
   baseline modal (141/200)** → candidato a **HOLD** (no promover).
3. El regex de producción, cuando acierta poco, **pisa** reglas mejores (validez 181 vs 200,
   solicitud 195 vs 200): poner la regla antes que el regex sube el sistema a 95,1%. **Pendiente**:
   confirmar fuera de muestra (estratificado-75) antes de cambiar el orden en producción.
4. La baseline modal (67,9%) supera al regex de producción (52,2%): los campos son mayormente
   plantilla; reportar siempre contra esa baseline.

### 9.4 Producción (F4; run `llmfb-qwen3-8b-fp8-3fe595a69e69bfd4`, tabla `document_features_v3`)
- 287.894 filas-campo, 0 fallidas; las reglas resuelven 252.182 (87,6%) en CPU, el LLM 35.712.
- La compuerta histórica (regla solo si el LLM no decía NULL) **suprimió 76.665 valores** que la
  regla sí produce (47.247 de ratios; su corrección se valida en F5), p. ej.:
  `ratio_liquidez_minimo` 876 → **25.951**, `ratio_endeudamiento_maximo` 3.262 → **25.434**
  (coherente con el GT: 85-86% de pliegos fijan esos ratios).
- `num_documentos_pago`: el regex devolvía el índice del primer ítem (1 en 25.343 docs, incluso
  años) → corregido (6/7, como el GT). `num_pasos_verificacion`: los misses usan ordinales ("1º")
  → regex con ordinales coincide con Qwen en los 3.921 casos.
- `dias_aceptacion_rechazo_factura` (15), `dias_notificacion_fuerza_mayor` (7) y
  `dias_presentacion_garantia` (10) son ≥99,99% constantes → **indicadores binarios** de cláusula
  estándar, no magnitudes (`docs/varianza_campos.md`).
- Producción NO promovida todavía: F5 (auditoría humana ciega) y F7 (aprobación).

### 9.5 Estado de las afirmaciones

| Firme (con evidencia limpia) | Hipótesis (falta F5 fuera de muestra) | Retirada |
|---|---|---|
| Qwen3-8B-FP8 > Llama-FP8 sobre salida cruda (C3 y GT-12) | Precisión real de las reglas en la población de producción | "239/239 contra humano" |
| Un postproceso compartido enmascara diferencias entre modelos | Orden regla → regex mejora producción | "finalistas intercambiables" |
| La compuerta LLM suprimió **76.665** valores que la regla sí produce (47.247 de ratios) | Que esos 76.665 sean correctos (validación F5) · `num_documentos_pago` y `num_pasos` 200/200 fuera de muestra | "87,4% Fase 5 v2" |
| 3 campos son constantes de plantilla | `num_componentes_formula` no es extraíble con calidad suficiente | "dias robusto al prompt" (C5) |
| | | "FP8 por desempate operativo" |

### 9.6 Contribución metodológica (para el capítulo)
**Cuando la capa de postproceso determinista es compartida entre evaluación y producción, puede
enmascarar por completo las diferencias entre modelos.** Recomendación operativa adoptada:
reportar siempre (a) salida cruda del modelo, (b) sistema completo y (c) una baseline trivial
(siempre-NULL y modal) pasada por el mismo postproceso; si la baseline empata al modelo, la
métrica no mide al modelo.

### 9.7 Limitaciones vigentes
- Reglas y regex fueron desarrollados sobre `gt_por_campo` (in-sample): sus cifras son cotas
  optimistas hasta anotar el estratificado-75.
- Un solo anotador (el autor); IAA-25 pendiente (κ).
- AWQ no se re-corrió en la ablación GT-12 (sí en C3 crudo).

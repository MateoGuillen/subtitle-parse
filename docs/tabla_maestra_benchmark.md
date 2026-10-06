# Tabla maestra del benchmark de modelos locales (tesis)

> **Fuente primaria**: `data/processed/llm_benchmark/tabla_maestra_benchmark.csv`
> (generado desde `Fase5_*.json` + `Conc_*.json`, 12/13-sep-2026).
> **Hardware único**: RTX 4070 Ti SUPER 16 GB · driver 615 · CUDA 13.4 · vLLM 0.29.0 · llama-server (commit 6d9c82e).

## 1. Tabla maestra (ordenada por agreement Fase 5)

| # | Modelo | Formato | Peso | Ctx | Guided | GT-100 | Pseudo-867 (agree) | Single P50 / P95 | Conc. techo | C techo | P50@C1 | VRAM conc. |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Llama 3.1 8B-FP8 | FP8 | 8.5G | 8192 | xgrammar | 98/100 | **61.8%** (536) | **0.17s** / 0.28s | **7.0 req/s** | 16 | 0.19s | 11.6G |
| 2 | Llama 3.1 8B-AWQ | AWQ | 5.4G | 8192 | xgrammar | 98/100 | 61.4% (532) | 0.22s / 0.38s | 5.2 req/s | 32 | 0.38s | 11.6G |
| 3 | Qwen3-8B-FP8 | FP8 | 8.9G | 4096 | xgrammar | 98/100 | 54.7% (474) | 0.24s / 0.39s | 4.5 req/s | 32 | 1.00s | 13.2G |
| 4 | Qwen3-14B-AWQ | AWQ | 9.4G | 4096 | xgrammar | 98/100 | 53.1% (460) | 0.38s / 0.64s | 3.2 req/s | 16 | 0.43s | 15.2G |
| 5 | Ministral 3 14B-Q4_K_M | GGUF | 7.7G | 16k/4slots | plain+parse | 97/100 | 50.2% (435) | 0.55s / 0.77s | 4.85 req/s | 8 | 0.41s | 10.5G |
| 6 | Mistral 7B v0.3-AWQ | AWQ | 3.9G | 8192 | xgrammar | 97/100 | 49.4% (428) | 0.29s / 0.96s | 4.3 req/s | 32 | 0.69s | 11.5G |
| 7 | EXAONE-3.5-7.8B-AWQ | AWQ | 5.0G | 8192 | xgrammar | 98/100 | 48.4% (420) | 0.26s / 0.43s | 5.05 req/s | 32 | 0.47s | 11.5G |
| 8 | Gemma 4 12B-Q4_K_XL | GGUF | 6.3G | 16k/4slots | plain+parse | 97/100 | 48.4% (420) | 1.45s / 1.87s | 0.9 req/s | 8 | 1.53s | 9.0G |
| 9 | Granite 4.2 8B-Q4_K_M | GGUF | 5.2G | 16k/4slots | plain+parse | 97/100 | 48.2% (418) | 1.09s / 1.24s | 2.5 req/s | 4-8 | 1.00s | 8.0G |
| 10 | Mistral Small 24B-AWQ | AWQ | 14G | 2048 | xgrammar¹ | 49/50 | 48.2% (418) | 2.99s / 5.28s | 0.27 req/s² | batch | — | 14.7G |

¹ Solo offline (el serve + cpu_offload cuelga el EngineCore en vLLM 0.29).
² Un solo `generate()` batch de 24 (max_num_seqs=4); sin latencias por request.

## 2. Muestras y método (para la sección metodológica)

- **GT-100** (`ground_truth_5_campos.csv`): 100 anotaciones humanas, 5 campos
  (`dias_validez_garantia`, `anos_experiencia_minima_tecnica`, `dias_pago_anticipo`,
  `dias_solicitud_anticipo`, `pct_monto_referencia_tecnica`), 20 por campo. Métrica: accuracy exacta.
- **Pseudo-867** (`pseudo900.json`, seed 42): 180×4 + 147 docs (el campo `pct_monto` es raro en
  producción) con valor regex de producción (`conf≥0.9`, no nulo, ≠0), excluyendo las GT-100.
  Métrica honesta: **agreement con el regex**, no accuracy.
- **Single request**: corrida secuencial offline (vLLM `LLM.generate`, 1 hilo) o llama-server
  secuencial; temp 0.0, max_tokens 64, guided regex `^(-?\d+(?:[.,]\d+)?|NULL)$` (vLLM/xgrammar).
- **Concurrencia** (`scripts/benchmark_concurrency.py`): servidor online + C hilos; workset 64
  prompts vLLM (C=1,4,8,16,32) / 32 GGUF (C=1,2,4,8, truncados a 15k chars, 4 slots de 4096).
  Se reporta techo de req/s, P50/P95 por nivel, errores, tok/s, pico VRAM y util GPU.
- **Reproducibilidad**: flags por modelo en `models/candidates.yaml`
  (`gt100_v029` / `fase5_867` / `conc`); resultados crudos en
  `data/processed/llm_benchmark/multimodel_results/` (`Fase5_*.json`, `Conc_*.json`).

## 3. Lectura ejecutiva (3 viñetas para la tesis)

1. **Convergencia**: con guided decoding + postproceso determinista, 7 arquitecturas empatan en
   GT curadas (97-98) y 4/5 campos pseudo saturan idéntico → el desempate es **latencia/VRAM**,
   no calidad. El único campo discriminador es `pct_monto_referencia_tecnica` (fracciones `0,50`).
2. **Doble corona Llama-FP8**: 1.º en agreement (61.8%), 1.º en single (P50 0.17s) y 1.º en
   throughput (7 req/s) → selección para el Pipeline 2 con triple sustento. FP8 ≈ AWQ en calidad.
3. **Límites físicos**: saturación GPU a C=8-16; Gemma no escala (compute-bound); Qwen3-14B sin
   margen VRAM (15.2/16G); Small-24B inviable online (offload PCIe, 0.27 req/s).

## 4. Veredicto

**Llama 3.1 8B-FP8** para el fallback del Pipeline 2 (≈324K docs, punto operativo C=8-16).
Alternativa inmediata: Llama 3.1 8B-AWQ. Descartados: Small-24B (online imposible),
Qwen3-14B (sin margen VRAM), Gemma/Granite (throughput bajo).

> ⚠ **SUPERSEDADO 15-sep por el Veredicto v2** (`docs/endurecimiento_benchmark_plan.md` §7):
> la cifra "61.8% agree" medía divergencia contra una referencia rota (el regex de
> `dias_pago_anticipo` acierta solo 29.4% contra humano). Sobre los 288 ítems humanos:
> FP8/AWQ/Qwen3-8B-FP8 empatan en **100% exacto en los 4 campos dias** (239/239, incl.
> heldout congelado) y son **indistinguibles en pct** (McNemar p≥0.55). La elección de
> **Llama-FP8 se mantiene por desempate operativo** (throughput/latencia), no por calidad.
> La cifra "~324K docs del fallback" está obsoleta: la población real es conf=0 (regex
> no-match) ≈260K field-rows (TODO: COUNT DISTINCT + re-lista de campos, ver changelog en
> `benchmark_local_resumen_general.md` §1).

### 4.1 Accuracy humana C3 (top-3, 288 ítems, parse post-AloLab, guided)

| Campo (n) | regex | FP8 | AWQ | Qwen3-8B-FP8 |
|---|---|---|---|---|
| dias_pago_anticipo (85) | 29.4% | 100% | 100% | 100% |
| dias_solicitud_anticipo (69) | 81.2% | 100% | 100% | 100% |
| dias_validez_garantia (50) | 94.0% | 100% | 100% | 100% |
| anos_exp_minima_tecnica (35) | 82.9% | 100% | 100% | 100% |
| pct_monto_ref_tecnica (49) | 83.7%† | 59.2% | 65.3% | 53.1% |
| **DIAS total (239)** | 67.8% | **100.0%** | **100.0%** | **100.0%** |

† cota optimista: el regex de pct fue validado sobre estos mismos archivos (Sesión 16).
McNemar pct: FP8-AWQ p=0.55, FP8-QWEN p=0.65 (α Bonferroni 0.0167) → sin diferencia.
> ⚠ **FASE 5 v2 (27-sep)**: FP8 **87.4%**, AWQ 87.0%, Qwen3-8B-FP8 80.3% sobre pseudo900
> regenerado con referencia gateada (detalle en plan endurecimiento §8). El ranking y el
> veredicto no cambian; el número ahora sí mide calidad.
**C5 (15-sep, replicación ciega)**: el "prompt-flip" replicó 3/3 reescrituras ciegas del
system (P2/P3/P4, generadas por Mistral-7B sin exposición a resultados) — pero el re-score
contra humano acota: los 239 dias siguen en 1.000 bajo P2; el flip vive solo en pct
(FP8 59→8% por sobre-abstención NULL; QWEN 54→81%). Veredicto FP8 condicional al prompt de
producción congelado (artefacto versionado; receta en plan §7.2).
**Condición de lectura para TODAS las cifras "vs humano" de §4.1 y C3/C5**: se miden contra
las etiquetas de agosto (`gt_por_campo`), cuya sustancia está bajo auditoría pendiente
(gate A4.7: ≥7/8 casos-M de pago; plan §6) — firmezas operativas, tesis en curso.

## 5. Glosario de métricas (qué mide cada columna y por qué importa)

### Formato (AWQ / FP8 / GGUF)
- **Qué es**: el esquema de cuantización del checkpoint. AWQ = 4-bit con escalas por canal
  (nativo vLLM, kernel Marlin); FP8 = 8-bit flotante (nativo vLLM/Hopper+Ada); GGUF = 4-bit
  (`Q4_K_M`) para CPU/llama.cpp, aquí corrido en GPU vía `llama-server`.
- **Por qué importa**: define el backend posible (vLLM vs llama-server), la VRAM base y el
  techo de velocidad. Hallazgo útil: FP8 ≈ AWQ en calidad con menor latencia.

### Peso (GB en disco)
- **Qué es**: tamaño de la descarga (caché HF o `./models/gguf/`).
- **Por qué importa**: costo de despliegue y pista de la VRAM mínima. Total del estudio:
  55G HF + 20G GGUF.

### Ctx (ventana de contexto)
- **Qué es**: `max_model_len` (vLLM) o `--ctx-size / -np` slots (llama-server, que **reparte**
  el ctx entre slots: 16k/4 = 4096 por slot).
- **Por qué importa**: los pliegos reales superan 8k tokens; un ctx chico obliga a trocear
  (Small-2048 truncó 6/24; GGUF truncó a 15k chars). Es restricción de producción, no detalle.

### Guided (decodificación guiada)
- **Qué es**: restricción a nivel de logits a la gramática `número|NULL` (vLLM+xgrammar) vs
  texto libre + parse por regex (GGUF plain).
- **Por qué importa**: con guided el output son 1-3 tokens (7× más rápido, sin postproceso);
  sin guided hay que parsear y falla más. Por eso GGUF y vLLM van en tablas separadas:
  distinto backend de constrained decoding.

### GT-100 (accuracy curada)
- **Qué es**: % exacto contra 100 anotaciones humanas (20×5 campos), temp 0.0.
- **Por qué importa**: única métrica de calidad "real". Límite: 100 muestras no discriminan
  (todos 97-98) → hizo falta la Fase 5.

### Pseudo-867 (agreement)
- **Qué es**: % de coincidencia con el valor regex de producción en 867 docs
  (`conf≥0.9`, excluye GT-100).
- **Por qué importa**: escala donde GT-100 no llega. **Trampa**: mide divergencia regex↔LLM,
  no verdad (textos ambiguos multi-número, artefactos de parse). Solo `pct_monto` discrimina.

### Single P50 / P95 (latencia secuencial, 1 hilo)
- **Qué es**: mediana y percentil 95 del tiempo por request en corrida secuencial
  (Fase 5, prompts reales largos).
- **Por qué importa**: P50 = caso típico (costo total del Pipeline 2); P95 = cola
  (prompts largos). Un P95 >> P50 avisa de textos patológicos (p.ej. Mistral 0.29→0.96s).

### Conc. techo (req/s máximo sostenido)
- **Qué es**: throughput máximo barriendo C=1→32 (vLLM) u 8 (GGUF): `N / wall_clock`.
- **Por qué importa**: es **la** métrica de dimensionamiento (docs/hora del Pipeline 2:
  7 req/s ≈ 25K docs/h). Incluye prefill+decode+cola reales, no teoría.

### C techo (concurrencia del techo)
- **Qué es**: nº de hilos donde se alcanza el techo.
- **Por qué importa**: punto operativo (C=8-16 en 8B); más hilos solo inflan P50 sin subir
  req/s. Pedirlo mayor es tirar latencia.

### P50@C1 (latencia base online)
- **Qué es**: mediana con 1 solo hilo contra el servidor (incluye overhead HTTP/cola).
- **Por qué importa**: compara contra el single offline (siempre algo mayor) y es la base
  para estimar degradación bajo carga.

### VRAM conc. (pico, MiB)
- **Qué es**: `nvidia-smi` muestreado cada 0.5s durante el sweep (pesos + KV cache + activaciones).
- **Por qué importa**: viabilidad en 16 GB (Qwen3-14B a 15.2G = sin margen; Small 14.7G con
  offload = inviable). Ojo: depende de `gpu_memory_utilization` y `max_num_seqs` configurados.

### tok/s (tokens de salida por segundo) — en `Conc_*.json`
- **Qué es**: `Σ completion_tokens / wall_clock` (del `usage` del servidor).
- **Por qué importa**: mide decode puro. Con guided los outputs son 1-3 tokens, así que
  tok/s bajo NO significa modelo lento (Ministral: 21 tok/s pero 4.85 req/s). **No comparar
  tok/s entre guided y plain**: longitudes de output distintas.

### agree / errors bajo concurrencia — en `Conc_*.json`
- **Qué es**: agreement y fallos (HTTP/ctx-overflow) por nivel de C.
- **Por qué importa**: verifica que la carga no corrompe resultados (agree estable en todos
  los niveles ✓) y que los errores son deterministas (la muestra de 8193 tokens falla igual
  en C=1 y C=32 → robustez, no flakiness).

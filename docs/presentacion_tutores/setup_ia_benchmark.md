# Setup de IA local y benchmark de modelos — detalle completo

> Documento de apoyo para tutores: describe el setup de IA usado para la extracción de campos de
> pliegos (Pipeline 2) y la evolución completa de modelos, prompts y precisión. Todos los números
> están verificados contra `docs/benchmark_resultados.md`, `AGENTS.md` y `docs/benchmark_qwen38_session.md`.

## 1. Setup de hardware y software

| Componente | Detalle |
|---|---|
| GPU | NVIDIA **RTX 4070 Ti SUPER — 16 GB VRAM** |
| VRAM útil | ~15.6 GB (tras matar NVIDIA Overlay, Warp, Edge, SearchApp → libera ~1.3 GB) |
| RAM | 32 GB |
| SO | Windows 11 + **WSL2 (Ubuntu)** · driver 581.29 · CUDA 12.4 |
| Motor principal | **vLLM 0.23.0** (V1 engine) en WSL2, entorno `/root/vllm-env/bin/python` |
| Runtime secundario | **llama.cpp `llama-server.exe` en Windows** (nightly b10566, CUDA 12.4, puerto 1234) |
| Formato de modelo | **AWQ (nativo, recomendado)**; GGUF experimental/deprecated en vLLM |

### Restricciones clave del entorno WSL2

- **`VLLM_USE_V2_MODEL_RUNNER=0` obligatorio**: WSL2 no tiene UVA (Unified Virtual Addressing), y el
  V2 Model Runner (Qwen3, Llama, Mistral) lo requiere. Sin el flag, los modelos no cargan.
- **vLLM 0.25.x no funciona en WSL**: el MRv2 (default) requiere UVA → se fija **0.23.0** como versión
  estable.
- **`awq_marlin`** (kernel más rápido) solo es compatible con arquitectura **Mistral/Llama**; Qwen y
  Phi caen al kernel `awq` normal (más lento).
- **Limpiar VRAM en Windows** antes de correr: `taskkill /F /IM "NVIDIA Overlay.exe" /T` + Warp +
  msedge + SearchApp.
- En WSL, usar `127.0.0.1` (no `localhost`) para conexiones al servidor.

### Por qué existe un runtime secundario (llama-server en Windows)

Para probar **Qwen3.8-27B** (Apache 2.0, sin gating) fuera de WSL2: el vhdx de WSL estaba en `D:` con
solo 0.26 GB libres y el modelo pesa 12.24 GB. Se instaló **llama.cpp nightly** en
`C:\models\llama_cuda\` (`llama-server.exe --version 0.2.0-dev bb4caa754`) con CUDA 12.4. Hubo que
copiar `ggml-cuda.dll` + `cublas*.dll` + `cudart64_12.dll` para que el backend CUDA cargara
(sin eso, `LoadLibrary 126` → corría solo en CPU a 1.8 tok/s).

## 2. GGUF vs AWQ

| Formato | Estado en vLLM | Performance | VRAM | Recomendación |
|---|---|---|---|---|
| **AWQ** (W4A16) | Nativo, `awq_marlin` | ✅ Excelente | Bajo | ✅ **Recomendado** |
| FP8 / GPTQ | Nativo | ✅ Bueno | Medio/bajo | Alternativas |
| **GGUF** | Plugin externo (comunitario) | ⚠️ Variable | Variable | ❌ Deprecado |

Impacto real (Mistral 7B): AWQ es **3× más rápido** que GGUF (1.61s vs 4.90s) y usa **−29% VRAM**
(3.89 GiB vs ~5.5 GiB).

## 3. Evolución del benchmark (muestras por modelo)

### GGUF (ronda inicial, vLLM en WSL)

| Modelo | Cuant | Muestras | Precisión | Velocidad | Notas |
|---|---|---|---|---|---|
| Qwen2.5 14B | Q5_K_L | 40+80 | 97.5% → 100% | 6.5-7.1s | primer viable |
| **Qwen3-14B** | Q5_K_M | 60 | **100% (60/60)** | 5.10s · 13-19 tok/s · 15.23× | campeón GGUF |
| Qwen3-8B | Q5_K_M | 60 | 100% (60/60) | 4.50s | necesita fix `enable_thinking=False` |
| Phi-4 14B | Q4_K_M | 60 | 100% (60/60) | 6.09s | robusto, lento |
| **Mistral 7B v0.3** | Q5_K_M | 60 | **100% (60/60)** | **4.90s** | nuevo campeón GGUF (−51% tamaño vs Qwen3-14B) |
| Llama 3.1 8B | Q5_K_M | — | ❌ | — | tokenizer gated (necesita HF_TOKEN) |
| Gemma2 27B | Q4_K_M | — | ❌ | — | OOM (no cabe en 16 GB) |
| Qwen2.5 7B | Q4_K_M | 40+ | ❌ 0% | ~9s | omite 1 campo por título |

### AWQ (formato nativo, ronda definitiva)

| Modelo | Kernel | Muestras | Struct OK | F1 vs GT | Tiempo | Output tok/s | VRAM | Concurrencia |
|---|---|---|---|---|---|---|---|---|
| **🥇 Mistral 7B v0.3 AWQ** | **awq_marlin** | 10 | **100%** | **94.0%** | **1.61s** | **64-77** | **3.89 GiB** | **35.22×** |
| **🥇 Llama 3.1 8B AWQ** | **awq_marlin** | 5 | **100%** | **96.4%** | **1.93s** | 55-68 | 5.39 GiB | 29.21× |
| Qwen2.5 14B AWQ | awq | 5 | 100% | 93.3% | 2.46s | — | 9.38 GiB | 8.18× |
| Qwen3-8B AWQ | awq | 60 | 100% | — | 4.80s | — | ~5.7 GiB | 24.58× |
| Qwen3-14B AWQ | awq | 60 | 100% | — | 5.29s | — | 9.44 GiB | 9.00× |
| Phi-4 14B AWQ | awq | 10 | 90% (9/10) | — | 4.47s | — | 8.51 GiB | 9.95× |
| Gemma 3 12B AWQ | awq | — | ❌ | — | — | — | — | requiere bfloat16, AWQ usa float16 |

### Runtime Windows (llama.cpp) — Qwen3.8-27B

| Métrica | Valor |
|---|---|
| Modelo | `Qwen3.8-27B-UD-Q3_K_XL.gguf` (12.24 GB) |
| Precisión (100 GT humanas, 5 campos duros ×20) | **97/100 = 97%** |
| Tiempo | 1.41s/muestra (141s/100) |
| Generación | 33-39 tok/s |
| Prompt | 1015-1432 tok/s |
| Nota | Fixes: `-c 16384` (n_ctx_slot 2048 crasheaba con textos de 2064 tok), `cache_prompt False` (bug KV LCP causaba OOM), reinicio batch cada 10 |

## 4. Benchmark del fallback LLM (460 muestras, campos hybrid conf<0.8)

| Etapa | Precisión | Tiempo | Notas |
|---|---|---|---|
| Baseline (Sesión 9) | 274/460 = **59.6%** | — | 106 fallos: 51 eran NULL legítimos (GT=NULL y el LLM acierta) |
| Seed fijo (Sesión 10) | 271/460 = **58.9%** | 304.5s | Llama 3.1 8B AWQ, 23 campos |
| Mistral 7B (Sesión 11) | 289/460 = **62.8%** | 222.6s | peor que Llama |
| **"Primera línea" (Sesión 11)** | **386/460 = 83.9%** | 347.5s | prompt: número en 1ª línea + max_tokens=256 + parser first-line |
| **Guided decoding (Sesión 12)** | **386/460 = 83.9%** | **51.4s** | regex vLLM; misma precisión, **7× más rápido** (8.95 vs 1.32 docs/s) |
| Few-shot retrieval (Sesión 13) | ❌ 85%/80% | — | descartado: empeoraba el baseline 95% |
| **AloLab (Sesión 14)** | **100/100** | ~9-10s/ronda | golden set humano de 100 |

**Campos persistentemente bajos** (causa real: el texto no contiene el valor → NULL correcto):
`dias_pago_anticipo` (25%), `dias_solicitud_anticipo` (30%), `anos_experiencia_minima_tecnica` (30%).

## 5. Proceso AloLab (agente como meta-analista) → 100%

Ciclo artesanal repetible:
1. `scripts/alolab_collect_errors.py` corre Llama 3.1 8B-AWQ sobre las 100 GT humanas y exporta SOLO
   los errores (campo, nro, gt, salida LLM, hint actual, texto completo) → `errors_round{N}.json`.
2. El agente diagnostica cada error (comparando GT vs salida vs hint vs texto).
3. Reescribe la hint o agrega una regla de postprocess determinista.
4. Re-run → se repite hasta 0 errores.

| Ronda | Score | Fix aplicado |
|---|---|---|
| 1 | 95/100 | 3 hints reescritas + regex `personales?`→`personal(?:es)?` |
| 2 | 95/100 | 5 FPs nuevos → 2 reglas de postprocess + regla `dias_solicitud_anticipo` |
| 3 | 95/100 | hint de `dias_validez_garantia` limpia + conversión determinista meses→días |
| 4 | **100/100** | — |

**Insights**:
- Los falsos positivos (GT=NULL) se arreglan mejor en **postprocess determinista** que en el prompt.
- Una hint que enseña un valor concreto induce sobre-extracción en pliegos de plantilla vacía (NULL).
- El regex `personales?` NO matchea "personal" (solo "personales"); correcto: `personal(?:es)?\b`.
- Conversión determinista meses→días {1:30, 2:61, 3:91, 6:182, 12:365} reemplaza la aritmética del LLM.

## 6. Ingeniería de prompts (resumen)

- **Sesión 9** — 10 hints de campo mejoradas en `master_schemas_v2.json` (+40pp `anos_experiencia_minima`,
  +20 `num_documentos_pago`, +35 `ratio_liquidez_minimo`). Reglas agresivas del system prompt
  REVERTIDAS: mataban campos de 60%→0%.
- **Sesión 11** — hints verbosas con ejemplos de patrones: **+4.5 pp** vs cortas (se mantienen verbosas).
  Estrategia "primera línea": +14.8 pp.
- **Sesión 12** — **guided decoding** con regex de vLLM (`^(-?\d+(?:[.,]\d+)?|NULL)$`): el modelo solo
  puede emitir el número o NULL (~2 tokens de salida) → misma precisión, **7× más rápido**, sin
  post-procesamiento complejo.
- **Sesión 14** — ciclo AloLab: hints + postprocess → **100/100**.
- **Sesiones 15-16** — capa de postproceso determinista compartida (`extraction_postprocess.py`) y
  portes hybrid→regex (7 campos: +25 a +88 pp). Schema final: **69 regex / 12 hybrid**.

### Comparativa final de estrategias (26 campos, 5,066 juicios GT congelados)

| Estrategia | Acc media | Notas |
|---|---|---|
| **H** — híbrido (determinista→regex→LLM) | **65.1** | mejor en 12/26 campos |
| L_pp — LLM + postproceso | 63.1 | el postproceso aporta +12pp al LLM |
| R — regex v2 puro | 61.0 | |
| Ri — regex + postproceso | 60.9 | |
| L_raw — LLM bruto (primer número) | 51.0 | |

## 7. Proyección para el fallback LLM en producción (~324K docs, 12 campos hybrid)

| Escenario | Tiempo estimado | Calidad |
|---|---|---|
| **Llama 3.1 8B AWQ + 29× concurrencia** | **~5.8 horas** | F1 96.4% (recomendado: fiabilidad > velocidad) |
| **Mistral 7B AWQ + 35× concurrencia** | **~4 horas** | F1 94.0% |
| Mistral 7B GGUF + ~27× | ~1 día | 100% |
| HTTP server baseline | ~34 días | — |

**Conclusión**: el modelo elegido para producción es **Llama 3.1 8B AWQ** (100/100 en GT humano con
postproceso, F1 96.4%), que permite completar el fallback en ~3h con continuous batching en vLLM
0.23.0 (WSL2, `VLLM_USE_V2_MODEL_RUNNER=0`).

## Archivos de referencia

- `docs/benchmark_resultados.md` — benchmark completo (12+ modelos, GGUF + AWQ, F1, throughput).
- `docs/benchmark_qwen38_session.md` — sesión del runtime llama.cpp en Windows (Qwen3.8-27B).
- `docs/wsl2_vllm_setup.md` + `docs/wsl2_vllm_errors.md` — instalación y catálogo de errores vLLM/WSL.
- `src/etl/transformers/llm_provider.py` — `VLLMOfflineProvider` (offline API con continuous batching).
- `scripts/benchmark_multi_vs_gt.py`, `scripts/alolab_collect_errors.py`, `scripts/benchmark_llm_fallback.py`.
- `data/processed/alolab/errors_round{1..4}.json` y `comparativa_estrategias.csv`.

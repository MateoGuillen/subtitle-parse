# Ranking Combinado — Todos los Formatos (AWQ + GGUF + FP8)

> **Fecha**: 31 de agosto de 2026 · **GPU**: 16 GB VRAM (RTX 4070 Ti SUPER)
> **Tarea**: extracción de 8 variables de pliegos de licitación paraguayos (DNCP) contra pseudo-GT de 900 muestras.

---

## ⚠️ AVISOS DE VALIDACIÓN (verificados en internet el 31/08/2026)

Estos hallazgos cambian la interpretación del ranking y son OBLIGATORIOS antes de correr los modelos marcados:

1. **Gemma 4 + guided decoding tiene un bug documentado (vLLM #39130)** — `--reasoning-parser gemma4` + `enable_thinking=false` **silenciosamente bypasea xgrammar** (el FSM de gramática nunca corre). Verificado en GitHub: root cause en `is_reasoning_end()` que retorna `False` con prompts sin tokens thinking → la restricción de schema no se aplica, aunque el output *parece* JSON válido. **El fix se mergeó en PR #45553 (16/06/2026)** + seguimientos (#45832). Consecuencia: **hay que validar empíricamente** que el guided decoding funcione (ver smoke test abajo) y medir `schema_valid_json_rate` separado de `campos_correctos`.
2. **Granite 4.2 tiene razonamiento nativo (thinking)** — IBM lo lanzó el 25/08/2026 (6 días antes de esta fecha). Modos: full thinking (default), non-thinking, low-effort. **Hay que desactivar thinking antes de extracción** (`--default-chat-template-kwargs '{"enable_thinking": false}'` + `--reasoning-parser granite_thinking_parser`) y verificar en la salida cruda que no haya tokens de razonamiento.
3. **Qwen3.8-27B es de alto riesgo** — lanzado 14/08/2026 (2 semanas antes), arquitectura `Qwen3_5ForConditionalGeneration` (multimodal, Gated DeltaNet híbrida), quantizadores community reportan cuantización delicada (gate multiplicativo en q_proj). Es **stretch goal**, no candidato central.
4. **Qwen3-30B-A3B MoE**: estudio arXiv 2512.23029 confirma que en hardware de consumo el throughput cae fuerte >2 usuarios concurrentes (bottleneck de prefill + coordinación host-GPU, no del tamaño). En tu 16 GB el efecto es peor por menos KV cache.
5. **Comparar GGUF (llama-server, GBNF) con AWQ/FP8 (vLLM, xgrammar) en un mismo ranking de precisión es confuso metodológico** — son backends de guided decoding distintos con garantías distintas. La tabla abajo marca el backend usado.

### Smoke test obligatorio antes de Gemma 4 / Granite 4.2
```
1. Generar 5-10 respuestas con el schema de extracción real
2. Verificar: (a) ausencia de tags "thinking"/"<|channel>" en output crudo,
   (b) JSON parseable, (c) schema_valid_json_rate (no solo campos correctos)
3. Si el output no es JSON válido o tiene tags de razonamiento → ajustar flags o descartar modelo
```

---

## 🏆 Ranking 1 — MAYOR PRECISIÓN DE INFERENCIA (calidad de extracción)

**Dos columnas de precisión**: `Medido` = datos reales de nuestros benchmarks (fiable) · `Estimado` = expectativa a priori sin validar (NO mezclar con medido en la tesis).

| # | Modelo | Formato | Medido | Estimado | Concurrencia | VRAM | Repo HF | Notas |
|---|---|---|---|---|---|---|---|---|
| 🥇 | **Mistral Small 3.1 24B** | 🅰️🅶 | — | ~95 | ~4× | 12.0 GB | [AWQ](https://huggingface.co/stelterlab/Mistral-Small-24B-Instruct-2501-AWQ) | Apache 2.0. JSON nativo. El denso más capaz que cabe. 4K ctx forzado |
| 🥈 | **Qwen3-14B** | 🅰️🅵 | **100% (60/60)** | ~94 | ~9× | 7.4 GB | [AWQ](https://huggingface.co/Qwen/Qwen3-14B-AWQ) · [FP8](https://huggingface.co/Qwen/Qwen3-14B-FP8) | ✅ Probado. xgrammar |
| 🥉 | **Llama 3.1 8B** | 🅰️🅵 | **F1 96.4%** | ~93 | ~29× | 4.0 GB | [AWQ](https://huggingface.co/hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4) · [FP8](https://huggingface.co/RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8) | ✅ Probado (mejor F1 medido). awq_marlin |
| 4 | **Granite 4.2 30B** | 🅶 | — | ~94 | ~1× | 15.9 GB IQ4 | [GGUF](https://huggingface.co/bartowski/granite-4.2-30b-GGUF) | IBM. **12 idiomas + español**. ⚠️ **Requiere validar desactivación de thinking** (lanzado 25/08/2026). GBNF |
| 5 | **Granite 4.2 8B** | 🅶 | — | ~88 | ~20× | 5.5 GB | [GGUF](https://huggingface.co/bartowski/granite-4.2-8b-GGUF) | IBM. **12 idiomas + español**. Enterprise extraction. ⚠️ **Requiere validar desactivación de thinking**. GBNF |
| 6 | **Gemma 4 12B** | 🅶🅵 | — | ~92 | ~17× | 7.66 GB | [GGUF](https://huggingface.co/unsloth/gemma-4-12B-it-qat-GGUF) · [FP8](https://huggingface.co/RedHatAI/gemma-4-12B-it-FP8-Dynamic) | Jul 2026. Apache 2.0. Multilingüe. ⚠️ **Bug #39130: validar guided decoding NO bypaseado** |
| 7 | **phi-4-FP8 / Phi-4 14B** | 🅵🅰️ | **AWQ 90% (9/10)** | ~91 | ~1-10× | 7.3-14.7 GB | [FP8](https://huggingface.co/RedHatAI/phi-4-FP8-dynamic) · [AWQ](https://huggingface.co/stelterlab/phi-4-AWQ) | MS. Razonamiento estructurado. ⚠️ AWQ 90% |
| 8 | **Qwen3.8-27B** | 🅰️🅶🅵 | **97/100** (llama-server) | ~91 | ~1× | 15.5 GB | [AWQ](https://huggingface.co/cyankiwi/Qwen3.8-27B-AWQ-INT4) · [GGUF](https://huggingface.co/unsloth/Qwen3.8-27B-GGUF) · [FP8](https://huggingface.co/Qwen/Qwen3.8-27B-FP8) | ⚠️ **STRETCH GOAL** — arq. de 2 semanas, quant delicado. Offload necesario |
| 9 | **Qwen3-8B** | 🅰️🅵 | **100% (60/60)** | ~89 | ~24× | 4.1 GB | [AWQ](https://huggingface.co/Qwen/Qwen3-8B-AWQ) · [FP8](https://huggingface.co/Qwen/Qwen3-8B-FP8) | ✅ Probado. xgrammar |
| 10 | **Ministral-3-14B** | 🅶 | — | ~89 | ~12× | 8.5 GB | [GGUF oficial](https://huggingface.co/mistralai/Ministral-3-14B-Instruct-2512-GGUF) | Dic 2025. GGUF oficial Mistral. JSON mode. GBNF |
| 11 | **Mistral 7B v0.3** | 🅰️ | **F1 94.0%** | ~88 | ~35× | 3.7 GB | [AWQ](https://huggingface.co/solidrust/Mistral-7B-Instruct-v0.3-AWQ) | ✅ Probado. THROUGHPUT champion. awq_marlin |
| 12 | **EXAONE-3.5-7.8B** | 🅰️ | — | ~86 | ~34× | 3.9 GB | [AWQ](https://huggingface.co/LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct-AWQ) | Feb 2026. Text-only. ⚠️ Foco coreano-inglés, **validar español** |
| 13 | **Mistral Nemo 12B** | 🅰️ | — | ~86 | ~20× | 6.1 GB | [AWQ](https://huggingface.co/casperhansen/mistral-nemo-instruct-2407-awq) | Sweet spot 12B. awq_marlin |
| 14 | **Bielik-11B-v3.0** | 🅰️ | — | ~85 | ~22× | 5.5 GB | [AWQ](https://huggingface.co/speakleash/Bielik-11B-v3.0-Instruct-awq) | Dic 2025. ⚠️ Foco polaco, **validar español** |
| 15 | **gemma-3-27b / gemma-3-12b** | 🅰️ | — | ~85 | ~1-20× | 6-13.5 GB | [AWQ 12B](https://huggingface.co/gaunernst/gemma-3-12b-it-int4-awq) · [AWQ 27B](https://huggingface.co/gaunernst/gemma-3-27b-it-int4-awq) | Bien probados. 27B ajustado |
| 16 | **Ornith-1.5-9B** | 🅶🅰️🅵 | — | ~82 | ~20× | 5.5 GB | [GGUF](https://huggingface.co/ornith-ai/Ornith-1.5-9B-GGUF) · [FP8](https://huggingface.co/cyankiwi/Ornith-1.5-9B-AWQ-FP8) | Ago 2026. MIT. Generalista |
| 17 | **Instella-MoE-16B-A3B** | 🅰️ | — | ~80 | ~15× | 7.9 GB | [AWQ](https://huggingface.co/cyankiwi/Instella-MoE-16B-A3B-Think-AWQ-INT4) | Jul 2026. AMD. Text-only. ⚠️ Custom code (posible incompatibilidad vLLM) |
| 18 | **GLM-5.2-FP8** | 🅵 | — | ~80 | ~19× | ~8 GB | [FP8](https://huggingface.co/zai-org/GLM-5.2-FP8) | zai-org. 1.5M↓. Calidad general alta. ⚠️ Español no verificado |
| 19 | **Nemotron Nano 9B v2** | 🅵 | — | ~78 | ~15× | ~9 GB | [FP8](https://huggingface.co/nvidia/NVIDIA-Nemotron-Nano-9B-v2-FP8) | NVIDIA. FP8 nativo. ⚠️ Inglés principal, validar español |
| 20 | **Qwen3-4B** | 🅰️🅵 | — | ~75 | ~50× | 2.0-4.0 GB | [AWQ](https://huggingface.co/Qwen/Qwen3-4B-AWQ) · [FP8](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507-FP8) | Sorprendentemente capaz para su tamaño |
| 21 | **Phi-4-mini 3.8B** | 🅰️ | — | ~72 | ~60-80× | 1.9 GB | [AWQ](https://huggingface.co/drawais/Phi-4-mini-instruct-AWQ-INT4) | Muy pequeño para extracción fina |

---

## 🏆 Ranking 2 — MAYOR CONCURRENCIA (requests @ 4K contexto)

| # | Modelo | Formato | VRAM pesos | KV cache | Concurrencia | Repo HF |
|---|---|---|---|---|---|---|
| 🥇 | **Qwen3-0.6B-FP8** | 🅵 | ~0.6 GB | ~14 GB | **~100×** | [FP8](https://huggingface.co/Qwen/Qwen3-0.6B-FP8) |
| 🥈 | **Phi-4-mini 3.8B** | 🅰️ | 1.9 GB | ~12.6 GB | **~60-80×** | [AWQ](https://huggingface.co/drawais/Phi-4-mini-instruct-AWQ-INT4) |
| 🥉 | **Qwen3-4B** | 🅰️🅵 | 2.0-4.0 GB | ~10-12.5 GB | **~50×** | [AWQ](https://huggingface.co/Qwen/Qwen3-4B-AWQ) · [FP8](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507-FP8) |
| 4 | **Gemma 4 E4B** | 🅶 | 2.5 GB | ~12 GB | ~45× | [GGUF](https://huggingface.co/unsloth/gemma-4-E4B-it-GGUF) |
| 5 | **Mistral 7B v0.3** | 🅰️ | 3.7 GB | ~10.3 GB | **~35×** | [AWQ](https://huggingface.co/solidrust/Mistral-7B-Instruct-v0.3-AWQ) |
| 6 | **EXAONE-3.5-7.8B** | 🅰️ | 3.9 GB | ~10.1 GB | ~34× | [AWQ](https://huggingface.co/LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct-AWQ) |
| 7 | **Llama 3.1 8B** | 🅰️ | 4.0 GB | ~10 GB | **~29×** | [AWQ](https://huggingface.co/hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4) |
| 8 | **Qwen3-8B** | 🅰️ | 4.1 GB | ~9.9 GB | ~24× | [AWQ](https://huggingface.co/Qwen/Qwen3-8B-AWQ) |
| 9 | **Bielik-11B-v3.0** | 🅰️ | 5.5 GB | ~8.5 GB | ~22× | [AWQ](https://huggingface.co/speakleash/Bielik-11B-v3.0-Instruct-awq) |
| 10 | **Granite 4.2 8B** | 🅶 | 5.5 GB | ~8.7 GB | ~20× | [GGUF](https://huggingface.co/bartowski/granite-4.2-8b-GGUF) |
| 11 | **Mistral Nemo 12B** | 🅰️ | 6.1 GB | ~7.9 GB | ~20× | [AWQ](https://huggingface.co/casperhansen/mistral-nemo-instruct-2407-awq) |
| 12 | **gemma-3-12b-it** | 🅰️ | 6.0 GB | ~8 GB | ~20× | [AWQ](https://huggingface.co/gaunernst/gemma-3-12b-it-int4-awq) |
| 13 | **Llama 3.1 8B-FP8** | 🅵 | ~8 GB | ~6 GB | ~19× | [FP8](https://huggingface.co/RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8) |
| 14 | **Qwen3-8B-FP8** | 🅵 | ~8.2 GB | ~6 GB | ~19× | [FP8](https://huggingface.co/Qwen/Qwen3-8B-FP8) |
| 15 | **GLM-5.2-FP8** | 🅵 | ~8 GB | ~6 GB | ~19× | [FP8](https://huggingface.co/zai-org/GLM-5.2-FP8) |
| 16 | **Gemma 4 12B** | 🅶 | 7.66 GB | ~6.5 GB | ~17× | [GGUF](https://huggingface.co/unsloth/gemma-4-12B-it-qat-GGUF) |
| 17 | **Instella-MoE-16B-A3B** | 🅰️ | 7.9 GB | ~6.1 GB | ~15× | [AWQ](https://huggingface.co/cyankiwi/Instella-MoE-16B-A3B-Think-AWQ-INT4) |
| 18 | **Nemotron Nano 9B v2** | 🅵 | ~9 GB | ~5 GB | ~15× | [FP8](https://huggingface.co/nvidia/NVIDIA-Nemotron-Nano-9B-v2-FP8) |
| 19 | **Ministral-3-14B** | 🅶 | 8.5 GB | ~5.7 GB | ~12× | [GGUF oficial](https://huggingface.co/mistralai/Ministral-3-14B-Instruct-2512-GGUF) |
| 20 | **Qwen3-14B** | 🅰️ | 7.4 GB | ~6.6 GB | ~9× | [AWQ](https://huggingface.co/Qwen/Qwen3-14B-AWQ) |
| 21 | **Mistral Small 3.1 24B** | 🅰️🅶 | 12.0 GB | ~2 GB | ~4× | [AWQ](https://huggingface.co/stelterlab/Mistral-Small-24B-Instruct-2501-AWQ) |
| 22 | **Gemma 4 12B-FP8** | 🅵 | ~12 GB | ~2 GB | ~4× | [FP8](https://huggingface.co/RedHatAI/gemma-4-12B-it-FP8-Dynamic) |
| 23 | **Qwen3-14B-FP8** | 🅵 | ~14.8 GB | ~0 GB | ~1-2× | [FP8](https://huggingface.co/Qwen/Qwen3-14B-FP8) |
| 24 | **phi-4-FP8** | 🅵 | ~14.7 GB | ~0 GB | ~1-2× | [FP8](https://huggingface.co/RedHatAI/phi-4-FP8-dynamic) |
| 25 | **Qwen3.8-27B** | 🅰️🅶🅵 | 15.5 GB | ~0 GB | ~1× | [AWQ](https://huggingface.co/cyankiwi/Qwen3.8-27B-AWQ-INT4) · [GGUF](https://huggingface.co/unsloth/Qwen3.8-27B-GGUF) |
| 26 | **Granite 4.2 30B** | 🅶 | 15.9 GB | ~0 GB | ~1× | [GGUF](https://huggingface.co/bartowski/granite-4.2-30b-GGUF) |

---

## 🏆 Ranking 3 — MEJOR BALANCE Calidad × Concurrencia (frente de Pareto)

> ⚠️ **Nota metodológica**: la columna Precisión mezcla **medido** (con ✅) y **estimado** (sin marca). La concurrencia es **capacidad teórica de slots** (KV cache ÷ tokens), NO throughput real — hay que validar con `vllm bench serve --max-concurrency N`. Un modelo con concurrencia teórica alta puede rendir mal bajo carga real por contención de prefill (ver Qwen3-30B-A3B en AVISOS).

| # | Modelo | Formato | Precisión | Concurrencia | Cal×Conc | Veredicto |
|---|---|---|---|---|---|---|
| 🥇 | **Llama 3.1 8B** | 🅰️ | 93 ✅ | 29× | **2697** | **GANADOR global**: mejor calidad medida + alta concurrencia |
| 🥈 | **Mistral 7B v0.3** | 🅰️ | 88 ✅ | 35× | **3080** | THROUGHPUT champion, calidad suficiente |
| 🥉 | **Qwen3-8B** | 🅰️ | 89 ✅ | 24× | **2136** | Alternativa balanceada |
| 4 | **Qwen3-14B** | 🅰️ | 94 ✅ | 9× | **846** | Mejor calidad por dólar, menos concurrencia |
| 5 | **EXAONE-3.5-7.8B** | 🅰️ | 86 | 34× | **2924** | Candidato 2026 text-only. ⚠️ Validar español |
| 6 | **Granite 4.2 8B** | 🅶 | 88 | 20× | **1760** | IBM español + enterprise. ⚠️ Validar desactivación thinking |
| 7 | **Gemma 4 12B** | 🅶 | 92 | 17× | **1564** | Mejor modelo 2026. ⚠️ Bug #39130: validar guided decoding |
| 8 | **Bielik-11B-v3.0** | 🅰️ | 85 | 22× | **1870** | Arquitectura novel. ⚠️ Validar español |
| 9 | **Llama 3.1 8B-FP8** | 🅵 | 93 | 19× | **1767** | FP8 del mejor modelo (estimado, no medido) |
| 10 | **Qwen3-8B-FP8** | 🅵 | 89 | 19× | **1691** | FP8 oficial Qwen (estimado, no medido) |
| 11 | **Mistral Nemo 12B** | 🅰️ | 86 | 20× | **1720** | Sweet spot 12B |
| 12 | **GLM-5.2-FP8** | 🅵 | 80 | 19× | **1520** | FP8 calidad general. ⚠️ Validar español |
| 13 | **Mistral Small 3.1 24B** | 🅰️ | 95 | 4× | **380** | Máxima calidad pura, contexto limitado |

---

## 🏆 Ranking 4 — POR FORMATO (para documentar en la tesis)

> ✅ = medido en nuestros benchmarks · sin marca = estimado a priori (NO citar como resultado en la tesis).

### AWQ (vLLM nativo, awq_marlin · xgrammar)

| # | Modelo | Medido | Estimado | Concurrencia | Repo HF |
|---|---|---|---|---|---|
| 🥇 | **Llama 3.1 8B** | F1 **96.4%** | — | **29×** | [AWQ](https://huggingface.co/hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4) |
| 🥈 | **Mistral 7B v0.3** | F1 94.0% | — | **35×** | [AWQ](https://huggingface.co/solidrust/Mistral-7B-Instruct-v0.3-AWQ) |
| 🥉 | **Qwen3-14B** | 100% (60/60) | — | 9× | [AWQ](https://huggingface.co/Qwen/Qwen3-14B-AWQ) |
| 4 | **Qwen3-8B** | 100% (60/60) | — | 24× | [AWQ](https://huggingface.co/Qwen/Qwen3-8B-AWQ) |
| 5 | **Mistral Small 3.1 24B** | — | ~95 | 4× | [AWQ](https://huggingface.co/stelterlab/Mistral-Small-24B-Instruct-2501-AWQ) |

### FP8 (vLLM nativo, W8A8, calidad ~fp16 · xgrammar)

| # | Modelo | Medido | Estimado | Concurrencia | Repo HF |
|---|---|---|---|---|---|
| 🥇 | **Llama 3.1 8B-FP8** | — | ~93 | **19×** | [FP8](https://huggingface.co/RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8) |
| 🥈 | **Qwen3-8B-FP8** | — | ~89 | **19×** | [FP8](https://huggingface.co/Qwen/Qwen3-8B-FP8) |
| 🥉 | **Qwen3-14B-FP8** | — | ~94 | 1-2× | [FP8](https://huggingface.co/Qwen/Qwen3-14B-FP8) |
| 4 | **phi-4-FP8-dynamic** | — | ~91 | 1-2× | [FP8](https://huggingface.co/RedHatAI/phi-4-FP8-dynamic) |
| 5 | **GLM-5.2-FP8** | — | ~80 | 19× | [FP8](https://huggingface.co/zai-org/GLM-5.2-FP8) |

### GGUF 2026 (llama-server, GBNF — NO comparar directamente con AWQ/FP8/xgrammar)

| # | Modelo | Medido | Estimado | Concurrencia | Repo HF | Nota |
|---|---|---|---|---|---|---|
| 🥇 | **Gemma 4 12B** | — | ~92 | **17×** | [GGUF](https://huggingface.co/unsloth/gemma-4-12B-it-qat-GGUF) | ⚠️ Validar guided decoding (bug #39130) |
| 🥈 | **Granite 4.2 8B** | — | ~88 | **20×** | [GGUF](https://huggingface.co/bartowski/granite-4.2-8b-GGUF) | ⚠️ Validar desactivación thinking |
| 🥉 | **Ministral-3-14B** | — | ~89 | **12×** | [GGUF oficial](https://huggingface.co/mistralai/Ministral-3-14B-Instruct-2512-GGUF) | — |
| 4 | **Qwen3.8-27B** | 97/100 | — | 1× | [GGUF](https://huggingface.co/unsloth/Qwen3.8-27B-GGUF) | STRETCH GOAL |
| 5 | **Mellum2-12B-A2.5B** | — | ~80 | 20× | [GGUF](https://huggingface.co/bartowski/Mellum2-12B-A2.5B-Instruct-GGUF) | ⚠️ Inglés solo |

---

## ⭐ Top 10 final para el benchmark (orden de ejecución sugerido)

> **Estrategia**: embudo de 2 fases — Fase 1 (smoke test 30-50 muestras sobre todos los candidatos, descartar por criterios) → Fase 2 (900 muestras solo top 5-8 sobrevivientes). Ver AVISOS de validación arriba.

| Orden | Modelo | Formato | Razón principal |
|---|---|---|---|
| 1 | **Llama 3.1 8B AWQ** | 🅰️ | Baseline (F1 96.4%, ya probado, awq_marlin, xgrammar) |
| 2 | **Qwen3-14B AWQ** | 🅰️ | 100% probado, calidad máxima con headroom |
| 3 | **Mistral 7B v0.3 AWQ** | 🅰️ | Throughput champion, 35× concurrencia |
| 4 | **Llama 3.1 8B FP8** | 🅵 | Comparación directa AWQ vs FP8 del mismo modelo |
| 5 | **Qwen3-8B FP8** | 🅵 | FP8 oficial Qwen, buen balance calidad/concurrencia |
| 6 | **Mistral Small 3.1 24B AWQ** | 🅰️ | Calidad máxima (probar 10 muestras, 4K ctx) |
| 7 | **Granite 4.2 8B GGUF** | 🅶 | IBM español + enterprise extraction. ⚠️ **Smoke test thinking ANTES** |
| 8 | **Gemma 4 12B GGUF** | 🅶 | Mejor modelo 2026 + multilingüe. ⚠️ **Smoke test guided decoding ANTES** |
| 9 | **EXAONE-3.5-7.8B AWQ** | 🅰️ | 2026 text-only, similar VRAM a Mistral 7B. ⚠️ Validar español |
| 10 | **Ministral-3-14B GGUF** | 🅶 | Oficial Mistral, JSON mode, Apache 2.0 |

### Criterios de descarte
- Si un modelo da <70% de acierto en las 8 variables → descartar y anotar razón
- Si falla al cargar (OOM, arquitectura no soportada) → descartar
- Si dos modelos dan >90% con diferencia <1% → queda el más rápido
- Si un modelo 24B+ requiere offload y da peor resultado que uno 8B sin offload → descartar
- **NUEVO**: si `schema_valid_json_rate` es alto pero `campos_correctos` es bajo, puede ser guided decoding bypaseado (Gemma 4) → investigar antes de descartar

### Métricas a capturar por modelo (4 ejes + 1 de integridad)
1. **Exactitud de campos**: F1 sobre pseudo-GT (900 muestras)
2. **`schema_valid_json_rate`** (NUEVO): % de outputs que son JSON parseable Y respetan el schema — **separado de** `campos_correctos`. Distingue "extrajo mal" de "guided decoding no se aplicó"
3. **Latencia P50/P95** en single-request
4. **Throughput agregado** a concurrencia 1, 4, 8, 16, 32 (`vllm bench serve --max-concurrency N`)
5. **VRAM pico** con `nvidia-smi` durante carga máxima

---

## 📊 Fase 5 — 867 pseudo-GT (MEDIDO 12-sep-2026, vLLM 0.29 + llama-server)

Set congelado `data/processed/llm_benchmark/pseudo900.json` (seed 42): 5 campos × 180 docs
(excepto `pct_monto_referencia_tecnica`, 147 — campo raro en producción).
Pseudo-truth = valor regex de producción (`conf>=0.9`, no nulo, ≠0), excluyendo las 100 GT humanas.
**Métrica honesta: agreement con el regex + latencia (NO accuracy)** — ver interpretación abajo.

| Modelo | Formato | Agree 867 | P50 | P95 | Tiempo total |
|---|---|---|---|---|---|
| **Llama 3.1 8B-FP8** | 🅵 | **536 = 61.8%** | **0.17s** | 0.28s | 132s |
| **Llama 3.1 8B AWQ** | 🅰️ | 532 = 61.4% | 0.22s | 0.38s | 167s |
| Qwen3-8B-FP8 | 🅵 | 474 = 54.7% | 0.24s | 0.39s | 187s |
| Qwen3-14B AWQ | 🅰️ | 460 = 53.1% | 0.38s | 0.64s | 288s |
| Ministral-3-14B | 🅶 | 435 = 50.2% | 0.55s | 0.77s | 421s |
| Mistral 7B v0.3 AWQ | 🅰️ | 428 = 49.4% | 0.29s | 0.96s | 203s |
| EXAONE-3.5 AWQ / Gemma 4 GGUF | 🅰️🅶 | 420 = 48.4% | 0.26s / 1.45s | — | 195s / 1124s |
| Granite 4.2 GGUF / Mistral Small 24B AWQ | 🅶🅰️ | 418 = 48.2% | 1.09s / 2.99s | — | 891s / 1511s |

### Interpretación (hallazgos, no solo ranking)

1. **El campo que discrimina es `pct_monto_referencia_tecnica`** (porcentajes fraccionarios tipo
   `0,50`): Llama-FP8 118/147, Llama-AWQ 114, Qwen3-8B-FP8 56, Qwen3-14B 42, Ministral 17,
   Mistral 10, EXAONE/Gemma 2, Small/Granite 0. Los otros 4 campos saturan idéntico en todos
   (validez 123/180, exp_tec 77/180, pago_ant 64/180, sol_ant 154/180) — techo del pipeline
   compartido (guided regex + parse estricto + textos ambiguos multi-número), no de los modelos.
2. **Diagnóstico de desacuerdos** (Llama-AWQ): `dias_pago_anticipo` = números distintos plausibles
   (15 vs 10 — textos con varios plazos); `exp_tecnica`/`validez` = NULL del LLM vs valor regex;
   hay artefactos del parse estricto (`raw='720'` → None con pseudo 720; `raw='1.5'` → None).
3. **Mistral 7B**: su template no acepta rol `system` (jinja TemplateError) → prompt mergeado
   obligatorio, y con él colapsa en `pct_monto` (10/147 vs 19/20 en GT curadas). Condición real
   de producción, comparación justa.
4. **Latencia**: Llama-FP8 es lo mejor en todo (P50 0.17s). GGUF secuencial: Ministral 0.55s,
   Granite 1.09s, Gemma 1.45s. Mistral Small 24B solo viable con `cpu_offload_gb=5` (P50 2.99s).
5. **Veredicto Fase 5**: **Llama 3.1 8B-FP8** = mejor agreement + menor latencia → candidato para el
   fallback Pipeline 2 (324K docs). Llama-AWQ segundo (mismo techo, +30% tiempo).

---

## 🚦 Fase 6 — Concurrencia online (MEDIDO 12/13-sep-2026)

**Método** (`scripts/benchmark_concurrency.py`): cada modelo servido (vLLM serve 0.29 con
`extra_body: {guided_regex}` vía `/v1/completions`, o llama-server `/v1/chat/completions`),
atacado con C hilos en paralelo. Workset fijo 64 prompts (vLLM) / 32 (GGUF, seed 1234, truncados
a 15k chars). Niveles C=1,4,8,16,32 (vLLM) y C=1,2,4,8 (GGUF, 4 slots de 4096).

| Modelo | C=1 (req/s · P50) | Techo req/s | P50 en techo | VRAM |
|---|---|---|---|---|
| **Llama-FP8** | 3.11 · 0.19s | **7.0 (C16)** | 1.47s | 11.6G |
| Llama-AWQ | 2.27 · 0.38s | 5.2 (C32) | 4.93s | 11.6G |
| EXAONE AWQ | 2.07 · 0.47s | 5.05 (C32) | 4.82s | 11.5G |
| Ministral GGUF | 2.44 · 0.41s | 4.85 (C8) | 1.30s | 10.5G |
| Qwen3-8B-FP8 | 1.31 · 1.00s | 4.5 (C32) | 6.47s | 13.2G |
| Mistral AWQ | 1.58 · 0.69s | 4.3 (C32) | 6.62s | 11.5G |
| Qwen3-14B AWQ | 2.23 · 0.43s | 3.2 (C16) | 4.19s | **15.2G** |
| Granite GGUF | 1.05 · 1.00s | 2.5 (C4-8) | 1.73s | 8.0G |
| Gemma 4 GGUF | 0.73 · 1.53s | 0.9 (no escala) | 8.80s | 9.0G |
| Small 24B AWQ | batch-offline | 0.27 (24/90s) | — | 14.7G |

### Lectura para la tesis

1. **El ranking de throughput replica el de calidad**: Llama-FP8 primero en todo
   (61.8% agree + 7 req/s + P50 0.19s). Doble sustento para elegirlo en Pipeline 2.
2. **Saturación**: los 8B vLLM saturan GPU (U=100%) entre C=8 y C=16; más hilos solo
   inflan P50/P95 sin subir req/s. Punto operativo recomendado: **C=8-16**.
3. **Gemma 4 no escala** (0.73→0.90): 12B compute-bound en 1 GPU — el paralelismo no ayuda.
4. **GGUF escala peor que vLLM** (slots fijos vs continuous batching), salvo Ministral
   (outputs de 1-3 tokens → 4.85 req/s a C=8, mejor GGUF).
5. **Qwen3-14B al límite**: 15.2/16 GB con C=16 — sin margen para producción.
6. **Small 24B**: `vllm serve` + `cpu_offload` cuelga el EngineCore en 0.29 (3 intentos);
   solo viable batch offline (0.27 req/s) → **descartado para Pipeline 2**.
7. `err=1` determinista en 4 modelos vLLM = la muestra de 8193 tokens (>max_len 8192):
   el servidor la rechaza igual en todos los niveles — robustez, no flakiness.

*Fuente: datos medidos en `docs/benchmark_resultados.md` + API Hugging Face (31/08/2026) + verificación de hallazgos en internet (GitHub vLLM #39130/#45553, IBM Granite docs, arXiv 2512.23029).*
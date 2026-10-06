# Benchmark de Modelos — Candidatos para Extracción Estructurada (16 GB VRAM)

> **Fecha**: 31 de agosto de 2026
> **GPU objetivo**: NVIDIA RTX 4070 Ti SUPER — 16 GB VRAM (utilizable ~15.6 GB)
> **Stack**: WSL2 · vLLM 0.23.0 (AWQ/FP8) · llama-server/llama.cpp (GGUF) · `VLLM_USE_V2_MODEL_RUNNER=0`
> **Tarea**: extracción de 8 variables de pliegos de licitación paraguayos (DNCP) contra pseudo-GT de 900 muestras

---

## Índice

| Archivo | Contenido |
|---|---|
| [`README.md`](README.md) | Este índice + tabla maestra combinada (38 modelos) + avisos de validación |
| [`modelos_awq.md`](modelos_awq.md) | Ranking de modelos AWQ (vLLM nativo, 4-bit, awq_marlin) |
| [`modelos_gguf.md`](modelos_gguf.md) | Ranking de modelos GGUF 2026 (llama-server / plugin vLLM) + avisos Gemma 4/Granite 4.2 |
| [`modelos_fp8.md`](modelos_fp8.md) | Ranking de modelos FP8 (vLLM nativo, 8-bit, calidad ~fp16) + aviso Gemma 4 |
| [`ranking_combinado.md`](ranking_combinado.md) | Rankings multi-criterio: precisión, concurrencia, balance, por formato |

---

## ⚠️ Avisos de validación (verificados en internet 31/08/2026)

1. **Gemma 4 + guided decoding tiene un bug documentado (vLLM #39130)**: `--reasoning-parser gemma4` + `enable_thinking=false` **silenciosamente bypasea xgrammar**. El fix se mergeó en PR #45553 (16/06/2026), pero hay que validar empíricamente. Ver `modelos_fp8.md` / `modelos_gguf.md`.
2. **Granite 4.2 tiene razonamiento nativo (thinking)** — IBM lo lanzó el 25/08/2026. Hay que desactivar thinking para extracción directa y verificar en la salida cruda. Ver `modelos_gguf.md`.
3. **Qwen3.8-27B es alto riesgo** (arq. de 2 semanas, quant delicado) — **stretch goal**, no central.
4. **La concurrencia teórica ≠ throughput real** — validar con `vllm bench serve --max-concurrency N` (ver ranking_combinado.md).
5. **No mezclar GGUF (GBNF) con AWQ/FP8 (xgrammar) en un ranking de precisión** — backends de guided decoding distintos.

---

## Tabla maestra combinada — 38 modelos que caben en 16 GB VRAM

**Leyenda de formato**: 🅰️ = AWQ · 🅶 = GGUF · 🅵 = FP8 · ✅ = text-only (sin vision encoder) · 👁️ = multimodal · ⚠️ = borderline

### Zona ALTA CALIDAD (14B-24B densos, 1 petición cada ~2-5s, 5-15× concurrencia)

| # | Modelo | Formato | Año | Params | VRAM pesos | KV cache | Repo(s) HF |
|---|---|---|---|---|---|---|---|
| 1 | Mistral Small 3.1 24B | 🅰️🅶 | 2025 | 24B | 12.0 GB | ⚠️ ~2 GB | [AWQ](https://huggingface.co/stelterlab/Mistral-Small-24B-Instruct-2501-AWQ) · [GGUF](https://huggingface.co/bartowski/mistralai_Mistral-Small-3.1-24B-Instruct-2503-GGUF) |
| 2 | Devstral Small 2 24B | 🅰️ | 2025 | 24B | 12.0 GB | ⚠️ ~2 GB | [AWQ](https://huggingface.co/cyankiwi/Devstral-Small-2-24B-Instruct-2512-AWQ-4bit) |
| 3 | **Qwen3-14B** ⭐ | 🅰️🅶🅵 | 2025 | 14.8B | 7.4 GB | ~6.6 GB | [AWQ](https://huggingface.co/Qwen/Qwen3-14B-AWQ) · [FP8](https://huggingface.co/Qwen/Qwen3-14B-FP8) |
| 4 | phi-4-FP8-dynamic | 🅵 | 2025 | 14.7B | ~14.7 GB | ⚠️ ~0 GB | [FP8](https://huggingface.co/RedHatAI/phi-4-FP8-dynamic) |
| 5 | Ministral-3-14B | 🅶 | Dic 2025 | 14B | 8.5 GB | ~5.7 GB | [GGUF](https://huggingface.co/mistralai/Ministral-3-14B-Instruct-2512-GGUF) |
| 6 | Mistral Nemo 12B | 🅰️ | 2024 | 12.2B | 6.1 GB | ~7.9 GB | [AWQ](https://huggingface.co/casperhansen/mistral-nemo-instruct-2407-awq) |
| 7 | Gemma 4 12B | 🅶🅵 | Jul 2026 | 12B | 7.66 GB | ~6.5 GB | [GGUF](https://huggingface.co/unsloth/gemma-4-12B-it-qat-GGUF) · [FP8](https://huggingface.co/RedHatAI/gemma-4-12B-it-FP8-Dynamic) |
| 8 | gemma-3-12b-it-int4-awq | 🅰️ | 2025 | 12B | 6.0 GB | ~8 GB | [AWQ](https://huggingface.co/gaunernst/gemma-3-12b-it-int4-awq) |
| 9 | Granite 4.2 8B | 🅶 | Ago 2026 | 8.8B | 5.5 GB | ~8.7 GB | [GGUF](https://huggingface.co/bartowski/granite-4.2-8b-GGUF) |

### Zona MEDIA (7B-11B, sweet spot, 15-35× concurrencia)

| # | Modelo | Formato | Año | Params | VRAM pesos | KV cache | Repo(s) HF |
|---|---|---|---|---|---|---|---|
| 10 | **Llama 3.1 8B** ⭐ | 🅰️🅵 | 2024 | 8.0B | 4.0 GB | ~10 GB | [AWQ](https://huggingface.co/hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4) · [FP8](https://huggingface.co/RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8) |
| 11 | **Qwen3-8B** | 🅰️🅶🅵 | 2025 | 8.2B | 4.1 GB | ~9.9 GB | [AWQ](https://huggingface.co/Qwen/Qwen3-8B-AWQ) · [FP8](https://huggingface.co/Qwen/Qwen3-8B-FP8) |
| 12 | **Mistral 7B v0.3** ⭐ | 🅰️🅶 | 2024 | 7.3B | 3.7 GB | ~10.3 GB | [AWQ](https://huggingface.co/solidrust/Mistral-7B-Instruct-v0.3-AWQ) |
| 13 | EXAONE-3.5-7.8B | 🅰️ | Feb 2026 | 7.8B | 3.9 GB | ~10.1 GB | [AWQ](https://huggingface.co/LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct-AWQ) |
| 14 | Bielik-11B-v3.0 | 🅰️ | Dic 2025 | 11B | 5.5 GB | ~8.5 GB | [AWQ](https://huggingface.co/speakleash/Bielik-11B-v3.0-Instruct-awq) |
| 15 | Qwen3.5-9B | 🅰️🅶🅵 | Mar 2026 | 9.65B | ~6 GB | ~8 GB | [AWQ](https://huggingface.co/QuantTrio/Qwen3.5-9B-AWQ) · [FP8](https://huggingface.co/RedHatAI/Qwen3.5-9B-FP8-dynamic) |
| 16 | Ornith-1.5-9B | 🅶🅰️🅵 | Ago 2026 | 9.4B | 5.5 GB | ~8.7 GB | [GGUF](https://huggingface.co/ornith-ai/Ornith-1.5-9B-GGUF) · [FP8](https://huggingface.co/cyankiwi/Ornith-1.5-9B-AWQ-FP8) |
| 17 | Nemotron Nano 9B v2 | 🅵 | 2026 | 9B | ~9 GB | ~5 GB | [FP8](https://huggingface.co/nvidia/NVIDIA-Nemotron-Nano-9B-v2-FP8) |
| 18 | GLM-5.2-FP8 | 🅵 | 2026 | ~8B | ~8 GB | ~6 GB | [FP8](https://huggingface.co/zai-org/GLM-5.2-FP8) |
| 19 | GLM-5.1-FP8 | 🅵 | 2026 | ~8B | ~8 GB | ~6 GB | [FP8](https://huggingface.co/zai-org/GLM-5.1-FP8) |
| 20 | Laguna-S-2.1-FP8 | 🅵 | 2026 | ~8B | ~8 GB | ~6 GB | [FP8](https://huggingface.co/poolside/Laguna-S-2.1-FP8) |

### Zona BORDERLINE (27B-30B, requieren offload, 0-2 GB KV cache)

| # | Modelo | Formato | Año | Params | VRAM pesos | KV cache | Repo(s) HF |
|---|---|---|---|---|---|---|---|
| 21 | Granite 4.2 30B | 🅶 | Ago 2026 | 29B | 15.9 GB IQ4 | ~0 GB | [GGUF](https://huggingface.co/bartowski/granite-4.2-30b-GGUF) |
| 22 | Qwen3.8-27B | 🅰️🅶🅵 | Ago 2026 | 27B | 15.5 GB Q4 | ~0 GB | [AWQ](https://huggingface.co/cyankiwi/Qwen3.8-27B-AWQ-INT4) · [GGUF](https://huggingface.co/unsloth/Qwen3.8-27B-GGUF) · [FP8](https://huggingface.co/Qwen/Qwen3.8-27B-FP8) |
| 23 | gemma-3-27b-it-int4-awq | 🅰️ | 2025 | 27B | 13.5 GB | ~0.5 GB | [AWQ](https://huggingface.co/gaunernst/gemma-3-27b-it-int4-awq) |
| 24 | Qwen3.6-27B | 🅶🅵 | 2026 | 27B | 15.4 GB Q4 | ~0 GB | [GGUF](https://huggingface.co/unsloth/Qwen3.6-27B-GGUF) · [FP8](https://huggingface.co/Qwen/Qwen3.6-27B-FP8) |
| 25 | Bonsai-27B | 🅶 | 2026 | 27B | 15.4 GB Q4 | ~0 GB | [GGUF](https://huggingface.co/prism-ml/Bonsai-27B-gguf) |
| 26 | Muse-Glimmer-30B | 🅶🅵 | 2026 | 30B | ❌ ~17 GB | NONE | [GGUF](https://huggingface.co/unsloth/Muse-Glimmer-30B-GGUF) |

### Zona ECONÓMICA (MoE con pocos activos)

| # | Modelo | Formato | Año | Params | VRAM pesos | KV cache | Repo(s) HF |
|---|---|---|---|---|---|---|---|
| 27 | Instella-MoE-16B-A3B-Think | 🅰️ | Jul 2026 | 16B/3B | 7.9 GB | ~6.1 GB | [AWQ](https://huggingface.co/cyankiwi/Instella-MoE-16B-A3B-Think-AWQ-INT4) |
| 28 | Mellum2-12B-A2.5B | 🅶 | Jun 2026 | 12B/2.5B | 8.17 GB | ~6 GB | [GGUF](https://huggingface.co/bartowski/Mellum2-12B-A2.5B-Instruct-GGUF) |
| 29 | Gemma 4 26B-A4B | 🅶🅵 | Jul 2026 | 25B/4B | 14.2 GB IQ4 | ~0 GB | [GGUF](https://huggingface.co/unsloth/gemma-4-26B-A4B-it-GGUF) · [FP8](https://huggingface.co/RedHatAI/gemma-4-26B-A4B-it-FP8-dynamic) |
| 30 | LFM2.5-8B-A1B | 🅰️🅵 | May 2026 | 8B/1B | ~4 GB | ~10 GB | [FP8](https://huggingface.co/cyankiwi/LFM2.5-8B-A1B-AWQ-FP8) |
| 31 | Gemma 4 E4B | 🅶 | Jul 2026 | 4B | 2.5 GB | ~12 GB | [GGUF](https://huggingface.co/unsloth/gemma-4-E4B-it-GGUF) |

### Zona BÁSICA (4B o menos, máxima concurrencia 40-100×)

| # | Modelo | Formato | Año | Params | VRAM pesos | KV cache | Repo(s) HF |
|---|---|---|---|---|---|---|---|
| 32 | Qwen3-4B-FP8 | 🅵 | 2025 | 4B | ~4 GB | ~10 GB | [FP8](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507-FP8) |
| 33 | Qwen3-4B | 🅰️🅶 | 2025 | 4B | 2.0 GB | ~12.5 GB | [AWQ](https://huggingface.co/Qwen/Qwen3-4B-AWQ) |
| 34 | Qwen3.5-4B | 🅰️🅶🅵 | 2026 | 4B | 2.0 GB | ~12.5 GB | [AWQ](https://huggingface.co/cyankiwi/Qwen3.5-4B-AWQ-4bit) · [FP8](https://huggingface.co/RedHatAI/Qwen3.5-4B-FP8-dynamic) |
| 35 | Phi-4-mini 3.8B | 🅰️ | May 2025 | 3.8B | 1.9 GB | ~12.6 GB | [AWQ](https://huggingface.co/drawais/Phi-4-mini-instruct-AWQ-INT4) |
| 36 | MiniCPM5-1B | 🅰️🅵 | May 2026 | 1B | ~1 GB | ~13 GB | [FP8](https://huggingface.co/cyankiwi/MiniCPM5-1B-AWQ-FP8) |
| 37 | Qwen3-0.6B | 🅵 | 2025 | 0.6B | ~0.6 GB | ~14 GB | [FP8](https://huggingface.co/Qwen/Qwen3-0.6B-FP8) |

---

## Notas de cálculo de VRAM

- **AWQ (W4A16)**: ~0.5 bytes/parámetro → `params × 0.5 + 1.5-2 GB overhead vLLM`
- **FP8 (W8A8)**: ~1.0 bytes/parámetro → `params × 1.0 + 1.5-2 GB overhead`
- **GGUF Q4_K_M**: ~0.56 bytes/parámetro (pesos + RMSNorm/GQA en fp16)
- **KV cache por token**: `2 × num_layers × num_kv_heads × head_dim × dtype_bytes`
  - fp16 = 2 bytes/token; fp8_e4m3 = 1 byte/token → **mitad de memoria**
- **Concurrencia** = KV cache disponible ÷ (KV/token × max_model_len)

## Notas sobre vLLM y formatos

- **AWQ**: nativo en vLLM 0.23.0+. `awq_marlin` kernel solo con Mistral/Llama. Qwen/Phi caen a `awq`.
- **FP8**: nativo en vLLM 0.23.0+. Requiere GPU Ada Lovelace+. Usar `--kv-cache-dtype fp8_e4m3`.
- **GGUF**: experimental/deprecado en vLLM 0.23.0. Para arq. 2026 nuevas usar **llama-server**.
- **WSL2**: todos requieren `VLLM_USE_V2_MODEL_RUNNER=0`. vLLM 0.25.x no funciona → fijar 0.23.0.
- **Guided decoding por formato**: AWQ/FP8 usan xgrammar; GGUF usa GBNF (llama.cpp). Son backends distintos — no comparar precisión entre formatos sin nota.

## Métricas a capturar por modelo (5 ejes)

1. **Exactitud de campos**: F1 sobre pseudo-GT (900 muestras)
2. **`schema_valid_json_rate`** (Obligatorio): % de outputs que son JSON parseable Y respetan el schema — **separado de** `campos_correctos`. Distingue "extrajo mal" de "guided decoding no se aplicó" (crítico por bug Gemma 4 #39130)
3. **Latencia P50/P95** en single-request
4. **Throughput agregado** a concurrencia 1, 4, 8, 16, 32 (`vllm bench serve --max-concurrency N`)
5. **VRAM pico** con `nvidia-smi` durante carga máxima

## Datos de referencia ya medidos en este repo

| Modelo | Formato | Tiempo | Concurrencia | VRAM modelo | F1 vs GT |
|---|---|---|---|---|---|
| Mistral 7B v0.3 | AWQ (awq_marlin) | 1.61s | 35.22× | 3.89 GiB | 94.0% |
| Llama 3.1 8B | AWQ (awq_marlin) | 1.93s | 29.21× | 5.39 GiB | **96.4%** |
| Qwen2.5 14B | AWQ | 2.46s | 8.18× | 9.38 GiB | 93.3% |
| Qwen3-8B | AWQ | 4.80s | 24.58× | 5.71 GiB | — |
| Qwen3-14B | AWQ | 5.29s | 9.00× | 9.44 GiB | — |
| Phi-4 14B | AWQ | 4.47s | 9.95× | 8.51 GiB | 90% |
| Qwen3.8-27B | GGUF (llama-server Windows) | 1.41s | — | — | 97/100 |
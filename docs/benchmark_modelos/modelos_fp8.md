# Ranking de Modelos FP8 — Extracción Estructurada (16 GB VRAM)

> **Fecha**: 31 de agosto de 2026 · **Formato**: FP8 (W8A8, 8-bit) — nativo en vLLM 0.23.0+
> **⚠️ Nota**: FP8 usa ~2× VRAM de AWQ pero ofrece **calidad casi igual a fp16**. En tu GPU Ada Lovelace (RTX 4070 Ti SUPER), los FP8 tensor cores son nativos y vLLM los acelera con `--kv-cache-dtype fp8_e4m3`.
> **Tarea**: extracción de 8 variables de pliegos de licitación paraguayos (DNCP) contra pseudo-GT de 900 muestras.

---

## ⚠️ Aviso de validación (verificado en internet 31/08/2026)

**Gemma 4 FP8 + guided decoding tiene un bug documentado (vLLM #39130)**: `--reasoning-parser gemma4` + `enable_thinking=false` **silenciosamente bypasea xgrammar** — el FSM de gramática nunca corre y el output parece JSON válido por entrenamiento, no por restricción. Afecta a todos los Gemma 4 (E2B/E4B/12B/26B/31B). Fix mergeado en PR #45553 (16/06/2026). **Antes de confiar en Gemma 4 12B-FP8: correr smoke test de 5-10 requests y medir `schema_valid_json_rate` separado de `campos_correctos`.**

---

## Ranking por PRECISIÓN (calidad de extracción)

> ✅ = medido en nuestros benchmarks · sin marca = estimado a priori.

| # | Modelo | Año | Params | VRAM pesos | KV cache | Concurrencia | Repo HF | Evidencia |
|---|---|---|---|---|---|---|---|---|
| 🥇 | **Qwen3-14B-FP8** | 2025 | 14.8B | ~14.8 GB | ⚠️ ~0 GB | ~1-2× | [FP8 oficial](https://huggingface.co/Qwen/Qwen3-14B-FP8) | Oficial Qwen. 74K↓. El mejor FP8 que cabe justo. Requiere offload KV |
| 🥈 | **phi-4-FP8-dynamic** | 2025 | 14.7B | ~14.7 GB | ⚠️ ~0 GB | ~1-2× | [FP8 RedHatAI](https://huggingface.co/RedHatAI/phi-4-FP8-dynamic) | FP8 dinámico de RedHat. 78K↓. Muy ajustado, offload necesario |
| 🥉 | **Llama 3.1 8B-FP8** | 2024 | 8.0B | **~8.0 GB** | **~6 GB** | **~19×** | [FP8 RedHatAI](https://huggingface.co/RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8) | 180K↓. FP8 del modelo con mejor F1 (96.4% en AWQ). Sweet spot FP8 |
| 4 | **Qwen3-8B-FP8** | 2025 | 8.2B | **~8.2 GB** | **~6 GB** | **~19×** | [FP8 oficial](https://huggingface.co/Qwen/Qwen3-8B-FP8) | Oficial Qwen. 606K↓. Buen balance calidad/concurrencia |
| 5 | **Gemma 4 12B-FP8-Dynamic** | Jul 2026 | 12B | **~12 GB** | ⚠️ **~2 GB** | ~4× | [FP8 RedHatAI](https://huggingface.co/RedHatAI/gemma-4-12B-it-FP8-Dynamic) | 514K↓. Multilingüe. 👁️ multimodal. ⚠️ **Bug #39130: validar guided decoding NO bypaseado** |
| 6 | **GLM-5.2-FP8** | 2026 | ~8B | **~8 GB** | **~6 GB** | ~19× | [FP8](https://huggingface.co/zai-org/GLM-5.2-FP8) | zai-org. 1.5M↓. Calidad general alta. ⚠️ Español no verificado |
| 7 | **GLM-5.1-FP8** | 2026 | ~8B | **~8 GB** | **~6 GB** | ~19× | [FP8](https://huggingface.co/zai-org/GLM-5.1-FP8) | 394K↓. Similar a GLM-5.2. ⚠️ Español no verificado |
| 8 | **Nemotron Nano 9B v2-FP8** | 2026 | 9B | **~9 GB** | **~5 GB** | ~15× | [FP8 nvidia](https://huggingface.co/nvidia/NVIDIA-Nemotron-Nano-9B-v2-FP8) | NVIDIA. 129K↓. FP8 nativo. ⚠️ Inglés principal, validar español |
| 9 | **Laguna-S-2.1-FP8** | 2026 | ~8B | **~8 GB** | **~6 GB** | ~19× | [FP8 poolside](https://huggingface.co/poolside/Laguna-S-2.1-FP8) | poolside. 63K↓. FP8 nativo |
| 10 | **Qwen3.8-27B-FP8** | Ago 2026 | 27B | ❌ ~27 GB | NONE | — | [FP8 oficial](https://huggingface.co/Qwen/Qwen3.8-27B-FP8) | ⚠️ STRETCH GOAL. 👁️ multimodal. 5.3M↓. **No cabe en 16 GB** |
| 11 | **Qwen3.6-27B-FP8** | 2026 | 27B | ❌ ~27 GB | NONE | — | [FP8 oficial](https://huggingface.co/Qwen/Qwen3.6-27B-FP8) | 👁️ multimodal. 8.2M↓. **No cabe en 16 GB** |
| 12 | **Qwen3-4B-FP8** | 2025 | 4B | **~4 GB** | **~10 GB** | **~50×** | [FP8 oficial](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507-FP8) | Oficial Qwen. 1.15M↓. Máxima concurrencia |
| 13 | **Qwen3.5-9B-FP8-dynamic** | Mar 2026 | 9.65B | **~9.65 GB** | **~4 GB** | ~12× | [FP8 RedHatAI](https://huggingface.co/RedHatAI/Qwen3.5-9B-FP8-dynamic) | 👁️ multimodal. 364K↓. FP8 dinámico |
| 14 | **Qwen3-0.6B-FP8** | 2025 | 0.6B | **~0.6 GB** | **~14 GB** | **~100×** | [FP8 oficial](https://huggingface.co/Qwen/Qwen3-0.6B-FP8) | Minúsculo. 464K↓. Para pruebas de concepto |

---

## Ranking por CONCURRENCIA

| # | Modelo | Params | VRAM pesos | KV cache | Concurrencia | Repo HF |
|---|---|---|---|---|---|---|
| 🥇 | Qwen3-0.6B-FP8 | 0.6B | ~0.6 GB | ~14 GB | **~100×** | [FP8](https://huggingface.co/Qwen/Qwen3-0.6B-FP8) |
| 🥈 | Qwen3-4B-FP8 | 4B | ~4 GB | ~10 GB | **~50×** | [FP8](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507-FP8) |
| 🥉 | Llama 3.1 8B-FP8 | 8.0B | ~8 GB | ~6 GB | **~19×** | [FP8](https://huggingface.co/RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8) |
| 4 | Qwen3-8B-FP8 | 8.2B | ~8.2 GB | ~6 GB | ~19× | [FP8](https://huggingface.co/Qwen/Qwen3-8B-FP8) |
| 5 | GLM-5.2-FP8 | ~8B | ~8 GB | ~6 GB | ~19× | [FP8](https://huggingface.co/zai-org/GLM-5.2-FP8) |
| 6 | GLM-5.1-FP8 | ~8B | ~8 GB | ~6 GB | ~19× | [FP8](https://huggingface.co/zai-org/GLM-5.1-FP8) |
| 7 | Laguna-S-2.1-FP8 | ~8B | ~8 GB | ~6 GB | ~19× | [FP8](https://huggingface.co/poolside/Laguna-S-2.1-FP8) |
| 8 | Nemotron Nano 9B v2-FP8 | 9B | ~9 GB | ~5 GB | ~15× | [FP8](https://huggingface.co/nvidia/NVIDIA-Nemotron-Nano-9B-v2-FP8) |
| 9 | Qwen3.5-9B-FP8-dynamic | 9.65B | ~9.65 GB | ~4 GB | ~12× | [FP8](https://huggingface.co/RedHatAI/Qwen3.5-9B-FP8-dynamic) |
| 10 | Gemma 4 12B-FP8-Dynamic | 12B | ~12 GB | ~2 GB | ~4× | [FP8](https://huggingface.co/RedHatAI/gemma-4-12B-it-FP8-Dynamic) |
| 11 | phi-4-FP8-dynamic | 14.7B | ~14.7 GB | ~0 GB | ~1-2× | [FP8](https://huggingface.co/RedHatAI/phi-4-FP8-dynamic) |
| 12 | Qwen3-14B-FP8 | 14.8B | ~14.8 GB | ~0 GB | ~1-2× | [FP8](https://huggingface.co/Qwen/Qwen3-14B-FP8) |

---

## FP8 vs AWQ — Comparativa directa por modelo

| Modelo | AWQ (4-bit) | FP8 (8-bit) | Diferencia |
|---|---|---|---|
| **Llama 3.1 8B** | 4.0 GB · 29× · F1 96.4% | 8.0 GB · 19× · F1 ~97% | AWQ gana en concurrencia (2×). FP8 gana en calidad |
| **Qwen3-8B** | 4.1 GB · 24× · 100% probado | 8.2 GB · 19× · calidad ~fp16 | AWQ gana 26% más concurrencia. FP8 calidad superior |
| **Qwen3-14B** | 7.4 GB · 9× · 100% probado | 14.8 GB · 1-2× · offload | AWQ claramente superior para 16 GB. FP8 solo si priorizas calidad |
| **Qwen3-4B** | 2.0 GB · 50× | 4.0 GB · 50× | Empate técnico. FP8 duplica VRAM pero misma concurrencia |

---

## ⭐ Recomendados para benchmark

| Orden | Modelo | Repo HF | Razón |
|---|---|---|---|
| 1 | **Llama 3.1 8B-FP8** | [FP8 RedHatAI](https://huggingface.co/RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8) | Comparación directa AWQ vs FP8 del mismo modelo baseline |
| 2 | **Qwen3-8B-FP8** | [FP8 oficial](https://huggingface.co/Qwen/Qwen3-8B-FP8) | FP8 oficial Qwen, buena calidad |
| 3 | **Qwen3-14B-FP8** | [FP8 oficial](https://huggingface.co/Qwen/Qwen3-14B-FP8) | Calidad máxima, probar 10 muestras con offload |

---

## Comandos de arranque (vLLM)

```bash
# Llama 3.1 8B-FP8 (sweet spot)
vllm serve RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8 \
  --dtype bfloat16 \
  --kv-cache-dtype fp8_e4m3 \
  --max-model-len 8192 \
  --gpu-memory-utilization 0.90 \
  --enforce-eager

# Qwen3-8B-FP8
vllm serve Qwen/Qwen3-8B-FP8 \
  --dtype bfloat16 \
  --kv-cache-dtype fp8_e4m3 \
  --max-model-len 8192 \
  --gpu-memory-utilization 0.90 \
  --enforce-eager

# Qwen3-14B-FP8 (borderline, requiere offload)
vllm serve Qwen/Qwen3-14B-FP8 \
  --dtype bfloat16 \
  --kv-cache-dtype fp8_e4m3 \
  --max-model-len 4096 \
  --gpu-memory-utilization 0.95 \
  --enforce-eager \
  --num-gpu-blocks-override 800  # forzar menos bloques
```

---

*Fuente: API Hugging Face (31/08/2026) + docs oficiales vLLM.*
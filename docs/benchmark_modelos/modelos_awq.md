# Ranking de Modelos AWQ — Extracción Estructurada (16 GB VRAM)

> **Fecha**: 31 de agosto de 2026 · **Formato**: AWQ (W4A16, 4-bit) — nativo en vLLM 0.23.0+
> **Nota**: `awq_marlin` kernel solo con arquitectura Mistral/Llama. Qwen/Phi caen a `awq` (más lento).
> **Tarea**: extracción de 8 variables de pliegos de licitación paraguayos (DNCP) contra pseudo-GT de 900 muestras.

---

## Ranking por PRECISIÓN (calidad de extracción)

| # | Modelo | Params | VRAM pesos | KV cache | Concurrencia | Repo HF | Evidencia |
|---|---|---|---|---|---|---|---|
| 🥇 | **Mistral Small 3.1 24B** | 24B | 12.0 GB | ⚠️ ~2 GB | ~4× | [AWQ](https://huggingface.co/stelterlab/Mistral-Small-24B-Instruct-2501-AWQ) | Apache 2.0. JSON nativo (`--tool-call-parser mistral`). El denso más capaz que cabe. 4K ctx forzado |
| 🥈 | **Qwen3-14B** | 14.8B | 7.4 GB | ~6.6 GB | ~9× | [AWQ](https://huggingface.co/Qwen/Qwen3-14B-AWQ) | ✅ **Probado: 100% (60/60)**. 2.24M↓. Sin marlin |
| 🥉 | **Llama 3.1 8B** | 8.0B | 4.0 GB | ~10 GB | ~29× | [AWQ](https://huggingface.co/hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4) | ✅ **Probado: F1 96.4%** (mejor F1 medido). 301K↓. awq_marlin |
| 4 | **Mistral 7B v0.3** | 7.3B | 3.7 GB | ~10.3 GB | ~35× | [AWQ](https://huggingface.co/solidrust/Mistral-7B-Instruct-v0.3-AWQ) | ✅ **Probado: F1 94.0%, 100% struct.** 445K↓. awq_marlin |
| 5 | **Phi-4 14B** | 14.7B | 7.3 GB | ~6.7 GB | ~10× | [AWQ](https://huggingface.co/stelterlab/phi-4-AWQ) | ✅ GGUF 100% probado. ⚠️ AWQ 90% (dtype float16) |
| 6 | **Qwen3-8B** | 8.2B | 4.1 GB | ~9.9 GB | ~24× | [AWQ](https://huggingface.co/Qwen/Qwen3-8B-AWQ) | ✅ **Probado: 100% (60/60)**. 1.78M↓. Sin marlin |
| 7 | **Ministral-3-14B** | 14B | ~8.5 GB | ~5.7 GB | ~12× | [AWQ](https://huggingface.co/cyankiwi/Ministral-3-14B-Instruct-2512-AWQ-4bit) | Dic 2025. Mistral. JSON mode sólido |
| 8 | **Mistral Nemo 12B** | 12.2B | 6.1 GB | ~7.9 GB | ~20× | [AWQ](https://huggingface.co/casperhansen/mistral-nemo-instruct-2407-awq) | Sweet spot 12B. awq_marlin. Sep 2024 |
| 9 | **EXAONE-3.5-7.8B** | 7.8B | 3.9 GB | ~10.1 GB | ~34× | [AWQ](https://huggingface.co/LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct-AWQ) | Feb 2026. Text-only. 370K↓. Multilingüe |
| 10 | **Bielik-11B-v3.0** | 11B | 5.5 GB | ~8.5 GB | ~22× | [AWQ](https://huggingface.co/speakleash/Bielik-11B-v3.0-Instruct-awq) | Dic 2025. Arquitectura novel. 213K↓ |
| 11 | **gemma-3-12b-it-int4-awq** | 12B | 6.0 GB | ~8 GB | ~20× | [AWQ](https://huggingface.co/gaunernst/gemma-3-12b-it-int4-awq) | 2025. Text-only. 213K↓. Bien probado |
| 12 | **gemma-3-27b-it-int4-awq** | 27B | 13.5 GB | ⚠️ ~0.5 GB | ~1-2× | [AWQ](https://huggingface.co/gaunernst/gemma-3-27b-it-int4-awq) | 2025. 1.35M↓. Muy ajustado en VRAM |
| 13 | **Qwen3.5-9B** | 9.65B | ~6 GB | ~8 GB | ~20× | [AWQ](https://huggingface.co/QuantTrio/Qwen3.5-9B-AWQ) | Mar 2026. 👁️ multimodal. 1.2M↓ |
| 14 | **Devstral Small 2 24B** | 24B | 12.0 GB | ⚠️ ~2 GB | ~4× | [AWQ](https://huggingface.co/cyankiwi/Devstral-Small-2-24B-Instruct-2512-AWQ-4bit) | 2025. Similares specs a Mistral Small |
| 15 | **Instella-MoE-16B-A3B-Think** | 16B/3B | 7.9 GB | ~6.1 GB | ~15× | [AWQ](https://huggingface.co/cyankiwi/Instella-MoE-16B-A3B-Think-AWQ-INT4) | Jul 2026. AMD. Text-only. ⚠️ Custom code en vLLM |
| 16 | **Qwen3.8-27B** | 27B | 15.5 GB | ⚠️ ~0 GB | ~1× | [AWQ](https://huggingface.co/cyankiwi/Qwen3.8-27B-AWQ-INT4) | Ago 2026. 👁️ multimodal. Offload requerido |
| 17 | **Qwen3-4B** | 4B | 2.0 GB | ~12.5 GB | ~50× | [AWQ](https://huggingface.co/Qwen/Qwen3-4B-AWQ) | 2025. Text-only. 169K↓ |
| 18 | **Qwen3.5-4B** | 4B | 2.0 GB | ~12.5 GB | ~50× | [AWQ](https://huggingface.co/cyankiwi/Qwen3.5-4B-AWQ-4bit) | 2026. 👁️ multimodal. 740K↓ |
| 19 | **Phi-4-mini 3.8B** | 3.8B | 1.9 GB | ~12.6 GB | ~60-80× | [AWQ](https://huggingface.co/drawais/Phi-4-mini-instruct-AWQ-INT4) | May 2025. MS. Máxima concurrencia |
| 20 | **Qwen2.5 14B** | 14.8B | 9.4 GB | ~3.1 GB | ~8× | [AWQ](https://huggingface.co/Qwen/Qwen2.5-14B-Instruct-AWQ) | ✅ Probado: F1 93.3%. Reemplazado por Qwen3 |

---

## Ranking por CONCURRENCIA

| # | Modelo | Params | VRAM pesos | KV cache | Concurrencia | Repo HF |
|---|---|---|---|---|---|---|
| 🥇 | Phi-4-mini 3.8B | 3.8B | 1.9 GB | ~12.6 GB | **~60-80×** | [AWQ](https://huggingface.co/drawais/Phi-4-mini-instruct-AWQ-INT4) |
| 🥈 | Qwen3-4B / Qwen3.5-4B | 4B | 2.0 GB | ~12.5 GB | **~50×** | [AWQ 3](https://huggingface.co/Qwen/Qwen3-4B-AWQ) · [AWQ 3.5](https://huggingface.co/cyankiwi/Qwen3.5-4B-AWQ-4bit) |
| 🥉 | Mistral 7B v0.3 | 7.3B | 3.7 GB | ~10.3 GB | **~35×** | [AWQ](https://huggingface.co/solidrust/Mistral-7B-Instruct-v0.3-AWQ) |
| 4 | EXAONE-3.5-7.8B | 7.8B | 3.9 GB | ~10.1 GB | ~34× | [AWQ](https://huggingface.co/LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct-AWQ) |
| 5 | Llama 3.1 8B | 8.0B | 4.0 GB | ~10 GB | ~29× | [AWQ](https://huggingface.co/hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4) |
| 6 | Qwen3-8B | 8.2B | 4.1 GB | ~9.9 GB | ~24× | [AWQ](https://huggingface.co/Qwen/Qwen3-8B-AWQ) |
| 7 | Bielik-11B-v3.0 | 11B | 5.5 GB | ~8.5 GB | ~22× | [AWQ](https://huggingface.co/speakleash/Bielik-11B-v3.0-Instruct-awq) |
| 8 | gemma-3-12b-it | 12B | 6.0 GB | ~8 GB | ~20× | [AWQ](https://huggingface.co/gaunernst/gemma-3-12b-it-int4-awq) |
| 9 | Mistral Nemo 12B | 12.2B | 6.1 GB | ~7.9 GB | ~20× | [AWQ](https://huggingface.co/casperhansen/mistral-nemo-instruct-2407-awq) |
| 10 | Instella-MoE-16B-A3B | 16B/3B | 7.9 GB | ~6.1 GB | ~15× | [AWQ](https://huggingface.co/cyankiwi/Instella-MoE-16B-A3B-Think-AWQ-INT4) |
| 11 | Ministral-3-14B | 14B | ~8.5 GB | ~5.7 GB | ~12× | [AWQ](https://huggingface.co/cyankiwi/Ministral-3-14B-Instruct-2512-AWQ-4bit) |
| 12 | Qwen3-14B | 14.8B | 7.4 GB | ~6.6 GB | ~9× | [AWQ](https://huggingface.co/Qwen/Qwen3-14B-AWQ) |
| 13 | Phi-4 14B | 14.7B | 7.3 GB | ~6.7 GB | ~10× | [AWQ](https://huggingface.co/stelterlab/phi-4-AWQ) |
| 14 | Mistral Small 3.1 24B | 24B | 12.0 GB | ~2 GB | ~4× | [AWQ](https://huggingface.co/stelterlab/Mistral-Small-24B-Instruct-2501-AWQ) |
| 15 | Devstral Small 2 24B | 24B | 12.0 GB | ~2 GB | ~4× | [AWQ](https://huggingface.co/cyankiwi/Devstral-Small-2-24B-Instruct-2512-AWQ-4bit) |
| 16 | gemma-3-27b | 27B | 13.5 GB | ~0.5 GB | ~1-2× | [AWQ](https://huggingface.co/gaunernst/gemma-3-27b-it-int4-awq) |
| 17 | Qwen3.8-27B | 27B | 15.5 GB | ~0 GB | ~1× | [AWQ](https://huggingface.co/cyankiwi/Qwen3.8-27B-AWQ-INT4) |

---

## ⭐ Recomendados para benchmark (orden de ejecución)

| Orden | Modelo | Repo HF | Razón |
|---|---|---|---|
| 1 | **Llama 3.1 8B** | [AWQ](https://huggingface.co/hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4) | Baseline (F1 96.4%, ya probado) |
| 2 | **Qwen3-14B** | [AWQ](https://huggingface.co/Qwen/Qwen3-14B-AWQ) | 100% probado, calidad |
| 3 | **Mistral 7B v0.3** | [AWQ](https://huggingface.co/solidrust/Mistral-7B-Instruct-v0.3-AWQ) | Throughput champion |
| 4 | **Mistral Small 3.1 24B** | [AWQ](https://huggingface.co/stelterlab/Mistral-Small-24B-Instruct-2501-AWQ) | Calidad máxima (probar 10 muestras, 4K ctx) |
| 5 | **EXAONE-3.5-7.8B** | [AWQ](https://huggingface.co/LGAI-EXAONE/EXAONE-3.5-7.8B-Instruct-AWQ) | 2026 text-only |

---

*Fuente: datos medidos en `docs/benchmark_resultados.md` + API Hugging Face (31/08/2026).*

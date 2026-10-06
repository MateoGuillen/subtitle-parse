# Ranking de Modelos GGUF 2026 — Extracción Estructurada (16 GB VRAM)

> **Fecha**: 31 de agosto de 2026 · **Formato**: GGUF (llama.cpp / plugin vLLM experimental)
> **⚠️ Nota clave**: GGUF es experimental/deprecado en vLLM 0.23.0 (plugin externo). Para arquitecturas 2026 muy nuevas (gemma4, qwen3_5, granite) usar **llama-server (llama.cpp)** en vez de vLLM.
> **⚠️ Metodología**: los GGUF usan **GBNF** (gramática de llama.cpp) como backend de guided decoding, NO xgrammar. **No comparar su precisión directamente con AWQ/FP8 (xgrammar) en un mismo ranking** — son mecanismos de restricción distintos con garantías distintas.
> **Tarea**: extracción de 8 variables de pliegos de licitación paraguayos (DNCP) contra pseudo-GT de 900 muestras.

---

## ⚠️ AVISOS DE VALIDACIÓN (verificados en internet 31/08/2026)

1. **Gemma 4 — bug de guided decoding documentado (vLLM #39130)**: si usás vLLM (no llama-server) con `--reasoning-parser gemma4` + `enable_thinking=false`, **xgrammar queda silenciosamente bypaseado** — el FSM de gramática nunca corre. Verificado en GitHub; fix mergeado en PR #45553 (16/06/2026). **Antes de confiar en su schema, correr el smoke test de 5-10 requests y medir `schema_valid_json_rate`**.
2. **Granite 4.2 — razonamiento nativo (thinking)**: IBM lo lanzó el 25/08/2026. Modos full/non-thinking/low-effort. Para extracción directa hay que desactivar thinking y **verificar en la salida cruda** que no aparezcan tokens de razonamiento. En vLLM: `--default-chat-template-kwargs '{"enable_thinking": false}'` + `--reasoning-parser granite_thinking_parser`. En llama-server verificar el flag equivalente del checkpoint.

---

## Ranking por PRECISIÓN (calidad de extracción)

| # | Modelo | Año | Params | VRAM pesos | KV cache | Concurrencia | Repo HF (Q4_K_M) | Evidencia |
|---|---|---|---|---|---|---|---|---|
| 🥇 | **Gemma 4 12B** | Jul 2026 | 12B | 7.66 GB | ~6.5 GB | ~17× | [GGUF](https://huggingface.co/unsloth/gemma-4-12B-it-qat-GGUF) · [QAT oficial](https://huggingface.co/google/gemma-4-12B-it-qat-q4_0-gguf) | Apache 2.0. Multilingüe 100+ idiomas. ⚠️ **Bug #39130: validar guided decoding NO bypaseado (smoke test 5-10 reqs)** |
| 🥈 | **Granite 4.2 8B** | Ago 2026 | 8.8B | 5.5 GB | ~8.7 GB | ~20× | [GGUF](https://huggingface.co/bartowski/granite-4.2-8b-GGUF) | IBM. **12 idiomas + español**. Diseñado para tool-calling/JSON empresarial. ⚠️ **Requiere validar desactivación de thinking (modo default: full thinking)** |
| 🥉 | **Ministral-3-14B** | Dic 2025 | 14B | 8.5 GB | ~5.7 GB | ~12× | [GGUF oficial](https://huggingface.co/mistralai/Ministral-3-14B-Instruct-2512-GGUF) | GGUF oficial Mistral. JSON mode sólido. Apache 2.0 |
| 4 | **Qwen3.8-27B** | Ago 2026 | 27B | 15.5 GB Q4_K_S | ⚠️ ~0 GB | ~1× | [GGUF unsloth](https://huggingface.co/unsloth/Qwen3.8-27B-GGUF) | ⚠️ **STRETCH GOAL** — arq. de 2 semanas, quant delicado (gate multiplicativo en q_proj). ✅ Ya probado: 97/100. Requiere offload KV |
| 5 | **Granite 4.2 30B** | Ago 2026 | 29B | 15.9 GB IQ4_XS | ⚠️ ~0 GB | ~1× | [GGUF](https://huggingface.co/bartowski/granite-4.2-30b-GGUF) | IBM. 12 idiomas + español. ⚠️ **Requiere validar desactivación de thinking** |
| 6 | **Qwen3.6-27B** | 2026 | 27B | 15.4 GB | ⚠️ ~0 GB | ~1× | [GGUF](https://huggingface.co/unsloth/Qwen3.6-27B-GGUF) | 👁️ multimodal. Muy ajustado |
| 7 | **Ornith-1.5-9B** | Ago 2026 | 9.4B | 5.5 GB | ~8.7 GB | ~20× | [GGUF](https://huggingface.co/ornith-ai/Ornith-1.5-9B-GGUF) | MIT. 2.4M↓. Generalista |
| 8 | **Qwen3.5-9B** | Mar 2026 | 9.65B | ~6 GB | ~8 GB | ~20× | [GGUF](https://huggingface.co/unsloth/Qwen3.5-9B-GGUF) | 👁️ multimodal. 1.5M↓ |
| 9 | **Bonsai-27B** | 2026 | 27B | 15.4 GB | ⚠️ ~0 GB | ~1× | [GGUF](https://huggingface.co/prism-ml/Bonsai-27B-gguf) | Arquitectura nueva, poca info |
| 10 | **Mellum2-12B-A2.5B** | Jun 2026 | 12B/2.5B MoE | 8.17 GB | ~6 GB | ~20× | [GGUF](https://huggingface.co/bartowski/Mellum2-12B-A2.5B-Instruct-GGUF) | JetBrains. BFCL v3 66.3%. **Inglés solo** |
| 11 | **Gemma 4 26B-A4B** | Jul 2026 | 25B/4B MoE | 14.2 GB IQ4_XS | ⚠️ ~0 GB | ~1× | [GGUF](https://huggingface.co/unsloth/gemma-4-26B-A4B-it-GGUF) | 👁️ multimodal. MoE 4B activos. MTP speculative |
| 12 | **Gemma 4 E4B** | Jul 2026 | 4B | 2.5 GB | ~12 GB | ~45× | [GGUF](https://huggingface.co/unsloth/gemma-4-E4B-it-GGUF) | 👁️ multimodal. Ultra rápido |
| 13 | **Mistral Small 3.1 24B** | 2025 | 24B | 14 GB | ⚠️ ~2 GB | ~4× | [GGUF](https://huggingface.co/bartowski/mistralai_Mistral-Small-3.1-24B-Instruct-2503-GGUF) | Mar 2025 (no 2026). Fits at Q4_K_S |
| 14 | **Muse-Glimmer-30B** | 2026 | 30B | ❌ ~17 GB | NONE | — | [GGUF](https://huggingface.co/unsloth/Muse-Glimmer-30B-GGUF) | No cabe en 16 GB |

---

## Ranking por CONCURRENCIA

| # | Modelo | Params | VRAM pesos | KV cache | Concurrencia | Repo HF |
|---|---|---|---|---|---|---|
| 🥇 | Gemma 4 E4B | 4B | 2.5 GB | ~12 GB | **~45×** | [GGUF](https://huggingface.co/unsloth/gemma-4-E4B-it-GGUF) |
| 🥈 | Granite 4.2 8B | 8.8B | 5.5 GB | ~8.7 GB | **~20×** | [GGUF](https://huggingface.co/bartowski/granite-4.2-8b-GGUF) |
| 🥉 | Ornith-1.5-9B | 9.4B | 5.5 GB | ~8.7 GB | ~20× | [GGUF](https://huggingface.co/ornith-ai/Ornith-1.5-9B-GGUF) |
| 4 | Qwen3.5-9B | 9.65B | ~6 GB | ~8 GB | ~20× | [GGUF](https://huggingface.co/unsloth/Qwen3.5-9B-GGUF) |
| 5 | Mellum2-12B-A2.5B | 12B/2.5B | 8.17 GB | ~6 GB | ~20× | [GGUF](https://huggingface.co/bartowski/Mellum2-12B-A2.5B-Instruct-GGUF) |
| 6 | Gemma 4 12B | 12B | 7.66 GB | ~6.5 GB | ~17× | [GGUF](https://huggingface.co/unsloth/gemma-4-12B-it-qat-GGUF) |
| 7 | Ministral-3-14B | 14B | 8.5 GB | ~5.7 GB | ~12× | [GGUF oficial](https://huggingface.co/mistralai/Ministral-3-14B-Instruct-2512-GGUF) |
| 8 | Mistral Small 3.1 24B | 24B | 14 GB | ~2 GB | ~4× | [GGUF](https://huggingface.co/bartowski/mistralai_Mistral-Small-3.1-24B-Instruct-2503-GGUF) |
| 9 | Qwen3.8-27B | 27B | 15.5 GB | ~0 GB | ~1× | [GGUF unsloth](https://huggingface.co/unsloth/Qwen3.8-27B-GGUF) |
| 10 | Granite 4.2 30B | 29B | 15.9 GB | ~0 GB | ~1× | [GGUF](https://huggingface.co/bartowski/granite-4.2-30b-GGUF) |
| 11 | Qwen3.6-27B | 27B | 15.4 GB | ~0 GB | ~1× | [GGUF](https://huggingface.co/unsloth/Qwen3.6-27B-GGUF) |
| 12 | Bonsai-27B | 27B | 15.4 GB | ~0 GB | ~1× | [GGUF](https://huggingface.co/prism-ml/Bonsai-27B-gguf) |
| 13 | Gemma 4 26B-A4B | 25B/4B | 14.2 GB | ~0 GB | ~1× | [GGUF](https://huggingface.co/unsloth/gemma-4-26B-A4B-it-GGUF) |

---

## ⭐ Recomendados para benchmark (orden de ejecución)

| Orden | Modelo | Repo HF | Razón |
|---|---|---|---|---|
| 1 | **Gemma 4 12B** | [GGUF](https://huggingface.co/unsloth/gemma-4-12B-it-qat-GGUF) | Mejor modelo 2026 + multilingüe + headroom KV cache. ⚠️ Smoke test guided decoding ANTES |
| 2 | **Granite 4.2 8B** | [GGUF](https://huggingface.co/bartowski/granite-4.2-8b-GGUF) | IBM español + enterprise extraction. ⚠️ Smoke test desactivación thinking ANTES |
| 3 | **Ministral-3-14B** | [GGUF oficial](https://huggingface.co/mistralai/Ministral-3-14B-Instruct-2512-GGUF) | GGUF oficial Mistral, JSON mode. Sin issues conocidos |
| 4 | **Qwen3.8-27B** | [GGUF unsloth](https://huggingface.co/unsloth/Qwen3.8-27B-GGUF) | Ya probado 97% (llama-server). STRETCH GOAL — arq. de 2 semanas |

---

## Comandos de arranque (llama-server)

```bash
# Gemma 4 12B Q4_K_M
llama-server -m gemma-4-12B-it-Q4_K_M.gguf \
  -ngl 99 --ctx-size 8192 --gpu-layers 99 \
  --host 127.0.0.1 --port 1234

# Granite 4.2 8B
llama-server -m granite-4.2-8b-Q4_K_M.gguf \
  -ngl 99 --ctx-size 8192 \
  --host 127.0.0.1 --port 1234

# Qwen3.8-27B (requiere offload, contexto corto)
llama-server -m Qwen3.8-27B-UD-Q4_K_S.gguf \
  -ngl 40 --ctx-size 4096 \
  --host 127.0.0.1 --port 1234
```

---

*Fuente: datos medidos en `docs/benchmark_resultados.md` + API Hugging Face (31/08/2026).*

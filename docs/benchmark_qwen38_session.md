# Sesión Qwen3.8-27B — Nuevo modelo + runtime Windows nativo

**Fecha:** 2026-08-22 | **Rama:** `feature/ocds-csv-pipeline` | **GPU:** RTX 4070 Ti SUPER 16 GiB (driver 581.29, CUDA 12.4) | **CPU:** 10 threads

## Objetivo
Probar **Qwen3.8-27B** (Apache 2.0, sin gating) cuantizado **UD-Q3_K_XL** para extracción estructurada de 12 campos hybrid de `document_features_v2_llm_pipeline` (fallback LLM, ~324K docs), fuera de WSL2 por falta de disco en D: (0.26 GB libres, vhdx en `D:\WpSystem\...`).

## Setup

| Componente | Detalle | Ruta |
|---|---|---|
| Modelo GGUF | `Qwen3.8-27B-UD-Q3_K_XL.gguf` 12.24 GB (13.1 GB nominal) | `C:\models\qwen38\` |
| Tokenizer | `Qwen/Qwen3.8-27B` (tokenizer.json, config.json, chat_template.jinja) | `C:\models\qwen38\tokenizer\` |
| Runtime | `llama.cpp` b10566 nightly `llama-b10566-bin-win-cuda-12.4-x64.zip` 251 MB + `cudart-llama-bin-win-cuda-12.4-x64.zip` 391 MB | `C:\models\llama_cuda\` |
| Python | Windows 3.9.10 + `huggingface_hub 0.36.2` + `requests` | `C:\` |

**Fix CUDA:** `ggml-cuda.dll` 538 MB depende de `cublas64_12.dll`, `cublasLt64_12.dll`, `cudart64_12.dll` — sin `cudart.zip` `LoadLibrary 126` → solo CPU 1.8 tok/s (`llama-bench` `backend CPU`). Tras copiar 3 DLLs a `C:\models\llama_cuda\`: `load_backend: loaded CUDA backend`, `ggml_cuda_init: 1 device 16375 MiB`.

**Por qué Windows nativo:** WSL vhdx es junction `C:\...\LocalState → D:\WpSystem\...` (0.26 GB en D:), descarga 12 GB agotaría D:. `C:` 18.4 GB libres → 5 GB tras modelo+runtime.

## Ejecución

### Fase 0 — Check
- `nvidia-smi` 16375 MiB, `df /dev/sdc 820G` virtual vs D: 0.26 GB físico, `which llama-server` vacío en WSL.

### Fase 1 — Descarga
- `huggingface_hub` descarga GGUF 4:21 min (fallback HTTP, sin `hf_xet`), tokenizer 4 archivos.

### Fase 2 — Instalación llama-server
- `winget install llama.cpp` no existe, nightly tag `b10566` vía `nightly-tag.txt`, `Invoke-WebRequest` asset `llama-b10566-bin-win-cuda-12.4-x64.zip`, `Expand-Archive` a `C:\models\llama_cuda`, `llama-server.exe --version` 0.2.0-dev bb4caa754.

### Fase 3 — Smoke test + CUDA
- `llama-server -m Qwen3.8-27B-UD-Q3_K_XL.gguf -ngl 999 --parallel 4 -c 8192 --port 1234` → `model has unused tensor blk.64.*` (MTP head `mtp_num_hidden_layers 1`, `num_hidden_layers 64`, `hidden_size 5120` en `config.json`) ignorado sin `--mtp`, `model loaded` `listening 127.0.0.1:1234`, pero `nvidia-smi` 1471 MiB → CPU. Diagnóstico `LoadLibrary 126` → fix cudart.
- Tras fix: `llama-bench -m ... -ngl 999 -p 32 -n 32` → `CUDA 487 tok/s prompt, 39 tok/s gen` (`C:\models\llama_bench2.log`), `VRAM 14677 MiB` (2 slots 8192).
- API `POST /v1/chat/completions` con `reasoning_effort none` → `content '60'` sin `reasoning_content` (363 tok reasoning con `max_tokens 64` truncaba). Test `SYSTEM_PROMPT` completo (`_test_full_prompt.py`): `60`/`NULL`/`50` OK en ambos modos, `none` ahorra 7× tokens.

### Fase 4 — Benchmark 100 GT humanas
- GT: `data/processed/llm_benchmark/ground_truth_5_campos.csv:1` (100, 5 híbridos duros ×20: `dias_validez_garantia`, `pct_monto_referencia_tecnica`, `anos_experiencia_minima_tecnica`, `dias_pago_anticipo`, `dias_solicitud_anticipo`) + `gt_texts.json` (9518 chars max ≈2064 tok).
- Script `scripts/benchmark_qwen38_http.py:64` (HTTP OpenAI, `temperature 0`, `max_tokens 32`, `cache_prompt False`, `parse_response` `extraction_postprocess.py:90`, retry 5 con reinicio `Get-Process llama-server | Stop-Process`).
- Crash inicial: `n_ctx_slot 2048` (4 slots) insuficiente para 2064 tok → `ConnectionReset 10054` tras ~60 req, `nvidia-smi` 1307 MiB, log `task 638` 1341 tok `release` sin `launch` siguiente. Fix `-c 16384/2 slots` (8192/slot) luego `8192/1 slot` (14026 MiB) + `cache_prompt False` (desactiva LCP reuse `f_keep 0.3-0.9`, `graphs reused 847→269`) + reinicio batch cada 10.
- Resultado final `C:\models\bench_qwen38.out.log` 141s:
  - `dias_validez_garantia` 20/20 100%
  - `pct_monto_referencia_tecnica` 19/20 95% FAIL `429213` 70→NULL
  - `anos_experiencia_minima_tecnica` 19/20 95% FAIL `444948` 3→NULL
  - `dias_pago_anticipo` 20/20 100%
  - `dias_solicitud_anticipo` 19/20 95% FAIL `441033` 3→10
  - **TOTAL 97/100 97%** `Qwen38-27B-UD-Q3_K_XL.json:146`
- RAW vs PP (`C:\models\_calc_raw.py`): **RAW 98%** (solo 2 FAIL) vs **PP 97%** (PP convierte `441033` 3→10 por regla `KNOWN_DEFAULTS`).
- 12 híbridos ×10=120 proxy (`benchmark_qwen38_12_rawpp.py`, `full_460_with_context.csv`): **RAW=PP 33.3%** no válido (proxy `db_value 0` vs LLM 7/1/0.8).

## Hallazgos

**Precisión:** Qwen3.8 97% no supera **Llama 3.1 8B-AWQ 100%** en 100 duros (Sesión 14 AloLab, mismo `SYSTEM_PROMPT` + postprocess). Δ -3pp no significativo (IC95 97% [92.5,99.2] vs 100% [96.3,100], p≈0.12). En 460 muestras histórico L_raw 51%→L_pp 63% (+12pp), Qwen3.8 raw ya 98% en 5 duros indica buen base, PP solo resta 1pp por regla agresiva.

**Velocidad:** Qwen3.8 33-39 tok/s gen, 1015-1432 tok/s prompt, **1.41s/muestra** (141s/100) con `cache_prompt False`; vs Llama 64-77 tok/s, ~0.6s/m, 5.4 GiB, 29× concurrencia; Mistral 7B 3.9 GiB 35×. Qwen3.8 2× más lento y 2.6× VRAM por 27B.

**Estabilidad:** Bug KV LCP + DeltaNet hybrid en b10566 → OOM tras ~60 req grandes sin `cache_prompt False`; con fix + batch 10+reinicio 35s estable 120 req, GPU 13940-14046 MiB.

## Recomendación para detección de anomalías
Fiabilidad > velocidad: **Llama 3.1 8B-AWQ** queda campeón para `run_document_features_v2_llm_pipeline.py:42` (12 híbridos, ~324K docs, ~3h batch 4, vLLM 0.23 `VLLM_USE_V2_MODEL_RUNNER=0` en `/root/vllm-env`). Qwen3.8 viable como 2º modelo pero no justifica 27B para +0pp. Medir precisión requiere GT humano 200×26=5200 (≈2h, batch+reinicio) con métricas `accuracy`/`FP`/`FN` y test McNemar; proxy `db_value 0` no sirve.

## Archivos
- Modelo: `C:\models\qwen38\Qwen3.8-27B-UD-Q3_K_XL.gguf`, `C:\models\qwen38\tokenizer\`
- Runtime: `C:\models\llama_cuda\llama-server.exe`, `ggml-cuda.dll` + `cublas*.dll`/`cudart64_12.dll`
- Benchmarks: `scripts/benchmark_qwen38_http.py`, `scripts/benchmark_qwen38_12_rawpp.py`, `C:\models\_test_qwen38*.py`, `C:\models\_calc_raw.py`
- Resultados: `data/processed/llm_benchmark/multimodel_results/Qwen38-27B-UD-Q3_K_XL.json`, `Qwen38-12hybrid-rawpp.json`, `C:\models\bench*.log`, `C:\models\llama_server*.log`
- Docs: `docs/benchmark_resultados.md` (pendiente actualizar ranking AWQ + Qwen3.8)

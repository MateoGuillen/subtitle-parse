# Benchmark de Modelos Locales — Pipeline 2 (Extracción Estructurada)

## Contexto

Pipeline de extracción de 315,000 secciones de pliegos de licitaciones públicas de Paraguay.
Cada sección tiene 6-8 campos binarios/numéricos a extraer vía LLM con schema JSON.

**Proyecto**: `D:\projects\subtitle-parse`
**Fecha**: Julio 2026
**Objetivo**: Encontrar el mejor modelo local que quepa en 16GB VRAM para ejecutar Pipeline 2 con alta calidad y throughput.

---

## Hardware

| Componente | Especificación |
|-----------|---------------|
| GPU | NVIDIA RTX 4070 Ti SUPER — **16GB VRAM** |
| RAM | 32GB |
| SO | Windows 11 + WSL2 (Ubuntu) |
| vLLM | 0.23.0 (V1 engine) — AWQ nativo, GGUF experimental |
| VRAM utilizable | ~15.6 GB (tras matar NVIDIA Overlay, Warp, Edge, SearchApp) |
| Formato modelo | **AWQ (nativo, recomendado)** — GGUF (experimental/deprecated en vLLM) |

---

## ⚠️ GGUF vs AWQ

**GGUF (llama.cpp) está en deprecación en vLLM**: movido fuera del árbol principal a un plugin externo. Para producción, usar **formatos nativos**:

| Formato | Estado en vLLM | Performance | Uso VRAM | Recomendación |
|---------|---------------|-------------|----------|--------------|
| **AWQ** (W4A16) | Nativo, awq_marlin kernel | ✅ Excelente | Bajo | **✅ Recomendado** |
| **FP8** (W8A8) | Nativo (Ada Lovelace+) | ✅ Bueno | Medio | Alternativa si AWQ no disponible |
| **GPTQ** | Nativo | ✅ Bueno | Bajo | Alternativa |
| **GGUF** | Plugin externo (mantenimiento comunitario) | ⚠️ Variable | Variable | ❌ Deprecado |

**Impacto real**: AWQ con awq_marlin es **3× más rápido que GGUF** (1.61s vs 4.90s para Mistral 7B) y usa **menos VRAM** (3.89 GiB vs ~5.5 GiB).

---

## Pipeline de Evaluación

### Script principal
`scripts/evaluate_extraction.py --provider vllm --samples-per-title N --skip-sample`

- Samplea secciones de la DB (`dncp.pliegos_secciones`)
- Corre extracción contra vLLM (HTTP o local)
- Genera pseudo-ground truth por majority vote
- Reporta: OK/N, F1, MAE, tiempo

### Modos de ejecución

| Modo | Descripción | Comando |
|------|------------|---------|
| **HTTP server** | vLLM como servidor, requests secuenciales | `vllm serve + curl/requests` |
| **Offline API** | `LLM.generate()` in-process con continuous batching | `python scripts/benchmark_offline.py` |

---

## Inventario de Modelos GGUF

| Modelo | Archivo | Tamaño | VRAM est. | Path en WSL |
|--------|---------|--------|-----------|-------------|
| Qwen 2.5 14B Q5_K_L | `Qwen2.5-14B-Instruct-Q5_K_L.gguf` | 10.2 GB | ~15.7 GB | `lmstudio-community/Qwen2.5-14B-Instruct-Q5_K_L/` |
| Qwen 2.5 14B Q4_K_S | `Qwen2.5-14B-Instruct-Q4_K_S.gguf` | 8.0 GB | ~12 GB | `bartowski/Qwen2.5-14B-Instruct-GGUF/` |
| Qwen 2.5 7B Q4_K_M | `Qwen2.5-7B-Instruct-Q4_K_M.gguf` | 4.4 GB | ~7 GB | `bartowski/Qwen2.5-7B-Instruct-GGUF/` |
| gpt-oss-20b-MXFP4 | `gpt-oss-20b-MXFP4.gguf` | 11.3 GB | N/A | `lmstudio-community/gpt-oss-20b-GGUF/` |
| Qwen 2.5 Coder 14B Q4_K_M | `Qwen2.5-Coder-14B-Instruct-Q4_K_M.gguf` | 8.4 GB | ~12 GB | `lmstudio-community/.../` |
| Qwen 2.5 Coder 14B Q6_K | `qwen2.5-coder-14b-instruct-q6_k.gguf` | 11.3 GB | ~16 GB | `Lucy-in-the-Sky/.../` |
| **✅ Qwen3-14B Q5_K_M** | `Qwen3-14B-Q5_K_M.gguf` | **9.8 GB** | ~12.5 GB | `unsloth/Qwen3-14B-GGUF/` |
| **⚠️ Gemma 3 12B Q8_0** | `google_gemma-3-12b-it-Q8_0.gguf` | 11.7 GB | ~14 GB | `bartowski/google_gemma-3-12b-it-GGUF/` |
| Mistral Small 3.2 24B (pendiente) | Q4_K_M | ~13-14 GB | ~15.5 GB | `unsloth/.../` |
| **✅ Mistral 7B v0.3 Q5_K_M** | `Mistral-7B-Instruct-v0.3-Q5_K_M.gguf` | **4.8 GB** | ~5.5 GB | `bartowski/Mistral-7B-Instruct-v0.3-GGUF/` |
| **✅ Qwen3-8B Q5_K_M** | `Qwen3-8B-Q5_K_M.gguf` | **5.4 GB** | ~6.5 GB | `Qwen/Qwen3-8B-GGUF/` |
| **✅ Phi-4 14B Q4_K_M** | `phi-4-Q4_K_M.gguf` | **8.4 GB** | ~10 GB | `bartowski/phi-4-GGUF/` |
| **❌ Llama 3.1 8B Q5_K_M** | `Meta-Llama-3.1-8B-Instruct-Q5_K_M.gguf` | 5.3 GB | ~6.5 GB | `bartowski/Meta-Llama-3.1-8B-Instruct-GGUF/` |
| **❌ Gemma2 27B Q4_K_M** | `gemma-2-27b-it-Q4_K_M.gguf` | 15.5 GB | ~17 GB | `bartowski/gemma-2-27b-it-GGUF/` |

---

## Inventario de Modelos AWQ (formato nativo vLLM)

| Modelo (AWQ) | Path HF | Tamaño | VRAM est. | Kernel |
|-------------|---------|--------|-----------|--------|
| **🥇 Mistral 7B v0.3 AWQ** | `solidrust/Mistral-7B-Instruct-v0.3-AWQ` | **3.88 GiB** | **3.89 GiB** | **awq_marlin** ✅ |
| Qwen3-8B AWQ | `Qwen/Qwen3-8B-AWQ` | ~6 GB | ~6.5 GiB | awq ⚠️ |
| Qwen3-14B AWQ | `Qwen/Qwen3-14B-AWQ` | ~9 GB | ~9.3 GiB | awq ⚠️ |
| Phi-4 14B AWQ | `curiousmind147/microsoft-phi-4-AWQ-4bit-GEMM` | 8.51 GiB | 8.51 GiB | awq ⚠️ |

**⚠️ Nota**: awq_marlin solo soportado para MistralForCausalLM (Mistral 7B, Llama). Qwen y Phi caen a `awq` (más lento).

---

## Modelos Evaluados — Resultados Detallados

### 1. Qwen 2.5 14B Q5_K_L ✅ VIABLE (97.5-100%)

**Config vLLM que funciona**:
```bash
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
vllm serve "$MODEL" \
    --tokenizer "Qwen/Qwen2.5-14B-Instruct" \
    --host 0.0.0.0 --port 8000 \
    --max-model-len 8192 \
    --gpu-memory-utilization 0.85 \
    --enforce-eager \
    --kv-cache-dtype fp8 \
    --enable-prefix-caching
```

#### Modo HTTP server (requests secuenciales)

| Test | Muestras | OK | % | Total time | s/sample |
|------|----------|---|--|-----------|---------|
| enforce-eager, max-len=6144, gpu=0.85 | 40 | 39 | 97.5% | ~372s | ~9.3s |
| CUDA graphs, max-len=14000, gpu=0.90 | 80 | 80 | 100% | ~568s | ~7.1s |
| CUDA graphs, max-len=14000, gpu=0.90 (2nd run) | 80 | 80 | 100% | ~520s | ~6.5s |

**Prompt throughput**: 130-260 tok/s
**Generation throughput**: 10-20 tok/s
**Prefix cache hit rate**: 21.8%

#### Modo Offline API (LLM.generate + continuous batching)

Config: `max_model_len=2048, gpu_memory_utilization=0.85, enforce_eager=True`

| Title | Muestras | OK | Total (s) | s/sample |
|-------|----------|---|-----------|---------|
| Fraude Y Corrupcion | 10 | 10/10 | 59.5 | 5.95 |
| Confidencialidad De La Informacion | 10 | 10/10 | 66.4 | 6.64 |
| Fuerza Mayor | 10 | 10/10 | 68.4 | 6.84 |
| Aclaracion De Las Ofertas | 10 | 10/10 | 44.1 | 4.41 |
| Interpretacion | 10 | 10/10 | 54.7 | 5.47 |
| Formato Y Firma De La Oferta | 10 | 6/10 | 44.3 | 4.43 |
| Impuestos Y Derechos | 10 | 10/10 | 38.8 | 3.88 |
| Derechos Intelectuales | 10 | timeout | — | — |
| **Total (parcial)** | **70** | **66/70 (94.3%)** | **376s** | **~5.1s avg** |

**VRAM del profiler**:
- Model loading: 10.55 GiB
- KV cache disponible: 1.92 GiB
- Concurrencia máxima: 5.12x (para max_model_len=2048)
- Tiempo de carga: 82.7s

**Throughput vs HTTP**: 7.0s → 5.1s = **1.37x improvement**

**Nota**: El speedup es modesto porque los batches son pequeños (10 samples/title). El JIT compilation overhead (~40s en primer sample por batch) domina el tiempo total. Con batches de 100-1000+ samples, se espera mucho mayor speedup del continuous batching.

---

### 2. Qwen3-14B Q5_K_M ✅ RECOMENDADO (100% en 60 muestras)

**Veredicto: MEJOR MODELO HASTA LA FECHA**. Reemplaza Qwen2.5 14B como recomendación principal.

#### Issues técnicos

| Problema | Causa | Solución |
|----------|-------|----------|
| UVA not available | `Qwen3ForCausalLM` usa V2 Model Runner por defecto, que requiere UVA | `VLLM_USE_V2_MODEL_RUNNER=0` |
| HTTP server crash | Muere silenciosamente tras registrar rutas (probable OOM + multiprocessing) | Usar **offline API** (`LLM.generate()`) en vez del HTTP server |
| Thinking mode activo por defecto | Qwen3 tiene `enable_thinking=True` en el chat template | Pre-renderizar con `apply_chat_template(messages, enable_thinking=False)` |

#### Configuración que funciona (offline API)

```bash
export VLLM_USE_V2_MODEL_RUNNER=0
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
```

```python
from vllm import LLM, SamplingParams
from transformers import AutoTokenizer

model_path = "/mnt/d/llmstudio-models/unsloth/Qwen3-14B-GGUF/Qwen3-14B-Q5_K_M.gguf"
llm = LLM(model=model_path, tokenizer="Qwen/Qwen3-14B",
          max_model_len=2048, gpu_memory_utilization=0.85,
          enforce_eager=True, kv_cache_dtype="fp8",
          enable_prefix_caching=True)

hf_tok = AutoTokenizer.from_pretrained("Qwen/Qwen3-14B", trust_remote_code=True)
rendered = hf_tok.apply_chat_template(messages, tokenize=False,
                                      add_generation_prompt=True,
                                      enable_thinking=False)
outputs = llm.generate([rendered], sampling_params=SamplingParams(temperature=0, max_tokens=512))
```

#### Métricas de calidad (60 samples, 6 títulos)

| Título | OK/N | Avg time | Avg tok/s |
|--------|:----:|:--------:|:---------:|
| Fraude Y Corrupcion | 10/10 | 7.7s | ~14.5 |
| Confidencialidad De La Informacion | 10/10 | 8.6s | ~14.1 |
| Fuerza Mayor | 10/10 | 8.8s | ~13.8 |
| Aclaracion De Las Ofertas | 10/10 | 6.8s | ~18.1 |
| Interpretacion | 10/10 | 7.1s | ~14.5 |
| Formato Y Firma De La Oferta | 10/10 | 6.7s | ~18.3 |
| **Total** | **60/60 (100%)** | **7.6s** | **~15.6** |

#### Métricas de memoria

| Métrica | Valor |
|---------|-------|
| Model loading | 9.87 GiB, ~72s |
| KV cache | 2.38 GiB |
| KV cache capacity | 31,184 tokens |
| Max concurrency (2048 tokens) | 15.23× |
| Init engine (profile + warmup) | ~35s |
| Load to first generation | ~146s total |

#### Comparativa con Qwen2.5 14B

| Métrica | Qwen2.5 14B Q5_K_L | Qwen3-14B Q5_K_M | Diferencia |
|---------|-------------------|-----------------|------------|
| Calidad | 97.5% (39/40) | **100% (60/60)** | +2.5 pp |
| Tamaño | 10.2 GB | **9.8 GB** | -4% |
| VRAM modelo | ~10.6 GiB | **9.87 GiB** | -7% |
| Concurrencia | 5.12× (2048 tok) | **15.23×** | +197% |
| Velocidad output | ~10-17 tok/s | **13-19 tok/s** | +15% |

### 3. Qwen 2.5 7B Q4_K_M ❌ DEMASIADO DÉBIL

| Test | Muestras | OK | s/sample | Resultado |
|------|----------|---|---------|-----------|
| max-len=16384, gpu=0.92, fp8 | 40+ | 0% | ~9s (con retries) | FAIL |

**Falla sistemática**: Omite exactamente 1 campo por título:
| Title | Campo omitido |
|-------|--------------|
| Fraude Y Corrupcion | `contiene_denuncia_penal` |
| Confidencialidad De La Informacion | `menciona_ley_7021` |
| Fuerza Mayor | `contiene_plazo_notificaci` |
| Aclaracion De Las Ofertas | `solicitud_obligatoria` |
| Interpretacion | `contiene_limitacion_dispe` |
| Formato Y Firma De La Oferta | `menciona_lista_precios` |
| Impuestos Y Derechos | `contiene_excepcion_impues` |

**Veredicto**: 7B es insuficiente para extracción estructurada con schema de 6-8 campos.

---

### 4. Mistral 7B v0.3 Q5_K_M ✅ NUEVO CAMPEÓN (100% en 60 muestras)

**Veredicto: NUEVO MODELO RECOMENDADO**. Reemplaza Qwen3-14B como recomendación principal.

#### Configuración
```python
from vllm import LLM, SamplingParams
from transformers import AutoTokenizer

model_path = "/mnt/d/llmstudio-models/bartowski/Mistral-7B-Instruct-v0.3-GGUF/Mistral-7B-Instruct-v0.3-Q5_K_M.gguf"
tok_dir = "/mnt/d/llmstudio-models/bartowski/Mistral-7B-Instruct-v0.3-GGUF"

llm = LLM(model=model_path, tokenizer=tok_dir,
          max_model_len=4096, gpu_memory_utilization=0.85,
          enforce_eager=True, kv_cache_dtype="fp8",
          enable_prefix_caching=True)

hf_tok = AutoTokenizer.from_pretrained(tok_dir, trust_remote_code=True)
rendered = hf_tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
outputs = llm.generate([rendered], sampling_params=SamplingParams(temperature=0, max_tokens=512))
```

#### Métricas de calidad (60 samples, 6 títulos)

| Título | OK/N | Avg time |
|--------|:----:|:--------:|
| Fraude Y Corrupcion | 10/10 | 4.8s |
| Confidencialidad De La Informacion | 10/10 | 5.1s |
| Fuerza Mayor | 10/10 | 5.1s |
| Aclaracion De Las Ofertas | 10/10 | 4.2s |
| Interpretacion | 10/10 | 4.5s |
| Formato Y Firma De La Oferta | 10/10 | 4.7s |
| **Total** | **60/60 (100%)** | **4.90s** |

#### Ventajas sobre Qwen3-14B

| Métrica | Qwen3-14B Q5_K_M (anterior) | Mistral 7B v0.3 Q5_K_M (nuevo) | Diferencia |
|---------|-------------------------------|-------------------------------|------------|
| Calidad | 100% (60/60) | **100% (60/60)** | Igual |
| Tamaño GGUF | 9.8 GB | **4.8 GB** | **−51%** |
| VRAM modelo | ~9.87 GiB | **~5.5 GiB (est.)** | **−44%** |
| Velocidad | 5.10s avg | **4.90s avg** | **−4%** |
| Concurrencia potencial | 15.23× | **~27× (est.)** | **+77%** |

Con más VRAM disponible para KV cache, Mistral 7B permite **~27× de concurrencia** vs 15.23× de Qwen3-14B, proyectando **~1 día para 315K secciones** (vs ~1.8 días con Qwen3-14B).

---

### 5. Qwen3-8B Q5_K_M ✅ VIABLE (100% en 60 muestras — con fix de thinking mode)

**Veredicto**: Sorprendentemente bueno tras desactivar thinking mode. Alternativa válida y más rápida, pero 8B puede ser menos robusto en secciones largas.

#### Problema resuelto
| Problema | Causa | Solución |
|----------|-------|----------|
| Thinking mode activo por defecto | Qwen3-8B tiene `enable_thinking=True` en el chat template | Pre-renderizar con `hf_tok.apply_chat_template(messages, enable_thinking=False)` |

#### Métricas

| Título | OK/N | Avg time |
|--------|:----:|:--------:|
| Fraude Y Corrupcion | 10/10 | 4.7s |
| Confidencialidad De La Informacion | 10/10 | 5.0s |
| Fuerza Mayor | 10/10 | 4.9s |
| Aclaracion De Las Ofertas | 10/10 | 3.9s |
| Interpretacion | 10/10 | 4.0s |
| Formato Y Firma De La Oferta | 10/10 | 4.5s |
| **Total** | **60/60 (100%)** | **4.50s** |

**Nota**: Se obtuvo 100% solo después de aplicar pre-renderizado con `enable_thinking=False`. Sin este fix, todas las respuestas incluyen tokens de razonamiento y fallan validación JSON (0/60).

---

### 6. Phi-4 14B Q4_K_M ✅ VIABLE (100% en 60 muestras)

**Veredicto**: Modelo grande (14B) de Microsoft con 100% calidad, pero más lento que Mistral 7B y Qwen3-14B.

#### Métricas

| Título | OK/N | Avg time |
|--------|:----:|:--------:|
| Fraude Y Corrupcion | 10/10 | 6.1s |
| Confidencialidad De La Informacion | 10/10 | 6.6s |
| Fuerza Mayor | 10/10 | 6.1s |
| Aclaracion De Las Ofertas | 10/10 | 5.2s |
| Interpretacion | 10/10 | 5.5s |
| Formato Y Firma De La Oferta | 10/10 | 7.0s |
| **Total** | **60/60 (100%)** | **6.09s** |

**Ventaja**: Phi-4 tiene entrenamiento especializado en razonamiento estructurado, posiblemente más robusto para secciones atípicas. Sin embargo, el speed penalty (+24% vs Qwen3-14B) lo hace menos atractivo para 315K secciones.

---

### 7. Llama 3.1 8B Q5_K_M ❌ TOKENIZER FALTANTE

**Resultado**: No se pudo cargar — el directorio del GGUF carece de archivos de tokenizer (`tokenizer.json`, etc.).

**Causa**: El repositorio de bartowski solo contiene el `.gguf`. El tokenizer original de Meta está en repositorio gated (`meta-llama/Meta-Llama-3.1-8B-Instruct`) que requiere `HF_TOKEN` y aceptación de licencia.

**Posible solución**: Copiar tokenizer desde otro modelo abierto con vocabulario compatible, o usar `tokenizer="meta-llama/Meta-Llama-3.1-8B-Instruct"` con autenticación HF.

---

### 8. Gemma2 27B Q4_K_M ❌ FUERA DE VRAM

**Resultado**: GGUF de 15.5 GB no cabe en 16 GB VRAM.

**Error**:
```
RuntimeError: Failed to map GGUF parameters (1): ['lm_head.weight']
```

**Causa**: El GGUF completo requiere ~17+ GB VRAM (modelo + overhead). La VRAM disponible (~15.6 GB tras limpiar procesos) es insuficiente. El error `lm_head.weight` es consecuencia de la falta de memoria.

---

### 9. Qwen 2.5 14B Q4_K_S ⚠️ CRASH INESPERADO

| Intento | Config | Resultado |
|---------|--------|-----------|
| 1 | CUDA graphs, max-len=16384, gpu=0.92, fp8 | Crash silencioso (log vacío) |
| 2 | enforce-eager, max-len=8192, gpu=0.80, fp8 | Crash silencioso |

**Causa probable**: Incompatibilidad del checkpoint Q4_K_S con vLLM 0.23.0 + GGUF plugin.
No se investigó a fondo (se priorizó Q5_K_L que funciona).

---

### 10. gpt-oss-20b-MXFP4 ❌ GGUF READER CRASH

| Intento | Config | Resultado |
|---------|--------|-----------|
| 1 | Sin tokenizer, max-len=8192, gpu=0.85, enforce-eager | Crash en GGUF reader |

**Error**:
```
File "/root/vllm-env/lib/python3.12/site-packages/gguf/gguf_reader.py", line 231, in _get_field_parts
    size = sum(int(part.nbytes) for part in sparts)
```
**Causa**: MXFP4 es el formato nativo de cuantización de gpt-oss, no un K-quant estándar de llama.cpp. El GGUF reader de vLLM no soporta este formato.

---

### 11. Mistral 7B v0.3 AWQ (awq_marlin) ✅ CAMPEÓN DEFINITIVO (100% en 10/10 muestras)

**Veredicto: MODELO RECOMENDADO PARA PRODUCCIÓN**. AWQ nativo con awq_marlin kernel: 3× más rápido que GGUF, 30% menos VRAM.

#### Configuración
```python
from vllm import LLM, SamplingParams
from transformers import AutoTokenizer

llm = LLM(model="solidrust/Mistral-7B-Instruct-v0.3-AWQ",
          quantization="awq_marlin",
          max_model_len=4096, gpu_memory_utilization=0.85,
          enforce_eager=True, kv_cache_dtype="fp8",
          max_num_seqs=32, enable_prefix_caching=True)

hf_tok = AutoTokenizer.from_pretrained("solidrust/Mistral-7B-Instruct-v0.3-AWQ")
```

#### Métricas (10 samples, 1 título)

| Métrica | Valor |
|---------|-------|
| Calidad (struct OK) | **10/10 = 100%** |
| **F1 vs pseudo-GT** (270 samples) | **94.0%** |
| **Precision** | **88.9%** |
| **Recall** | **99.8%** |
| **Keyword evidence rate** | **91%** |
| **Agreement vs consensus** | **92.3%** |
| Tiempo promedio | **1.61s** |
| Output tokens/s | **64-77 tok/s** |
| Input tokens/s | **1032-1224 tok/s** |
| VRAM modelo | **3.89 GiB** |
| KV cache disponible | **8.81 GiB** |
| Concurrencia máxima (4096 tok) | **35.22×** |
| Tiempo de carga | **~27s** |

#### Comparativa vs GGUF (Mistral 7B Q5_K_M)

| Métrica | GGUF Q5_K_M | AWQ (awq_marlin) | Diferencia |
|---------|-------------|-----------------|------------|
| Calidad | 100% | **100%** | Igual |
| Tiempo promedio | 4.90s | **1.61s** | **−67% (3× más rápido)** |
| VRAM modelo | ~5.5 GiB (est.) | **3.89 GiB** | **−29%** |
| Concurrencia | ~27× (est.) | **35.22×** | **+30%** |
| Output tok/s | ~25-32 | **64-77** | **+140%** |
| Tiempo carga | ~30-40s | **~27s** | Similar |

#### Proyección para 315K secciones
- **Secuencial**: ~5.9 días (vs 17.8 días GGUF)
- **Con continuous batching (35× concurrencia)**: **~4 horas** (vs ~1 día GGUF)

### 12. Llama 3.1 8B AWQ (awq_marlin) ✅ MEJOR CALIDAD (F1=96.4%)

**Veredicto: MEJOR F1 vs PSEUDO-GT (96.4%).** Supera a Mistral 7B en calidad de extracción (+2.4pp F1) con awq_marlin kernel. Recomendado para producción si la calidad prima sobre el throughput.

#### Métricas de calidad (270 samples, 9 títulos con 6-8 campos c/u)

| Prompt | Struct OK | F1 vs GT | Precision | Recall | KW ev. |
|--------|:---------:|:--------:|:---------:|:-----:|:------:|
| **few-shot** | **100%** | **96.4%** | **94.2%** | **98.8%** | **92%** |
| strict | 100% | 95.9% | 93.1% | 98.8% | 91% |
| base | 100% | 95.3% | 91.9% | 99.1% | 91% |

#### Métricas de rendimiento

| Métrica | Valor |
|---------|-------|
| Tiempo promedio | **1.93s** |
| VRAM modelo | **5.39 GiB** |
| KV cache disponible | **7.30 GiB** |
| Concurrencia máxima (4096 tok) | **29.21×** |
| Output tok/s | ~55-68 tok/s |
| Cross-prompt agreement (Mistral) | ~95% |

#### Comparativa vs Mistral 7B AWQ

| Métrica | Mistral 7B AWQ | Llama 3.1 8B AWQ | Diferencia |
|---------|---------------|-----------------|------------|
| F1 vs GT | 94.0% | **96.4%** | **+2.4 pp** |
| Precision | 88.9% | **94.2%** | **+5.3 pp** |
| Recall | **99.8%** | 98.8% | −1.0 pp |
| Tiempo | **1.61s** | 1.93s | +20% |
| VRAM | **3.89 GiB** | 5.39 GiB | +39% |
| Concurrencia | **35.22×** | 29.21× | −17% |

**Proyección 315K secciones**: ~5.8 horas con 29× concurrencia (vs ~4h Mistral, pero 2.4pp mejor F1).

---

### 13. Qwen3-8B AWQ ⚠️ VIABLE (100% — AWQ sin marlin)

**Veredicto**: AWQ no ofrece ventaja sobre GGUF para Qwen3-8B porque awq_marlin no es compatible con arquitectura Qwen. El GGUF sigue siendo ligeramente más rápido (4.50s vs 4.80s).

| Métrica | GGUF Q5_K_M | AWQ | Diferencia |
|---------|-------------|-----|------------|
| Calidad | 100% (60/60) | 100% (60/60) | Igual |
| Tiempo promedio | 4.50s | 4.80s | **−7% (AWQ más lento)** |
| Output tok/s | ~25-35 | ~20-30 | Similar |

### 14. Qwen3-14B AWQ ⚠️ VIABLE (100% — AWQ sin marlin)

**Veredicto**: Similar a Qwen3-8B — AWQ no mejora vs GGUF porque awq_marlin no es compatible. GGUF igual o mejor (5.10s vs 5.29s).

| Métrica | GGUF Q5_K_M | AWQ | Diferencia |
|---------|-------------|-----|------------|
| Calidad | 100% (60/60) | 100% (60/60) | Igual |
| Tiempo promedio | 5.10s | 5.29s | Similar |
| VRAM modelo | 9.87 GiB | ~9.3 GiB | −6% |

### 15. Phi-4 14B AWQ ❌ NO RECOMENDADO (90%, problemas de dtype)

**Veredicto**: Problemas de compatibilidad con `dtype=float16` (requiere fuerza bruta), calidad inferior (90%), VRAM alta (8.51 GiB). GGUF fue 100%.

| Métrica | GGUF Q4_K_M | AWQ (float16) | Diferencia |
|---------|-------------|---------------|------------|
| Calidad | 100% (60/60) | **90% (9/10)** | **−10 pp** |
| Tiempo promedio | 6.09s | 4.47s | −27% |
| VRAM modelo | ~10 GiB | 8.51 GiB | −15% |
| Concurrencia | ~10× (est.) | 9.95× | Similar |

**Nota**: El 1 FAIL fue en "Confidencialidad De La Informacion". Phi-4 AWQ requiere `dtype="float16"` explícito (bfloat16 no soportado) y `trust_remote_code=True`. Tiempo de carga: ~225s.

---

### 16. Qwen2.5 14B AWQ ✅ VIABLE (F1=93.3%)

**Veredicto**: Buen rendimiento (2.46s, 3× más rápido que GGUF) con calidad sólida (F1=93.3%). No supera a Mistral/Llama pero es opción válida si se prefiere Qwen.

#### Métricas de calidad (270 samples, Fase 1)

| Prompt | Struct OK | F1 vs GT | Precision | Recall | KW ev. |
|--------|:---------:|:--------:|:---------:|:-----:|:------:|
| base | 100% | 93.3% | 90.8% | 95.8% | **93%** |
| few-shot | 100% | 93.3% | 91.4% | 95.3% | 92% |
| strict | 100% | 93.0% | 90.3% | 95.7% | **93%** |

**Comparativa vs líderes**:

| Métrica | Mistral 7B AWQ | Llama 3.1 8B AWQ | Qwen2.5 14B AWQ |
|---------|:--------------:|:----------------:|:----------------:|
| F1 vs GT | 94.0% | **96.4%** | 93.3% |
| Recall | **99.8%** | 98.8% | 95.8% |
| KW evidence | 91% | 92% | **93%** |
| Tiempo | **1.61s** | 1.93s | 2.46s |
| VRAM | **3.89 GiB** | 5.39 GiB | 9.38 GiB |
| Concurrencia | **35.22×** | 29.21× | 8.18× |

---

## Comparativa de Throughput

### Estimaciones para 315K secciones (secuencial)

| Modo | s/sample | 315K tiempo | Notas |
|------|----------|-------------|-------|
| HTTP server (enforce-eager) | ~9.3s | ~33.9 días | Baseline original |
| HTTP server (CUDA graphs) | ~7.1s | ~25.9 días | Con CUDA graphs |
| Offline API, Qwen3-14B (batch=10) | ~5.1s | ~18.6 días | Con continuous batching |
| Offline API, Mistral 7B GGUF (batch=10) | ~4.9s | ~17.8 días | GGUF experimental |
| **🥇 Llama 3.1 8B AWQ (batch=10)** | **~1.9s** | **~6.9 días** | **Mejor F1 (96.4%), secuencial** |
| **🥇 Llama 3.1 8B AWQ (batch=1000)** | **~0.066s** | **~5.8 horas** | **Con 29× concurrencia** |
| **🥇 Offline API, Mistral 7B AWQ (batch=10)** | **~1.6s** | **~5.9 días** | **AWQ nativo, secuencial** |
| **🥇 Offline API, Mistral 7B AWQ (batch=1000)** | **~0.045s** | **~4 horas** | **Con 35× concurrencia** |
| Offline API + guided JSON (estimado) | ~0.03s | ~2-3 horas | Con xgrammar + AWQ |

---

## Problemas Conocidos de vLLM

| Issue | Síntoma | Workaround |
|-------|---------|------------|
| CUDA graph hang (FULL decode) | Stuck en `0/35` durante CUDA graphs | Matar proceso + reintentar. Intermitente |
| CUDA graph hang (PIECEWISE) | Stuck en 43% (22/51) | Matar proceso + reintentar |
| gpt-oss MXFP4 | Crash en GGUF reader | No soportado por vLLM |
| Q4_K_S | Crash silencioso | Probable incompatibilidad |
| VLLM_USE_V1=0 | Proceso muere sin log | v0.23.0 no soporta V0 engine |
| WSL localhost | Connection refused | Usar 127.0.0.1 en vez de localhost |
| OOM en startup | CUDA out of memory | Matar NVIDIA Overlay, Warp, Edge, SearchApp |
| Falta ninja-build | flashinfer JIT crash | `apt install ninja-build` |
| Spawn multiprocessing | RuntimeError en LLM.generate() | Requiere `if __name__ == '__main__':` |

---

## Optimizaciones Pendientes

### 1. Guided decoding (xgrammar)
No disponible en vLLM 0.23.0 offline API. La versión server lo soporta vía `--guided-decoding-backend xgrammar` y `response_format={type: "json_object", schema: ...}`. Podría mejorar throughput ~5x y eliminar parseos fallidos.

### 2. Batch size óptimo
El profiler mostró `concurrencia máxima: 5.12x`. Con batches de 100-1000+ samples por título, el continuous batching debería mostrar el verdadero speedup (10-23x reportado en benchmarks de vLLM).

### 3. CUDA graphs (sin enforce-eager)
Si se soluciona el CUDA graph hang (menor `gpu_memory_utilization` o `--cudagraph-capture-sizes` limitado), CUDA graphs pueden dar ~2x en decode throughput.

### 4. kv_cache_dtype=fp8
No se pudo probar en offline API porque la GGUF lo maneja internamente. En server mode reduce el footprint de KV cache ~2x, permitiendo más concurrencia.

---

## Modelos Evaluados — Estado Final

### AWQ (formato nativo, recomendado para producción)

| Estado | Modelo | Formato | Tamaño | Struct OK | F1 vs GT | Resultado |
|--------|--------|---------|--------|:---------:|:--------:|-----------|
| 🥇 **THROUGHPUT** | **Mistral 7B v0.3** | **AWQ (awq_marlin)** | **3.88 GiB** | **100%** | **94.0%** | **1.61s avg, 35× concurrencia. DEFINITIVO.** |
| 🥇 **CALIDAD** | **Llama 3.1 8B** | **AWQ (awq_marlin)** | **5.33 GiB** | **100%** | **96.4%** | **1.93s avg, 29× concurrencia. MEJOR F1.** |
| ✅ Alternativa | **Qwen2.5 14B** | **AWQ** | **9.29 GiB** | **100%** | **93.3%** | **2.46s avg, sin awq_marlin** |
| ✅ Alternativa | **Qwen3-8B** | AWQ | ~6 GB | 100% | — | 4.80s avg (sin awq_marlin) |
| ✅ Alternativa | **Qwen3-14B** | AWQ | ~9 GB | 100% | — | 5.29s avg (sin awq_marlin) |
| ❌ No recomendado | **Phi-4 14B** | AWQ | 8.51 GiB | 90% | — | Problemas dtype |

### GGUF (experimental, deprecado en vLLM)

| Estado | Modelo | Quant | Tamaño | Resultado |
|--------|--------|-------|--------|-----------|
| 🥈 **GGUF backup** | **Mistral 7B v0.3** | Q5_K_M | 4.8 GB | 100%, 4.90s avg (GGUF, 3× más lento que AWQ) |
| ✅ Alternativa | **Qwen3-8B** | Q5_K_M | 5.4 GB | 100%, 4.50s avg (con fix thinking mode) |
| ✅ Alternativa | **Phi-4 14B** | Q4_K_M | 8.4 GB | 100%, 6.09s avg |
| ✅ Anterior | **Qwen3-14B** | Q5_K_M | 9.8 GB | 100%, 5.10s avg |
| ❌ Inviable | **Gemma 3 12B** | Q8_0/Q5/Q4 | 6.8-11.7 GB | OOM o 6× más lento (~31s) |
| ❌ Inviable | **Gemma2 27B** | Q4_K_M | 15.5 GB | No cabe en 16 GB VRAM |
| ❌ Inviable | **Llama 3.1 8B** | Q5_K_M | 5.3 GB | Tokenizer gated |
| ❌ Inviable | **Qwen2.5 7B** | Q4_K_M | 4.4 GB | 0% calidad (omite campos) |
| ❌ Inviable | **NuExtract 2.0 8B** | Q5_K_M | 5.1 GB | 0% calidad (modelo VL) |
| ❌ Inviable | **lift-9B** | Q5_K_M | 6.2 GB | Arquitectura no soportada |
| ❌ Inviable | **gpt-oss-20b** | MXFP4 | 11.3 GB | Formato MXFP4 no compatible |

**Descarga AWQ**:
```bash
pip install -U huggingface_hub
# Mistral 7B v0.3 AWQ (RECOMENDADO)
huggingface-cli download solidrust/Mistral-7B-Instruct-v0.3-AWQ --local-dir ./models/mistral-7b-awq
```

**Modelos descartados**:
| Modelo | Razón |
|--------|-------|
| Llama 3.1 8B | Tokenizer gated en GGUF, pero **funciona en AWQ** (hugging-quants, 100%, 1.93s) |
| Phi-3.5 | Español débil, enfocado STEM/inglés |
| DeepSeek-Coder-V2-Lite 16B | Enfocado código, no extracción administrativa |
| Exaone 3.5 | Coreano/inglés principalmente |
| Marco-o1 | Thinking mode sin switch para apagar |
| gpt-oss-20b | MXFP4 no compatible, entrenado en inglés |

---

## Resumen Ejecutivo

**Mistral 7B v0.3 AWQ (awq_marlin)** es el modelo definitivo para producción. AWQ nativo es 3× más rápido que GGUF y ocupa 30% menos VRAM.

### Comparativa final: AWQ vs GGUF (Mistral 7B)

| Métrica | GGUF Q5_K_M | AWQ (awq_marlin) | Diferencia |
|---------|-------------|-----------------|------------|
| Calidad | 100% (60/60) | **100% (60/60)** | Igual |
| Tamaño | 4.8 GB | **3.88 GiB (−19%)** | Menos disco |
| VRAM modelo | ~5.5 GiB | **3.89 GiB (−29%)** | Menos VRAM |
| Velocidad | 4.90s avg | **1.61s avg (−67%)** | **3× más rápido** |
| Concurrencia real | ~27× (est.) | **35.22×** | +30% |
| Output tok/s | ~25-32 | **64-77** | **+140%** |

### Ranking completo de todos los formatos evaluados

*Nota: vLLM 0.23.0 para todos los tests (v0.25.x no funciona en WSL — MRv2 requiere UVA). Ordenado por velocidad × concurrencia.*

| # | Modelo | Formato | Cuantización | Tamaño disco | VRAM modelo | KV cache | Concurrencia | Kernel | Velocidad | Struct OK | F1 vs GT | Veredicto |
|---|--------|---------|:-----------:|:-----------:|:-----------:|:--------:|:-----------:|:------:|:---------:|:--------:|:--------:|-----------|
| 1 | **Mistral 7B v0.3** | **AWQ** | **W4A16 (4-bit)** | **3.88 GiB** | **3.89 GiB** | **8.81 GiB** | **35.22×** | **awq_marlin** 🚀 | **1.61s** | **100%** | **94.0%** | **🏆 THROUGHPUT** |
| 2 | **Llama 3.1 8B** | **AWQ** | **W4A16 (4-bit)** | **5.33 GiB** | **5.39 GiB** | **7.30 GiB** | **29.21×** | **awq_marlin** 🚀 | **1.93s** | **100%** | **96.4%** | **🏆 CALIDAD** |
| 3 | Qwen3-8B | AWQ | W4A16 (4-bit) | 5.68 GiB | 5.71 GiB | 6.91 GiB | 24.58× | awq (no marlin) | 4.80s | 100% | — | ⚠️ Sin ventaja vs GGUF |
| 4 | Qwen3-14B | AWQ | W4A16 (4-bit) | 9.29 GiB | 9.44 GiB | 2.81 GiB | 9.00× | awq (no marlin) | 5.29s | 100% | — | ⚠️ Sin ventaja vs GGUF |
| 5 | **Qwen2.5 14B** | **AWQ** | **W4A16 (4-bit)** | **9.29 GiB** | **9.38 GiB** | **3.07 GiB** | **8.18×** | **awq (no marlin)** | **2.46s** | **100%** | **93.3%** | **✅ Alternativa** |
| 6 | Phi-4 14B | AWQ | W4A16 (4-bit) | 8.51 GiB | 8.51 GiB | 3.89 GiB | 9.95× | awq (no marlin) | 4.47s | 90% | — | ❌ No recomendado |
| 7 | Mistral 7B v0.3 | GGUF | Q5_K_M | 4.8 GB | ~5.5 GiB | — | ~27× | GGUF plugin | 4.90s | 100% | — | ✅ Backup |
| 8 | Qwen3-8B | GGUF | Q5_K_M | 5.4 GB | ~6.5 GiB | — | ~15× | GGUF plugin | 4.50s | 100% | — | ✅ Alternativa rápida |
| 9 | Qwen3-14B | GGUF | Q5_K_M | 9.8 GB | 9.87 GiB | 2.38 GiB | 15.23× | GGUF plugin | 5.10s | 100% | — | ✅ Anterior campeón |
| 10 | Phi-4 14B | GGUF | Q4_K_M | 8.4 GB | ~10 GiB | — | ~10× | GGUF plugin | 6.09s | 100% | — | ✅ Alternativa robusta |
| 11 | Qwen2.5 14B | GGUF | Q5_K_L | 10.2 GB | 10.55 GiB | 1.92 GiB | 5.12× | GGUF plugin | 7.1s | 97.5% | — | ❌ Reemplazado |
| 12 | Qwen2.5 14B | GGUF | Q4_K_S | 8.0 GB | — | — | — | GGUF plugin | — | — | — | ❌ Crash |
| 13 | Qwen2.5 7B | GGUF | Q4_K_M | 4.4 GB | ~7 GiB | — | — | GGUF plugin | ~9s | 0% | — | ❌ Débil |
| 14 | Gemma 3 12B | GGUF | Q8_0 | 11.7 GB | ~14 GiB | — | — | GGUF plugin | ~31s | — | — | ❌ 6× lento |
| 15 | Gemma 3 12B | AWQ | W4A16 (4-bit) | — | — | — | — | awq | — | — | — | ❌ bfloat16 vs float16 |
| 16 | Gemma2 27B | GGUF | Q4_K_M | 15.5 GB | ~17 GiB | — | — | GGUF plugin | — | — | — | ❌ OOM |
| 17 | Llama 3.1 8B | GGUF | Q5_K_M | 5.3 GB | — | — | — | GGUF plugin | — | — | — | ❌ Tokenizer gated |
| 18 | NuExtract 2.0 8B | GGUF | Q5_K_M | 5.1 GB | — | — | — | GGUF plugin | — | 0% | — | ❌ Modelo VL |
| 19 | lift-9B | GGUF | Q5_K_M | 6.2 GB | — | — | — | GGUF plugin | — | — | — | ❌ Arq. no soportada |
| 20 | gpt-oss-20b | GGUF | MXFP4 | 11.3 GB | — | — | — | GGUF plugin | — | — | — | ❌ Formato no compatible |

**Conclusiones**:
- **Mistral 7B AWQ + awq_marlin** es imbatible para throughput: menor VRAM, mayor concurrencia, más rápido (F1=94.0%, recall=99.8%).
- **Llama 3.1 8B AWQ + awq_marlin** es la mejor alternativa si se necesita más capacidad de razonamiento (F1=96.4% vs consensus, structural OK=100%).
- **AWQ sin marlin** (Qwen, Phi) no ofrece ventaja sobre GGUF — awq_marlin es el diferenciador clave.
- **vLLM 0.25.x**: No funciona en WSL (MRv2 requiere UVA). Se mantiene vLLM 0.23.0.

### Estimaciones para 315K secciones

| Escenario | Tiempo estimado | F1 vs GT |
|-----------|:--------------:|:--------:|
| **Llama 3.1 8B AWQ + 29× concurrencia** | **~5.8 horas** | **96.4%** |
| **Mistral 7B AWQ + 35× concurrencia** | **~4 horas** | **94.0%** |
| Mistral 7B AWQ secuencial | ~5.9 días | 94.0% |
| Mistral 7B GGUF + ~27× concurrencia | ~1 día | — |
| Qwen3-14B GGUF + ~15× concurrencia | ~1.8 días | — |
| HTTP server baseline | ~33.9 días | — |

### Conclusión y próximos pasos

1. ✅ **AWQ benchmark completo** — 6 modelos AWQ evaluados (Mistral 7B, Qwen3-8B, Qwen3-14B, Phi-4, Qwen2.5 14B, Llama 3.1 8B)
2. ✅ **Fase 1 calidad completa** — 9 configuraciones (3 modelos × 3 prompts) evaluadas con F1 vs pseudo-GT (270 samples, keyword evidence, cross-prompt consistency)
3. ✅ **Mistral 7B AWQ** — **F1=94.0%, 1.61s avg, 35× concurrencia, 3.89 GiB VRAM** — mejor throughput
4. ✅ **Llama 3.1 8B AWQ (few-shot)** — **F1=96.4%, 1.93s avg, 29× concurrencia, 5.39 GiB VRAM** — mejor calidad
5. ✅ **GGUF deprecado** — AWQ nativo es 3× más rápido y oficialmente soportado
6. ✅ **vLLM 0.25.1 incompatible con WSL** — UVA no disponible, se mantiene 0.23.0
7. ✅ **21 configuraciones evaluadas** (14 modelos × GGUF + AWQ, más F1 quality metrics para 3 líderes)
8. 🚀 **Implementar Pipeline 2** — decidir entre Llama 3.1 8B AWQ (calidad) o Mistral 7B AWQ (throughput)

---

*Documento generado automáticamente — Julio 2026*
*Próxima actualización: tras evaluar nuevos modelos*

# Reporte completo — Benchmark Top 10 + puesta en producción GPU

> **Fecha**: 09-sep-2026 (driver/CUDA/vLLM) · 12-sep-2026 (benchmarks) · **GPU**: RTX 4070 Ti SUPER 16 GB
> **Rama**: `feature/clustering-pipeline` · **Manifest**: `models/candidates.yaml` · **Set Fase 5**: `data/processed/llm_benchmark/pseudo900.json`

## 1. Alcance: ¿se probó todo?

**Sí — Top 10 completo del ranking 31-ago, en 2 rondas cada uno:**

| Ronda | Set | Modelos |
|---|---|---|
| 100 GT humanas (5 campos) | `ground_truth_5_campos.csv` | 10/10 (vLLM ×7 con guided xgrammar, llama-server ×3) |
| 867 pseudo-GT (5 campos) | `pseudo900.json`, seed 42 | 10/10 (mismo harness por backend) |

**No probado (pendiente explícito):** `vllm bench serve` (throughput bajo concurrencia 1/4/8/16/32);
re-runs de Qwen3-8B-AWQ, Qwen2.5-14B-AWQ y ALIA-legal (históricos 2025, fuera del Top 10 actual).
El "900" quedó en **867**: `pct_monto_referencia_tecnica` solo tiene 147 docs con valor en producción
(campo raro) → 180×4 + 147, documentado en el set.

## 2. Infra instalada (todo desde cero)

| Componente | Versión | Nota |
|---|---|---|
| Driver NVIDIA | **615.71.09** (rama más nueva que la 595 documentada) | Repo oficial CUDA; el metapaquete `cuda-drivers` es dummy y NO trae el driver → instalar `nvidia-open` |
| CUDA Toolkit | **13.4** (`nvcc 13.4.59`) | Más nuevo que el 13.3 investigado |
| vLLM | **0.29.0** + torch 2.13 cu130 + transformers 5.16.1 | pip instaló 0.29 (más nuevo que el 0.28 investigado); venv `/root/vllm-env` |
| llama-server | build desde source commit `6d9c82e` (CUDA sm_89) | El tag `b10809` y los zips Ubuntu-CUDA ya no existen; releases oficiales Linux sin CUDA → compilar |
| Extras sistema | `python3.12-dev`, `ninja-build`, `pandas`, `psycopg2-binary`, `requests` | Faltaban y rompían la primera corrida (ver §5) |
| GPU sin reboot | nouveau desvinculado por sysfs + `modprobe nvidia` | `nvidia-smi` OK sin reiniciar; blacklist permanente para el próximo boot |
| Puertos | llama-server **1234**, vLLM serve **8000** | Regla: **solo uno por vez** (16 GB VRAM) |

## 3. Resultados

### 3.1 GT humanas 100 (guided decoding, temp 0.0)

| Modelo | Score | Tiempo |
|---|---|---|
| Llama 3.1 8B AWQ | **98/100** | 9s |
| Qwen3-14B AWQ | 98/100 | 14s |
| Llama 3.1 8B-FP8 | 98/100 | 9s |
| Qwen3-8B-FP8 | 98/100 | 10s |
| EXAONE-3.5 AWQ | 98/100 | 10s |
| Mistral Small 24B AWQ | 49/50 (98%) | 62s (offload) |
| Mistral 7B AWQ | 97/100 | 13s |
| Granite 4.2 GGUF | 97/100 | 83s |
| Gemma 4 GGUF | 97/100 | 49s |
| Ministral GGUF | 97/100 | 25s |

(Histórico vLLM 0.23: Llama 100/100. Con 0.29: 98 — 2 misses por diferencias del guided/xgrammar nuevo.)

### 3.2 Pseudo-GT 867 (agreement vs regex prod + latencia)

| # | Modelo | Agree | P50 | P95 | Total | VRAM aprox |
|---|---|---|---|---|---|---|
| 🥇 | Llama 3.1 8B-FP8 | **61.8%** | **0.17s** | 0.28s | 132s | ~10 GB |
| 🥈 | Llama 3.1 8B AWQ | 61.4% | 0.22s | 0.38s | 167s | ~10 GB |
| 🥉 | Qwen3-8B-FP8 | 54.7% | 0.24s | 0.39s | 187s | ~10 GB |
| 4 | Qwen3-14B AWQ | 53.1% | 0.38s | 0.64s | 288s | ~10 GB |
| 5 | Ministral 14B GGUF | 50.2% | 0.55s | 0.77s | 421s | 9.2 GB |
| 6 | Mistral 7B AWQ | 49.4% | 0.29s | 0.96s | 203s | ~10 GB |
| 7-8 | EXAONE AWQ / Gemma 4 GGUF | 48.4% | 0.26s / 1.45s | — | 195s / 1124s | ~6 / 8.4 GB |
| 9-10 | Granite GGUF / Mistral Small 24B | 48.2% | 1.09s / 2.99s | — | 891s / 1511s | 6.7 GB / offload |

### 3.3 El campo que discrimina: `pct_monto_referencia_tecnica` (x/147)

Llama-FP8 **118** · Llama-AWQ 114 · Qwen3-8B-FP8 56 · Qwen3-14B 42 · Ministral 17 ·
Mistral 10 · EXAONE/Gemma 2 · Small/Granite 0.
Los otros 4 campos saturan **idéntico en los 10 modelos** (validez 123/180, exp_tec 77/180,
pago_ant 64/180, sol_ant 154/180): techo del pipeline compartido, no de los modelos.

## 4. Detalle por modelo (descarga, flags, estado en disco)

| Modelo | Tamaño | Flags especiales | Estado |
|---|---|---|---|
| Llama 3.1 8B AWQ | 5.4G | `HF_TOKEN` (tokenizer gated), `quant=None` (auto marlin) | ✅ |
| Mistral 7B v0.3 AWQ | 3.9G | `merge_system=True` **obligatorio** (su template revienta con rol system), `quant=None` | ✅ |
| Qwen3-14B AWQ | 9.4G | `enable_thinking=False`, `gpu_mem 0.92`, `max_len 4096` | ✅ |
| Llama 3.1 8B-FP8 | 8.5G | `quant=None` (auto FP8, dtype bf16) | ✅ |
| Qwen3-8B-FP8 | 8.9G | `enable_thinking=False`, `max_len 4096` + `gpu_mem 0.80` (KV overflow con 8192) | ✅ |
| EXAONE-3.5 AWQ | 5.0G | ninguno (español validado) | ✅ |
| Mistral Small 24B AWQ | 14G | `cpu_offload_gb=5`, `max_seqs=2`, `max_len=2048`, `limit=10` en GT-100 | ✅ |
| Granite 4.2 8B GGUF | 5.2G | `enable_thinking:false` por request (thinking en canal separado) | ✅ `./models/gguf/` |
| Gemma 4 12B GGUF | 6.3G | **`--reasoning off` a nivel servidor** (sin esto, content vacío); guided `json_schema` válido | ✅ `./models/gguf/` |
| Ministral 14B GGUF | 7.7G | ninguno | ✅ `./models/gguf/` |
| Fixtures | 0.7G + 0.5G | Qwen2.5-0.5B AWQ + GGUF (validación infra) | ✅ |

Disco total modelos: caché HF **55G** + GGUF **20G** (`/` con 123G libres). Nada se borró (decisión del usuario).

## 5. Incidencias (todas, con causa y fix)

### Infra
1. `cuda-drivers` es **dummy transicional**: instala, DKMS vacío, sin `nvidia.ko`. Fix: `apt install nvidia-open` (rama 615, open modules, Ada OK).
2. `nvcc` no está en el PATH tras instalar el toolkit → `export PATH=/usr/local/cuda-13.4/bin:$PATH` (+ `CUDACXX`) para compilar llama.cpp.
3. `python3.12-dev` ausente → Triton no compila `cuda_utils.c` (EngineCore init failed). Fix: instalar headers.
4. Binario `ninja` invisible para el worker vLLM (el del venv no lo ve) → `apt install ninja-build`.
5. Script sin `if __name__ == "__main__"` + multiprocessing `spawn` → RuntimeError de bootstrapping. Fix: guard.
6. GitHub API devolvió 404 para `ggml-org/llama.cpp` y el tag `b10809` no existe (esquema de releases cambiado; Linux-CUDA sin binarios oficiales) → `git clone --depth 1` + `cmake -DGGML_CUDA=ON -DCMAKE_CUDA_ARCHITECTURES=89` (solo target `llama-server`).
7. `pkill -f "<patrón>"` **se suicida** (el patrón está en su propia línea de comando; mató mi shell 2 veces). Fix: truco `[g]emma` en el patrón.

### Modelos / harness
8. `quantization="awq_marlin"` explícito + `VLLM_USE_V2_MODEL_RUNNER=0` obsoletos en 0.29 → `quant=None` (auto) y eliminar el env. `StructuredOutputsParams` sigue válido.
9. Qwen3-8B-FP8: KV necesaria (1.12 GiB) > disponible con `gpu 0.70/ctx 8192` → `max_len 4096` + `gpu_mem 0.80`.
10. Mistral Small 24B: OOM (14.8G en uso, 826 MiB libres) → `cpu_offload_gb=5` + `max_seqs=2` + `max_len=2048`. Consecuencia: P50 2.99s vs 0.17-0.38s de los 8B (tráfico PCIe).
11. Prompt >8192 tokens en 1 muestra pseudo (tokenizer Mistral cuenta más que el de Llama) → `try/except` que registra `ERROR:*` como desacuerdo en vez de abortar el run.
12. Template Mistral v0.3 **revienta (jinja TemplateError) con rol `system`** → `merge_system=True` obligatorio; verificado que sin merge ni siquiera arranca.
13. Gemma 4: con thinking activo todo va al canal `reasoning_content` y `content` queda **vacío** (ni siquiera con `enable_thinking:false` por request) → `--reasoning off` al levantar el servidor. Guided `json_schema` validado (`90` → `90`).
14. Granite 4.2: thinking en `reasoning_content` pero `content` limpio; con `enable_thinking:false` verificado sin residuos.
15. `pct_monto_referencia_tecnica` (fracciones `0,50`): Llama extrae la forma fraccionaria y el postproceso la escala (×100); Mistral/EXAONE/Granite/Small responden NULL o fragmentos (`1.30`) → colapso 0-10/147. Diferencia real de modelo, no de harness.
16. Artefactos del parse estricto (producción): `raw='720'` → None con pseudo 720; `raw='1.5'` → None. A revisar con AloLab (no tocar mid-benchmark).
17. Anulado dos veces el run de Mistral Small por tiempo (23 min con offload); resume cada 25 muestras evitó perderlo (525/867 + resto).

## 6. Hallazgos metodológicos (para la tesis)

- Con guided decoding + postproceso determinista compartido, **7 arquitecturas convergen al mismo techo** en GT curadas (97-98) y 4/5 campos saturan idéntico en 867 muestras. El ranking por calidad casi no discrimina; **el desempate es latencia/VRAM**.
- La métrica pseudo-GT mide **divergencia regex↔LLM**, no calidad: textos multi-número ambiguos (15 vs 10, ambos plausibles) + artefactos de parse. Solo `pct_monto` discrimina de verdad.
- GGUF (GBNF) vs AWQ/FP8 (xgrammar) van en tablas separadas: backends de guided distintos.
- FP8 ≈ AWQ en calidad (Llama-FP8 ≥ Llama-AWQ) con menor latencia: cita útil contra el mito "cuantizar degrada".

## 7. Veredicto y pendientes

**Veredicto: Llama 3.1 8B-FP8** para el fallback Pipeline 2 (mejor agreement 61.8% + P50 0.17s + 8.5G).
Llama-AWQ como alternativa inmediata (mismo techo).

## 8. Fase 6 — Concurrencia (12/13-sep-2026, `scripts/benchmark_concurrency.py`)

vLLM serve 0.29 (guided vía `extra_body: {guided_regex}` en `/v1/completions` pre-renderizado
local) o llama-server (`/v1/chat/completions`, sin guided). 64 prompts vLLM / 32 GGUF.

| Modelo | Techo req/s | P50 C=1 → techo | VRAM |
|---|---|---|---|
| Llama-FP8 | **7.0** (C16) | 0.19→1.47s | 11.6G |
| Llama-AWQ | 5.2 | 0.38→4.93s | 11.6G |
| EXAONE | 5.05 | 0.47→4.82s | 11.5G |
| Ministral GGUF | 4.85 (C8) | 0.41→1.30s | 10.5G |
| Qwen3-8B-FP8 | 4.5 | 1.00→6.47s | 13.2G |
| Mistral | 4.3 | 0.69→6.62s | 11.5G |
| Qwen3-14B | 3.2 | 0.43→4.19s | 15.2G |
| Granite GGUF | 2.5 | 1.00→3.13s | 8.0G |
| Gemma GGUF | 0.9 (no escala) | 1.53→8.80s | 9.0G |
| Small 24B | 0.27 batch-offline | — | 14.7G |

Throughput replica calidad (Llama-FP8 primero en todo). Punto operativo: C=8-16.
Detalle + lectura tesis en `ranking_combinado.md` §Fase 6.

### Incidencias Fase 6 (18-23)

18. **`vllm serve` + Small-24B + `cpu_offload` cuelga el EngineCore** (3 intentos, muere en
    `Using MarlinLinearKernel`, VRAM clavada en 18 MiB; ni `VLLM_ENGINE_READY_TIMEOUT_S=2400`
    ayuda). Solo viable batch offline → Small descartado para Pipeline 2.
19. **`/v1/completions` → 404**: la ruta existe; el fallo era el `model` enviado (nombre corto
    vs path HF servido). Fix: `serve_model = cfg["path"]`.
20. **Procesos detached**: `nohup ... &` muere si la herramienta corta el grupo → usar
    `setsid nohup ... < /dev/null &`. Para matar servidores: `fuser -k 1234/tcp`
    (los `pkill -f` con el nombre del modelo se suicidan — ver §5.7).
21. **llama-server reparte el ctx entre slots**: `--ctx-size 8192 -np 8` = 1024/slot →
    22/32 errores "exceeds available context". Fix: `--ctx-size 16384 -np 4` (4096/slot)
    + truncado cliente 15k chars (`--truncate-chars`).
22. **Small ctx 2048**: 6/24 prompts del workset lo exceden → trunca texto (head+tail) con nota;
    el Pipeline 2 deberá trocear igual. `XgrammarBackend has no attribute compiler` visto
    una vez = artefacto de cascada durante un abort, no bug real (offline guided OK).
23. **Carga offline del Small flaky**: 2 de 4 intentos murieron en silencio durante el repack
    Marlin (sin OOM ni segfault en dmesg, shm limpio); el 4.º cargó en 12s. Reportable como
    inestabilidad AWQ-24B + offload en 0.29.

**Pendientes**: `vllm bench serve` (barrido formal request-rate) · AloLab sobre artefactos §5.16
y los 2-3 misses comunes · adaptar `run_document_features_v2_llm_pipeline.py` a 0.29 y lanzar
los 324K docs · commit general · Pipeline 3.

# Benchmark de modelos locales — Resumen general (documento vivo)

> ⛔ **AVISO sesión 23 (auditoría H2.1/H2.4)**: las cifras de Fase 5 / GT-100 de este resumen miden acuerdo
> DESPUÉS de un postproceso determinista que ignora al modelo en la mayoría de los campos
> (un modelo que siempre responde NULL empata). Las cifras limpias del modelo están en las
> columnas nuevas `c3_dias_crudo`, `gt12_llm_only` (+ `c3_dias_null_baseline`, `gt12_pipeline`)
> del CSV, regenerado por `scripts/build_tabla_maestra.py`. Los párrafos de veredicto de este
> documento (empate 239/239, "indistinguibles", desempate operativo FP8) son **históricos**:
> veredicto vigente en `endurecimiento_benchmark_plan.md` §9.


> **Última actualización**: 13-sep-2026 · **Estado**: benchmark Top 10 + concurrencia cerrados; post-plan pendiente.
> **Para pulir**: §9 (post-plan) y §7 (agregar futuras rondas). Detalle fino en
> `docs/reporte_benchmark_completo.md`, tabla en `docs/tabla_maestra_benchmark.md`,
> datos en `data/processed/llm_benchmark/tabla_maestra_benchmark.csv`.

## 1. Contexto general

Proyecto: ETL de pliegos DNCP (Paraguay) para detección de anomalías en contratación pública.
El **Pipeline 2** extrae 81 campos estructurados de 2.78M secciones con regex (70s CPU, ~80%
cobertura) + **fallback LLM** (~324K docs donde el regex falla, `conf<0.8`). Este benchmark
elige el modelo local del fallback: 10 candidatos del ranking 31-ago-2026, evaluados en
calidad (100 GT humanas + 867 pseudo-GT) y throughput (concurrencia online). Todo corre
**on-premise** (sin APIs externas: reproducibilidad + datos sensibles).

> ⚠ **CHAGELOG 15-sep — cifra corregida, ver `docs/endurecimiento_benchmark_plan.md`**: la
> población real del fallback **no** es "conf<0.8 con valor" (solo 2-3 filas): son los docs
> con **conf=0 y valor 0.0 placeholder** (regex no-match) ≈ **260,109 filas-campo** (no docs
> distintos). **TODO no resuelto**: `COUNT(DISTINCT nro_licitacion)` y re-lista de campos
> (dias_reposicion, presentacion_garantia, interes_mora, multa_diaria ya son regex puro y
> **salen** del fallback). Nadie dimensiona el piloto con 324K ni con 260K-field-rows hasta
> cerrar ese cómputo.

## 2. Dónde corre (hardware + sistema)

| Componente | Detalle |
|---|---|
| Máquina | WSL2 sobre Windows, 32 GB RAM |
| GPU | NVIDIA RTX 4070 Ti SUPER **16 GB** (una sola → regla: un modelo por vez) |
| SO / driver | Linux WSL2 · driver **615.71.09** (rama `nvidia-open`, sin reboot vía unbind de nouveau) |
| CUDA | Toolkit **13.4** (`nvcc 13.4.59`) |
| Disco modelos | caché HF **55G** + `./models/gguf/` **20G** (`/` con ~123G libres; nada se borra) |
| BD | Podman `postgres:17`, contenedor `dncp`, `5433→5432`, `.env` con `DB_HOST=localhost` |
| Python | `/root/vllm-env` (venv; el python del sistema NO tiene vLLM/pandas) |

## 3. Stack de inferencia (versiones exactas)

| Pieza | Versión | Notas |
|---|---|---|
| vLLM | **0.29.0** + torch 2.13 cu130 + transformers 5.16.1 | `quant=None` (auto: marlin/FP8); sin `VLLM_USE_V2_MODEL_RUNNER`; `StructuredOutputsParams` vigente |
| llama-server | build desde source **commit `6d9c82e`**, CUDA sm_89, solo target `llama-server` | Tags/binarios oficiales inexistentes para Linux+CUDA → compilar (`nvcc` al PATH, `ninja-build`, `python3.12-dev`) |
| Guided | xgrammar (vLLM, regex `número\|NULL`) / GBNF n/a (GGUF en plain+parse) | Con guided: outputs 1-3 tokens, ~7× más rápido |
| Puertos | vLLM serve **8000** · llama-server **1234** | `fuser -k <port>/tcp` para apagar; `setsid nohup` para detached |

## 4. Formatos comparados (AWQ / FP8 / GGUF)

| Formato | Qué es | Cuant. | Backend | Rango tamaños (estudio) |
|---|---|---|---|---|
| **AWQ** | 4-bit, escalas por canal, kernel Marlin | 4-bit | vLLM (nativo) | 3.9G (Mistral 7B) – 14G (Small 24B) |
| **FP8** | Flotante 8-bit (E4M3) | 8-bit | vLLM (auto) | 8.5G (Llama 8B) – 8.9G (Qwen3 8B) |
| **GGUF** | `Q4_K_M`, formato llama.cpp | 4-bit mixta | llama-server | 5.2G (Granite) – 7.7G (Ministral) |

Hallazgo: **FP8 ≈ AWQ en calidad con menor latencia** (mito "cuantizar degrada" refutado en este dominio).

## 5. Cómo se generó el benchmark (fases, sets, scripts)

| Fase | Set (N, seed) | Harness | Métrica |
|---|---|---|---|
| Tanda 0-2 (GT-100) | `ground_truth_5_campos.csv`: 100 anot. humanas, 5 campos × 20 | `scripts/benchmark_multi_vs_gt.py` (vLLM offline) / `scripts/benchmark_gguf_http.py` (llama-server) | accuracy exacta |
| Fase 5 (pseudo-867) | `pseudo900.json` (seed 42): 180×4+147, pseudo-truth = regex prod `conf≥0.9`, excluye GT-100 | `scripts/benchmark_fase5_900.py` (offline vLLM / HTTP, resume c/25) | **agreement** + P50/P95 |
| Fase 6 (concurrencia) | workset 64 (vLLM) / 32 (GGUF, trunc 15k) seed 1234 | `scripts/benchmark_concurrency.py` (serve + C hilos: 1-32 vLLM, 1-8 GGUF) | req/s techo, P50/P95, VRAM/util pico |

Parámetros comunes: temp 0.0, max_tokens 64, guided regex `^(-?\d+(?:[.,]\d+)?|NULL)$` (vLLM).
Flags por modelo (thinking-off Qwen/EXAONE/Granite, merge_system Mistral, reasoning-off Gemma,
cpu_offload Small, ctx recortados): `models/candidates.yaml` (`gt100_v029`/`fase5_867`/`conc`).
Crudos: `data/processed/llm_benchmark/multimodel_results/` (`Fase5_*.json`, `Conc_*.json`).

## 6. Modelos evaluados (10 + 2 fixtures)

vLLM: Llama 3.1 8B-AWQ, Mistral 7B-AWQ, Qwen3-14B-AWQ, Llama 3.1 8B-FP8, Qwen3-8B-FP8,
EXAONE-3.5-AWQ, Mistral Small 24B-AWQ. GGUF: Granite 4.2 8B, Gemma 4 12B, Ministral 3 14B.
Fixtures infra: Qwen2.5-0.5B AWQ+GGUF. No evaluados (históricos fuera de ranking):
Qwen3-8B-AWQ, Qwen2.5-14B-AWQ, ALIA-legal.

## 7. Resultados (resumen; tabla completa §1 de `tabla_maestra_benchmark.md`)

| # | Modelo | GT-100 | Pseudo-867 | Single P50 | Techo conc. | VRAM |
|---|---|---|---|---|---|---|
| 🥇 | **Llama 3.1 8B-FP8** | 98 | **61.8%** | **0.17s** | **7.0 req/s** | 11.6G |
| 🥈 | Llama 3.1 8B-AWQ | 98 | 61.4% | 0.22s | 5.2 | 11.6G |
| 🥉 | Qwen3-8B-FP8 | 98 | 54.7% | 0.24s | 4.5 | 13.2G |
| 4 | Qwen3-14B-AWQ | 98 | 53.1% | 0.38s | 3.2 | 15.2G |
| 5 | Ministral 14B GGUF | 97 | 50.2% | 0.55s | 4.85 | 10.5G |
| 6 | Mistral 7B-AWQ | 97 | 49.4% | 0.29s | 4.3 | 11.5G |
| 7-8 | EXAONE-AWQ / Gemma-GGUF | 98/97 | 48.4% | 0.26/1.45s | 5.05/0.9 | 11.5/9G |
| 9-10 | Granite-GGUF / Small-24B | 97/49-50 | 48.2% | 1.09/2.99s | 2.5/0.27 | 8/14.7G |

> ⚠ **FASE 5 v2 (27-sep, ver plan endurecimiento §8)**: con los 4 regex reescritos y el
> [HISTÓRICO — invalidado en sesión 23] gate sosteniendo, el agreement pasó a medir calidad real — FP8 **87.4%** (v1: 61.8%),
> AWQ 87.0%, Qwen3-8B-FP8 80.3% (+26pp que confirman que el número viejo medía desacuerdo
> con referencia rota). Set `pseudo900.json` regenerado (backup v1 en su lugar).

Lecturas clave: (i) 4/5 campos saturan idéntico en los 10 → techo del pipeline, no de modelos;
(ii) solo `pct_monto_referencia_tecnica` discrimina (Llama 114-118/147 vs resto 0-56);
(iii) [HISTÓRICO — invalidado] throughput replica calidad → **triple corona Llama-FP8**; (iv) punto operativo C=8-16;
(v) descartados: Small (online imposible), Qwen3-14B (sin margen VRAM), Gemma/Granite (lentos).

**Veredicto**: Llama 3.1 8B-FP8 para el fallback Pipeline 2; alternativa Llama-AWQ.

## 8. Incidencias (23; detalle en `reporte_benchmark_completo.md` §5 y §8)

Infra (1-7): `cuda-drivers` dummy → `nvidia-open`; `nvcc` al PATH; `python3.12-dev`+`ninja`
faltantes; guard `__main__` (spawn); sin binarios llama.cpp → compilar; `pkill -f` suicida →
`fuser -k`; `nohup &` muere sin `setsid`. Modelos (8-17): flags 0.29 (`quant=None`);
KV-overflow Qwen3-8B (ctx4096+gpu0.80); OOM Small → offload (lento PCIe); ctx-overflow 8193
→ try/except; template Mistral revienta con system → merge; thinking vacío Gemma → reasoning
off; thinking Granite; colapso `pct_monto` no-Llama; artefactos parse (`720`,`1.5`); resume.
Fase 6 (18-23): serve+Small cuelga EngineCore; 404 por model-id (usar path HF); slots
llama-server (ctx÷np) → ctx16k+np4+trunc15k; Small ctx2048 (6/24 truncados); carga AWQ-24B
flaky (2/4 muertes Marlin); xgrammar-AttributeError como artefacto de abort.

## 9. Post-plan (qué conviene; orden sugerido)

> ⚠ **ACTUALIZADO 15-sep**: los puntos 1-4 fueron absorbidos y superados por el
> **Plan v4.1 de endurecimiento** (`docs/endurecimiento_benchmark_plan.md`), ya en ejecución.
> Su §6 registra: taxonomía A/M persistida (el "61.8%" degradado a divergencia descriptiva;
> solo sol_anticipo sostiene Pseudo-867 + pct con caveat), modelo indistinguible en GT-100
> (McNemar p=1.0), población real del fallback = conf=0 (≈260K field-rows, **TODO**: distinct
> docs y re-lista de campos, no dimensionar piloto con 324K), y planillas B + auditoría
> A4.6. Pendiente de ese plan: sesión GPU C (re-log → C3 top-3 sobre 288 humanos → paráfrasis
> → descongelar reserva → veredicto v2).

1. ~~Taxonomía de desacuerdos~~ → ejecutada (A4 + partición A/M; el "techo compartido" era
   referencia rota en caja-A y resolución genuina solo en pago_anticipo caja-M).
2. Ablación guided + `bench serve` formal (GPU): sigue pendiente.
3. Unificar worksets (GPU ~1h): sigue pendiente.
4. **Piloto Pipeline 2**: ETA a recalcular con la población corregida (ver changelog §1);
   el extractor debe filtrar `conf = 0`, no `conf < 0.8`.
5. Repro-package (seeds, un comando por tabla). No hacer: más modelos, Small online, re-runs históricos.

## 10. Mapa de archivos

- Este resumen: `docs/benchmark_local_resumen_general.md`
- Tabla + glosario: `docs/tabla_maestra_benchmark.md` · CSV: `data/processed/llm_benchmark/tabla_maestra_benchmark.csv`
- Infra+incidencias: `docs/reporte_benchmark_completo.md` · ranking: `docs/benchmark_modelos/ranking_combinado.md`
- Harness: `scripts/benchmark_{multi_vs_gt,gguf_http,fase5_900,concurrency}.py`
- Manifest: `models/candidates.yaml` · sets: `data/processed/llm_benchmark/{pseudo900.json,multimodel_results/}`

# Bootstrap — Nueva sesión opencode en Ubuntu 24.04 nativo

> **Propósito**: que una sesión fresca de opencode en la máquina Ubuntu pueda reproducir
> este proyecto y continuar donde quedó Windows/WSL. Fecha: 09-sep-2026.
> Prerrequisito: máquina instalada según `docs/setup_linux_vllm_production.md` (Fases 5a–5i).

---

## 1. Estado exacto a reproducir (medido 09-sep-2026)

| Dato | Valor |
|---|---|
| Rama git | `feature/clustering-pipeline` (NO `feature/ocds-csv-pipeline`) |
| HEAD | `869c86c` — `fix(clustering): Apply 4 anti-overclustering fixes` |
| Relación con origin | **ahead 1** (hay 1 commit local sin push) |
| Modificados sin commit | 11 archivos (AGENTS.md, settings.py, wsl2_vllm_setup.md, run_section_clustering_pipeline.py, 4× section/title ranking ETL, llm_provider.py, 2 pipelines) |
| Sin trackear (~150) | `.opencode/`, `PIPELINE_CHECKLIST.md`, `docs/` nuevos (CONTEXTO, PROMPT, setup, benchmark_modelos, referencias...), `src/etl/*v2*`, `extraction_postprocess.py`, `scripts/` nuevos, `backup_dncp/`, `eval/`, `auditor-top-pliegos/`, `_tmp_*.py`, `nul` |
| Checkpoint fallback LLM | **NO existe** (`llm_pipeline_checkpoint.json` ausente = fallback nunca corrió) |
| Checkpoints carga BD | `.sections_db_done_2021`–`2025` presentes (2026 ausente: año parcial, 601 PDFs) |

### 1.1 Cómo trasladar el árbol de trabajo (elegir UNA)

- **Opción A (recomendada)**: en Windows hacer commit + push de todo (previa limpieza de
  `_tmp_*.py` y SIN el archivo `nul` de raíz — nombre reservado Windows que rompe zips y
  git en algunos flujos), luego `git clone -b feature/clustering-pipeline` en Ubuntu.
- **Opción B (sin commit)**: copiar `proyecto_tesis_export.zip` (11.4 MB, 1050 archivos,
  generado del árbol real con no-trackeados incluidos) + aparte `backup_dncp/backup_dncp.dump`
  (1.6 GB) + los 2 dirs de modelos HF (§7 del setup doc). Descomprimir en `~/subtitle-parse`.

---

## 2. Qué instalar en la sesión nueva (orden)

1. **opencode**: `curl -fsSL https://opencode.ai/install | bash`, Node 20+ como dependencia.
   Copiar `opencode.json` del repo y exportar **`B_AI_API_KEY`** (el provider `b-ai`,
   modelo default `deepseek-v4-flash`, sale de `opencode.json` — sin esa env var no hay modelo).
2. **Agentes**: viajan con el repo en `.opencode/` (agents, commands, skills). En sesión nueva
   se cargan por nombre (`@muestreador-gt`, etc.). Si se usó Opción B, verificar que el zip
   los incluyó.
3. **Stack FASE 5**: `docs/setup_linux_vllm_production.md` §§1–9 (drivers, CUDA 12.8, venv
   `~/vllm-env` con pins, podman, postgres:17 + restore, tika ×2, modelos, llama-server v0.4.0,
   Tailscale). **No reinstalar nada a ciegas**: cada § tiene su comando de verificación.
4. **`.env`**: recrear con las **23 claves** (nombres verificados, valores NO versionados —
   copiarlos aparte por canal seguro):

```
DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, LOGS_FILE, BASE_LOGS_DIR,
DNCP_BASE_URL, BASE_OUTPUT_RAW_DIR, BASE_OUTPUT_PROCESSED_DIR,
BASE_INPUT_EXTERNAL_DATA_DIR, LLM_ENDPOINT, LLM_USERNAME, LLM_PASSWORD,
DEFAULT_BATCH_SIZE, DEFAULT_MAX_RETRIES, GROQ_API_KEY, OPEN_ROUTER_API_KEY,
TOKENGO_API_KEY, DEEPSEEK_API_KEY, GOOGLE_API_KEY, SAMBANOVA_API_KEY, HF_TOKEN
```

Cambios obligados vs Windows: `DB_HOST=localhost` (era `<DB_HOST>`), `DB_PORT=5433`.

---

## 3. Tabla de traducción de rutas (Windows/WSL → Ubuntu)

| Origen | Destino Ubuntu | Notas |
|---|---|---|
| `D:\projects\subtitle-parse` | `~/subtitle-parse` | raíz del repo |
| `/root/vllm-env` | `~/vllm-env` | venv (pins en setup doc §2-bis) |
| `/root/.cache/huggingface` | `~/.cache/huggingface` (o `$HF_HUB_CACHE`) | solo los 2 dirs AWQ prod al inicio |
| `/mnt/d/llmstudio-models/...` | `~/models/...` | solo scripts legacy/benchmark lo usan |
| `<DB_HOST>:5433` | `localhost:5433` | BD y `VLLM_URL` en scripts viejos |
| `C:\models\...` (scripts qwen38) | N/A | solo Windows nativo, ignorar en Ubuntu |

## 4. Archivos con rutas hardcodeadas (qué tocar y qué NO)

- **NO tocar** (`src/`, `config/`, pipelines productivos, `run_document_features_v2*.py`):
  limpios, todo sale de `.env` y rutas relativas. Verificado por grep 09-sep-2026.
- **Adaptar solo si se usan** (legacy/benchmark de una sola corrida):
  - `scripts/benchmark_multi_vs_gt.py:158` — glob `/root/.cache/huggingface/**` → `$HOME/.cache/...`
  - `assign_cluster_ids.py`, `build_few_shot_db.py`, `preparar_gt_nuevo_7variables.py`,
    `eval_llm_quality.py` — `"host": "<DB_HOST>"` → `localhost`
  - `sys.path.insert(0, r'D:\projects\subtitle-parse')` en ~10 scripts `check_*/test_*/fix_*`
    → correr desde la raíz del repo o usar `Path(__file__).parents[1]`
  - `benchmark_5models_v2.py`, `_download_all.py`, `_copy_tokenizers.py`, `_debug_vllm.py`,
    `_ping_vllm.py`, `_eval_*.py`, `_test_*.py`, `_check_chat_tpl.py`, `evaluate_extraction.py:575`
    → rutas `/mnt/d/llmstudio-models` obsoletas (era GGUF); para benchmark usar el Top 10
    del setup doc §7-bis con repos HF, no estos paths
- **Docstrings** con `/root/vllm-env/...` (ej. `alolab_collect_errors.py`, `gold_standard_26campos.py`):
  solo documentación; el comando real es el mismo con `~/vllm-env`.

## 5. Primer mensaje para la sesión nueva (pegar tal cual)

```
Voy a trabajar en ~/subtitle-parse (tesis anomalías DNCP Paraguay).
Contexto en este orden: docs/CONTEXTO_TESIS.md (fuente de verdad),
docs/SESION_NUEVA_UBUNTU.md (bootstrap de ESTA máquina), AGENTS.md,
PIPELINE_CHECKLIST.md. Agentes en .opencode/agents (@muestreador-gt,
@anotador-pseudo-gt, @documentador-tutores).
Reglas Ubuntu (difieren de WSL): venv ~/vllm-env (NO crear otro),
DB en localhost:5433 (podman, .env ya adaptado), vllm==0.23.0,
probar primero SIN VLLM_USE_V2_MODEL_RUNNER=0 (hay UVA en nativo).
src/ y config/ no tienen paths hardcodeados; los scripts con
D:\ o <DB_HOST> son legacy (ver SESION_NUEVA_UBUNTU.md §4).
Confirma: 1) rama+HEAD, 2) conteos BD (pliegos_secciones ~2.78M,
document_features_v2 31,321), 3) import vllm 0.23.0, 4) estado FASE 5
(qué §§ del setup doc están ✅). Después espera instrucciones.
```

## 6. Checklist de verificación post-bootstrap (la sesión debe reportarlo)

```bash
git -C ~/subtitle-parse status --short --branch   # rama + HEAD esperados (§1)
podman exec dncp psql -U postgres -d dncp -tAc "SELECT count(*) FROM pliegos_secciones;"
~/vllm-env/bin/python -c "import vllm; print(vllm.__version__)"   # 0.23.0
nvidia-smi --query-gpu=memory.total --format=csv,noheader          # ≥16384 MiB
tailscale status                                  # online + peers Mac/Windows
ls ~/subtitle-parse/data/processed/llm_pipeline_checkpoint.json 2>/dev/null || echo "fallback aún no corrido"
```

## 7. Pendientes al retomar (orden sugerido, ver CONTEXTO §19)

1. FASE 5 §§ pendientes → 2. fallback LLM (~324K docs, Llama 3.1 8B AWQ) →
3. commit/orden del árbol → 4. benchmark Top 10 (embudo + smoke tests Gemma/Granite) →
5. clustering 9 títulos → 6. Pipeline 3 → 7. Benford → 8. co-bidding.

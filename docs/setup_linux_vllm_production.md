# Setup Linux vLLM Production — Ubuntu 24.04 nativo + RTX 4070 Ti SUPER 16 GB

> **Ítem**: llevar el código fuente, todas sus dependencias, la BD y los modelos a una
> máquina Linux dedicada y dejarla corriendo el pipeline LLM fallback + benchmark.
> **Fecha**: 09-sep-2026 · **Origen verificado**: WSL2 Ubuntu 24.04.4 + `/root/vllm-env`.

Toda versión fijada abajo fue **medida en la máquina de origen**, no estimada.
`[MEDIDO]` = relevado el 09-sep-2026 · `[DOC]` = viene de docs existentes del repo.

---

## 0. Recursos necesarios

### 0.1 Hardware y software mínimo

| Recurso | Requerido | Referencia medida en origen |
|---|---|---|
| GPU | NVIDIA 16 GB VRAM, Ada Lovelace o superior (compute ≥ 8.9) | RTX 4070 Ti SUPER 16 GB |
| RAM sistema | ≥ 16 GB | 15 GiB (origen), 16 CPUs |
| Disco | **150–200 GB libres en NVMe** (ver §0.2) | — |
| OS | Ubuntu 24.04 LTS **Server** (ver §1) | Ubuntu 24.04.4 LTS [MEDIDO] |
| Red | Internet **solo durante el setup** (apt, PyPI, HF, podman pulls) | Luego opera offline con `HF_HUB_OFFLINE=1` |
| Secretos a mano | `HF_TOKEN` (tokenizer Llama gated), credenciales `.env`, login Tailscale | — |

### 0.2 Disco: presupuesto y particionado

Presupuesto medido por componente:

| Componente | Tamaño | Cómo se obtuvo |
|---|---|---|
| Ubuntu 24.04 Server + utilidades | ~25 GB | instalación típica + swapfile |
| NVIDIA driver + CUDA toolkit 12.8 | **6.7 GB** [MEDIDO] | `du -sh /usr/local/cuda-12.8` |
| venv vLLM + proyecto | **9.6 GB** [MEDIDO] | `du -sh /root/vllm-env` |
| Modelos producción (2 AWQ) | **9.3 GB** [MEDIDO] | 5.4 Llama + 3.9 Mistral en caché HF |
| Modelos benchmark extra (8) | ~65 GB progresivo, **pico ~30–40 GB** borrando perdedores | §7, estrategia embudo |
| Postgres restaurada | ~3–5 GB estimados | dump custom 1.6 GB × ~2–3 |
| Código + docs + deps proyecto | ~1 GB | zip exportación 11.4 MB + venv ya contado |
| `data/` de trabajo | **28 GB** [MEDIDO] | `du -sh data/` |
| Staging backup + logs + crecimiento | ~12 GB | holgura |
| **Total** | **~100 GB → mínimo 120 GB, recomendado 150–200 GB** | — |

**Particionado (UEFI) — esquema raíz única + swapfile:**

| Partición | Tamaño | Formato | Notas |
|---|---|---|---|
| EFI existente de Windows (dual-boot) | no tocar | vfat | seleccionar como "EFI System Partition", **NO formatear** |
| `/` (raíz, todo adentro) | 150–200 GB (todo lo asignado) | ext4 | Incluye `/home`: ahí caen caché HF (`~/.cache`), volúmenes podman (`~/.local/share/containers`) y `data/` |
| swap | swapfile 8 GB | — | lo crea el instalador por defecto |

> **Por qué no separar `/` y `/home`**: el grueso (modelos, podman, data) vive bajo
> `/home`; una partición `/` chica es la causa #1 de quedarse sin espacio a mitad de corrida.
> **Dual-boot**: achicar desde *Administración de discos de Windows* → dejar espacio "sin
> asignar" (≥150 GB) → instalador → "Más opciones" → crear `/` ext4 en el espacio libre.
> **Disco dedicado**: instalación automática "Borrar disco", verificando que sea el disco correcto.

**Checkpoint de espacio antes de empezar** (en destino):
```bash
nvidia-smi --query-gpu=memory.total --format=csv,noheader  # ≥ 16384 MiB
free -h      # ≥ 16 GB RAM
df -h /      # ≥ 150 GB libres
```

---

## 1. Instalación Ubuntu 24.04.4 Server

**Decisión cerrada**: imagen **Server** (`ubuntu-24.04.4-live-server-amd64.iso`, 3.2 GB),
no Desktop (6.2 GB). Motivo: GNOME/compositor reservan ~300–500 MB de VRAM y ~1.5 GB de
RAM — en 16 GB VRAM eso es KV cache perdido (duele con Mistral Small 24B, que deja ~2 GB).
Todo lo operativo es terminal (`nvidia-smi`, `tmux`, podman). Si se extraña el GUI, se
agrega después sin reinstalar: `sudo apt install ubuntu-desktop-minimal`.

```bash
# 1. Descargar 24.04.4 (kernel HWE más nuevo, mejor para Ada Lovelace)
#    https://releases.ubuntu.com/24.04/  -> ubuntu-24.04.4-live-server-amd64.iso
# 2. Verificar hash (obligatorio antes de flashear)
sha256sum ubuntu-24.04.4-live-server-amd64.iso
grep ubuntu-24.04.4-live-server-amd64.iso SHA256SUMS   # comparar
# 3. Flashear con Ventoy o Balena Etcher, instalar con el esquema del §0.2
sudo apt update && sudo apt upgrade -y
```

---

## 2. Stack base: drivers, CUDA, sistema

```bash
# Herramientas de compilación (vLLM, hdbscan, umap-learn, FlashInfer JIT)
sudo apt install -y gcc g++ python3-dev python3-pip python3-venv ninja-build \
  git curl wget tmux htop unzip pkg-config libpq-dev

# Driver NVIDIA (serie 580+ para Ada; el instalador elige el recomendado)
sudo ubuntu-drivers install
sudo reboot
nvidia-smi   # debe mostrar la 4070 Ti SUPER, 16384 MiB

# CUDA toolkit 12.8 (solo toolkit, NO el driver; en nativo usar repo ubuntu2404,
# NO el wsl-ubuntu del doc viejo docs/wsl2_vllm_setup.md)
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt update && sudo apt install -y cuda-toolkit-12-8
export PATH=/usr/local/cuda-12.8/bin:$PATH
nvcc --version   # cuda_12.8.x
```

Persistencia en `~/.bashrc`:
```bash
cat >> ~/.bashrc << 'EOF'

# === CUDA Toolkit ===
export PATH="/usr/local/cuda-12.8/bin:$PATH"
export LD_LIBRARY_PATH="/usr/local/cuda-12.8/lib64:${LD_LIBRARY_PATH:-}"
# === WSL no aplica aquí; en nativo localhost funciona sin workarounds ===
EOF
source ~/.bashrc
```

### 2-bis. Versiones pinneadas (inventario verificado [MEDIDO])

| Paquete | Versión origen | Instalación destino |
|---|---|---|
| vllm | **0.23.0** | `pip install vllm==0.23.0` (NO subir: 0.25.x rompe en este stack) |
| torch / torchaudio / torchvision / torchcodec | **2.11.0** / 2.11.0 / 0.26.0 / 0.15.0 | vienen como dependencia de vllm; si se fijan, usar estas |
| transformers / tokenizers | **5.12.1** / 0.22.2 | `pip install transformers==5.12.1 tokenizers==0.22.2` |
| xgrammar / outlines_core | **0.2.2** / 0.2.14 | backend de guided decoding (xgrammar, NO GBNF en vLLM) |
| numpy / pandas | 2.3.5 / 3.0.5 | proyecto |
| psycopg2-binary / SQLAlchemy | 2.9.12 / 2.0.51 | proyecto (psycopg2, no asyncpg) |
| scikit-learn / sentence-transformers / sentencepiece | 1.9.0 / 5.6.1 / 0.2.1 | proyecto |
| umap-learn | 0.5.12 | proyecto (requiere gcc + python3-dev) |
| ninja | 1.13.0 | JIT de kernels |
| podman | **4.9.3** [MEDIDO] | `sudo apt install -y podman` (igual o superior) |
| Java (respaldo Tika local) | OpenJDK **21.0.12** [MEDIDO] | `sudo apt install -y openjdk-21-jre-headless` |
| Python sistema | **3.12.3** [MEDIDO] | ya viene con 24.04 |
| Node + opencode | solo lado Windows en origen | **opcional** (comodidad dev): Node 20 via nodesource + script oficial opencode |

---

## 3. Código fuente

```bash
# Opción A (recomendada): git
git clone <repo-url> ~/subtitle-parse
cd ~/subtitle-parse
git checkout feature/ocds-csv-pipeline

# Opción B: zip de exportación (11.4 MB, 1050 archivos, sin datos grandes)
unzip proyecto_tesis_export.zip -d ~/subtitle-parse

# .env NO viaja con el código: recrear desde example.env
cp example.env .env
# Completar: DB_* (ver §4), HF_TOKEN (tokenizer Llama gated), API keys
```

Qué **no** copiar: `data/` (28 GB, se regenera o se traslada aparte si hace falta),
`backup_dncp/` (viaja el dump, §4), `.env` (secretos).

---

## 4. Postgres + restore del backup

Imagen: `localhost/postgres-dncp:17` es un tag local de `docker.io/library/postgres:17`
(ambas 461 MB [MEDIDO]) = PG **17.9** (pgdg, Debian trixie). En destino usar la oficial:

```bash
# Si hay internet: pull directo. Si no: 'podman save' en origen + 'podman load' aquí.
podman pull docker.io/library/postgres:17

podman run -d --name dncp \
  -p 5433:5432 \
  -e POSTGRES_DB=dncp \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=<DB_PASSWORD> \
  -v dncp_data:/var/lib/postgresql/data \
  docker.io/library/postgres:17

# Restore del dump custom -Fc -Z9 (1.6 GB) — NO necesita zstd, va directo a pg_restore
podman cp /ruta/dncp_backup_2026-09-03.dump dncp:/tmp/
podman exec dncp pg_restore -U postgres -d dncp -Fc --clean --if-exists /tmp/backup_dncp.dump

# Verificación por conteos (deben coincidir con origen)
podman exec dncp psql -U postgres -d dncp -tAc \
  "SELECT count(*) FROM pliegos_secciones;"            -- ~2.78M
podman exec dncp psql -U postgres -d dncp -tAc \
  "SELECT count(*) FROM document_features_v2;"         -- 31,321
```

`.env` de producción: `DB_HOST=localhost DB_PORT=5433 DB_NAME=dncp DB_USER=postgres
DB_PASSWORD=<DB_PASSWORD>`.

---

## 5. Tika (podman, puertos 9998/9999)

Imagen medida: `docker.io/apache/tika:latest` (361 MB [MEDIDO]); en origen corre como
`tika-server-9998` (9998→9998) y `tika-server-9999` (9999→9999) [MEDIDO]:

```bash
podman pull docker.io/apache/tika:latest
podman run -d --name tika-server-9998 -p 9998:9998 docker.io/apache/tika:latest
podman run -d --name tika-server-9999 -p 9999:9999 docker.io/apache/tika:latest
curl -s http://localhost:9998/tika  # 200 = ok
```

---

## 6. venv vLLM + dependencias del proyecto (un solo venv)

Verificado en origen: `/root/vllm-env` **ya contiene** pandas, psycopg2, sklearn,
sentence-transformers → un solo entorno sirve para vLLM y pipelines:

```bash
python3 -m venv ~/vllm-env
source ~/vllm-env/bin/activate
pip install --upgrade pip
pip install vllm==0.23.0 transformers==5.12.1 tokenizers==0.22.2 ninja
# GGUF solo si se quiere el formato viejo: pip install vllm-gguf-plugin (experimental)
cd ~/subtitle-parse
pip install -r requirements.txt
python -c "import vllm; print(vllm.__version__)"   # 0.23.0
```

---

## 7. Modelos por fases (NO descargar todo junto)

### 7.1 Fase setup — 2 AWQ producción, copia offline (9.3 GB [MEDIDO])

Ya están en la caché HF de origen; copiar **solo estos dos directorios** (caché total 63 GB, no copiar entera):

```bash
# En origen (donde están): ~/.cache/huggingface/hub/
models--thesven--Meta-Llama-3.1-8B-Instruct-AWQ        # 5.4 GB [MEDIDO]
models--solidrust--Mistral-7B-Instruct-v0.3-AWQ        # 3.9 GB [MEDIDO]
# -> USB/disco -> misma ruta en destino, o apuntar HF_HUB_CACHE a un disco de datos
export HF_HUB_OFFLINE=1   # operar sin red tras la copia
# Alternativa con red: huggingface-cli download thesven/Meta-Llama-3.1-8B-Instruct-AWQ
# (Llama requiere HF_TOKEN por tokenizer gated)
```

### 7.2 Fase benchmark — resto progresivo con embudo

Estrategia del ranking (`docs/benchmark_modelos/ranking_combinado.md`): Fase 1 smoke
30–50 muestras por modelo → **descartar <70% y borrarlo del disco antes del siguiente**
→ Fase 2 900 muestras solo top 5–8. Pico real ~30–40 GB, nunca los ~75 GB juntos.

### 7-bis. Matriz de memoria — Top 10 a probar (RTX 4070 Ti SUPER 16 GB)

VRAM total ≈ pesos + KV + ~1 GB overhead. Fuente: `docs/benchmark_modelos/*` (31-ago-2026).

| # | Modelo | Formato / servidor | VRAM pesos | KV cache | En 16 GB | Disco | Flags / avisos |
|---|---|---|---|---|---|---|---|
| 1 | Llama 3.1 8B | AWQ · vLLM `awq_marlin` | 4.0 | ~10 | ✅ holgado | 5.4 GB | baseline F1 96.4% |
| 2 | Qwen3-14B | AWQ · vLLM `awq` (sin marlin) | 7.4 | ~6.6 | ✅ justo | ~8 GB | `VLLM_USE_V2_MODEL_RUNNER=0` |
| 3 | Mistral 7B v0.3 | AWQ · vLLM `awq_marlin` | 3.7 | ~10.3 | ✅ holgado | 3.9 GB | throughput 35× |
| 4 | Llama 3.1 8B-FP8 | FP8 · vLLM `fp8_e4m3` | ~8.0 | ~6 | ✅ | ~8.5 GB | `--dtype bfloat16 --gpu-memory-utilization 0.90` |
| 5 | Qwen3-8B-FP8 | FP8 · vLLM `fp8_e4m3` | ~8.2 | ~6 | ✅ | ~8.5 GB | igual + flag runner Qwen |
| 6 | Mistral Small 3.1 24B | AWQ · vLLM `awq_marlin` | 12.0 | ~2 | ⚠️ **ctx 4K forzado** | ~12 GB | `--max-model-len 4096`, 10 muestras |
| 7 | Granite 4.2 8B | GGUF · **llama-server** | 5.5 | ~8.7 | ✅ | ~5 GB | ⚠️ smoke test desactivación thinking ANTES |
| 8 | Gemma 4 12B | GGUF · **llama-server** | 7.66 | ~6.5 | ✅ | ~8 GB | ⚠️ smoke test guided decoding ANTES (bug vLLM #39130) |
| 9 | EXAONE-3.5-7.8B | AWQ · vLLM | 3.9 | ~10.1 | ✅ holgado | ~4.5 GB | validar español |
| 10 | Ministral-3-14B | GGUF · **llama-server** | 8.5 | ~5.7 | ✅ | ~8.5 GB | sin issues conocidos |

**Descartados por memoria** (no probar): Qwen3.8-27B AWQ 15.5 GB/KV~0, Granite 30B 15.9 GB,
Qwen3-14B-FP8 14.8 GB/KV~0, phi-4-FP8 14.7 GB, Muse-Glimmer-30B ~17 GB, Qwen3.x-27B-FP8 ~27 GB.

**Params validados del pipeline fallback** (`scripts/run_document_features_v2_llm_pipeline.py`):
`--gpu-mem 0.70 --max-num-seqs 32 --max-model-len 8192 --quantization awq_marlin
--conf-threshold 0.8 --chunk-size 500`, `enforce_eager=True`, `dtype float16`,
`VLLM_USE_V2_MODEL_RUNNER=0`. Carga pendiente origen: ~324K docs × 12 campos hybrid.

---

## 8. llama-server (GGUF, modelos 7/8/10)

vLLM **no** sirve para GGUF (plugin experimental/deprecado en 0.23.0). GGUF 2026 va por
llama-server con backend GBNF — no comparar su precisión con AWQ/FP8 (xgrammar).

```bash
# Binario oficial con CUDA (sin compilar). Versión fijada 09-sep-2026:
LLAMA_VER=v0.4.0   # https://github.com/ggml-org/llama.cpp/releases (verificar si hay posterior)
wget https://github.com/ggml-org/llama.cpp/releases/download/${LLAMA_VER}/llama-b10809-bin-ubuntu-x64.zip
unzip llama-*-bin-ubuntu-x64.zip -d ~/llama.cpp
~/llama.cpp/llama-server --version
# Arranques de referencia en docs/benchmark_modelos/modelos_gguf.md §Comandos de arranque
```

---

## 9. Tailscale + SSH (Mac y Windows como clientes)

Topología: Ubuntu + Mac + Windows en una tailnet privada (IPs `100.x.y.z`), SSH directo
punto a punto con WireGuard, **sin abrir puertos en el router ni IP pública**.

```bash
# En Ubuntu
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --ssh   # activa tailscaled + servidor SSH propio de Tailscale
sudo systemctl enable tailscaled
tailscale status          # debe mostrar la máquina como online
tailscale ip -4           # ej. 100.64.0.1
```

**Clientes**: app Tailscale en Mac (App Store) y Windows (msi oficial), mismo login.
Conexión desde PowerShell/Terminal: `ssh usuario@nombre-maquina` o `ssh usuario@100.x.y.z`.

**Endurecimiento SSH** (una vez validado el acceso por llave):
```bash
ssh-keygen -t ed25519                 # en cada cliente
ssh-copy-id usuario@100.x.y.z        # instalar la pública en Ubuntu
# /etc/ssh/sshd_config: PasswordAuthentication no  ->  sudo systemctl reload ssh
```
fail2ban queda opcional: la tailnet no expone SSH a internet. ACLs: en tailnet personal
el acceso es total por defecto; restringir vía panel admin (ACL JSON) solo si se suman máquinas.
**Cierre de fase**: `tailscale status` en los 3 nodos + `ssh` cruzado Mac→Ubuntu y
Windows→Ubuntu. Beneficio extra: `vllm serve --host 0.0.0.0` y Postgres quedan visibles
solo dentro de la tailnet.

---

## 10. Validación end-to-end

```bash
source ~/vllm-env/bin/activate
python -c "import vllm; print(vllm.__version__)"   # 0.23.0
# Smoke offline ambos AWQ (incl. guided decoding con regex ^(-?\d+([.,]\d+)?|NULL)$)
VLLM_USE_V2_MODEL_RUNNER=0 ~/vllm-env/bin/python scripts/benchmark_multi_vs_gt.py  # muestra corta
# Restore counts (§4) + Tika (§5) + tailscale status (§9)
# Corrida corta del fallback con checkpoint reanudable:
VLLM_USE_V2_MODEL_RUNNER=0 ~/vllm-env/bin/python \
  scripts/run_document_features_v2_llm_pipeline.py --chunk-size 50
# Checkpoint: data/processed/llm_pipeline_checkpoint.json (borrar para re-correr todo)
```

Dejar corriendo con tmux (`tmux new-session -d -s llm '...'`) o unidad systemd
(ver `docs/wsl2_vllm_setup.md` §13 como plantilla, adaptando rutas `/root/...` → `~/...`).

---

## 11. Troubleshooting Linux nativo

Qué **cambia** vs WSL2 (origen): `localhost` funciona sin workarounds; no hay procesos
Windows comiendo VRAM; UVA está disponible → **probar primero SIN `VLLM_USE_V2_MODEL_RUNNER=0`**
y solo si falla, setearlo. Lo demás del catálogo (`docs/wsl2_vllm_errors.md`) aplica igual:
OOM en carga → bajar `--gpu-mem` 0.70→0.60 o `--max-model-len` 8192→4096;
CUDA graphs hang → `enforce_eager=True` (ya es default del pipeline);
`localhost:8000` → verificar `ss -tlnp | grep 8000` y firewall `ufw`.

**PowerShell quoting** (operando desde Windows): no usar `python -c` con comillas/backticks
vía `wsl.exe`/`ssh`; escribir script temporal y ejecutarlo (ver AGENTS.md §PowerShell Bug Workaround).

---

*Referencias del repo: `docs/wsl2_vllm_setup.md` (instalación base),
`docs/wsl2_vllm_errors.md` (catálogo de errores), `docs/benchmark_modelos/` (38 modelos),
`docs/benchmark_resultados.md` (mediciones), `PIPELINE_CHECKLIST.md` FASE 5 (índice).*

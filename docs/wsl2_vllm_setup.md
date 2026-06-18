# Guía Completa: vLLM en WSL2 Ubuntu (Windows)

Cómo instalar, configurar y ejecutar vLLM con un modelo GGUF en WSL2 Ubuntu dentro de Windows.
Incluye todas las optimizaciones descubiertas durante el desarrollo del Pipeline 2.

## Resumen rápido (copy-paste)

```bash
# 1. Instalar dependencias
sudo apt install gcc g++ python3-dev python3-pip -y

# 2. Instalar CUDA toolkit
wget https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt update && sudo apt install cuda-toolkit-12-8 -y

# 3. Crear entorno virtual + vLLM
python3 -m venv /root/vllm-env
source /root/vllm-env/bin/activate
pip install vllm vllm-gguf-plugin ninja

# 4. Variables de entorno (agregar a ~/.bashrc)
echo 'export PATH="/usr/local/cuda-12.8/bin:/root/vllm-env/bin:$PATH"' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH="/usr/local/cuda-12.8/lib64:${LD_LIBRARY_PATH:-}"' >> ~/.bashrc
source ~/.bashrc

# 5. Iniciar vLLM (con optimizaciones)
bash /tmp/vllm_run.sh
```

## Prerrequisitos

- Windows 10/11 con WSL2 habilitado
- GPU NVIDIA con 16+ GB VRAM (probado con RTX 4070 Ti SUPER)
- Modelo GGUF descargado (ej: Qwen2.5-14B-Instruct-Q5_K_L)
- WSL2 con Ubuntu 22.04+

## Paso 1: Habilitar WSL2

```powershell
# En PowerShell como Administrador
wsl --install
# Reiniciar PC
# Se instala Ubuntu por defecto
```

## Paso 2: Entrar a WSL2

```powershell
wsl
# O específicamente:
wsl -d Ubuntu
```

## Paso 3: Actualizar sistema

```bash
sudo apt update && sudo apt upgrade -y
```

## Paso 4: Instalar dependencias del sistema

```bash
# Compilador C/C++ (necesario para vLLM y FlashInfer)
sudo apt install gcc g++ -y

# Headers de Python (necesario para compilar extensiones)
sudo apt install python3-dev -y

# pip
sudo apt install python3-pip -y

# Verificar versiones
gcc --version   # Debería mostrar gcc 11.x o 13.x
g++ --version
python3 --version
```

## Paso 5: Instalar CUDA Toolkit en WSL2

WSL2 NO viene con CUDA toolkit. Sin él, `nvcc` no existe y vLLM no puede compilar kernels.

```bash
# Descargar keyring de CUDA
wget https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/cuda-keyring_1.1-1_all.deb

# Instalar keyring
sudo dpkg -i cuda-keyring_1.1-1_all.deb

# Actualizar apt
sudo apt update

# Instalar CUDA toolkit (solo toolkit, NO el driver - WSL2 usa el driver de Windows)
sudo apt install cuda-toolkit-12-8 -y
```

### Verificar instalación de CUDA

```bash
export PATH=/usr/local/cuda-12.8/bin:$PATH
nvcc --version
# Debería mostrar: cuda_12.8.93 o similar
```

## Paso 6: Crear entorno virtual para vLLM

```bash
# Crear entorno virtual
python3 -m venv /root/vllm-env

# Activar
source /root/vllm-env/bin/activate

# Actualizar pip
pip install --upgrade pip
```

## Paso 7: Instalar vLLM

```bash
# Activar entorno si no está activo
source /root/vllm-env/bin/activate

# Instalar vLLM con soporte GGUF
pip install vllm

# Instalar plugin GGUF (necesario para cargar modelos .gguf)
pip install vllm-gguf-plugin

# Instalar ninja (necesario para FlashInfer JIT)
pip install ninja
```

## Paso 8: Verificar instalación de vLLM

```bash
source /root/vllm-env/bin/activate
python -c "import vllm; print(vllm.__version__)"
# Debería mostrar: 0.23.0 o similar
```

## Paso 9: Modelo GGUF

El modelo debe estar en un path accesible desde WSL2. Si el modelo está en Windows (ej: `D:\llmstudio-models\`), se accede vía `/mnt/d/llmstudio-models/`.

### Estructura esperada

```
/mnt/d/llmstudio-models/
  lmstudio-community/
    Qwen2.5-14B-Instruct-Q5_K_L/
      Qwen2.5-14B-Instruct-Q5_K_L.gguf   # ~10.5 GB
```

### Verificar que el modelo es accesible

```bash
ls -lh /mnt/d/llmstudio-models/lmstudio-community/Qwen2.5-14B-Instruct-Q5_K_L/
# Debería mostrar el archivo .gguf de ~10.5 GB
```

## Paso 10: Variables de entorno (permanentes)

Para que CUDA y vLLM estén disponibles siempre:

```bash
# Editar ~/.bashrc
echo '' >> ~/.bashrc
echo '# === CUDA Toolkit ===' >> ~/.bashrc
echo 'export PATH="/usr/local/cuda-12.8/bin:$PATH"' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH="/usr/local/cuda-12.8/lib64:${LD_LIBRARY_PATH:-}"' >> ~/.bashrc
echo '' >> ~/.bashrc
echo '# === vLLM ===' >> ~/.bashrc
echo 'export PATH="/root/vllm-env/bin:$PATH"' >> ~/.bashrc

# Aplicar
source ~/.bashrc
```

**IMPORTANTE**: Después de agregar a `~/.bashrc`, cerrar y abrir nueva terminal WSL para que tome efecto.

## Paso 11: Script de startup (vLLM optimizado)

### Script final: `vllm_run.sh`

Este es el script que funciona con todas las optimizaciones:

```bash
#!/bin/bash
export PATH="/usr/local/cuda-12.8/bin:/root/vllm-env/bin:$PATH"
export LD_LIBRARY_PATH="/usr/local/cuda-12.8/lib64:${LD_LIBRARY_PATH:-}"

MODEL="/mnt/d/llmstudio-models/lmstudio-community/Qwen2.5-14B-Instruct-Q5_K_L/Qwen2.5-14B-Instruct-Q5_K_L.gguf"
TOKENIZER="Qwen/Qwen2.5-14B-Instruct"

/root/vllm-env/bin/vllm serve "$MODEL" \
    --tokenizer "$TOKENIZER" \
    --host 0.0.0.0 \
    --port 8000 \
    --max-model-len 14000 \
    --gpu-memory-utilization 0.90 \
    --enable-prefix-caching \
    --kv-cache-dtype fp8
```

### Cómo copiar el script a WSL2

```powershell
# Desde Windows PowerShell:
wsl -e bash -c "cp /mnt/c/Users/Giovanni/AppData/Local/Temp/opencode/vllm_run.sh /tmp/vllm_run.sh && chmod +x /tmp/vllm_run.sh"
```

### Cómo iniciar vLLM

```bash
# En WSL2:
bash /tmp/vllm_run.sh

# O con tmux (para que sobreviva al cierre de terminal):
tmux new-session -d -s vllm 'bash /tmp/vllm_run.sh'
tmux attach -t vllm  # Para ver los logs
```

### Parámetros explicados

| Parámetro | Valor | Por qué |
|-----------|-------|---------|
| `--tokenizer` | `Qwen/Qwen2.5-14B-Instruct` | Nombre del tokenizer en HuggingFace (no el path del GGUF) |
| `--host` | `0.0.0.0` | Escuchar en todas las interfaces (necesario para acceder desde Windows) |
| `--port` | `8000` | Puerto del servidor |
| `--max-model-len` | `14000` | Longitud máxima de contexto. 16384 causaba OOM en KV cache |
| `--gpu-memory-utilization` | `0.90` | Usar 90% de VRAM. 0.95 excedía VRAM por overhead de display |
| `--enable-prefix-caching` | (flag) | Cachea el KV cache del system prompt (se repite 320K veces en Pipeline 2) |
| `--kv-cache-dtype` | `fp8` | Reduce el tamaño del KV cache a 50%, permite más concurrencia |

### Optimizaciones aplicadas (y por qué)

| Optimización | Impacto | Estado |
|-------------|:-------:|:------:|
| CUDA graphs (sin --enforce-eager) | +23% throughput | ✅ Activo |
| Prefix caching | +10-30% para prompts repetidos | ✅ Activo |
| FP8 KV cache | +20-40% más concurrencia | ✅ Activo |
| Chunked prefill | +5-10% (default en V1) | ✅ Automático |
| torch.compile | +10-15% (con caché) | ✅ Automático |
| Multi-step scheduling | +10-15% | ❌ No existe en vLLM 0.23.0 |

## Paso 12: Verificar que vLLM está funcionando

### Desde WSL2

```bash
curl http://localhost:8000/v1/models
# Debería retornar JSON con el modelo listado
```

### Desde Windows PowerShell

```powershell
Invoke-WebRequest -Uri "http://localhost:8000/v1/models" -UseBasicParsing
# Status: 200
```

### Test de inferencia

```python
import requests
r = requests.post("http://localhost:8000/v1/chat/completions", json={
    "model": "/mnt/d/llmstudio-models/lmstudio-community/Qwen2.5-14B-Instruct-Q5_K_L/Qwen2.5-14B-Instruct-Q5_K_L.gguf",
    "messages": [{"role": "user", "content": "Hola, responde solo con OK"}],
    "max_tokens": 10,
    "temperature": 0
})
print(r.json()["choices"][0]["message"]["content"])
# → "OK"
```

**IMPORTANTE**: El `model` en vLLM es el **path completo del GGUF**, no el nombre corto como en LM Studio.

## Paso 13: Ejecutar en background (persistente)

### Opción recomendada: tmux

```bash
# Iniciar en tmux
tmux new-session -d -s vllm 'bash /tmp/vllm_run.sh 2>&1 | tee /tmp/vllm_optimized.log'

# Ver logs
tmux attach -t vllm

# Detach: Ctrl+B, D

# Verificar que está corriendo
tmux list-sessions
ps aux | grep vllm | grep -v grep
```

### Opción alternativa: nohup

```bash
nohup bash /tmp/vllm_run.sh > /tmp/vllm.log 2>&1 &
```

**NOTA**: `nohup` puede no funcionar correctamente en WSL2 si el proceso padre termina. Usar tmux es más confiable.

## Errores comunes y soluciones

Ver `docs/wsl2_vllm_errors.md` para el catálogo completo.

### Error más común: PowerShell expande `$PATH`

```powershell
# ❌ Esto NO funciona - PowerShell expande $PATH con el valor de Windows
wsl -e bash -c "export PATH=/usr/local/cuda-12.8/bin:$PATH && vllm serve ..."

# ✅ Esto SÍ funciona - usar scripts en archivo
wsl -e bash -c "cp /mnt/c/.../vllm_run.sh /tmp/ && bash /tmp/vllm_run.sh"
```

### Error: `localhost:8000` no responde desde Windows

Si vLLM está corriendo en WSL2 pero `localhost:8000` no responde desde Windows:

```bash
# Verificar que vLLM está escuchando
ss -tlnp | grep 8000

# Obtener IP de WSL2
hostname -I
# Usar esa IP en Windows: http://172.x.x.x:8000
```

### Error: `CUDA out of memory`

```bash
# Reducir VRAM usage
--gpu-memory-utilization 0.85

# O reducir contexto
--max-model-len 8000
```

### Error: vLLM se cae después de un tiempo

Verificar logs:
```bash
# Si usas tmux:
tmux capture-pane -t vllm -p | tail -50

# Si usas nohup:
cat /tmp/vllm.log | tail -50
```

## Archivos importantes

| Archivo | Ubicación |
|---------|-----------|
| Script de startup (Windows) | `C:\Users\Giovanni\AppData\Local\Temp\opencode\vllm_run.sh` |
| Script de startup (WSL2) | `/tmp/vllm_run.sh` |
| Modelo GGUF | `D:\llmstudio-models\lmstudio-community\Qwen2.5-14B-Instruct-Q5_K_L\` |
| Entorno virtual | `/root/vllm-env/` |
| CUDA toolkit | `/usr/local/cuda-12.8/` |
| Log de vLLM | `/tmp/vllm_optimized.log` o en tmux |
| Benchmark scripts | `C:\Users\Giovanni\AppData\Local\Temp\opencode\benchmark_vllm*.py` |

## Resumen de comandos (quick reference)

```bash
# Entrar a WSL2
wsl

# Activar entorno
source /root/vllm-env/bin/activate
export PATH="/usr/local/cuda-12.8/bin:/root/vllm-env/bin:$PATH"
export LD_LIBRARY_PATH="/usr/local/cuda-12.8/lib64:${LD_LIBRARY_PATH:-}"

# Iniciar vLLM (con optimizaciones)
bash /tmp/vllm_run.sh

# Iniciar en background con tmux
tmux new-session -d -s vllm 'bash /tmp/vllm_run.sh'

# Verificar
curl http://localhost:8000/v1/models

# Ver logs en tmux
tmux capture-pane -t vllm -p | tail -30

# Test de inferencia con Python
python3 /tmp/benchmark_vllm2.py
```

## Historia: De LM Studio a vLLM optimizado

### Contexto
- Pipeline 2 necesita procesar 320,000 secciones de pliegos de licitación
- LM Studio (localhost:1234) alcanzaba ~103 tok/s secuencial
- Con 4 workers concurrentes: ~400 tok/s → ~7 días de ejecución

### Iteración 1: vLLM con --enforce-eager
- **Resultado**: 245 tok/s secuencial (2.4x sobre LM Studio)
- **Problema**: --enforce-eager desactiva CUDA graphs, perdiendo 20-30% de performance
- **Causa**: Se usó por problemas con FlashInfer JIT en WSL2

### Iteración 2: vLLM con optimizaciones
- **Optimizaciones aplicadas**:
  - Quitado `--enforce-eager` → CUDA graphs activados
  - `--enable-prefix-caching` → cachea system prompt
  - `--kv-cache-dtype fp8` → reduce KV cache a 50%
- **Resultado**: 301 tok/s secuencial (+23%), 999.7 tok/s con 8 workers (+81%)
- **Tiempo Pipeline 2**: ~3-4 días (vs ~7 días con LM Studio)

### Errores encontrados y resueltos
1. **`Python.h`** → `sudo apt install python3-dev -y`
2. **`nvcc not found`** → `sudo apt install cuda-toolkit-12-8 -y`
3. **`ninja not found`** → `pip install ninja`
4. **KV cache OOM** → `--max-model-len 14000` (no 16384)
5. **PowerShell expande `$PATH`** → scripts en archivo, no inline
6. **`--num-scheduler-steps` no existe** → quitar flag
7. **nohup no funciona en WSL2** → usar tmux
8. **vLLM model name** → path completo del GGUF, no nombre corto

# Catálogo de Errores: vLLM en WSL2

Todos los errores encontrados al instalar y ejecutar vLLM en WSL2 Ubuntu dentro de Windows, con sus soluciones.
Orden cronológico de aparición durante el desarrollo del Pipeline 2.

---

## 1. Errores de compilación (durante `pip install vllm`)

### 1.1 `fatal error: Python.h: No such file or directory`

```
fatal error: Python.h: No such file or directory
compilation terminated
error: command 'gcc' failed with exit status 1
```

**Causa**: Falta el paquete `python3-dev` que contiene los headers de Python necesarios para compilar extensiones C.

**Solución**:
```bash
sudo apt install python3-dev -y
```

---

### 1.2 `nvcc not found` / `The nvcc binary could not be found in your PATH`

```
The nvcc binary could not be found in your PATH.
Please ensure that CUDA is installed and available in your PATH.
```

**Causa**: CUDA toolkit no está instalado en WSL2, o no está en el PATH.

**Solución**:
```bash
# Instalar CUDA toolkit
wget https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt update
sudo apt install cuda-toolkit-12-8 -y

# Agregar al PATH
export PATH=/usr/local/cuda-12.8/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/cuda-12.8/lib64:$LD_LIBRARY_PATH

# Verificar
nvcc --version
# Debería mostrar: Cuda compilation tools, release 12.8, V12.8.93
```

**Nota**: WSL2 usa el driver de Windows para la GPU. NO instalar `cuda-drivers` en WSL2, solo `cuda-toolkit`.

---

### 1.3 `gcc: error: unrecognized command-line option '-Wno-maybe-uninitialized'`

```
gcc: error: unrecognized command-line option '-Wno-maybe-uninitialized'
```

**Causa**: Falta el compilador `gcc` o es una versión muy vieja.

**Solución**:
```bash
sudo apt install gcc g++ -y
gcc --version  # Debería mostrar 11.x o 13.x
```

---

### 1.4 `ninja: build stopped: subcommand failed.`

```
ninja: build stopped: subcommand failed.
error: error: command 'ninja' failed
```

**Causa**: FlashInfer necesita `ninja` para compilar kernels JIT. No está instalado en el sistema ni en el entorno virtual.

**Solución**:
```bash
source /root/vllm-env/bin/activate
pip install ninja
```

---

### 1.5 `error: Microsoft Visual C++ 14.0 or greater is required`

Este error NO ocurre en WSL2, solo en Windows nativo. En WSL2 se usa `gcc`.

---

## 2. Errores de runtime (al ejecutar `vllm serve`)

### 2.1 `torch.cuda.OutOfMemoryError: CUDA out of memory`

```
torch.cuda.OutOfMemoryError: CUDA out of memory.
Tried to allocate XX GiB. GPU X has XX GiB total capacity...
```

**Causa**: El modelo + KV cache exceden la VRAM disponible.

**Soluciones** (en orden de preferencia):

```bash
# 1. Reducir uso de VRAM
--gpu-memory-utilization 0.85

# 2. Reducir longitud de contexto (reduce KV cache)
--max-model-len 8000

# 3. Usar FP8 KV cache (reduce tamaño a 50%)
--kv-cache-dtype fp8
```

**Cálculo del KV cache**:
- Qwen2.5-14B: 40 capas, 8 KV heads, head_dim=128
- 16K contexto → ~3.0 GiB KV cache
- 14K contexto → ~2.67 GiB KV cache (cabe en 16 GiB VRAM con modelo de 10.55 GiB)
- Con FP8: 14K contexto → ~1.34 GiB KV cache (cabe holgadamente)

---

### 2.2 `KeyError: 'PyTorch version < 2.0 is not supported'`

**Causa**: PyTorch demasiado viejo.

**Solución**:
```bash
pip install --upgrade torch torchvision torchaudio
```

---

### 2.3 FlashInfer JIT compilation timeout

```
Compiling FlashInfer kernels (this may take a few minutes the first time)...
```

**Causa**: La primera ejecución compila kernels JIT. Toma 2-5 minutos.

**Solución**: Esperar. No es un error. Los kernels se cachean para siguientes ejecuciones.

---

### 2.4 `--num-scheduler-steps` no existe en vLLM 0.23.0

```
vllm: error: unrecognized arguments: --num-scheduler-steps 4
```

**Causa**: Este flag no existe en vLLM 0.23.0. Está documentado en docs pero no implementado.

**Solución**: Quitar el flag del comando.

```bash
# ❌ Incorrecto
vllm serve MODEL --num-scheduler-steps 4

# ✅ Correcto
vllm serve MODEL  # Sin ese flag
```

---

### 2.5 vLLM se cae después de ~30 segundos

**Causa**: Posible OOM diferido o conflicto con display driver.

**Diagnóstico**:
```bash
# Verificar VRAM disponible antes de iniciar
nvidia-smi

# Verificar logs
cat /tmp/vllm.log | tail -50
```

**Soluciones**:
```bash
# Reducir VRAM
--gpu-memory-utilization 0.85

# O desactivar CUDA graphs (que pueden causar OOM diferido)
--enforce-eager
```

---

## 3. Errores de conectividad (desde Windows)

### 3.1 `localhost:8000` no responde desde Windows

```
Invoke-WebRequest : No es posible conectar con el servidor remoto
```

**Causas posibles**:

1. **vLLM no está corriendo** en WSL2
2. **vLLM aún está cargando** el modelo (puede tomar 1-2 minutos)
3. **WSL2 networking** no está configurado correctamente

**Diagnóstico**:
```bash
# En WSL2: verificar que vLLM está escuchando
ss -tlnp | grep 8000
# Debería mostrar: LISTEN  0  128  0.0.0.0:8000  ...

# En WSL2: probar localmente
curl http://localhost:8000/v1/models
```

**Soluciones**:
```bash
# 1. Obtener IP de WSL2
hostname -I
# Ejemplo: 172.28.123.45

# 2. Usar esa IP desde Windows
# Invoke-WebRequest -Uri "http://172.28.123.45:8000/v1/models"
```

---

### 3.2 `ConnectionRefusedError: [Errno 111] Connection refused`

**Causa**: El servidor no está escuchando en el puerto.

**Solución**: Verificar que vLLM está corriendo y en el puerto correcto:
```bash
ss -tlnp | grep 8000
```

---

## 4. Errores de modelo

### 4.1 `The model 'X' does not exist`

```json
{
  "error": {
    "message": "The model `qwen2.5-14b-instruct` does not exist.",
    "type": "NotFoundError"
  }
}
```

**Causa**: En vLLM, el nombre del modelo es el **path completo del GGUF**, no el nombre corto como en LM Studio.

**Solución**:
```python
# ❌ Incorrecto (funciona en LM Studio)
model = "qwen2.5-14b-instruct"

# ✅ Correcto (vLLM)
model = "/mnt/d/llmstudio-models/lmstudio-community/Qwen2.5-14B-Instruct-Q5_K_L/Qwen2.5-14B-Instruct-Q5_K_L.gguf"
```

---

### 4.2 `Failed to load model "X"`

```json
{
  "error": {
    "message": "Failed to load model. Error: ..."
  }
}
```

**Causa**: El modelo no se pudo cargar en VRAM.

**Soluciones**:
- Verificar que el path del GGUF es correcto
- Reducir `--gpu-memory-utilization`
- Reducir `--max-model-len`
- Verificar que no hay otro proceso usando GPU

---

## 5. Errores de PowerShell (cuando se ejecuta desde Windows)

### 5.1 PowerShell expande `$PATH` y `$LD_LIBRARY_PATH` dentro de strings

```powershell
# ❌ PowerShell expanda $PATH y $LD_LIBRARY_PATH antes de pasar a WSL
wsl -e bash -c "export PATH=/usr/local/cuda-12.8/bin:$PATH && vllm serve ..."
# Resultado: PATH se expande con el valor de Windows, rompiendo el script
# El PATH de Linux (/usr/bin, /bin, etc.) desaparece → 'head', 'cat', 'curl' no found
```

**Causa**: PowerShell interpreta `$PATH` como variable de PowerShell y la expande antes de pasar el string a WSL.

**Solución**: SIEMPRE usar scripts en archivo, nunca strings inline con variables `$`:

```powershell
# ✅ Correcto: escribir script a archivo, copiar a WSL2, ejecutar
Set-Content -Path "C:\...\vllm_run.sh" -Value @'
#!/bin/bash
export PATH="/usr/local/cuda-12.8/bin:/root/vllm-env/bin:$PATH"
export LD_LIBRARY_PATH="/usr/local/cuda-12.8/lib64:${LD_LIBRARY_PATH:-}"
...
'@
wsl -e bash -c "cp /mnt/c/.../vllm_run.sh /tmp/ && bash /tmp/vllm_run.sh"
```

**Nota**: Las comillas dobles en el script (`"$PATH"`) protegen la expansión DENTRO de bash, pero no protegen contra la expansión de PowerShell ANTES de llegar a bash.

---

### 5.2 `Select-Object : No se encuentra ningún parámetro de posición`

```
Select-Object : No se encuentra ningún parámetro de posición que acepte el argumento 'information_schema.tables'
```

**Causa**: PowerShell interpreta `SELECT` como cmdlet de PowerShell, no como SQL.

**Solución**: Usar scripts Python en archivo, no inline:
```powershell
# ❌ Incorrecto
python -c "cur.execute('SELECT EXISTS (...)')"

# ✅ Correcto
Set-Content -Path "C:\...\check.py" -Value @'
import psycopg2
...
'@
python C:\...\check.py
```

---

### 5.3 `ScriptBlock solo se debe especificar como un valor del parámetro Command`

**Causa**: Cadena multiline demasiado completa para PowerShell.

**Solución**: Siempre usar `Set-Content` con here-string `@'...'@` para crear scripts, luego copiar a WSL2.

---

## 6. Errores de proceso/background en WSL2

### 6.1 `nohup` no funciona en WSL2

```bash
# Este comando parece funcionar pero el proceso se mata cuando WSL termina
nohup bash /tmp/vllm_run.sh > /tmp/vllm.log 2>&1 &
```

**Causa**: Cuando ejecutas `wsl -e bash -c "..."`, PowerShell espera a que termine y mata todos los procesos hijos.

**Solución**: Usar `tmux` en lugar de `nohup`:

```bash
# ✅ Correcto: usar tmux
tmux new-session -d -s vllm 'bash /tmp/vllm_run.sh'

# Para ver los logs
tmux attach -t vllm

# Para detach (dejar corriendo en background)
# Presionar: Ctrl+B, luego D

# Para verificar que está corriendo
tmux list-sessions
ps aux | grep vllm | grep -v grep
```

---

### 6.2 `setsid` no funciona en WSL2

```bash
# Este comando no funciona correctamente
setsid bash /tmp/vllm_run.sh > /tmp/vllm.log 2>&1 &
```

**Causa**: Similar a nohup, el proceso se mata cuando la sesión de WSL termina.

**Solución**: Usar `tmux` (ver 6.1).

---

### 6.3 `tmux` no inicia servidor

```bash
# Error
no server running on /tmp/tmux-0/default
```

**Causa**: El servidor tmux no se inició automáticamente.

**Solución**:
```bash
# Iniciar servidor manualmente
tmux start-server

# Luego crear sesión
tmux new-session -d -s vllm 'bash /tmp/vllm_run.sh'
```

---

## 7. Errores de WSL2

### 7.1 `WLS 2 requires a virtual machine feature`

**Solución**:
```powershell
# En PowerShell como Administrador
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
# Reiniciar PC
wsl --update
```

---

### 7.2 GPU no disponible en WSL2

```bash
nvidia-smi
# NVIDIA-SMI has failed because it couldn't communicate with the NVIDIA driver.
```

**Solución**: Actualizar driver de NVIDIA en Windows a versión 525+ (soporte WSL2 CUDA).

---

### 7.3 `pin_memory=False` warning

```
WARNING: Using 'pin_memory=False' as WSL is detected. This may slow down the performance.
```

**Causa**: WSL2 no soporta `pin_memory` para CUDA.

**Solución**: Ignorar. Es un warning, no un error. vLLM funciona correctamente sin pin_memory.

---

## 8. Parámetros de vLLM explicados

| Parámetro | Valor | Error si se omite |
|-----------|-------|-------------------|
| `--max-model-len 14000` | 14K tokens | OOM con 16384 (3.0 GiB KV cache > 2.67 disponibles) |
| `--gpu-memory-utilization 0.90` | 90% VRAM | 0.95 excede VRAM por overhead de display |
| `--enable-prefix-caching` | (flag) | Sin este flag, el system prompt se re-calcula en cada request |
| `--kv-cache-dtype fp8` | FP8 | Sin este flag, KV cache usa FP16 (2x más memoria) |
| `--host 0.0.0.0` | Todas las interfaces | Solo accesible desde WSL2, no desde Windows |
| `--tokenizer Qwen/...` | Nombre HuggingFace | Error de tokenizer si se pone path del GGUF |
| `--enforce-eager` | (flag) | DESACTIVADO - quitar para habilitar CUDA graphs |

---

## 9. Flujo de resolución de problemas

```
vLLM no funciona
    │
    ├── ¿nvcc existe?
    │   └── NO → sudo apt install cuda-toolkit-12-8 -y
    │
    ├── ¿gcc existe?
    │   └── NO → sudo apt install gcc g++ -y
    │
    ├── ¿python3-dev instalado?
    │   └── NO → sudo apt install python3-dev -y
    │
    ├── ¿ninja existe en venv?
    │   └── NO → pip install ninja
    │
    ├── ¿El modelo GGUF existe?
    │   └── NO → Verificar path en /mnt/d/...
    │
    ├── ¿vLLM está corriendo?
    │   └── NO → bash /tmp/vllm_run.sh (o tmux)
    │
    ├── ¿CUDA out of memory?
    │   └── --gpu-memory-utilization 0.85 --kv-cache-dtype fp8
    │
    ├── ¿localhost:8000 no responde desde Windows?
    │   └── Usar IP de WSL2: hostname -I
    │
    ├── ¿Modelo "does not exist"?
    │   └── Usar path completo del GGUF como model name
    │
    ├── ¿PowerShell expande $PATH?
    │   └── Usar scripts en archivo, no inline
    │
    └── ¿Proceso se muere al cerrar terminal?
        └── Usar tmux en lugar de nohup
```

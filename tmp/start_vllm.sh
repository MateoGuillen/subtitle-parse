#!/bin/bash
# ============================================================
# vLLM Startup Script — WSL2 Ubuntu
# ============================================================
# Ejecutar con: bash /tmp/start_vllm.sh
# O copiar a ~/.bashrc como alias: alias start-vllm='bash /tmp/start_vllm.sh'
# ============================================================

# --- CUDA Toolkit (requerido para compilar kernels) ---
export PATH=/usr/local/cuda-12.8/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/cuda-12.8/lib64:$LD_LIBRARY_PATH

# --- vLLM virtual environment ---
export PATH=/root/vllm-env/bin:$PATH

# --- Modelo GGUF ---
MODEL="/mnt/d/llmstudio-models/lmstudio-community/Qwen2.5-14B-Instruct-Q5_K_L/Qwen2.5-14B-Instruct-Q5_K_L.gguf"
TOKENIZER="Qwen/Qwen2.5-14B-Instruct"

# --- Verificaciones previas ---
echo "=== Verificaciones ==="

if ! command -v nvcc &> /dev/null; then
    echo "ERROR: nvcc no encontrado. CUDA toolkit no instalado."
    echo "Solución: sudo apt install cuda-toolkit-12-8 -y"
    exit 1
fi

if [ ! -f "$MODEL" ]; then
    echo "ERROR: Modelo no encontrado en $MODEL"
    exit 1
fi

if [ ! -d "/root/vllm-env" ]; then
    echo "ERROR: Entorno virtual no encontrado en /root/vllm-env"
    echo "Solución: python3 -m venv /root/vllm-env && source /root/vllm-env/bin/activate && pip install vllm vllm-gguf-plugin ninja"
    exit 1
fi

echo "OK: CUDA $(nvcc --version | grep release | awk '{print $6}')"
echo "OK: Modelo $(ls -lh "$MODEL" | awk '{print $5}')"
echo "OK: vLLM $(python -c 'import vllm; print(vllm.__version__)' 2>/dev/null || echo 'no verificado')"
echo ""

# --- Iniciar vLLM (con optimizaciones) ---
echo "=== Iniciando vLLM (optimizado) ==="
echo "Optimizaciones: CUDA graphs + prefix caching + fp8 KV cache + multi-step scheduling"
exec /root/vllm-env/bin/vllm serve "$MODEL" \
    --tokenizer "$TOKENIZER" \
    --host 0.0.0.0 \
    --port 8000 \
    --max-model-len 14000 \
    --gpu-memory-utilization 0.90 \
    --enable-prefix-caching \
    --kv-cache-dtype fp8 \
    --num-scheduler-steps 4

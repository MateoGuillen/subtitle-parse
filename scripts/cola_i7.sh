#!/usr/bin/env bash
# I7: corre los 4 modelos de I6 sobre la planilla de I7 (GPU única: uno por vez).
# Uso: bash scripts/cola_i7.sh   (logs en /root/backups/i7_<modelo>.log); luego scripts/evaluar_i7.py
cd "$(dirname "$0")/.." || exit 1
for m in gemma-4-12b-qat-w4a16 qwen3-14b-awq qwen3-8b-fp8 gemma-4-26b-a4b-gguf; do
  timeout 3600 /root/vllm-env/bin/python -u scripts/benchmark_i6.py --model "$m" --i7 > "/root/backups/i7_$m.log" 2>&1
  echo "$m exit=$?" | tee -a "/root/backups/i7_$m.log"
  pkill -x llama-server 2>/dev/null; sleep 10
done

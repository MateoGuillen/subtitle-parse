#!/usr/bin/env bash
# I6: corre el benchmark reducido modelo por modelo (GPU única: uno por vez) y al final el resumen.
# Uso: bash scripts/cola_i6.sh   (logs en /root/backups/i6_<modelo>.log)
cd "$(dirname "$0")/.." || exit 1
for m in gemma-4-12b-qat-w4a16 qwen3-14b-awq qwen3-8b-fp8 gemma-4-26b-a4b-gguf; do
  timeout 7200 /root/vllm-env/bin/python -u scripts/benchmark_i6.py --model "$m" > "/root/backups/i6_$m.log" 2>&1
  echo "$m exit=$?" | tee -a "/root/backups/i6_$m.log"
  pkill -x llama-server 2>/dev/null; sleep 10
done
/root/vllm-env/bin/python scripts/benchmark_i6.py --resumen > /root/backups/i6_resumen.log 2>&1
echo "resumen exit=$?"

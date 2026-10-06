#!/usr/bin/env bash
# Corre --set pseudo (pseudo-GT Opus 5.5, 341 ítems no ambiguos) para todos los modelos locales.
# Espera a que termine la ronda 2 (marcador: exit= en el log de Qwen3.5-9B GGUF).
cd "$(dirname "$0")/.." || exit 1
until grep -q "exit=" /root/backups/retest_qwen3.5-9b-gguf.log 2>/dev/null; do sleep 30; done
MODELS="qwen3-8b-fp8 llama-fp8 exaone-3.5-7.8b-awq qwen3-14b-awq gemma-4-12b-qat-w4a16 \
gemma-4-12b-gguf ministral-3-14b-gguf qwen3.8-27b-gguf qwen3.5-9b-gguf ministral-3-8b-gguf \
ling-3.0-tiny-gguf mimo-v2.6-9b-gguf clef-flash-gguf gemma-4-26b-a4b-gguf qwen3.6-35b-a3b-gguf"
for m in $MODELS; do
  timeout 3600 /root/vllm-env/bin/python -u scripts/ablacion_gt.py --model "$m" --set pseudo \
    > "/root/backups/pseudo_$m.log" 2>&1
  echo "$m exit=$?" | tee -a "/root/backups/pseudo_$m.log"
  pkill -x llama-server 2>/dev/null; sleep 5
done
echo "COLA PSEUDO TERMINADA"

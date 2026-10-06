#!/usr/bin/env bash
# Cola de retest: testea cada modelo apenas su descarga termina (marcadores de descargar_candidatos.py).
# Uso: bash scripts/cola_retest.sh modelo1 modelo2 ...
cd "$(dirname "$0")/.." || exit 1
for m in "$@"; do
  until [ -f "/root/models/gguf/.ok_$m" ] || [ -f "/root/models/gguf/.fail_$m" ]; do sleep 20; done
  if [ -f "/root/models/gguf/.fail_$m" ]; then echo "$m DESCARGA FALLÓ" | tee "/root/backups/retest_$m.log"; continue; fi
  timeout 7200 /root/vllm-env/bin/python -u scripts/ablacion_gt.py --model "$m" > "/root/backups/retest_$m.log" 2>&1
  echo "$m exit=$?" | tee -a "/root/backups/retest_$m.log"
  pkill -x llama-server 2>/dev/null; sleep 5   # -x: nombre exacto (no matchea esta cola)
done

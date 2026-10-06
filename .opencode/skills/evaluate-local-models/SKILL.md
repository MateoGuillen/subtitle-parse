---
name: evaluate-local-models
description: Benchmark LLMs for structured extraction using local vLLM on RTX 4070 Ti SUPER 16GB. Covers Qwen 14B/7B, gpt-oss-20b GGUF models with CUDA graph configs, VRAM cleanup, and quality/throughput evaluation via pipeline 2 extraction test.
---

# evaluate-local-models

Benchmark LLMs for structured extraction (Pipeline 2) using local vLLM + RTX 4070 Ti SUPER 16GB.

## Inventory — GGUF Models

| Model | Size | VRAM est. | Path |
|-------|------|-----------|------|
| **Qwen 2.5 14B Q5_K_L** ✅ | 10.2 GB | ~15.7 GB | `lmstudio-community/.../Qwen2.5-14B-Instruct-Q5_K_L.gguf` |
| Qwen 2.5 14B Q4_K_S ⚠️ | 8.0 GB | ~12 GB | `bartowski/Qwen2.5-14B-Instruct-GGUF/Qwen2.5-14B-Instruct-Q4_K_S.gguf` |
| Qwen 2.5 7B Q4_K_M ❌ | 4.4 GB | ~7 GB | `bartowski/Qwen2.5-7B-Instruct-GGUF/Qwen2.5-7B-Instruct-Q4_K_M.gguf` |
| gpt-oss-20b-MXFP4 ❌ | 11.3 GB | N/A | `lmstudio-community/gpt-oss-20b-GGUF/gpt-oss-20b-MXFP4.gguf` |
| Qwen 2.5 Coder 14B Q4_K_M | 8.4 GB | ~12 GB | `lmstudio-community/.../Qwen2.5-Coder-14B-Instruct-Q4_K_M.gguf` |
| Qwen 2.5 Coder 14B Q6_K | 11.3 GB | ~16 GB | `Lucy-in-the-Sky/.../qwen2.5-coder-14b-instruct-q6_k.gguf` |

## vLLM Config Templates

### Safe config (no CUDA graphs, reliable)
```bash
VLLM_SCRIPT=/tmp/vllm_safe.sh
MODEL="<gguf_path>"
TOKENIZER="<hf_name>"

cat > "$VLLM_SCRIPT" << 'EOF'
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
/root/vllm-env/bin/vllm serve "$MODEL" \
    --tokenizer "$TOKENIZER" \
    --host 0.0.0.0 --port 8000 \
    --max-model-len 8192 \
    --gpu-memory-utilization 0.85 \
    --enforce-eager \
    --kv-cache-dtype fp8 \
    --enable-prefix-caching
EOF
```

### CUDA graphs config (speed, may hang during graph capture)
```bash
VLLM_SCRIPT=/tmp/vllm_optimized.sh
MODEL="<gguf_path>"
TOKENIZER="<hf_name>"

cat > "$VLLM_SCRIPT" << 'EOF'
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
/root/vllm-env/bin/vllm serve "$MODEL" \
    --tokenizer "$TOKENIZER" \
    --host 0.0.0.0 --port 8000 \
    --max-model-len 14000 \
    --gpu-memory-utilization 0.90 \
    --kv-cache-dtype fp8 \
    --enable-prefix-caching
EOF
```

### Low VRAM config (for Q4_K_S or tight memory)
```bash
VLLM_SCRIPT=/tmp/vllm_q4.sh
MODEL="<gguf_path>"
TOKENIZER="<hf_name>"

cat > "$VLLM_SCRIPT" << 'EOF'
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
/root/vllm-env/bin/vllm serve "$MODEL" \
    --tokenizer "$TOKENIZER" \
    --host 0.0.0.0 --port 8000 \
    --max-model-len 8192 \
    --gpu-memory-utilization 0.80 \
    --enforce-eager \
    --kv-cache-dtype fp8
EOF
```

### Aggressive VRAM cleanup (when Windows processes eat VRAM)
```powershell
# Kill these processes from PowerShell (admin)
taskkill /F /IM "NVIDIA Overlay.exe" /T
taskkill /F /IM "Warp.exe" /T
taskkill /F /IM "msedge.exe" /T
taskkill /F /IM "SearchApp.exe" /T
# Can free 1.3 GB -> 15.6 GB
```

## Known Issues

| Issue | Symptom | Workaround |
|-------|---------|------------|
| **CUDA graph hang** | Stuck at `Capturing CUDA graphs (decode, FULL): 0/35` or `43%` | Kill vLLM + retry. Intermittent — usually works 2nd attempt |
| **gpt-oss MXFP4** | GGUF reader crash parsing MXFP4 metadata | Not compatible with vLLM. MXFP4 quant not supported |
| **Q4_K_S crash** | vLLM dies silently after `non-default args` | Probable incompat with vLLM 0.23.0. Try `--enforce-eager` |
| **WSL `localhost`** | `Connection refused` on `localhost:8000` | Use `127.0.0.1:8000` instead |
| **VLLM_USE_V1=0** | Process exits immediately | v0.23.0 doesn't support V0 engine |
| **OOM on startup** | `CUDA out of memory` | Kill Windows GPU processes (see above) + set `expandable_segments:True` |

## Benchmark Workflow

### 1. Start vLLM (via tmux)
```bash
# Kill previous + check VRAM
pkill -f vllm 2>/dev/null
sleep 5
nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits

# Write config script (use template above)
# Adjust MODEL and TOKENIZER paths for target model
bash "$VLLM_SCRIPT"

# OR use tmux for persistence
tmux new-session -d -s vllm "bash $VLLM_SCRIPT"
```

### 2. Wait for ready (poll API)
```bash
# Check every 10s
curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8000/v1/models
```

### 3. Run evaluation
```bash
source /root/vllm-env/bin/activate
cd /mnt/d/projects/subtitle-parse

# Quick check (10 samples per title)
python scripts/evaluate_extraction.py --provider vllm --samples-per-title 10 --skip-sample

# Full eval (20 samples per title)
python scripts/evaluate_extraction.py --provider vllm --samples-per-title 20 --skip-sample
```

### 4. Collect metrics
- Report at `eval/comparison_report.md`
- Key metrics: `OK/N`, `F1`, `MAE`, `total_time`
- For throughput: check avg `response_time` per sample

### 5. Shutdown
```bash
tmux send-keys -t vllm C-c
sleep 3
tmux kill-session -t vllm 2>/dev/null
pkill -f vllm 2>/dev/null
```

## Expected Results (RTX 4070 Ti SUPER 16GB)

| Model | Quality | s/sample | 315K sections | Status |
|-------|---------|----------|---------------|--------|
| Qwen 14B Q5_K_L | 97.5-100% | ~7s | ~25 days seq | **Current best** |
| gpt-oss-20b-MXFP4 | ? | ~8s est. | ~29 days | Untested |
| Qwen 7B Q4_K_M | 0% | ~3s est. | ~11 days | Too weak |
| Qwen 14B Q4_K_S | ? | ~5s est. | ~18 days | Crashed in vLLM |
| gpt-oss-20b-MXFP4 | N/A | N/A | N/A | GGUF reader incompatible |

## Key Scripts & Files

- `scripts/evaluate_extraction.py` — benchmark runner (`--provider vllm`)
- `config/settings.py` — DB config, API keys
- `src/etl/transformers/llm_provider.py` — `LocalLLMProvider` class
- `docs/wsl2_vllm_setup.md` — full vLLM install guide
- `eval/comparison_report.md` — latest benchmark results
- `.env` — API keys

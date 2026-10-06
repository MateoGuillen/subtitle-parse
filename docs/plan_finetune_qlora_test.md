# Plan: Prueba de Fine-Tuning QLoRA (2 campos peores)

> **Estado:** Pendiente de implementación (aprobado como prueba de concepto).
> **Objetivo:** Determinar si fine-tuning de `meta-llama/Llama-3.1-8B-Instruct` mejora la extracción de campos por encima del baseline de 95% (100 muestras GT) y de los campos débiles (25-30%).
> **Fecha de creación:** 2026-07-31

## Contexto validado

- **GPU**: NVIDIA RTX 4070 Ti SUPER, 16 GB VRAM (suficiente para QLoRA 4-bit de un 8B, ~6-8 GB)
- **Entorno WSL**: `/root/vllm-env/bin/python` — tiene torch 2.11+cu130, transformers 5.12. **FALTA** `peft`, `bitsandbytes`, `trl`, `accelerate`, `datasets`
- **Base `dncp`** (levanta el container podman):
  - `pliegos_secciones_2021..2025`: 2.77M secciones con `content_text` + metadata
  - `document_features_v2`: 31,321 filas × 136 cols; 26 campos hybrid con `_conf`
  - Labels con conf≥0.9 disponibles:
    - `dias_pago_anticipo`: 2,023 positivos (10:1043, 5:378, 3:349, 15:96, 7:76, ...)
    - `dias_solicitud_anticipo`: 1,522 positivos (10:1208, 5:120, 3:112, ...)
- **GT de evaluación**: `data/processed/llm_benchmark/ground_truth_5_campos.csv` (100 muestras, 5 campos × 20). 83/100 son NULL. NO usar para entrenar (evitar data leakage).
- **Baseline**: Llama 3.1 8B-AWQ = 95%. Campos débiles: `dias_pago_anticipo` (25%), `dias_solicitud_anticipo` (30%).

## Paso 1 — Entorno (~45 min)

```bash
# En WSL, con el env de vLLM (NUNCA crear venv nuevo)
/root/vllm-env/bin/pip install peft bitsandbytes trl accelerate datasets
```

Verificar que vLLM 0.23.0 sigue funcionando tras el install:
```bash
VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/benchmark_multi_vs_gt.py --model 0
# Debe dar 95% (sanity check del baseline)
```

## Paso 2 — Dataset de entrenamiento (~1.5 h)

Crear `scripts/build_finetune_dataset.py`:

- Generar pares `(prompt, respuesta)` con el **mismo** formato de `build_prompt()` de `scripts/benchmark_multi_vs_gt.py` (system + hint + texto → valor o NULL). El fine-tune optimiza exactamente lo que se evalúa.
- **Positivos**: filas `document_features_v2` con `{campo}_conf >= 0.9` y valor no-nulo. Balancear por valor (el 10 domina → muestrear para cubrir 3/5/10/15/30, etc.).
- **Negativos**: secciones donde el título existe pero el campo está ausente ("No Aplica" del GT) + muestras de `conf < 0.9` donde el regex falló.
- **Target**: ~500 ejemplos/campo balanceados.
- **Guardar** en `data/processed/finetune/`.
- **Excluir**: los 100 `nro_licitacion` del GT (data leakage).

## Paso 3 — Entrenamiento QLoRA (~30-45 min)

Crear `scripts/finetune_llama_8b.py`:

- Base: `meta-llama/Llama-3.1-8B-Instruct` (tokenizer local ya cacheado)
- `load_in_4bit` NF4 (bitsandbytes)
- `peft.LoraConfig(r=16, lora_alpha=32, target_modules=q/k/v/o/gate/up/down)`
- 1 epoch, lr 2e-4, cosine, batch efectivo ~8 (grad accumulation), gradient checkpointing
- Chat template con `add_generation_prompt=False` (aprende la respuesta como completion)
- Guardar adapter en `data/models/lora_anticipo/`

## Paso 4 — Merge + AWQ (~30-60 min)

1. Merge adapter → base FP16
2. Cuantizar con AutoAWQ → modelo AWQ para vLLM

## Paso 5 — Re-evaluar (~15 min)

1. Registrar el modelo fine-tuneado en `MODELS` de `scripts/benchmark_multi_vs_gt.py`
2. `VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/benchmark_multi_vs_gt.py --model <idx>`
3. Comparar:
   - Campos objetivo: `dias_pago_anticipo`, `dias_solicitud_anticipo` (baseline 25%/30%)
   - Total vs 95% (no debe degradar el resto)

## Criterio de éxito

- Mejora en los 2 campos objetivo **sin** degradar el resto
- Total > 95% = ganancia real → expandir a los 5 campos
- Total ≤ 95% = revertir (adapter descartable, cero riesgo)

## Tiempo total estimado

~4-5 horas efectivas. Fracaso barato (adapter descartable).

## Notas

- Los labels de entrenamiento vienen del regex/hybrid (débiles). Riesgo: heredar sus errores. Mitigar balanceando y usando GT solo para evaluación.
- Si el resultado es positivo, el plan se expande a los 5 campos del benchmark (o los 26 hybrid).
- Referencia del prompt de producción: `SYSTEM_PROMPT` + `build_prompt()` en `scripts/benchmark_multi_vs_gt.py` (líneas 42-100).

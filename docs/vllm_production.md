# Formatos de cuantización para vLLM en producción — Contexto de referencia

**Versión de vLLM de referencia:** v0.25.0 (última release estable, 11 jul 2026)
**Repo:** https://github.com/vllm-project/vllm
**Releases/changelog:** https://github.com/vllm-project/vllm/releases
**Índice general de cuantización en vLLM:** https://docs.vllm.ai/en/latest/features/quantization/
**Modelos soportados:** https://docs.vllm.ai/en/latest/models/supported_models.html

---

## 1. GGUF — por qué NO usarlo en producción con vLLM

**Qué es:** formato binario de un solo archivo creado por el proyecto llama.cpp, pensado para inferencia en CPU/GPU híbrida en hardware de consumo.

**Por qué evitarlo en vLLM producción:**
- El soporte fue marcado oficialmente como "highly experimental and under-optimized" por el propio equipo de vLLM.
- El soporte *in-tree* fue deprecado y movido a un plugin externo (out-of-tree), justificado por bajo uso (~0.1% de las cargas) frente a alto coste de mantenimiento (~6.000 líneas de kernels CUDA dedicados).
- Solo carga modelos GGUF de un único archivo (los multi-archivo requieren fusión previa con `gguf-split`).
- Puede ser incompatible con otras features de vLLM (batching avanzado, ciertos kernels optimizados, tensor parallelism completo).
- Es un formato de cuantización estática pensado para llama.cpp/Ollama, no para servir con alto throughput concurrente.

**Enlaces oficiales:**
- Doc GGUF en vLLM (versión estable): https://docs.vllm.ai/en/stable/features/quantization/gguf/
- Doc GGUF (histórico v0.8.4, mismo aviso de "experimental"): https://docs.vllm.ai/en/v0.8.4/features/quantization/gguf.html
- Plugin OOT tras la deprecación in-tree: https://github.com/vllm-project/vllm-gguf-plugin
- RFC de deprecación de GGUF y bitsandbytes (razones técnicas detalladas, líneas de código, mantenimiento): https://github.com/vllm-project/vllm/issues/39583
- Especificación del formato GGUF (contexto general, no específico de vLLM): https://en.wikipedia.org/wiki/GGUF

---

## 2. AWQ (Activation-aware Weight Quantization) — recomendado para producción

**Qué es:** cuantización W4A16 (pesos en 4 bits, activaciones en 16 bits) que usa un pequeño dataset de calibración para preservar los pesos más "salientes" y minimizar pérdida de precisión.

**Por qué usarlo en vLLM producción:**
- Kernels nativos optimizados dentro de vLLM (no un plugin externo).
- Buen balance memoria/calidad: reduce ~4x el tamaño de los pesos con degradación mínima.
- Amplia disponibilidad de checkpoints ya cuantizados en Hugging Face (no siempre hace falta cuantizar uno mismo).
- Soportado en múltiples arquitecturas de GPU (Ampere, Ada Lovelace, Hopper).

**Enlaces oficiales:**
- Doc API AWQ (última versión dev): https://docs.vllm.ai/en/latest/api/vllm/model_executor/layers/quantization/awq.html
- Doc AutoAWQ (guía de uso, ejemplo histórico v0.6.1): https://docs.vllm.ai/en/v0.6.1/quantization/auto_awq.html
- Guía AWQ con LLM Compressor (herramienta oficial para generar tus propios checkpoints AWQ): https://docs.vllm.ai/projects/llm-compressor/en/latest/examples/awq/
- Paper de referencia (AWQ original): https://arxiv.org/abs/2306.00978

---

## 3. GPTQ — alternativa a AWQ

**Qué es:** cuantización post-entrenamiento (PTQ) en 4/8 bits basada en aproximación de segundo orden del error de cuantización, capa por capa.

**Por qué usarlo en vLLM producción:**
- Igual que AWQ, tiene kernels nativos en vLLM (no experimental).
- Ecosistema maduro: soportado también en Hugging Face Transformers y text-generation-webui, con muchos checkpoints comunitarios ya publicados.
- Suele ser la opción por defecto cuando no hay checkpoint AWQ disponible para un modelo específico.

**Enlaces oficiales:**
- Guía general de cuantización (incluye tabla de compatibilidad por hardware para GPTQ): https://docs.vllm.ai/en/latest/features/quantization/
- LLM Compressor, soporte de algoritmos incluyendo GPTQ: https://docs.vllm.ai/en/latest/features/quantization/llm_compressor/

---

## 4. FP8 (W8A8 / W8A16) — máxima velocidad si el hardware lo soporta

**Qué es:** cuantización a punto flotante de 8 bits (formato E4M3), con escalado dinámico o estático por tensor.

**Por qué usarlo en vLLM producción:**
- Da hasta ~2x reducción de memoria y hasta ~1.6x mejora de throughput con impacto mínimo en precisión.
- **Soporte oficial de hardware:** GPUs Ada Lovelace (arquitectura SM 8.9, la misma familia que la RTX 4070 Ti Super) y Hopper están oficialmente soportadas para W8A8. GPUs Ampere/Turing solo soportan W8A16 (peso en FP8, activación en 16 bits) vía kernels Marlin.
- Es el formato que vLLM recomienda cuando el hardware lo permite, antes que AWQ/GPTQ, si la calidad es prioridad y el hardware es Ada/Hopper o superior.

**Enlaces oficiales:**
- Doc FP8 W8A8 (versión estable, con tabla de hardware soportado): https://docs.vllm.ai/en/stable/features/quantization/llm_compressor/fp8/
- Doc FP8 (histórico v0.5.4, mismo contenido técnico): https://docs.vllm.ai/en/v0.5.4/quantization/fp8.html
- Herramienta AutoFP8 para cuantizar offline: https://github.com/neuralmagic/AutoFP8

---

## 5. compressed-tensors — formato unificado (recomendado como "contenedor" general)

**Qué es:** extensión del formato safetensors que unifica en un solo esquema de checkpoint distintos métodos de compresión (GPTQ, AWQ, SmoothQuant, FP8, INT8, sparsity 2:4, etc.), evitando que cada técnica tenga su propio formato de archivo.

**Por qué usarlo en vLLM producción:**
- Es el formato de salida estándar de LLM Compressor, la librería oficial del proyecto vLLM para producir checkpoints cuantizados.
- Evita fragmentación: un mismo pipeline de carga en vLLM soporta múltiples esquemas de cuantización sin lógica especial por formato.
- Es la dirección en la que vLLM está consolidando su soporte de cuantización a futuro (a diferencia de GGUF/bitsandbytes, que van en dirección contraria).

**Enlaces oficiales:**
- Repo compressed-tensors: https://github.com/vllm-project/compressed-tensors
- Doc API compressed-tensors en vLLM: https://docs.vllm.ai/en/latest/api/vllm/model_executor/layers/quantization/compressed_tensors/compressed_tensors/
- Repo LLM Compressor (librería para generar estos checkpoints): https://github.com/vllm-project/llm-compressor
- Doc LLM Compressor en vLLM: https://docs.vllm.ai/en/latest/features/quantization/llm_compressor/

---

## 6. bitsandbytes — evitar en producción (mismo motivo que GGUF)

**Qué es:** cuantización NF4/INT8 popularizada por Hugging Face Transformers, usada también en QLoRA.

**Por qué evitarlo en vLLM producción:**
- Deprecado en el mismo RFC que GGUF por bajo uso (~0.5%) y alto coste de mantenimiento.
- No soporta tensor parallelism (error duro documentado en el propio RFC).
- Pensado principalmente para fine-tuning (QLoRA), no para inferencia de alto throughput.

**Enlace oficial:**
- Mismo RFC de deprecación que GGUF: https://github.com/vllm-project/vllm/issues/39583

---

## Resumen de la lógica de decisión

| Formato | Estado en vLLM | Uso recomendado |
|---|---|---|
| GGUF | Experimental, plugin OOT, deprecado in-tree | Evitar en producción; válido solo para pruebas rápidas o CPU-offload |
| bitsandbytes | Deprecado, sin tensor parallelism | Evitar en producción; solo fine-tuning (QLoRA) |
| AWQ | Nativo, maduro | Buena opción por defecto si no hay FP8 soportado por hardware |
| GPTQ | Nativo, maduro | Alternativa a AWQ, mayor disponibilidad de checkpoints comunitarios |
| FP8 (W8A8) | Nativo, acelerado por hardware en Ada/Hopper | Preferido en GPUs Ada Lovelace/Hopper si se prioriza calidad + velocidad |
| compressed-tensors | Nativo, formato unificado recomendado | Contenedor preferido a futuro, generado vía LLM Compressor |

**Fuente de la tabla de compatibilidad hardware↔cuantización:** https://docs.vllm.ai/en/latest/features/quantization/
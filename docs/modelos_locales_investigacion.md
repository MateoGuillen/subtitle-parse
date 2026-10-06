Voy a investigar el estado actual de modelos y de vLLM para darte recomendaciones precisas (tu lista de candidatos es de modelos de 2024, y en julio 2026 hay opciones más nuevas que probablemente rindan mejor).

Sintetizó investigación técnica y priorizó recomendaciones de modelos
Sintetizó investigación técnica y priorizó recomendaciones de modelos

Antes de sumar modelos a la lista, hay algo que probablemente tenga más impacto que cualquier modelo nuevo: 7s/sample secuencial es vLLM corriendo en el peor modo posible para su arquitectura. El motor está construido alrededor de continuous batching, y el propio ecosistema de vLLM reporta mejoras de hasta 23x en throughput frente a batching estático o secuencial simplemente por dejar que el scheduler intercale requests en vuelo, según un análisis ampliamente citado sobre cómo el continuous batching habilita mejoras de throughput mientras reduce la latencia p50. Voy a separar la respuesta en: (1) qué cambiar en el pipeline antes de tocar el modelo, (2) modelos candidatos actualizados a julio 2026 —tu lista es de mediados de 2024, y hubo generaciones nuevas—, y (3) cómo descargarlos.
vLLM

1. El pipeline es probablemente el cuello de botella, no el modelo

Batching real. Como tenés las 315K secciones completas de antemano, esto no es un servicio interactivo — es un batch job. El camino más simple no es levantar vllm serve y pegarle con requests secuenciales (ni siquiera con asyncio): es usar la API de inferencia offline de vLLM, LLM(...).generate(lista_de_prompts, sampling_params). Le pasás la lista completa y vLLM arma el paged attention, el scheduling y el batching internamente, agregando los prompts a la cola del motor y ejecutándolo para generar las salidas con alto throughput, devolviendo una lista de objetos RequestOutput — sin overhead de HTTP ni concurrencia manual.
vLLM

Salida restringida (guided JSON). Si evaluate_extraction.py todavía depende de que el modelo devuelva JSON válido "por las buenas", estás perdiendo velocidad y fiabilidad a la vez. vLLM soporta guided decoding vía xgrammar: le pasás tu schema de 6-8 campos como GuidedDecodingParams(json=schema) y el motor solo puede emitir tokens válidos según ese schema, lo que en la práctica de vLLM e integraciones similares reporta mejoras de hasta ~5x en throughput por token de salida bajo carga, además de eliminar los parseos fallidos que probablemente están inflando tu MAE/F1.
arxiv

--max-model-len sobredimensionado. 8192 es casi seguro mucho más de lo que necesitás — una sección de pliego más las instrucciones del schema raramente supera 1-2K tokens de entrada, y la salida son 6-8 campos. Cada token de contexto no usado es KV cache reservado y no disponible para correr más secuencias en paralelo. Medí tu p99 real y bajalo (2048 es un punto de partida razonable) — eso libera GB que se traducen directo en más concurrencia.

Ejemplo combinando los tres puntos, adaptado a tu config actual:

python
from vllm import LLM, SamplingParams
from vllm.sampling_params import GuidedDecodingParams
import json

schema = { "type": "object", "properties": { ... }, "required": [...] } # tus 6-8 campos

llm = LLM(
model="./models/qwen3-14b/Qwen3-14B-Q5_K_M.gguf",
tokenizer="Qwen/Qwen3-14B",
max_model_len=2048, # medí tu p99 real antes de fijar esto
gpu_memory_utilization=0.85,
kv_cache_dtype="fp8",
enforce_eager=True, # probá sin esto también, por si el hang no aparece offline
)

sampling = SamplingParams(
temperature=0,
max_tokens=150,
guided_decoding=GuidedDecodingParams(json=schema),
)

prompts = [build_prompt(seccion) for seccion in secciones] # las 315K, o chunks de 20-50K
outputs = llm.generate(prompts, sampling) # batching interno, sin asyncio

for seccion, out in zip(secciones, outputs):
guardar(seccion.id, json.loads(out.outputs[0].text)) # a disco cada N, no solo al final

Con esto, y sin descargar nada nuevo, deberías ver una mejora de orden de magnitud sobre los 25 días — vale la pena medirlo antes de evaluar modelos, porque cambia el cálculo: con más VRAM libre por request, un modelo "peor" corriendo con alta concurrencia puede ganarle en throughput total a uno "mejor" corriendo casi en serie. Tip aparte dado el tamaño del job: chunkeá y guardá progreso incrementalmente (cada 10-20K secciones), no solo al final — un job de horas/días necesita ser reanudable.

2. Modelos candidatos (tu lista es de 2024 — esto es lo nuevo)
   Modelo Quant sugerido VRAM (peso) Margen libre\*
   Qwen3-14B Q5_K_M / Q6_K ~10.5GB / ~12.1GB, según las cuantizaciones GGUF listadas para el modelo (Q4_K_M 9GB, Q5_K_M 10.5GB, Q6_K 12.1GB, Q8_0 15.7GB)
   Hugging Face
   ~5GB / ~3.5GB
   Gemma 3 12B Q6_K / Q8_0 ~9.7GB / ~12.5GB, según las cuantizaciones GGUF publicadas (Q4_K_M 7.3GB, Q5_K_M 8.44GB, Q6_K 9.66GB, Q8_0 12.5GB)
   Hugging Face
   ~5.9GB / ~3.1GB
   Mistral Small 3.2 (24B) Q4_K_M ~13-14GB (confirmar) ~1.6-2.6GB
   Gemma 4 12B Unified — por confirmar —

\*margen libre = ~15.6GB menos el peso del modelo, para KV cache + concurrencia. Como referencia, tu Qwen2.5 14B Q5_K_L actual usa ~15.7GB — es decir, corre al límite, sin margen para batching.

Los dos que probaría primero

Qwen3-14B — sucesor directo de tu baseline: mismo tamaño denso (14.8B), licencia Apache 2.0, lanzado el 29 de abril de 2025, forma parte de la familia de modelos densos y MoE de la tercera generación de Qwen, publicada bajo licencia Apache 2.0, entrenado en un corpus multilingüe de 119 idiomas, con un pipeline de preentrenamiento de tres etapas enfocado en conocimiento general, razonamiento y contexto largo — contra el Qwen2.5 que ya conocés. Importante: Qwen3 tiene modo "thinking" activado por default (el modelo razona paso a paso antes de responder), que para extracción estructurada es puro overhead de latencia y tokens. Hay que desactivarlo explícitamente con chat_template_kwargs={"enable_thinking": False} en cada request, o --default-chat-template-kwargs '{"enable_thinking": false}' a nivel server, ya sea pasando el parámetro en el cuerpo de la petición o como flag por defecto del servidor. Sin desactivarlo, vas a medir mal tanto velocidad como formato de salida.
Openlaboratory + 3

Gemma 3 12B — más chico que tu Qwen 14B actual incluso en Q8_0 (~12.5GB, prácticamente sin pérdida frente a bf16), lo que te da más margen de batching que tu setup actual con calidad cercana a full precision. De Google DeepMind, maduro (marzo 2025), con soporte vLLM+GGUF bien asentado. Candidato fuerte específicamente para tu problema de throughput: si iguala a Qwen 14B en tu benchmark, ganás en las dos dimensiones a la vez.

Vale la pena probar, con reservas

Mistral Small 3.2 (24B) — probablemente la mejor calidad bruta de la lista por ser más grande, pero en Q4_K_M deja casi nada de margen para KV cache/concurrencia — el mismo problema que ya tenés con Qwen2.5 14B Q5_K_L. Nota: Mistral movió su línea "Small" a una arquitectura MoE de 119B totales / 6.5B activos en la versión 4, un modelo Mixture-of-Experts que activa solo 6.5B parámetros por token de sus 119B totales, que no entra en 16GB de ninguna forma — la 3.2 (24B denso) sigue siendo la última versión densa relevante para tu hardware.
Hugging Face

Gemma 4 12B Unified — salió el 3 de junio de 2026, un modelo multimodal denso con 11.95B parámetros, 48 capas, ventana de contexto de 256K y soporte de 35+ idiomas listos para usar sobre un preentrenamiento de más de 140 idiomas. Generacionalmente por delante de Gemma 3, pero por ser tan reciente el soporte del lector GGUF de vLLM para su arquitectura (atención con ventana deslizante, KV cache compartido entre capas) es un signo de interrogación. Yo lo probaría después de validar los dos de arriba, no como primera opción, dado que ya estás lidiando con incompatibilidades del plugin GGUF.
Analytics Vidhya

De tu lista original
Qwen 2.5 14B Q4_K_M — probalo rápido ya que tenés todo armado, pero baja prioridad: Qwen3-14B es una generación más nueva al mismo costo de VRAM.
Llama 3.1 8B — descartar. Meta no sacó nada denso nuevo entre 8B y 70B desde jul-2024; Llama 4 es MoE demasiado grande (Scout/Maverick); y el modelo nuevo de Meta, Muse Spark, es de pesos cerrados — no se puede correr localmente, a diferencia de los modelos Llama anteriores, no tiene pesos abiertos y no se puede correr localmente. Además 8B está cerca del 7B que ya descartaste por insuficiente.
The Decoder
Phi-3.5-mini — la familia Phi está optimizada mayormente para inglés/STEM; su cobertura de español históricamente es más débil que Qwen o Gemma. Riesgo alto para texto jurídico en español.
DeepSeek-Coder-V2-Lite 16B — MoE especializado en código (HumanEval, benchmarks de programación), no en extracción de texto administrativo. Sin motivo para esperar que rinda mejor que un generalista del mismo tamaño acá.
gpt-oss-20b — atractivo a primera vista (MoE, solo 3.6B activos de 20.9B totales, con 24 capas y 20.9B de parámetros totales, de los cuales 3.6B son activos por token, corre rápido), pero OpenAI lo entrenó mayormente en un dataset de texto en inglés, un dataset mayormente en inglés y solo texto, con foco en STEM, programación y conocimiento general — riesgo real para extracción jurídica en español. Tu error "MXFP4 no soportado" tiene sentido: MXFP4 es el formato nativo de cuantización del modelo, no un K-quant estándar de llama.cpp. Si igual querés probarlo, buscá un re-quant de la comunidad (unsloth/bartowski) en Q4_K_M o Q8_0, pero de última prioridad.
arxiv
OpenAI
Exaone 3.5 / Marco-o1 — Exaone está optimizado principalmente para coreano/inglés. Marco-o1 es un modelo de razonamiento tipo "pensar paso a paso" — mismo problema que el modo thinking de Qwen3, pero sin un switch fácil para apagarlo. No invertiría tiempo en ninguno de los dos. 3. Cómo descargarlos

El comando huggingface-cli está deprecado — el reemplazo oficial es hf, tras el anuncio de que la CLI de Hugging Face se renombró oficialmente de huggingface-cli a hf:
huggingface

bash
pip install -U huggingface_hub
hf version # confirmá que dice "hf", no el viejo huggingface-cli

# Qwen3-14B GGUF (unsloth, incluye Dynamic 2.0 quants)

hf download unsloth/Qwen3-14B-GGUF --include "_Q5_K_M_" --local-dir ./models/qwen3-14b
hf download unsloth/Qwen3-14B-GGUF --include "_Q6_K_" --local-dir ./models/qwen3-14b

# Gemma 3 12B GGUF (bartowski)

hf download bartowski/google_gemma-3-12b-it-GGUF --include "_Q8_0_" --local-dir ./models/gemma3-12b

# Mistral Small 3.2 24B GGUF (unsloth)

hf download unsloth/Mistral-Small-3.2-24B-Instruct-2506-GGUF --include "_Q4_K_M_" --local-dir ./models/mistral-small-3.2 4. Sobre tus bugs conocidos de vLLM+GGUF

Dos conexiones que vale la pena hacer:

El hang de CUDA graphs no es específico de tu setup. Es una clase de bug que aparece reportada una y otra vez en el repo de vLLM, en configuraciones completamente distintas entre sí (multi-GPU, ROCm, modelos MoE), incluyendo casos donde requests quedan colgados indefinidamente al capturar CUDA graphs con ciertos tamaños de batch configurados explícitamente. Tu --enforce-eager es el mitigante estándar, con el costo real de throughput en decode que ya conocés.
GitHub

Ambos bugs (el hang y Q4_K_S incompatible) probablemente comparten origen: el soporte de GGUF en vLLM es un plugin comunitario, no el camino nativo, y está documentado como más limitado que los formatos nativos (AWQ, GPTQ, FP8), ya que vLLM prefiere safetensors y su soporte de GGUF es más limitado en comparación. Por eso vale la pena correr, en paralelo a tus pruebas GGUF, la versión AWQ oficial de Qwen3-14B — Qwen/Qwen3-14B-AWQ, disponible como modelo cuantizado en formato AWQ de 4 bits corre por el camino nativo de vLLM sin pasar por el plugin gguf en absoluto, usando el mismo comando de siempre solo cambiando el nombre del modelo, ya que vLLM soporta directamente los modelos AWQ y FP8 preexistentes de Qwen3. Tamaño en VRAM comparable a un Q4 GGUF (~8-9GB) — es probable que tanto el hang como la incompatibilidad de quant desaparezcan solos.
Spheron + 2

Orden sugerido
Medí tu pipeline actual (Qwen 14B Q5_K_L) con LLM.generate() offline + guided JSON + max-model-len ajustado, antes de tocar el modelo — probablemente ya achique buena parte de los 25 días.
En paralelo, corré evaluate_extraction.py contra Qwen3-14B (Q5_K_M/Q6_K, con thinking desactivado) y Gemma 3 12B (Q6_K/Q8_0).
Si alguno iguala tu 97.5% con más margen de VRAM que tu Qwen2.5 actual, ganaste en calidad y throughput a la vez.
Qwen3-14B-AWQ como plan B si el plugin GGUF te sigue dando problemas.
Mistral Small 3.2 y Gemma 4 12B como stretch goals si los primeros no alcanzan el umbral.

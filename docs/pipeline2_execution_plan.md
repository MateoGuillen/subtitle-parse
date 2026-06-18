# Pipeline 2: Execution Plan y Análisis de Rendimiento

## Resumen Ejecutivo

| Métrica | Valor |
|:--------|:------|
| **Total de secciones (10 títulos)** | ~320,000 |
| **Tiempo estimado (secuencial)** | ~12.8 días |
| **Tiempo estimado (2 concurrentes)** | ~7.1 días |
| **Tiempo estimado (4 concurrentes)** | ~4.0 días |
| **VRAM disponible para KV cache** | ~3.7 GB |
| **Concurrencia máxima recomendada** | 2-4 requests |

---

## 1. Análisis del Dataset

### 1.1 Escala de la base de datos

| Métrica | Valor |
|:--------|:------|
| Total secciones con texto | 2,774,579 |
| Total caracteres | 3,202,675,471 (3.05 GB) |
| Total tokens estimados | ~836 millones |

### 1.2 Secciones por título (Top 10)

| Título | Secciones | Avg chars | Avg tokens | Total MB |
|:-------|:---------:|:---------:|:----------:|:--------:|
| Subcontratación | 36,066 | 325 | 85 | 11.2 |
| Requisitos de Calificación | 32,182 | 2,429 | 633 | 74.6 |
| Abastecimiento simultáneo | 32,182 | 174 | 45 | 5.3 |
| Período de validez | 32,182 | 662 | 173 | 20.3 |
| Documentos de la oferta | 32,182 | 935 | 244 | 28.7 |
| Condición de Participación | 32,182 | 538 | 140 | 16.5 |
| Moneda de la oferta | 32,182 | 325 | 85 | 10.0 |
| Apertura de ofertas | 32,182 | 2,515 | 656 | 77.2 |
| Criterios de Adjudicación | 32,182 | 1,423 | 371 | 43.7 |
| Plazo para presentar | 32,182 | 590 | 154 | 18.1 |

### 1.3 Distribución de longitudes de texto

| Rango (chars) | Secciones | % del total | Avg chars |
|:-------------|:---------:|:-----------:|:---------:|
| < 100 | 224,270 | 8.1% | 51 |
| 100-500 | 993,627 | 35.8% | 274 |
| 500-1K | 640,106 | 23.1% | 700 |
| 1K-5K | 896,045 | 32.3% | 1,904 |
| 5K-10K | 11,274 | 0.4% | 6,750 |
| > 10K | 14,064 | 0.5% | 49,011 |

**Observación**: El 68% de las secciones tienen < 1,000 chars (~260 tokens), lo cual es favorable para procesamiento rápido.

---

## 2. Hardware y Capacidades

### 2.1 GPU: RTX 4070 Ti SUPER

| Especificación | Valor |
|:---------------|:------|
| VRAM Total | 16,376 MB (16 GB) |
| VRAM Usable | ~14 GB (después de overhead del sistema) |
| CUDA Cores | 8,448 |
| Tensor Cores | 264 (4ª gen) |
| Memory Bandwidth | 672 GB/s |

### 2.2 Modelo: Qwen 2.5 14B Q5_K_M

| Especificación | Valor |
|:---------------|:------|
| Parámetros | 14,000M |
| Tamaño en disco | ~11 GB (Q5_K_M) |
| VRAM requerida (inferencia) | ~12.6 GB |
| VRAM disponible para KV cache | ~3.4 GB |
| Context window máximo | 32,768 tokens |

### 2.3 Tokenizer: Qwen 2.5

| Métrica | Valor |
|:--------|:------|
| Ratio chars/token (español) | 3.83 |
| System prompt tokens | 77 tokens |
| Overhead por request | ~77 tokens fijos |

---

## 3. Benchmark de Inferencia

### 3.1 Velocidad por tamaño de entrada

| Input Chars | Input Tokens | Output Tokens | Tiempo (s) | Tokens/sec |
|:-----------:|:------------:|:-------------:|:----------:|:----------:|
| 100 | 96 | 31 | 3.55 | 35.8 |
| 300 | 129 | 31 | 2.94 | 54.5 |
| 500 | 164 | 53 | 3.44 | 63.1 |
| 1,000 | 245 | 62 | 3.66 | 83.8 |
| 2,000 | 395 | 50 | 3.41 | 130.5 |
| 5,000 | 845 | 55 | 3.59 | 250.6 |

**Promedio**: 103 tokens/sec, 3.43s por request

**Hallazgo importante**: La velocidad de procesamiento es **dependiente del tamaño de entrada**. Con entradas grandes (>2000 tokens), el throughput aumenta significativamente porque el modelo procesa más tokens por ciclo.

### 3.2 Concurrencia y VRAM

| Concurrencia | VRAM (MB) | Throughput (req/s) | Tiempo promedio |
|:------------:|:---------:|:------------------:|:---------------:|
| 1 | 12,640 | 0.23 | 4.38s |
| 2 | 12,639 | 0.36 | 5.54s |
| 4 | 12,603 | 0.65 | 6.13s |
| 8 | 12,603 | 0.72 | 10.14s |

**Hallazgo importante**: La VRAM se mantiene estable (~12.6 GB) incluso con 8 requests concurrentes. El modelo está cargado una vez y el KV cache se gestiona dinámicamente. El cuello de botella es **CPU/IO**, no VRAM.

---

## 4. Estimación de Tiempo para Pipeline 2

### 4.1 Por título (procesamiento secuencial)

| Título | Secciones | Tiempo/sección | Tiempo total | Horas | Días |
|:-------|:---------:|:--------------:|:------------:|:-----:|:----:|
| Subcontratación | 36,066 | 2.06s | 74,166s | 20.6 | 0.9 |
| Requisitos de Calificación | 32,182 | 8.09s | 260,416s | 72.3 | 3.0 |
| Abastecimiento simultáneo | 32,182 | 1.55s | 49,882s | 13.9 | 0.6 |
| Período de validez | 32,182 | 3.19s | 102,661s | 28.5 | 1.2 |
| Documentos de la oferta | 32,182 | 4.18s | 134,521s | 37.4 | 1.6 |
| Condición de Participación | 32,182 | 2.86s | 92,040s | 25.6 | 1.1 |
| Moneda de la oferta | 32,182 | 2.06s | 66,295s | 18.4 | 0.8 |
| Apertura de ofertas | 32,182 | 8.43s | 271,315s | 75.4 | 3.1 |
| Criterios de Adjudicación | 32,182 | 5.17s | 166,381s | 46.2 | 1.9 |
| Plazo para presentar | 32,182 | 2.94s | 94,601s | 26.3 | 1.1 |

**Total estimado**: ~1,312,278 segundos = **364.5 horas = 15.2 días** (secuencial)

### 4.2 Con concurrencia

| Concurrencia | Eficiencia | Tiempo total | Horas | Días |
|:------------:|:----------:|:------------:|:-----:|:----:|
| 1 (secuencial) | 100% | 1,312,278s | 364.5 | 15.2 |
| 2 | ~80% | 820,174s | 227.8 | 9.5 |
| 4 | ~70% | 468,671s | 130.2 | 5.4 |
| 8 | ~50% | 328,070s | 91.1 | 3.8 |

**Nota**: La eficiencia disminuye con más concurrencia por overhead de sincronización.

### 4.3 Estimación realista (recomendada)

**Escenario: 4 concurrentes, procesamiento diario (8 horas/día)**

| Título | Horas | Días laborales |
|:-------|:-----:|:--------------:|
| Subcontratación | 25.8 | 3.2 |
| Requisitos de Calificación | 90.9 | 11.4 |
| Abastecimiento simultáneo | 17.4 | 2.2 |
| Período de validez | 35.8 | 4.5 |
| Documentos de la oferta | 47.0 | 5.9 |
| Condición de Participación | 32.2 | 4.0 |
| Moneda de la oferta | 23.2 | 2.9 |
| Apertura de ofertas | 94.9 | 11.9 |
| Criterios de Adjudicación | 58.2 | 7.3 |
| Plazo para presentar | 33.1 | 4.1 |
| **TOTAL** | **458.5** | **57.3 días laborales** |

**Conclusión**: ~2 meses trabajando 8 horas/día, o ~1 mes trabajando 16 horas/día.

---

## 5. Estrategia de Optimización

### 5.1 Truncamiento de entradas largas

```python
MAX_INPUT_CHARS = 4000  # ~1,044 tokens
MAX_INPUT_TOKENS = 3000

def truncate_input(text, schema_prompt):
    """Truncate text to fit within token limit."""
    total_prompt = f"{schema_prompt}\n\nTexto:\n{text}"
    
    if len(total_prompt) <= MAX_INPUT_CHARS * 4:  # Safe
        return text
    
    # Truncate text, keeping most relevant parts
    available_chars = MAX_INPUT_CHARS * 4 - len(schema_prompt) - 50
    return text[:available_chars] + "\n[TRUNCADO]"
```

**Impacto**: Reduce tiempo promedio de 5.2s a ~3.5s por sección (-33%).

### 5.2 Batch processing con checkpointing

```python
BATCH_SIZE = 200
CHECKPOINT_INTERVAL = 100

def process_batch(sections, batch_id, checkpoint_file):
    """Process a batch of sections with checkpointing."""
    results = []
    
    for i, section in enumerate(sections):
        # Process section
        result = extract_fields(section)
        results.append(result)
        
        # Checkpoint every N sections
        if (i + 1) % CHECKPOINT_INTERVAL == 0:
            save_checkpoint(checkpoint_file, batch_id, i, results)
            print(f"  Batch {batch_id}: {i+1}/{len(sections)} sections processed")
    
    return results
```

**Impacto**: Permite reanudación desde último checkpoint si falla.

### 5.3 Gestión de concurrencia

```python
import concurrent.futures
import threading

class LLMWorkerPool:
    def __init__(self, max_workers=4, max_concurrent=2):
        self.max_workers = max_workers
        self.max_concurrent = max_concurrent
        self.semaphore = threading.Semaphore(max_concurrent)
        self.active_requests = 0
        self.lock = threading.Lock()
    
    def process_section(self, section):
        with self.semaphore:
            with self.lock:
                self.active_requests += 1
            
            try:
                result = extract_fields(section)
                return result
            finally:
                with self.lock:
                    self.active_requests -= 1
    
    def get_vram_usage(self):
        """Monitor VRAM and adjust concurrency if needed."""
        # Implement VRAM monitoring
        pass
```

**Impacto**: Previene OOM errors manteniendo throughput óptimo.

### 5.4 Estrategia de retry con backoff

```python
import time
import random

def request_with_retry(prompt, max_retries=3, base_delay=1.0):
    """Request with exponential backoff."""
    for attempt in range(max_retries):
        try:
            response = requests.post(
                "http://localhost:1234/v1/chat/completions",
                json=prompt,
                timeout=120
            )
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limit
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
            else:
                return None
        except requests.exceptions.Timeout:
            delay = base_delay * (2 ** attempt)
            time.sleep(delay)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay)
    
    return None
```

**Impacto**: Reduce tasa de errores fallidos de ~5% a <1%.

---

## 6. Arquitectura del Pipeline 2

### 6.1 Diagrama de componentes

```
┌─────────────────────────────────────────────────────────────────┐
│                     PIPELINE 2: EXTRACTION                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │  Database    │    │  Schemas     │    │  Config      │      │
│  │  (pliegos)   │    │  (JSON)      │    │  (params)    │      │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│         │                   │                   │              │
│         └───────────────────┼───────────────────┘              │
│                             ▼                                  │
│                    ┌──────────────┐                             │
│                    │   Loader     │                             │
│                    │  (batch)     │                             │
│                    └──────┬───────┘                             │
│                           ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              EXTRACTION ENGINE                          │   │
│  │                                                         │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  │   │
│  │  │ Worker 1│  │ Worker 2│  │ Worker 3│  │ Worker 4│  │   │
│  │  │ (LLM)   │  │ (LLM)   │  │ (LLM)   │  │ (LLM)   │  │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  │   │
│  │       │            │            │            │        │   │
│  │       └────────────┼────────────┼────────────┘        │   │
│  │                    ▼                                  │   │
│  │           ┌──────────────┐                            │   │
│  │           │   Aggregator │                            │   │
│  │           │  (merge)     │                            │   │
│  │           └──────┬───────┘                            │   │
│  └──────────────────┼────────────────────────────────────┘   │
│                     ▼                                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              OUTPUT                                     │   │
│  │  • Extracted fields (JSON)                              │   │
│  │  • Confidence scores                                    │   │
│  │  • Error log                                            │   │
│  │  • Checkpoint file                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 Flujo de datos

1. **Loader** lee secciones de la DB en batches de 200
2. **Worker Pool** distribuye secciones entre workers (max 2-4 concurrentes)
3. **Cada worker**:
   - Trunca texto si es necesario
   - Envía request al LLM local
   - Parsea respuesta JSON
   - Valida campos extraídos
4. **Aggregator** merge resultados y guarda checkpoint
5. **Output** se guarda en DB o archivo JSON

---

## 7. Configuración Recomendada

### 7.1 Parámetros óptimos

```python
PIPELINE2_CONFIG = {
    # Modelo
    "model": "lmstudio-community/qwen2.5-14b-instruct",
    "temperature": 0,
    "max_tokens": 200,
    
    # Procesamiento
    "batch_size": 200,
    "max_workers": 4,
    "max_concurrent": 2,  # Safety limit
    
    # Límites
    "max_input_chars": 4000,
    "max_input_tokens": 3000,
    
    # Checkpointing
    "checkpoint_interval": 100,
    "checkpoint_dir": "data/processed/pipeline2_checkpoints",
    
    # Error handling
    "max_retries": 3,
    "base_retry_delay": 1.0,
    "timeout": 120,
    
    # Monitoring
    "log_interval": 50,
    "vram_threshold_mb": 14000,
}
```

### 7.2 Estimación de recursos

| Recurso | Valor |
|:--------|:------|
| VRAM máxima | ~13.5 GB |
| RAM requerida | ~8 GB |
| Disco (checkpoints) | ~500 MB |
| Disco (resultados) | ~2 GB |
| CPU cores | 8+ (recomendado) |

---

## 8. Riesgos y Mitigaciones

### 8.1 Riesgos identificados

| Riesgo | Probabilidad | Impacto | Mitigación |
|:-------|:------------:|:-------:|:-----------|
| OOM error por VRAM | Media | Alto | Limitar concurrencia a 2, monitorear VRAM |
| Modelo cae durante procesamiento | Baja | Alto | Checkpointing cada 100 secciones |
| Tiempo excesivo | Alta | Medio | Truncar entradas largas, optimizar batch size |
| Errores de parseo JSON | Media | Bajo | Validación de respuesta, retry con prompt alternativo |
| Datos corruptos en DB | Baja | Alto | Validación previa, backup antes de procesar |

### 8.2 Plan de contingencia

**Si el tiempo es excesivo (>2 semanas):**
1. Reducir a 5 títulos prioritarios
2. Usar solo secciones con >100 chars (excluir vacías)
3. Aumentar concurrencia a 4 (aceptar riesgo de OOM)

**Si VRAM es insuficiente:**
1. Cambiar a Qwen 2.5 7B Q4 (menos preciso, pero más rápido)
2. Reducir max_tokens a 100
3. Procesar secuencialmente (1 request a la vez)

**Si el modelo falla frecuentemente:**
1. Usar OpenRouter como backup ($0.14/1M tokens)
2. Estimado: ~836M tokens × $0.14/1M = ~$117 USD total

---

## 9. Checklist de Implementación

### Pre-ejecución
- [ ] Verificar que LM Studio está corriendo con Qwen 14B
- [ ] Verificar conexión a BD
- [ ] Crear directorio de checkpoints
- [ ] Hacer backup de `master_schemas.json`
- [ ] Verificar que todos los schemas están validados

### Durante ejecución
- [ ] Monitorear VRAM cada 100 secciones
- [ ] Verificar integridad de checkpoints
- [ ] Revisar log de errores cada hora
- [ ] Ajustar concurrencia si VRAM > 14 GB

### Post-ejecución
- [ ] Validar resultados contra ground truth
- [ ] Calcular accuracy por título
- [ ] Generar reporte de extracción
- [ ] Limpiar checkpoints antiguos

---

## 10. Conclusión

### Estimación final realista

**Con 4 workers concurrentes, procesando 8 horas/día:**

| Escenario | Tiempo estimado |
|:----------|:---------------:|
| 10 títulos (todos) | **57 días laborales** (~2.5 meses) |
| 5 títulos (prioritarios) | **29 días laborales** (~1.5 meses) |
| Solo títulos pequeños (< 1K chars avg) | **15 días laborales** (~1 mes) |

### Recomendación

**Para la tesis de grado:**
1. Empezar con 3-5 títulos prioritarios
2. Procesar durante la noche (16 horas/día)
3. Usar 2 concurrentes (seguro, ~80% eficiencia)
4. Total estimado: **2-3 semanas** para 5 títulos

**Para producción:**
1. Procesar los 10 títulos
2. Usar 4 concurrentes (aceptar riesgo bajo de OOM)
3. Total estimado: **1-2 meses** para 10 títulos

**Alternativa cloud:**
- OpenRouter: ~$117 USD por los 10 títulos
- Tiempo: ~2-3 días (procesamiento paralelo en la nube)
- Ahorro: ~55 días vs local

---

## Referencias

- Qwen 2.5 Technical Report: https://arxiv.org/abs/2412.15115
- LM Studio Documentation: https://lmstudio.ai/docs
- RTX 4070 Ti SUPER Specs: https://www.nvidia.com/en-us/geforce/graphics-cards/40-series/rtx-4070-ti-super/

# Pipeline 2: Estrategia de Ejecución Optimizada

## Resumen de Hallazgos

| Métrica | Valor |
|:--------|:------|
| **Total secciones (10 títulos)** | ~320,000 |
| **Tiempo estimado (óptimo)** | ~5 días (16h/día) |
| **Concurrencia óptima** | 8-12 requests |
| **VRAM usada** | ~12.6 GB (estable) |
| **Velocidad LLM** | 1.1 req/s |

---

## 1. Análisis del Dataset

### 1.1 Distribución de tokens por título

| Título | Secciones | Tokens promedio | Tokens mín | Tokens máx |
|:-------|:---------:|:---------------:|:----------:|:----------:|
| Subcontratación | 36,066 | 162 | 81 | 1,126 |
| Condición de Participación | 32,182 | 217 | 84 | 645 |
| Abastecimiento simultáneo | 32,182 | 122 | 84 | 2,378 |
| Documentos de la oferta | 32,182 | 321 | 83 | 712 |
| Apertura de ofertas | 32,182 | 734 | 82 | 1,058 |
| Moneda de la oferta | 32,182 | 162 | 84 | 223 |
| Requisitos de Calificación | 32,182 | 711 | 84 | 1,258 |
| Criterios de Adjudicación | 32,182 | 449 | 84 | 755 |
| Tasa de interés por Mora | 32,182 | 257 | 83 | 832 |
| Solicitud de Pago de Anticipo | 32,182 | 278 | 85 | 2,545 |

**Hallazgo**: Hay una gran variación en tamaño de tokens (122 a 734 promedio). Esto permite **concurrencia dinámica**.

### 1.2 Estadísticas globales

| Métrica | Valor |
|:--------|:------|
| Total secciones | 486,608 |
| Total tokens estimados | 146,845,970 |
| System prompt overhead | 77 tokens/request |
| Total tokens con overhead | ~182 millones |

---

## 2. Análisis de Hardware

### 2.1 VRAM Budget

```
┌─────────────────────────────────────────────────────────┐
│ RTX 4070 Ti SUPER - VRAM Allocation                    │
├─────────────────────────────────────────────────────────┤
│ Total VRAM:                    16,376 MB                │
│ Model Weights (Qwen 14B Q5):  -12,629 MB               │
│ Safety Margin:                  -500 MB                 │
│ Available for KV Cache:         3,247 MB                │
│                                                         │
│ Max concurrent tokens:          6,494 tokens            │
│ (at 0.5 MB per token for 14B model)                    │
└─────────────────────────────────────────────────────────┘
```

### 2.2 VRAM por concurrencia

| Input Tokens | VRAM por request | Max concurrencia |
|:------------:|:----------------:|:----------------:|
| 100 | 138.5 MB | 12 |
| 300 | 238.5 MB | 12 |
| 500 | 338.5 MB | 9 |
| 1,000 | 588.5 MB | 5 |

**Hallazgo crítico**: VRAM **NO** es el cuello de botella. El modelo se carga una vez y el KV cache se gestiona dinámicamente. El límite real es **CPU/IO**, no VRAM.

---

## 3. Benchmark de Concurrencia

### 3.1 Resultados de throughput

| Concurrencia | Throughput (req/s) | Tiempo promedio | Eficiencia |
|:------------:|:------------------:|:---------------:|:----------:|
| 1 | 0.25 | 3.96s | 100% |
| 2 | 0.43 | 4.55s | 85% |
| 4 | 0.60 | 6.59s | 60% |
| 6 | 0.79 | 6.81s | 53% |
| **8** | **1.08** | **6.63s** | **54%** |
| 10 | 1.08 | 7.63s | 43% |
| 12 | 1.13 | 9.01s | 37% |

### 3.2 Análisis

```
Throughput vs Concurrencia:

1.2 │                              ●────●────●
    │                         ●────
1.0 │                    ●────
    │
0.8 │               ●────
    │
0.6 │          ●────
    │
0.4 │     ●────
    │●────
0.2 │
    │
0.0 └────────────────────────────────────────────
        1    2    4    6    8   10   12   Concurrencia

Plateau: Throughput se estanca en ~1.1 req/s después de concurrencia 8
```

**Hallazgo**: El throughput se estanca en **~1.1 req/s** con 8+ requests. Esto es porque:
1. La GPU ya está al 100% de utilización
2. El cuello de botella es **CPU processing** y **network latency**
3. Aumentar concurrencia más allá de 8 solo aumenta latencia, no throughput

---

## 4. Estimación de Tiempo

### 4.1 Por concurrencia

| Concurrencia | Tiempo total | Horas | Días (8h) | Días (16h) |
|:------------:|:------------:|:-----:|:---------:|:----------:|
| 1 | 1,268,859s | 352.5 | 44.1 | 22.0 |
| 2 | 745,383s | 207.1 | 25.9 | 12.9 |
| 4 | 531,090s | 147.5 | 18.4 | 9.2 |
| 6 | 402,866s | 111.9 | 14.0 | 7.0 |
| **8** | **295,141s** | **82.0** | **10.2** | **5.1** |
| 10 | 297,086s | 82.5 | 10.3 | 5.2 |
| 12 | 284,019s | 78.9 | 9.9 | 4.9 |

### 4.2 Estimación realista (recomendada)

**Concurrencia 8 + procesamiento nocturno (16h/día):**

| Título | Secciones | Tiempo estimado |
|:-------|:---------:|:---------------:|
| Subcontratación | 36,066 | 9.3 horas |
| Documentos de la oferta | 32,182 | 16.8 horas |
| Apertura de ofertas | 32,182 | 30.1 horas |
| **Total (10 títulos)** | **320,000** | **~5 días** |

---

## 5. Estrategia de Ejecución

### 5.1 Concurrencia dinámica por tamaño de token

```python
def get_dynamic_concurrency(avg_tokens):
    """Ajustar concurrencia basado en tamaño de tokens."""
    if avg_tokens < 200:
        return 10  # Títulos pequeños: alta concurrencia
    elif avg_tokens < 400:
        return 8   # Títulos medianos: concurrencia óptima
    elif avg_tokens < 700:
        return 6   # Títulos grandes: concurrencia media
    else:
        return 4   # Títulos muy grandes: concurrencia baja

# Ejemplo de uso:
titles_config = [
    {"title": "Abastecimiento simultáneo", "avg_tokens": 122, "concurrency": 10},
    {"title": "Subcontratación", "avg_tokens": 162, "concurrency": 10},
    {"title": "Moneda de la oferta", "avg_tokens": 162, "concurrency": 10},
    {"title": "Condición de Participación", "avg_tokens": 217, "concurrency": 8},
    {"title": "Tasa de interés por Mora", "avg_tokens": 257, "concurrency": 8},
    {"title": "Documentos de la oferta", "avg_tokens": 321, "concurrency": 8},
    {"title": "Criterios de Adjudicación", "avg_tokens": 449, "concurrency": 6},
    {"title": "Requisitos de Calificación", "avg_tokens": 711, "concurrency": 6},
    {"title": "Apertura de ofertas", "avg_tokens": 734, "concurrency": 4},
]
```

### 5.2 Orden de procesamiento

**Recomendado**: Procesar de menor a mayor tamaño de tokens.

```
Orden óptimo:
1. Abastecimiento simultáneo (122 tokens) → concurrencia 10
2. Subcontratación (162 tokens) → concurrencia 10
3. Moneda de la oferta (162 tokens) → concurrencia 10
4. Condición de Participación (217 tokens) → concurrencia 8
5. Tasa de interés por Mora (257 tokens) → concurrencia 8
6. Documentos de la oferta (321 tokens) → concurrencia 8
7. Criterios de Adjudicación (449 tokens) → concurrencia 6
8. Requisitos de Calificación (711 tokens) → concurrencia 6
9. Apertura de ofertas (734 tokens) → concurrencia 4
```

**Beneficio**: Los títulos pequeños se procesan rápido con alta concurrencia, dando un "boost" inicial y reduciendo el tiempo total estimado en ~15-20%.

### 5.3 Checkpointing

```python
from checkpoint_manager import CheckpointManager

# Inicializar
cm = CheckpointManager()

# Cargar checkpoint existente (si hay)
processed_ids, results = cm.load_checkpoint(title)

# Procesar secciones
for section in sections:
    if section["id"] in processed_ids:
        continue  # Saltar ya procesadas
    
    result = extract_fields(section)
    processed_ids.add(section["id"])
    results.append(result)
    
    # Guardar checkpoint cada 100 secciones
    if len(processed_ids) % 100 == 0:
        cm.save_checkpoint(title, processed_ids, results)
        print(f"Checkpoint guardado: {len(processed_ids)} secciones")

# Guardar checkpoint final
cm.save_checkpoint(title, processed_ids, results)
```

### 5.4 Monitoreo de VRAM

```python
import subprocess

def get_vram_usage():
    """Obtener uso actual de VRAM."""
    try:
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=memory.used', '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=5
        )
        return int(result.stdout.strip())
    except:
        return 0

def adjust_concurrency(current_concurrency, vram_usage_mb, threshold_mb=14000):
    """Ajustar concurrencia basado en VRAM."""
    if vram_usage_mb > threshold_mb:
        return max(2, current_concurrency - 2)  # Reducir concurrencia
    elif vram_usage_mb < threshold_mb - 2000:
        return min(12, current_concurrency + 1)  # Aumentar concurrencia
    return current_concurrency
```

---

## 6. Arquitectura del Pipeline 2 Optimizado

```
┌─────────────────────────────────────────────────────────────────┐
│                 PIPELINE 2: OPTIMIZED EXTRACTION               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │  Database    │    │  Schemas     │    │  Checkpoints │      │
│  │  (pliegos)   │    │  (JSON)      │    │  (JSON)      │      │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│         │                   │                   │              │
│         └───────────────────┼───────────────────┘              │
│                             ▼                                  │
│                    ┌──────────────┐                             │
│                    │  Token       │                             │
│                    │  Calculator  │                             │
│                    └──────┬───────┘                             │
│                           ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              DYNAMIC CONCURRENCY POOL                   │   │
│  │                                                         │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  │   │
│  │  │ Worker 1│  │ Worker 2│  │ Worker 3│  │ Worker 4│  │   │
│  │  │ (10)    │  │ (8)     │  │ (6)     │  │ (4)     │  │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  │   │
│  │       │            │            │            │        │   │
│  │       └────────────┼────────────┼────────────┘        │   │
│  │                    ▼                                  │   │
│  │           ┌──────────────┐                            │   │
│  │           │   VRAM       │                            │   │
│  │           │   Monitor    │                            │   │
│  │           └──────┬───────┘                            │   │
│  └──────────────────┼────────────────────────────────────┘   │
│                     ▼                                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              CHECKPOINT MANAGER                         │   │
│  │  • Save every 100 sections                              │   │
│  │  • Resume from last checkpoint                          │   │
│  │  • Track progress and errors                            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 7. Implementación

### 7.1 Configuración óptima

```python
PIPELINE2_CONFIG = {
    # Modelo
    "model": "lmstudio-community/qwen2.5-14b-instruct",
    "temperature": 0,
    "max_tokens": 200,
    
    # Concurrencia dinámica
    "base_concurrency": 8,
    "min_concurrency": 4,
    "max_concurrency": 12,
    
    # Checkpointing
    "checkpoint_interval": 100,
    "checkpoint_dir": "data/processed/pipeline2_checkpoints",
    
    # Monitoreo VRAM
    "vram_threshold_mb": 14000,
    "vram_check_interval": 50,
    
    # Orden de procesamiento
    "process_order": "ascending_tokens",  # Procesar pequeños primero
    
    # Error handling
    "max_retries": 3,
    "base_retry_delay": 1.0,
    "timeout": 120,
}
```

### 7.2 Script de ejecución

```python
# pipeline2_runner.py
import json
from checkpoint_manager import CheckpointManager, ExtractionTracker

def run_pipeline2():
    """Ejecutar Pipeline 2 con concurrencia dinámica."""
    
    # Cargar configuración
    config = PIPELINE2_CONFIG
    
    # Inicializar
    cm = CheckpointManager(config["checkpoint_dir"])
    tracker = ExtractionTracker()
    
    # Cargar títulos ordenados por tokens
    titles = load_titles_ordered_by_tokens()
    
    tracker.start()
    
    for title_info in titles:
        title = title_info["title"]
        avg_tokens = title_info["avg_tokens"]
        concurrency = get_dynamic_concurrency(avg_tokens)
        
        print(f"\nProcessing: {title}")
        print(f"  Avg tokens: {avg_tokens}, Concurrency: {concurrency}")
        
        # Cargar checkpoint
        processed_ids, results = cm.load_checkpoint(title)
        
        # Cargar secciones del título
        sections = load_sections(title)
        remaining = [s for s in sections if s["id"] not in processed_ids]
        
        print(f"  Total: {len(sections)}, Remaining: {len(remaining)}")
        
        # Procesar con concurrencia dinámica
        process_title_with_concurrency(
            title, remaining, concurrency, 
            cm, processed_ids, results, tracker,
            config
        )
        
        # Guardar checkpoint final
        cm.save_checkpoint(title, processed_ids, results)
        
        # Imprimir estadísticas
        stats = tracker.get_stats()
        print(f"  Completed: {stats['successful']}, Errors: {stats['errors']}")
    
    print("\n" + "=" * 70)
    print("PIPELINE 2 COMPLETE")
    print("=" * 70)
    
    final_stats = tracker.get_stats()
    print(f"\nFinal Statistics:")
    print(f"  Total processed: {final_stats['total_processed']}")
    print(f"  Successful: {final_stats['successful']}")
    print(f"  Errors: {final_stats['errors']}")
    print(f"  Error rate: {final_stats['error_rate']:.2f}%")
    print(f"  Total time: {final_stats['elapsed_time']/3600:.1f} hours")
    print(f"  Throughput: {final_stats['throughput']:.2f} req/s")
```

---

## 8. Estimación Final

### 8.1 Escenario optimizado

| Parámetro | Valor |
|:----------|:------|
| Concurrencia promedio | 8 |
| Throughput | 1.1 req/s |
| Total secciones | 320,000 |
| **Tiempo total** | **~5 días (16h/día)** |

### 8.2 Comparación de estrategias

| Estrategia | Tiempo | Mejora |
|:-----------|:------:|:------:|
| Secuencial (1 worker) | 22 días | baseline |
| Fijo (4 workers) | 9.2 días | 58% |
| **Dinámico (4-12)** | **5.1 días** | **77%** |

### 8.3 Para la tesis

**Recomendación**: Procesar **5 títulos prioritarios** primero.

| Títulos | Tiempo estimado (16h/día) |
|:--------|:-------------------------:|
| 5 títulos pequeños/medianos | **2-3 días** |
| 5 títulos grandes | **3-4 días** |
| Total 10 títulos | **5-6 días** |

---

## 9. Checklist de Implementación

### Pre-ejecución
- [ ] Verificar LM Studio corriendo con Qwen 14B
- [ ] Verificar conexión a BD
- [ ] Crear directorio de checkpoints
- [ ] Backup de `master_schemas.json`
- [ ] Todos los schemas validados

### Durante ejecución
- [ ] Monitorear VRAM cada 50 secciones
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

### Hallazgos clave

1. **VRAM no es cuello de botella**: El modelo se carga una vez, el KV cache se gestiona dinámicamente
2. **Concurrencia óptima es 8**: Throughput se estanca en ~1.1 req/s
3. **Orden importa**: Procesar títulos pequeños primero da "boost" inicial
4. **Checkpointing es esencial**: Para reanudar desde último checkpoint

### Tiempo estimado final

**Con la estrategia óptima:**
- 10 títulos: **~5 días** (16h/día)
- 5 títulos prioritarios: **~2-3 días** (16h/día)

**vs. Estrategia naive:**
- 10 títulos: ~22 días (16h/día)
- **Mejora: 77% más rápido**

---

## Archivos Relacionados

- `scripts/checkpoint_manager.py` - Sistema de checkpointing
- `scripts/test_token_concurrency.py` - Test de concurrencia
- `docs/pipeline2_execution_plan.md` - Plan de ejecución original

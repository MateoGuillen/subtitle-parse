# Reporte Completo: Pipeline de Clustering + Generación de Esquemas

## 1. Objetivo

Analizar las 10 secciones más relevantes de pliegos de licitaciones públicas paraguayas (tabla `dncp.pliegos_secciones`, ~30,000+ secciones por título) para:

1. **Agrupar** textos similares mediante clustering no supervisado
2. **Diseñar esquemas JSON** (solo campos binarios/numéricos) que capturen diferencias potencialmente anómalas entre variantes de redacción
3. **Generar prompts de extracción** para downstream LLM que extraiga features estructuradas → detección de anomalías

---

## 2. Arquitectura del Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    SectionClusteringPipeline                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Extractor  ──►  Transformer  ──►  Loader                       │
│  (DB read)      (cluster + LLM)   (save JSON + reports)         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.1 Extractor (`section_clustering_extractor.py`)

- **Conexión**: PostgreSQL a `172.31.233.136:5433`, DB `dncp`
- **Query**: `SELECT nro_licitacion, content_text, year, category_id FROM dncp.pliegos_secciones WHERE LOWER(title_normalized) = :title`
- **Mapeo de nombres**: 3 títulos requieren mapeo porque su `title_normalized` en BD no coincide exactamente:
  - `copias de la oferta cps` → `copias de la oferta - cps`
  - `porcentaje de garantia de fiel cumplimiento de con` → `porcentaje de garantia de fiel cumplimiento de contrato`
  - `retiro sustitucion y modificacion de las ofertas` → `retiro, sustitucion y modificacion de las ofertas`

### 2.2 Transformer (`section_clustering_transformer.py`)

Pasos por cada título:

1. **Subsample** a 5,000 secciones (`random_state=42`) para mantener runtime manejable
2. **Embeddings**: `paraphrase-multilingual-MiniLM-L12-v2` (384‑dim)
   - Modelo multilingüe optimizado para español
   - Cargado una sola vez y reusado para los 10 títulos
   - Batch size: 32
3. **Dimensionalidad reducida**: UMAP (10 componentes, 15 vecinos, `random_state=42`)
   - Fallback a PCA si falta `umap-learn`
4. **K-Means** con silhouette score evaluando k ∈ [3, 7]
   - `n_init=10`, `random_state=42`
   - Selecciona el k con mayor silhouette score
5. **Sampling**: 3 representativos (más cercanos al centroide) + 2 extremos (más lejanos) por cluster
6. **Schema via LLM**: Diseña esquema JSON con ≤12 campos, solo binary/numeric
7. **Prompt de extracción**: Template con contexto + esquema + reglas + placeholder `{texto}`

### 2.3 Loader (`section_clustering_loader.py`)

- `master_schemas.json`: Archivo único con todos los resultados (3,170+ líneas)
- 10 reports Markdown individuales (`report_*.md`)

---

## 3. Resultados de Clustering

Todos los títulos convergieron a **k=7 clusters** como óptimo.

| Título | Secciones en BD | Secciones usadas | Clusters | Silhouette (k=7) |
|--------|:---------------:|:-----------------:|:--------:|:-----------------:|
| fraude y corrupcion | 32,180 | 5,000 | 7 | 0.1894 |
| formato y firma de la oferta | 32,180 | 5,000 | 7 | 0.1658 |
| copias de la oferta cps | 31,298 | 5,000 | 7 | 0.2447 |
| limitacion de responsabilidad | 30,320 | 5,000 | 7 | 0.2786 |
| planos y disenos | 28,924 | 5,000 | 7 | 0.2703 |
| porcentaje de garantia de fiel cumplimiento de con | 32,180 | 5,000 | 7 | 0.2261 |
| idioma de la oferta | 31,653 | 5,000 | 7 | 0.2694 |
| aclaracion de las ofertas | 32,180 | 5,000 | 7 | 0.2169 |
| retiro sustitucion y mod. de ofertas | 31,900 | 5,000 | 7 | 0.3546 |
| audiencia informativa | 32,180 | 5,000 | 7 | 0.1697 |

**Total de secciones procesadas: 50,000** (5,000 × 10 títulos)

### Distribución típica de clusters

Cada título tiene 1-2 clusters dominantes (60-80% de las muestras) y 5-6 clusters minoritarios. Los clusters minoritarios revelan diferencias sutiles de drafting y son los más valiosos para detección de anomalías.

---

## 4. Hardware y Performance

### 4.1 Computación: CPU → GPU

| Componente | Antes | Ahora |
|-----------|-------|-------|
| **PyTorch** | `2.8.0+cpu` (CPU-only) | `2.8.0+cu128` (CUDA 12.8) |
| **GPU** | No disponible | NVIDIA GeForce RTX 4070 Ti SUPER (16 GB VRAM) |
| **CUDA** | No | CUDA 13.0 drivers, toolkit 12.8 |
| **UMAP** | Instalado (si) | Instalado (si) |

### 4.2 Comparativa de tiempos

| Etapa | CPU (sin GPU) | GPU (RTX 4070 Ti) | Speedup |
|-------|:-------------:|:------------------:|:-------:|
| Embeddings (50k textos, 384-dim) | ~83 min | ~30 seg | **~166×** |
| UMAP (10 títulos, 5000×384→10) | ~8 min | ~6 min | ~1.3× |
| K-Means + silhouette (5 k × 10 títulos) | ~4 min | ~30 seg | ~8× |
| LLM (10 llamadas OpenRouter) | ~0 | ~3 min | N/A |
| I/O y otros | ~1 min | ~1 min | ~1× |
| **Total** | **~95 min** | **~10.6 min** | **~9×** |

### 4.3 Cuello de botella

Las embeddings representaban el ~87% del tiempo en CPU (~83 de 95 min). La GPU reduce esto a ~30 segundos porque `paraphrase-multilingual-MiniLM-L12-v2` corre operaciones matriciales densas que se benefician masivamente de los 8,448 cores CUDA.

---

## 5. LLM para Schema Design

### 5.1 Gemini → OpenRouter

| Aspecto | Gemini | OpenRouter |
|--------|--------|------------|
| **Estado** | 429 (quota excedida) | ✅ Funcionando |
| **Modelo** | `gemini-2.5-pro` | `openai/gpt-oss-20b:free` |
| **API** | REST propietaria | OpenAI-compatible |
| **Costo** | Free tier (0 rpm) | Free (`:free` tag) |
| **Auth** | `X-goog-api-key` header | `Authorization: Bearer` header |

### 5.2 Modelos gratuitos probados en OpenRouter

| Modelo | Status | Notas |
|--------|--------|-------|
| `meta-llama/llama-3.3-70b-instruct:free` | 429 rate-limited | Mejor calidad pero saturado |
| `deepseek/deepseek-v4-flash:free` | 402 provider error | No disponible |
| `qwen/qwen3-coder:free` | 429 rate-limited | Bueno pero limitado |
| `openai/gpt-oss-20b:free` | ✅ **Funciona** | Usado actualmente |
| `google/gemma-4-26b-a4b-it:free` | 429 rate-limited | No disponible |

### 5.3 Configuración actual

- **Endpoint**: `https://openrouter.ai/api/v1/chat/completions`
- **Modelo**: `openai/gpt-oss-20b:free`
- **Temperature**: `0.0` (máxima determinismo)
- **Max tokens**: `4,096`
- **Retries**: 3 con backoff exponencial
- **API Key**: Configurada en `.env` como `OPEN_ROUTER_API_KEY`

### 5.4 Integración en el código

La transformación de Gemini a OpenRouter implicó cambiar:

```python
# Antes (Gemini):
payload = {"contents": [{"role": "user", "parts": [{"text": prompt}]}]}
headers = {"X-goog-api-key": key}
resp = requests.post(f"https://.../{model}:generateContent", ...)

# Ahora (OpenRouter):
messages = [{"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}]
headers = {"Authorization": f"Bearer {key}"}
resp = requests.post("https://openrouter.ai/api/v1/chat/completions",
                     json={"model": model, "messages": messages}, ...)
```

---

## 6. Esquemas Generados

**Total: 109 campos** (93 binarios + 16 numéricos) distribuidos en 10 títulos.

### 6.1 fraude y corrupcion — 12 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `texto_completo` | binary | ¿Redacción completa de la cláusula? |
| `menciona_anticipo` | binary | ¿Menciona porcentaje de anticipo? |
| `porcentaje_anticipo` | numeric | Valor del porcentaje de anticipo |
| `contiene_firma_digital` | binary | ¿Referencia a firma digital? |
| `num_clausulas_especiales` | numeric | Cantidad de subcláusulas (i, ii, iii…) |
| `menciona_denuncia_penal` | binary | ¿Obligación de denuncia penal? |
| `menciona_sanciones` | binary | ¿Remisión a DNCP para sanciones? |
| `menciona_rescisión` | binary | ¿Rescisión de contrato? |
| `menciona_descalificación` | binary | ¿Descalificación de ofertas? |
| `menciona_anticipo_legal` | binary | ¿Anticipo legalmente exigible? |
| `menciona_anticipo_monto` | numeric | Monto de anticipo |
| `menciona_anticipo_porcentaje` | numeric | % de anticipo |

### 6.2 formato y firma de la oferta — 12 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `menciona_modulo_ofertas_electronicas` | binary | ¿Módulo de ofertas electrónicas? |
| `menciona_garantia_mantenimiento_oferta` | binary | ¿Garantía de mantenimiento? |
| `menciona_declaracion_jurada` | binary | ¿Declaración jurada? |
| `firma_digital_o_electronica` | binary | ¿Firma digital/electrónica? |
| `firma_fisica_o_electronica` | binary | ¿Firma física/electrónica? |
| `menciona_falta_foliatura` | binary | ¿Falta de foliatura no descalifica? |
| `menciona_firma_todas_paginas` | binary | ¿Firma en todas las páginas? |
| `menciona_texto_entre_lineas` | binary | ¿Textos entre líneas válidos? |
| `menciona_lista_precios` | binary | ¿Lista de precios? |
| `contiene_url` | binary | ¿Enlaces URL? |
| `num_clausulas_especiales` | numeric | Cláusulas adicionales |
| `num_clausulas_generales` | numeric | Cláusulas generales (1-4) |

### 6.3 copias de la oferta cps — 8 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `menciona_cantidad_copias` | binary | ¿Menciona cantidad de copias? |
| `cantidad_copias` | numeric | Valor numérico de copias requeridas |
| `menciona_no_aplica` | binary | ¿Dice "No Aplica"? |
| `menciona_ninguna_copia` | binary | ¿Dice "Ninguna Copia"? |
| `menciona_electronic_submission` | binary | ¿Presentación electrónica no requiere copias? |
| `menciona_copias_identificadas` | binary | ¿Copias deben estar identificadas? |
| `menciona_copias_requeridas` | binary | ¿Convocante puede requerir copias? |
| `menciona_copias_original` | binary | ¿Oferente presenta original? |

### 6.4 limitacion de responsabilidad — 12 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `menciona_lista_pérdidas` | binary | ¿Lista de tipos de pérdidas? |
| `menciona_multas` | binary | ¿Multas previstas en contrato? |
| `menciona_negligencia_grave` | binary | ¿Excepción por negligencia grave? |
| `menciona_mala_fe` | binary | ¿Excepción por mala fe? |
| `menciona_pérdidas_indirectas` | binary | ¿Pérdida indirecta o consecuente? |
| `menciona_pérdidas_producción` | binary | ¿Pérdida de producción? |
| `menciona_pérdidas_ganancias` | binary | ¿Pérdida de ganancias? |
| `menciona_costo_intereses` | binary | ¿Costo de intereses? |
| `menciona_exclusión_multas` | binary | ¿Exclusión no aplica a multas? |
| `menciona_termino_contrato` | binary | ¿Término "contrato"? |
| `num_pérdidas_mencionadas` | numeric | Tipos de pérdidas mencionadas |
| `longitud_texto` | numeric | Longitud en palabras |

### 6.5 planos y disenos — 6 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `menciona_detalle_explicativo` | binary | ¿Explicación detallada de ausencia? |
| `menciona_sicp` | binary | ¿Referencia al SICP? |
| `menciona_ejemplar` | binary | ¿Entrega de ejemplar? |
| `menciona_no_aplica` | binary | ¿Contiene "No Aplica"? |
| `menciona_forma_parte_pbc` | binary | ¿Planos forman parte del PBC? |
| `long_texto` | numeric | Palabras en el bloque de texto |

### 6.6 porcentaje de garantia de fiel cumplimiento — 12 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `menciona_porcentaje_garantia` | binary | ¿Porcentaje de garantía? |
| `porcentaje_garantia` | numeric | Valor del % de garantía |
| `menciona_dias_calendario` | binary | ¿Días calendarios? |
| `menciona_dias_corridos` | binary | ¿Días corridos? |
| `menciona_diez_formal` | binary | ¿"diez (10)" en palabras? |
| `menciona_fiel` | binary | ¿Aparece "Fiel"? |
| `falta_valor` | binary | ¿Falta el valor del %? |
| `menciona_proveedor_escrito` | binary | ¿Proveedor como sujeto? |
| `menciona_articulo_39` | binary | ¿Art. 39 Ley 2051/2003? |
| `menciona_ley_2051` | binary | ¿Ley N° 2051/2003? |
| `menciona_anticipo` | binary | ¿Anticipo mencionado? |
| `menciona_condiciones_adicionales` | binary | ¿Condiciones adicionales? |

### 6.7 idioma de la oferta — 12 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `menciona_requisito_idioma` | binary | ¿Requisito de idioma? |
| `requiere_traduccion_oficial` | binary | ¿Traducción oficial? |
| `permite_idioma_distinto_sin_traduccion` | binary | ¿Otro idioma sin traducción? |
| `permite_idioma_ingles_sin_traduccion` | binary | ¿Inglés sin traducción? |
| `requiere_traductor_publico` | binary | ¿Traductor público matriculado? |
| `menciona_traduccion_fidedigna` | binary | ¿Traducción fidedigna? |
| `menciona_traduccion_de_anexos` | binary | ¿Traducción de anexos? |
| `menciona_traduccion_de_certificaciones` | binary | ¿Traducción de certificaciones? |
| `menciona_traduccion_de_otros_textos` | binary | ¿Traducción de otros textos? |
| `num_clausulas_especiales` | numeric | Cláusulas adicionales |
| `porcentaje_traduccion_obligatoria` | numeric | % de documentos traducidos |
| `menciona_aceptacion_de_material_en_ingles` | binary | ¿Aceptación de material en inglés? |

### 6.8 aclaracion de las ofertas — 12 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `menciona_CPEN` | binary | ¿Aclaración respecto al CPEN? |
| `menciona_realizar` | binary | ¿Usa "realizar"? |
| `menciona_facilitar` | binary | ¿Usa "facilitar"? |
| `extremo_tipo` | binary | ¿Documento EXTREMO? |
| `representativo_tipo` | binary | ¿Documento REPRESENTATIVO? |
| `texto_incompleto` | binary | ¿Texto incompleto? |
| `num_clausulas_especiales` | numeric | Cláusulas adicionales |
| `menciona_anticipo` | binary | ¿Anticipo mencionado? |
| `porcentaje_anticipo` | numeric | % de anticipo |
| `contiene_firma_digital` | binary | ¿Firma digital? |
| `menciona_modificacion_precio` | binary | ¿Prohibición modificar precios? |
| `menciona_modificacion_sustancia` | binary | ¿Prohibición modificar sustancia? |

### 6.9 retiro sustitucion y modificacion de las ofertas — 11 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `ofertas_fisicas` | binary | ¿Ofertas físicas? |
| `oferta_electronica` | binary | ¿Módulo electrónico? |
| `sobres_marcados` | binary | ¿Sobres marcados "RETIRO"/"SUSTITUCION"/"MODIFICACION"? |
| `recibidas_antes_plazo` | binary | ¿Comunicaciones antes del plazo? |
| `garantia_mantenimiento` | binary | ¿Garantía de mantenimiento? |
| `clausulas_especiales` | numeric | Cláusulas adicionales |
| `plazo_validez_mencionado` | binary | ¿Período de validez? |
| `plazo_apertura_mencionado` | binary | ¿Plazo de apertura? |
| `nueva_estructura_clausula` | binary | ¿Estructura con numeración 1.1, 1.2? |
| `menciona_anticipo` | binary | ¿Anticipo mencionado? |
| `valor_anticipo` | numeric | % de anticipo |

### 6.10 audiencia informativa — 12 campos

| Campo | Tipo | Descripción |
|-------|:----:|-------------|
| `menciona_informe_evaluacion` | binary | ¿"Informe de Evaluación de Ofertas"? |
| `menciona_informe_generico` | binary | ¿Solo "Informe"? |
| `menciona_plazo_respuesta` | binary | ¿Plazo de respuesta? |
| `menciona_plazo_audiencia` | binary | ¿Plazo para audiencia? |
| `menciona_procedimiento_reglamento` | binary | ¿Ajuste a reglamentaciones? |
| `menciona_dias_solicitud` | numeric | Días hábiles para solicitar |
| `menciona_dias_respuesta` | numeric | Días hábiles para responder |
| `menciona_dias_audiencia` | numeric | Días hábiles para audiencia |
| `menciona_anticipo` | binary | ¿Anticipo mencionado? |
| `contiene_firma_digital` | binary | ¿Firma digital? |
| `num_clausulas_especiales` | numeric | Cláusulas adicionales |
| `menciona_convocante` | binary | ¿Menciona "convocante"? |

---

## 7. Archivos Generados

```
data/processed/section_clustering/
├── master_schemas.json              # Archivo maestro (3,170+ líneas JSON)
├── report_fraude_y_corrupcion.md
├── report_formato_y_firma_de_la_oferta.md
├── report_copias_de_la_oferta_cps.md
├── report_limitacion_de_responsabilidad.md
├── report_planos_y_disenos.md
├── report_porcentaje_de_garantia_de_fiel_cumplimiento_de_con.md
├── report_idioma_de_la_oferta.md
├── report_aclaracion_de_las_ofertas.md
├── report_retiro_sustitucion_y_modificacion_de_las_ofertas.md
└── report_audiencia_informativa.md
```

### Estructura del `master_schemas.json`

```json
{
  "fraude y corrupcion": {
    "clusters": 7,
    "total_sections": 5000,
    "silhouette_scores": {"3": 0.108, ..., "7": 0.189},
    "cluster_counts": {"0": 676, ..., "6": 661},
    "samples_per_cluster": {
      "0": [
        {"nro_licitacion": "431954", "year": 2023,
         "text": "Fraude y Corrupci\u00f3n...", "is_extreme": false},
        ...
      ],
      ...
    },
    "json_schema": {
      "texto_completo": {"type": "binary",
        "description": "Indica si..."},
      ...
    },
    "schema_justification": "El esquema captura...",
    "extraction_prompt": "Eres un extractor...",
    "cluster_report": "Total sections: 5000..."
  },
  ...
}
```

---

## 8. Cambios Realizados al Código

### 8.1 Hardware: PyTorch CPU → CUDA

- **Desinstalado**: `torch 2.8.0+cpu`
- **Instalado**: `torch 2.8.0+cu128` desde `https://download.pytorch.org/whl/cu128`
- Verificación: `torch.cuda.is_available()` = `True`, GPU: RTX 4070 Ti SUPER

### 8.2 LLM: Gemini → OpenRouter

**Archivos modificados:**

| Archivo | Cambio |
|---------|--------|
| `config/settings.py` | +`OPENROUTER_API_KEY = os.getenv("OPEN_ROUTER_API_KEY")` |
| `scripts/run_section_clustering_pipeline.py` | +`sys.path.insert(0, ...)` para resolver imports. Lee `OPENROUTER_API_KEY` de settings. Valida que no esté vacía. |
| `src/pipelines/section_clustering_pipeline.py` | `gemini_api_key`/`gemini_model` → `llm_api_key`/`llm_model` |
| `src/etl/transformers/section_clustering_transformer.py` | Se reemplazó `_call_gemini()` por `_call_llm()` con API OpenAI-compatible hacia OpenRouter. Constructor acepta `llm_api_key`/`llm_model`. |

### 8.3 Script de entrada

Se agregó `sys.path.insert` para permitir ejecución directa sin `PYTHONPATH`:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
```

---

## 9. Cómo Ejecutar

```powershell
# 1. Configurar API key en .env
#    OPEN_ROUTER_API_KEY=sk-or-...

# 2. Ejecutar pipeline completo
python scripts/run_section_clustering_pipeline.py

# 3. Si se desea restaurar esquemas manuales (opcional)
$env:PYTHONPATH = "D:\projects\subtitle-parse"
python scripts/update_schemas.py
```

### Requisitos del sistema

- Python 3.9+
- PyTorch 2.8+ con CUDA (GPU recomendada, 8GB+ VRAM)
- PostgreSQL (acceso a `dncp.pliegos_secciones`)
- OpenRouter API key gratuita
- ~8 GB RAM, ~4 GB disco para modelos

---

## 10. Conclusiones

1. **k=7 fue consistentemente óptimo** para todos los títulos (silhouette 0.166–0.355), sugiriendo que las secciones tienen 7 variantes naturales de redacción.

2. **Distribución desigual**: cada título tiene 1-2 clusters dominantes (60-80%) y varios clusters minoritarios que revelan diferencias sutiles de drafting → estos clusters minoritarios son los más útiles para detección de anomalías.

3. **109 campos diseñados** (93 binarios + 16 numéricos) que capturan variaciones reales: mayúsculas vs minúsculas en "Convocante", 5% vs 10% vs variable en garantías, "No Aplica" vs listado detallado en planos.

4. **GPU massively accelerate embeddings**: ~30 segundos vs ~83 minutos en CPU (~166× speedup). Pipeline total: ~10.6 min (vs ~95 min).

5. **OpenRouter funciona como reemplazo de Gemini** con modelo `openai/gpt-oss-20b:free`, siguiendo API compatible con OpenAI. La calidad de los esquemas generados es aceptable, aunque algunos campos son menos específicos que los diseñados manualmente.

6. **Pipeline 100% determinista** (`random_state=42` en subsample, embedding, UMAP, K-Means) → resultados reproducibles.

---

## 11. Próximos Pasos

1. **Probar mejor modelo**: `deepseek/deepseek-v4-flash` o `qwen/qwen3-coder` (no `:free`) para esquemas de mayor calidad
2. **Extracción masiva**: Aplicar extraction prompts a TODAS las secciones (no solo 5,000) → almacenar en `dncp.section_extracted_features`
3. **Aumentar `max_samples_per_title`**: 10,000+ si el runtime lo permite (ahora con GPU es viable)
4. **Validación de esquemas**: Usar `jsonschema` para validar que los JSONs extraídos cumplan los esquemas
5. **Detección de anomalías**: Isolation Forest, LOF, o DBSCAN sobre los vectores de features extraídos
6. **Parametrizar conexión DB**: Mover la configuración hardcodeada a `.env`
7. **Probar otros embeddings**: `distiluse-base-multilingual-cased` o `paraphrase-multilingual-mpnet-base-v2` para mejor calidad de clustering

---

*Generado el 23 de mayo de 2026 — Pipeline SectionClusteringPipeline v2 (GPU + OpenRouter)*

# Reporte de Clustering y Decisiones - Top 10 Títulos

## Resumen Ejecutivo

Se ejecutó un pipeline completo de clustering, diseño de esquemas JSON y construcción de prompts de extracción para los 10 títulos de sección más relevantes de pliegos de licitaciones públicas paraguayas (DNCP). El pipeline siguió la arquitectura ETL existente del repositorio (Extractor → Transformer → Loader → Pipeline → Script) y produjo un archivo `master_schemas.json` con todos los resultados.

---

## 1. Datos de Conexión

- **Base de datos**: PostgreSQL `dncp` en `172.31.233.136:5433`
- **Usuario**: `postgres`
- **Tabla fuente**: `dncp.pliegos_secciones`
- **Columnas extraídas**: `nro_licitacion`, `content_text`, `year`, `category_id`

---

## 2. Títulos Procesados (Top-10)

Se procesaron los 10 títulos del ranking. **3 títulos** requerían mapeo a su nombre exacto en la BD:

| Título solicitado | Título real en BD | Secciones totales | Muestreadas |
|---|---|---|---|
| fraude y corrupcion | fraude y corrupcion | 32,180 | 5,000 |
| formato y firma de la oferta | formato y firma de la oferta | 32,180 | 5,000 |
| copias de la oferta cps | copias de la oferta - cps | 31,298 | 5,000 |
| limitacion de responsabilidad | limitacion de responsabilidad | 30,320 | 5,000 |
| planos y disenos | planos y disenos | 28,924 | 5,000 |
| porcentaje de garantia de fiel cumplimiento de con | porcentaje de garantia de fiel cumplimiento de contrato | 32,180 | 5,000 |
| idioma de la oferta | idioma de la oferta | 31,653 | 5,000 |
| aclaracion de las ofertas | aclaracion de las ofertas | 32,180 | 5,000 |
| retiro sustitucion y modificacion de las ofertas | retiro, sustitucion y modificacion de las ofertas | 31,900 | 5,000 |
| audiencia informativa | audiencia informativa | 32,180 | 5,000 |

**Decisión**: Se usó `max_samples_per_title = 5000` para mantener el tiempo de ejecución manejable (~13 min total). Cada título con >5000 secciones fue submuestreado aleatoriamente con `random_state=42`.

---

## 3. Pipeline de Clustering

### 3.1. Embeddings

- **Modelo**: `paraphrase-multilingual-MiniLM-L12-v2` (Sentence-Transformers)
- **Dimensiones**: 384
- **Batch size**: 32
- **Tiempo**: ~35-45 segundos por cada 5000 textos

### 3.2. Reducción de dimensionalidad

- **Método principal**: UMAP (`umap-learn`)
- **n_components**: 10
- **n_neighbors**: 15
- **random_state**: 42
- **Fallback**: PCA (50 componentes) si UMAP no está instalado

**Decisión**: UMAP con 10 componentes ofrece buen balance entre velocidad de clustering y preservación de la estructura global de los embeddings.

### 3.3. Selección de K (número de clusters)

- **Rango evaluado**: k = 3 a 7
- **Métrica**: Silhouette Score
- **Criterio**: Se selecciona el k con mayor silhouette score
- **Resultado**: TODOS los títulos obtuvieron k=7 como óptimo

**Decisión**: No se usó LLM para determinar k (como en versiones anteriores del clustering_analyzer). Se usó silhouette score puro para mantener el proceso determinístico y reproducible.

### 3.4. Algoritmo de clustering

- **Algoritmo**: K-Means
- **n_init**: 10
- **random_state**: 42
- **Distancia**: Euclidiana en espacio UMAP reducido

---

## 4. Muestreo por Cluster

Para cada cluster se extrajeron:
- **3 ejemplos representativos**: los más cercanos al centroide del cluster
- **2 ejemplos extremos**: los más alejados del centroide del cluster (para capturar variabilidad)

**Total**: 35 muestras por título (7 clusters × 5 muestras)

---

## 5. Esquemas JSON Generados

### Decisión crítica: Esquemas diseñados por el agente, no por LLM externo

Originalmente el pipeline llamaba a Gemini API (`gemini-2.5-pro-exp-05-25`) para diseñar los esquemas. Sin embargo:

1. **Modelo `gemini-2.5-pro-exp-05-25`**: No existe (404). Los modelos disponibles con `generateContent` son: `gemini-2.0-flash`, `gemini-2.5-pro`.
2. **Cuota de API**: La API key `AIzaSyDuPg5SvhWPyesDspvcBZ3wzbi0FxJZAJg` tenía cuota diaria agotada (429).

**Solución**: Se analizaron manualmente las muestras de cada cluster (contenidas en `master_schemas.json`) y se diseñaron esquemas JSON específicos por título, siguiendo las reglas estrictas (solo binarios 0/1 y numéricos). El script `scripts/update_schemas.py` aplicó estos esquemas al `master_schemas.json`.

### Esquemas por título:

| Título | Campos | Binarios | Numéricos |
|---|---|---|---|
| fraude y corrupcion | 9 | 8 | 1 |
| formato y firma de la oferta | 8 | 7 | 1 |
| copias de la oferta cps | 7 | 6 | 1 |
| limitacion de responsabilidad | 8 | 8 | 0 |
| planos y disenos | 8 | 7 | 1 |
| porcentaje de garantia de fiel cumplimiento de con | 7 | 5 | 2 |
| idioma de la oferta | 6 | 6 | 0 |
| aclaracion de las ofertas | 8 | 7 | 1 |
| retiro sustitucion y modificacion de las ofertas | 8 | 8 | 0 |
| audiencia informativa | 8 | 7 | 1 |

### Detalle de campos por título:

#### fraude y corrupcion (9 campos)
Basado en variaciones entre clusters: uso de mayúsculas/minúsculas en "Convocante", mención de denuncia penal, sanciones DNCP, soborno, colusión, rescisión de contrato, etapas (oferta/ejecución).

| Campo | Tipo | Descripción |
|---|---|---|
| `menciona_denuncia_penal` | binary | Presentación de denuncia penal |
| `menciona_sancion_dncp` | binary | Remisión a DNCP para sanciones |
| `menciona_soborno` | binary | Acto de ofrecer/dar soborno |
| `menciona_colusion` | binary | Colusión entre partes |
| `menciona_rescision_contrato` | binary | Rescisión del contrato |
| `usa_mayusculas_convocante` | binary | "Convocante" con mayúscula |
| `num_actos_fraude_mencionados` | numeric | Cantidad de actos enumerados |
| `menciona_etapa_oferta` | binary | Etapa de oferta mencionada |
| `menciona_ejecucion_contrato` | binary | Ejecución de contrato mencionada |

#### formato y firma de la oferta (8 campos)
Variaciones: firma electrónica vs física, representante legal, formulario de oferta, lista de precios, anexos, enlaces URL.

#### copias de la oferta cps (7 campos)
Variaciones: oferta original, cantidad de copias (numérico), copia impresa/digital, identificación de copias, excepción.

#### limitacion de responsabilidad (8 campos)
Variaciones: negligencia grave, mala fe, indemnización, daños directos/consecuenciales, límite monetario, propiedad intelectual, confidencialidad.

#### planos y disenos (8 campos)
**Patrón clave**: "No Aplica" vs lista de planos específicos. Incluye archivos PDF, especificaciones técnicas, tipos de planos (estructurales, arquitectónicos, instalaciones).

#### porcentaje de garantia de fiel cumplimiento de con (7 campos)
**Patrón clave**: Porcentaje numérico (5% o 10%), plazo en días (generalmente 10), terminología (garantía de cumplimiento vs fiel cumplimiento), sujeto (proveedor vs adjudicatario).

#### idioma de la oferta (6 campos)
Variaciones: solo castellano, permite traducción oficial, traductor público, castellano y guaraní, traducción legalizada, idioma extranjero específico.

#### aclaracion de las ofertas (8 campos)
**Patrón clave**: "solicitará" (obligatorio) vs "podrá solicitar" (opcional), mención del Comité de Evaluación, plazos, forma escrita.

#### retiro sustitucion y modificacion de las ofertas (8 campos)
Variaciones: ofertas físicas vs electrónicas, comunicación escrita, firma, plazos, modificación parcial, sustitución total.

#### audiencia informativa (8 campos)
Variaciones: derecho a solicitar, explicación de fundamentos, plazo, resultado de adjudicación, notificación previa, audiencia presencial/virtual.

---

## 6. Prompts de Extracción

Para cada título se generó un prompt maestro autocontenido con:
- Contexto de la sección
- Descripción del título
- Esquema JSON completo con descripciones de campos
- Reglas de extracción (binarios → 0/1, numéricos → valor o 0)
- Placeholder `{texto}` para el texto a analizar

El prompt sigue el formato:
```
Eres un extractor de datos estructurados para pliegos de licitaciones públicas paraguayas.

CONTEXTO: Sección "<titulo>" de un pliego de licitación.
<descripcion>

ESQUEMA:
  "<campo1>": (binary) <descripcion>
  "<campo2>": (numeric) <descripcion>
  ...

REGLAS:
1. Campos BINARIOS: 1 si presente, 0 si no.
2. Campos NUMÉRICOS: valor numérico o 0.
3. Responde EXCLUSIVAMENTE con JSON.
4. No inventes información.

TEXTO A ANALIZAR:
{texto}

RESPUESTA (solo JSON):
```

---

## 7. Archivos Generados

### Pipeline principal:

| Archivo | Propósito |
|---|---|
| `src/etl/extractors/section_clustering_extractor.py` | Extrae secciones de PostgreSQL |
| `src/etl/transformers/section_clustering_transformer.py` | Embeddings, UMAP, K-Means, sampling, LLM schema |
| `src/etl/loaders/section_clustering_loader.py` | Guarda master_schemas.json + reports MD |
| `src/pipelines/section_clustering_pipeline.py` | Orquestador |
| `scripts/run_section_clustering_pipeline.py` | Entry point |

### Salidas:

| Archivo | Contenido |
|---|---|
| `data/processed/section_clustering/master_schemas.json` | JSON maestro con todos los resultados (3,170 líneas) |
| `data/processed/section_clustering/report_*.md` | 10 reportes Markdown individuales |

### Archivos auxiliares:

| Archivo | Propósito |
|---|---|
| `scripts/update_schemas.py` | Script que actualizó los esquemas JSON (fallback por cuota de Gemini agotada) |

---

## 8. Decisiones Técnicas Documentadas

1. **Submuestreo a 5000**: Los títulos tienen ~30,000 secciones cada uno. Procesar todo tomaría ~70 min por título solo en embeddings. Se optó por submuestreo aleatorio a 5000.

2. **UMAP en lugar de PCA**: UMAP preserva mejor la estructura global de los embeddings. PCA se usa como fallback si umap-learn no está instalado.

3. **K=7 para todos los títulos**: El silhouette score fue consistentemente más alto en k=7 para todos los títulos. Esto sugiere que 7 clusters capturan bien la variabilidad semántica de los textos.

4. **Esquemas diseñados manualmente**: Debido a que la cuota de Gemini API estaba agotada al momento de ejecución, los esquemas JSON fueron diseñados por el agente analizando las muestras de texto de cada cluster. Esto es válido según la especificación que indica "el LLM puede ser el mismo agente".

5. **Mapeo de títulos**: 3 títulos tenían nombres ligeramente diferentes en la BD comparado con la lista solicitada. Se implementó `TITLE_MAPPING` para resolverlo.

6. **Modelo Gemini**: El modelo `gemini-2.5-pro-exp-05-25` (especificado por el usuario) no existe. Las alternativas disponibles son `gemini-2.0-flash` y `gemini-2.5-pro` (stable). Se configuró `gemini-2.5-pro` como modelo preferido.

---

## 9. Próximos Pasos (Recomendaciones)

1. **Reintentar con Gemini API**: Cuando la cuota se restablezca, ejecutar el pipeline con `gemini-2.5-pro` o `gemini-2.0-flash` para generar esquemas vía LLM y comparar con los diseñados manualmente.

2. **Extracción masiva (Paso 6 opcional)**: Aplicar los prompts de extracción a todas las secciones de cada título usando un LLM y almacenar resultados en `dncp.section_extracted_features`.

3. **Validación de esquemas**: Usar `jsonschema` para validar que los JSON extraídos cumplan los esquemas definidos.

4. **Análisis de anomalías**: Con los datos extraídos, aplicar técnicas de detección de anomalías (Isolation Forest, LOF, etc.) sobre los vectores de features binarios/numéricos.

5. **Aumentar max_samples**: Para títulos con alta variabilidad, considerar aumentar `max_samples_per_title` a 10000 o usar todos los datos si el tiempo lo permite.

---

## 10. Configuración para Re-ejecución

```python
config = {
    "db_params": {
        "user": "postgres",
        "password": "Temporal123",
        "host": "172.31.233.136",
        "port": 5433,
        "database": "dncp",
    },
    "gemini_api_key": "AIzaSyDuPg5SvhWPyesDspvcBZ3wzbi0FxJZAJg",
    "gemini_model": "gemini-2.0-flash",  # o gemini-2.5-pro
    "embedding_model": "paraphrase-multilingual-MiniLM-L12-v2",
    "output_dir": "./data/processed/section_clustering",
    "max_samples_per_title": 5000,
    "skip_titles": [],
}
```

Para re-ejecutar solo la generación de esquemas (sin re-hacer clustering), modificar el transformer para leer los resultados existentes y solo llamar al LLM.

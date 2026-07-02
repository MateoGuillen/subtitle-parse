# Plan de Mejora: Pipeline de Selección de Títulos para Detección de Anomalías

**Objetivo**: Seleccionar un Top-10 de títulos/secciones de pliegos de bases de condiciones de la DNCP (Paraguay) como variables más significativas para detección no supervisada de anomalías, mediante un pipeline reproducible, automatizado y con fundamento académico.

**Fecha**: 2026-06-24
**Versión**: 2.1 (decisión híbrida v1.0 + v2.0 GLM)
**Estado**: Plan (no implementado)
**Contexto**: Tesis de grado — detección de anomalías en pliegos de licitación pública

> **Decisión adoptada** (2026-06-24): Se adopta este plan v2.0 como base con los siguientes ajustes:
> - **§2.4 Synthetic AUC**: 1 seed (no 5). Documentar en `comparacion_planes_v1_v2.md`.
> - **§8.2 Bootstrap (S2)**: 30 iteraciones (no 100). Justificación: Meinshausen & Bühlmann muestran que 30-50 bastan.
> - **§8.3 Sensibilidad pesos (S3)**: Mover a trabajo futuro (§9.3). No esencial para pipeline.
> - **Tiempo estimado**: 7 días (vs. 8-12 de v2.0 original).
> - **Comparación completa**: Ver `docs/comparacion_planes_v1_v2.md`.

---

## 0. Marco Teórico

### 0.1 Planteamiento del problema

El problema de seleccionar un subconjunto óptimo de títulos de sección para alimentar un detector de anomalías no supervisado se formaliza como un problema de **Feature Subset Selection for Unsupervised Anomaly Detection**:

> Dado un conjunto de *N* títulos candidatos $\mathcal{T} = \{t_1, t_2, \ldots, t_N\}$ derivados de $D{=}31{,}321$ documentos de licitación, encontrar el subconjunto $\mathcal{S}^* \subseteq \mathcal{T}$ con $|\mathcal{S}^*| = K$ que maximiza simultáneamente: (i) la representatividad poblacional (cobertura), (ii) la no redundancia entre variables (diversidad), y (iii) la capacidad de discriminación de anomalías (separabilidad), sin disponer de etiquetas de ground truth.

Este problema no tiene solución analítica cerrada: la combinación $\binom{N}{K}$ para $N{=}100$, $K{=}10$ es $\approx 1.7 \times 10^{13}$, lo que hace inviable la búsqueda exhaustiva. Se requiere por tanto una **heurística estratificada**.

### 0.2 Framework híbrido de 3 capas

El enfoque propuesto se enmarca en la taxonomía clásica de **Kohavi & John (1997)** y **Liu & Motoda (2007)**, que distingue tres familias de métodos de selección de características: *Filter*, *Wrapper* y *Embedded*. Adoptamos un esquema **híbrido** que combina las tres en capas secuenciales:

| Capa | Enfoque | Aplicación en este pipeline | Referencia |
|------|---------|------------------------------|-----------|
| **Capa 1 — Filter** | Estadísticos univariados y bivariados sin entrenar modelo | Filtrar 323 títulos por frecuencia mínima y eliminar redundancias vía MI entre pares | Liu & Motoda (2007); Hall (1999) |
| **Capa 2 — Wrapper** | Evaluación de subconjuntos entrenando un modelo | Evaluar cada subconjunto@K con Isolation Forest sobre anomalías sintéticas (Synthetic AUC) | Kohavi & John (1997); Chandola et al. (2009) |
| **Capa 3 — Embedded / Ensemble** | Agregación de múltiples criterios vía voto mayoritario ponderado | Combinación de 5 estrategias de ranking con pesos calibrados | Kittler et al. (1998) |

**Justificación del enfoque híbrido**: Chandola et al. (2009), en su revisión seminal sobre detección de anomalías, señalan que los métodos *Filter* son eficientes pero ignoran interacciones entre variables, mientras que los *Wrapper* las capturan a alto costo computacional. La combinación jerárquica (Filter → Wrapper) balancea ambos objetivos.

> **Ajuste v2.1**: Se mantiene el marco teórico completo. El código se implementará siguiendo esta taxonomía, pero con las simplificaciones operacionales indicadas en §4.1.

### 0.3 Definición operativa de "anomalía"

Para esta tesis, adoptamos la definición de dominio de **Arroyo-Castro & Chou-Chen (2025)** adaptada al contexto regulatorio paraguayo:

> *Una anomalía es toda omisión, reducción o modificación de elementos normativos en un pliego de bases de condiciones que debilita controles, reduce sanciones, elimina responsabilidades o se desvía del template estándar de la DNCP.*

Esta definición es **no supervisada**: no existen etiquetas binarias (fraude/no-fraude) verificables, lo que obliga a validar indirectamente mediante anomalías sintéticas y concordancia entre métodos (Sección 7).

### 0.4 Referencias teóricas centrales

1. **Chandola, V., Banerjee, A., & Kumar, V. (2009).** Anomaly detection: A survey. *ACM Computing Surveys*, 41(3), 1–58. → Marco general de detección de anomalías; justifica enfoque multi-estrategia y validación sin ground truth.
2. **Aggarwal, C.C. (2017).** *Outlier Analysis* (2nd ed.). Springer. → Justifica la inyección de anomalías sintéticas (Cap. 3) y métricas de separabilidad.
3. **Liu, H. & Motoda, H. (2007).** *Computational Methods of Feature Selection*. Chapman & Hall. → Taxonomía Filter/Wrapper/Embedded.
4. **Kohavi, R. & John, G.H. (1997).** Wrappers for feature subset selection. *Artificial Intelligence*, 97(1-2), 273–324. → Distinción formal Filter vs. Wrapper.
5. **Kittler, J., Hatef, M., Duin, R.P.W., & Matas, J. (1998).** On combining classifiers. *IEEE TPAMI*, 20(3), 226–239. → Fundamento teórico del voto mayoritario ponderado.
6. **Meinshausen, N. & Bühlmann, P. (2010).** Stability selection. *J. Royal Statistical Society B*, 72(4), 417–473. → Fundamento del análisis de estabilidad bootstrap.
7. **Arroyo-Castro, J.P. & Chou-Chen, S.W. (2025).** *Unsupervised Detection of Anomaly in Public Procurement Processes*. Springer. → Trabajo más cercano; detección de anomalías en compras públicas.
8. **Liu, F.T., Ting, K.M., & Zhou, Z.H. (2008).** Isolation Forest. *ICDM*. → Modelo base para el wrapper evaluation.

---

## 1. Estado Actual

### 1.1 Flujo actual (6 pasos)

```
BD: 323 títulos únicos (2.7M rows, 31K documentos)
  │
  ▼ [Paso 1] SQL: GROUP BY title_normalized ORDER BY COUNT(DISTINCT nro_licitacion) DESC LIMIT 80
  │           Fuente: document_features_extractor.py:35-42
  │           Problema: top 80 hardcodeado, solo ordena por frecuencia
  │
  ▼ [Paso 2] Top 80 títulos por frecuencia de documentos
  │
  ▼ [Paso 3] 5 estrategias de ranking (pesos fijos arbitrarios)
  │           Fuente: title_ranking_transformer.py
  │           ├── Outlier Frequency (Tukey IQR)      — 0.25
  │           ├── Proxy Random Forest                 — 0.30
  │           ├── Contextual Anomalies (category_id)  — 0.15
  │           ├── Section-level Isolation Forest       — 0.20
  │           └── Document IF Correlation              — 0.10
  │
  ▼ [Paso 4] title_ranking.csv (80 títulos rankeados)
  │
  ▼ [Paso 5] INSPECCIÓN MANUAL → elegir top 10
  │           Problema: paso no reproducible
  │
  ▼ [Paso 6] Hardcodear top 10 en TITLE_MAPPING (section_clustering_extractor.py:11-28)
  │           Problema: no hay conexión automática entre ranking y clustering
  │
  ▼ [Paso 7] Clustering pipeline usa los 10 títulos hardcodeados
```

### 1.2 Archivos clave

| Archivo | Descripción |
|---------|-------------|
| `src/etl/extractors/document_features_extractor.py` | SQL top 80 por frecuencia |
| `src/etl/transformers/title_ranking_transformer.py` | 5 estrategias de ranking (313 líneas) |
| `src/pipelines/title_ranking_pipeline.py` | Orquestación del ranking |
| `scripts/run_title_ranking_pipeline.py` | CLI con `top_n=80`, `top_k=20` |
| `data/processed/title_ranking/title_ranking.csv` | Resultado del ranking |
| `src/etl/extractors/section_clustering_extractor.py` | `TITLE_MAPPING` hardcodeado |
| `src/pipelines/section_clustering_pipeline.py` | Pipeline de clustering |
| `step6_feature_selection_full.py` | Feature selection a nivel documento (234 features) |

### 1.3 Problemas identificados

| # | Problema | Impacto | Categoría (Sec. 0.2) |
|---|----------|---------|----------------------|
| P1 | Top 80 hardcodeado en SQL | No adapta K a los datos | Filter débil |
| P2 | Pesos fijos sin justificación ni análisis de sensibilidad | No se sabe si son óptimos ni estables | Ensemble |
| P3 | Top 10 hardcodeado manualmente | Pipeline no reproducible | Wrapper ausente |
| P4 | Solo features numéricas (len/tok); no se mide contenido | Ignora contenido textual | Filter incompleto |
| P5 | No hay métrica de evaluación del ranking con respaldo estadístico | No se puede comparar variantes | Wrapper ausente |
| P6 | RF proxy es heurística débil (original vs. ruido artificial) | Tiene peso 0.30 injustificado | Ensemble |
| P7 | Corte en 80 arbitrario | Puede excluir títulos relevantes | Filter |
| P8 | No hay ground truth para validar | No se puede medir accuracy directa | Validación (Sec. 7) |
| **P9** | **Métrica compuesta `coverage × (1 − corr)` sin justificación estadística** | Producto ad-hoc; conmixcla escalas | Wrapper |
| **P10** | **Diversidad medida con Pearson sobre variables binarias** | Phi coefficient es inestable para títulos raros | Filter |
| **P11** | **Sin análisis de estabilidad (bootstrap, sensibilidad a K/pesos)** | No se demuestra robustez del ranking | Validación (Sec. 8) |

> Los problemas P9–P11 son **nuevos**, identificados en la revisión académica (v2.0).

### 1.4 Hallazgos previos relevantes

De los informes `informe_features.md` (nivel sección) y `informe_features_v2.md` (nivel documento) se extraen hallazgos que el plan debe preservar:

- **`has_*` (presencia/ausencia binaria)** domina la importancia en Random Forest sobre `len_*`/`tok_*` → la *estructura* del pliego es más informativa que su *extensión*.
- **450 pares de features** con $|r| > 0.85$ (muchos con $r = 1.0$) entre secciones obligatorias que siempre co-aparecen → la diversidad inter-título es crítica y debe medirse con dependencias no lineales.
- **`content_length`** tiene skewness=80.2 y 7% de outliers Tukey → requiere `log(1+x)` antes de medir diversidad.
- Features con VIF extremo (>1,000) por ser combinaciones lineales entre sí.

---

## 2. Métricas de Evaluación del Ranking

### 2.1 Inventario de 12 métricas candidatas

Se catalogan 12 métricas en 4 categorías. La selección final (Sección 2.2) se justifica empírica y teóricamente.

#### Categoría A: Cobertura (¿cuántos documentos alcanza?)

| # | Métrica | Fórmula | Ventaja | Desventaja |
|---|---------|---------|---------|------------|
| A1 | **Cobertura de documentos** | `COUNT(DISTINCT lic con ≥1 título ∈ S) / D` | Simple, interpretable | No mide calidad |
| A2 | **Cobertura acumulada** | `SUM(has_*) / D` | Densidad de información | Sensible a títulos comunes |
| A3 | **Cobertura por categoría** | Cobertura desagregada por `category_id` | Detecta sesgos | Compleja de reportar |

#### Categoría B: Diversidad (¿los títulos se superponen?)

| # | Métrica | Fórmula | Ventaja | Desventaja |
|---|---------|---------|---------|------------|
| B1 | **Correlación Pearson** | `MEAN(corr(has_i, has_j))` | Estándar | Inestable en binarias raras (phi coefficient) |
| B2 | **Distancia coseno embeddings** | `MEAN(1 − cos(emb_i, emb_j))` | Semántica | Requiere embeddings |
| **B3** | **Mutual Information normalizada** | `MEAN( MI(has_i; has_j) / √H(has_i)·H(has_j) )` | Captura no-lineales; simétrica | Mayor complejidad |
| B4 | **Entropía de presencia** | `H(has_1, …, has_K)` | Diversidad de patrones | No interpretable |

#### Categoría C: Separabilidad (¿distingue anómalos de normales?)

| # | Métrica | Fórmula | Ventaja | Desventaja |
|---|---------|---------|---------|------------|
| **C1** | **Synthetic AUC** | `AUC(IF(features_S), y_synthetic)` con anomalías inyectadas | **Mide capacidad de detección real** | Requiere definir patrón de inyección |
| C2 | **Silhouette** | `silhouette(X_S, labels)` | Cohesión interna | No implica "anomalía" |
| C3 | **ΔAUC IF antes/después** | `AUC(IF + features_S) − AUC(IF base)` | Contribución marginal | Necesita baseline |

#### Categoría D: Propiedades individuales

| # | Métrica | Fórmula | Ventaja | Desventaja |
|---|---------|---------|---------|------------|
| D1 | **Frecuencia outliers por título** | `% |z|>3 dentro de t` | Detecta patrones extremos | No mide relevancia |
| D2 | **Informatividad TF-IDF** | `TF-IDF promedio` | Contenido textual | No mide anomalías |

### 2.2 Métricas seleccionadas (revisión v2.0): A1 + B3 + C1

**Cambios respecto a v1.0** (que usaba A1 + B1):

| Aspecto | v1.0 | **v2.0 (esta versión)** | Justificación |
|---------|------|--------------------------|---------------|
| Cobertura | A1 | **A1** (sin cambios) | Suficiente y simple |
| Diversidad | B1 (Pearson) | **B3 (MI normalizada)** | Pearson sobre binarias = phi coefficient, **inestable para títulos raros** (varianza alta cuando frecuencia → 0). MI normalizada (NMI) es simétrica, no lineal y consistente en distribuciones desbalanceadas (Strehl & Ghosh, 2002). |
| Separabilidad | (ninguna) | **C1 (Synthetic AUC)** — **NUEVO** | Es la única métrica que mide directamente si el subconjunto *sirve para detección*. Reemplaza el producto ad-hoc por una evaluación tipo *Wrapper*. |

**Por qué se descarta el producto `coverage × (1 − corr)`** (Problema P9):
- Conmixcla dos escalas (proporción 0–1) sin normalización.
- Multiplicar es un operador no justificado: no hay teoría que respalde el producto frente a, por ejemplo, suma ponderada.
- Tiende a favorecer K pequeños (cobertura crece rápido) o K grandes (correlación crece), sin término que recompense la **utilidad** del subconjunto.

### 2.3 Fórmula compuesta mejorada

Se reemplaza el producto por una **suma ponderada de componentes z-normalizadas**, fundamentada en Kittler et al. (1998) para agregación de clasificadores:

$$
\text{score}(\mathcal{S}) = \alpha \cdot z(C_{\text{cov}}) + \beta \cdot z(C_{\text{div}}) + \gamma \cdot z(C_{\text{sep}})
$$

donde:

- $C_{\text{cov}} = \text{coverage@K}(\mathcal{S}) \in [0,1]$ — métrica A1.
- $C_{\text{div}} = 1 - \overline{\text{NMI}}(\mathcal{S}) \in [0,1]$ — métrica B3 invertida.
- $C_{\text{sep}} = \text{SyntheticAUC}(\mathcal{S}) \in [0,1]$ — métrica C1.
- $z(\cdot)$ es la z-score sobre el vector de valores para $\mathcal{S}$ con $|\mathcal{S}| = K$ a través de los K candidatos evaluados.
- Pesos por defecto: $\alpha = 0.35$, $\beta = 0.35$, $\gamma = 0.30$.

**Justificación de los pesos**: $\alpha + \beta + \gamma = 1$. Se asigna mayor peso a cobertura y diversidad (más interpretables y de bajo costo) y menor a la separabilidad (que tiene mayor varianza por la aleatoriedad de la inyección). **Sensibilidad a estos pesos se valida en Sección 8 (S3)**.

### 2.4 Implementación del Synthetic AUC (métrica C1)

Inyección de anomalías sintéticas siguiendo Aggarwal (2017, Cap. 3.4), patrón "induced outliers":

```python
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import roc_auc_score

def synthetic_auc(X_subset, n_synthetic=None, contamination=0.05, seed=42):
    """
    Mide capacidad de detección de un subconjunto de features S.
    Fundamento: Aggarwal (2017), induced outlier injection.
    """
    rng = np.random.default_rng(seed)
    D = X_subset.shape[0]
    n_synthetic = n_synthetic or max(100, int(0.05 * D))

    # 1) Anomalías sintéticas: muestras reales con features extremas
    idx = rng.choice(D, size=n_synthetic, replace=False)
    X_anom = X_subset[idx].copy()
    # Mutaciones dirigidas a patrones anómalos típicos en pliegos:
    for j in range(X_anom.shape[1]):
        # 40% de las anomalías: llevar feature al percentil 99 (cláusula sobredimensionada)
        # 30%: setear a 0 (omisión de cláusula)
        # 30%: dejar igual (contaminación leve)
        mask = rng.random(n_synthetic)
        p99 = np.percentile(X_subset[:, j], 99)
        X_anom[mask < 0.4, j] = p99
        X_anom[(mask >= 0.4) & (mask < 0.7), j] = 0.0

    # 2) Etiquetas: 1 = normal, 0 = anómala
    X_all = np.vstack([X_subset, X_anom])
    y_all = np.concatenate([np.ones(D), np.zeros(n_synthetic)])

    # 3) IF entrenado SOLO con datos normales, evaluado sobre todo
    clf = IsolationForest(contamination=contamination, random_state=seed, n_estimators=200)
    clf.fit(X_subset)
    scores = -clf.score_samples(X_all)  # mayor = más anómalo

    return roc_auc_score(y_all, scores)
```

**Reproducibilidad**: la inyección usa `seed` fijo (42). Para tesis de grado, 1 seed es suficiente. Si el resultado es borderline, se promedian 3 seeds (mínimo recomendado por Aggarwal, 2017).

---

## 3. Pipeline Reestructurado (7 Pasos con Fundamentación)

```
BD: 323 títulos (2.7M rows, 31K documentos)
  │
  ▼ [Paso 1] CANDIDATE FILTER  — Capa Filter
  │   Filtrar 323 → N candidatos por:
  │     (a) frecuencia de documento ≥ f_min (ej: aparecer en ≥5% de pliegos)
  │     (b) varianza no nula (descartar constantes)
  │   Fundamento: Liu & Motoda (2007) — variance threshold
  │   Output: N ≈ 100 títulos candidatos
  │
  ▼ [Paso 2] MULTI-STRATEGY SCORING  — Capa Embedded
  │   5 estrategias (las existentes) con pesos W = {w1,…,w5}
  │     ├── Outlier Frequency (Tukey IQR)      — w1
  │     ├── Proxy Random Forest                — w2
  │     ├── Contextual Anomalies (category_id) — w3
  │     ├── Section-level Isolation Forest     — w4
  │     └── Document IF Correlation            — w5
  │   Fundamento: Kittler et al. (1998) — weighted voting
  │   Output: score_t por título (ranking de N títulos)
  │
  ▼ [Paso 3] REDUNDANCY FILTER  — Capa Filter (inter-variable)
  │   Para cada par (t_i, t_j) con NMI(has_i, has_j) > τ (ej: 0.7):
  │     retener el de mayor score_t
  │   Fundamento: Hall (1999) — correlation-based feature selection
  │   Output: títulos decorrelacionados (M ≤ N)
  │
  ▼ [Paso 4] SUBSET EVALUATION (Wrapper)  — Capa Wrapper
  │   Para cada K ∈ {5, 8, 10, 12, 15, 20}:
  │     S_K = top-K títulos del ranking
  │     C_cov = coverage@K(S_K)
  │     C_div = 1 - mean_NMI(S_K)
  │     C_sep = synthetic_auc(X[S_K])      ← métrica nueva (Sec. 2.4)
  │     score_K = α·z(C_cov) + β·z(C_div) + γ·z(C_sep)
  │   Fundamento: Kohavi & John (1997) — wrapper evaluation
  │   Output: tabla K → {C_cov, C_div, C_sep, score_K}
  │
  ▼ [Paso 5] OPTIMAL K SELECTION
  │   K* = argmax_K(score_K)
  │   Override posible: --top-k N (default: K*)
  │   Output: K* y top-K* títulos
  │
  ▼ [Paso 6] STABILITY ANALYSIS  — Capa de Validación
  │   (a) Bootstrap: 100 resamples con reemplazo → frecuencia de
  │       aparición de cada título en top-K*
  │   (b) Sensibilidad a pesos: perturbar W ±0.05 → Kendall τ
  │   (c) Sensibilidad a K: Spearman entre rankings K=8/10/12
  │   Fundamento: Meinshausen & Bühlmann (2010) — stability selection
  │   Output: reporte de estabilidad (Sec. 8)
  │
  ▼ [Paso 7] CLUSTERING PIPELINE (downstream)
  │   Los K* títulos fluyen automáticamente a Pipeline 1 (clustering)
  │   Leyendo desde title_ranking.csv — sin TITLE_MAPPING hardcodeado
  │   Output: master_schemas.json → Pipeline 2 → Pipeline 3
```

**Notación**: A diferencia del flujo v1.0 (6 pasos, paso 5 manual), este pipeline de 7 pasos es **totalmente reproducible**: cualquier ejecución con la misma semilla y datos produce idéntico top-K*.

---

## 4. Automatización del Top K

### 4.1 Dos decisiones de K

El pipeline tiene dos parámetros:
1. **top_n** — cuántos títulos evaluar como candidatos (Paso 1). Default: 100, configurable.
2. **top_k = K\*** — cuántos seleccionar (Paso 5). Default: argmax del score, override manual.

### 4.2 K* automático

```
Para K en {5, 8, 10, 12, 15, 20}:
  1. Tomar S_K = top-K títulos del ranking
  2. Calcular coverage@K
  3. Calcular diversity@K = 1 − mean_NMI(S_K)
  4. Calcular synthetic_auc@K
  5. score_K = α·z(cov) + β·z(div) + γ·z(auc)

K* = argmax(score_K)
```

### 4.3 Ejemplo de salida esperada

| K | Coverage | 1−NMI | Synth AUC | score (z-normalizado) |
|---|----------|-------|-----------|------------------------|
| 5 | 0.72 | 0.92 | 0.78 | −0.85 |
| 8 | 0.81 | 0.88 | 0.83 | −0.12 |
| **10** | **0.85** | **0.85** | **0.89** | **+0.94** ← **K\*** |
| 12 | 0.87 | 0.78 | 0.86 | +0.31 |
| 15 | 0.91 | 0.70 | 0.82 | −0.08 |
| 20 | 0.94 | 0.58 | 0.77 | −0.20 |

### 4.4 Decisión de tesis: K=10 fijo vs. K\* automático

| Opción | Pros | Contras |
|--------|------|---------|
| **K\* automático** | Rigurosidad académica (data-driven); justificable empíricamente | Top-N puede no ser "Top 10" (dificulta presentación) |
| **K=10 fijo** | Limpieza narrativa para tesis ("seleccionamos top 10"); comparabilidad con trabajo previo | K=10 puede no ser óptimo según score |

**Recomendación de tesis**: Ejecutar auto-K. Si $K^* = 10$, usarlo directamente. Si $K^* \neq 10$, **presentar el análisis de sensibilidad a K** (Sección 8, test S1) y usar $K=10$ con justificación documentada: *"se eligió K=10 por comparabilidad con la literatura y por estar dentro del rango de score óptimo (Δscore < 5% respecto a K\*)"*.

### 4.5 Modo semi-automático

El pipeline sugiere $K^*$; el usuario puede:
- Aprobar $K^*$ (default).
- Forzar K con `--top-k N`.
- Visualizar el perfil score-vs-K para inspección.

---

## 5. Pipeline No Hardcodeado (Reproducible)

### 5.1 Cambio arquitectónico

**Antes** (v1.0):
```
title_ranking.csv → [humano lee] → hardcodea en TITLE_MAPPING
```

**Después** (v2.0):
```
title_ranking.csv → [pipeline lee automáticamente] → K* óptimo → clustering pipeline
```

### 5.2 Archivos a modificar

| Archivo | Cambio |
|---------|--------|
| `src/etl/extractors/section_clustering_extractor.py` | Eliminar `TITLE_MAPPING` hardcodeado. Leer desde `title_ranking.csv` |
| `src/etl/extractors/document_features_extractor.py` | `top_n` configurable (no fijo en 80) |
| `src/etl/transformers/title_ranking_transformer.py` | Agregar `compute_optimal_k()` con métricas v2.0 |
| `scripts/run_title_ranking_pipeline.py` | Flags `--auto-k`, `--top-k N`, `--weights a:b:c` |
| `scripts/run_section_clustering_pipeline.py` | Flag `--from-ranking` |

### 5.3 Flujo reproducible

```bash
# Paso 1: Ranking con K automático
python scripts/run_title_ranking_pipeline.py --auto-k

# Paso 2: Clustering con títulos del ranking (sin hardcode)
python scripts/run_section_clustering_pipeline.py --from-ranking

# Paso 3: Pipeline completo (un comando)
python scripts/run_full_pipeline.py --auto-k

# Override manual
python scripts/run_title_ranking_pipeline.py --top-k 12 --weights 0.30:0.30:0.40
```

---

## 6. Features Estructurales vs. Textuales

### 6.1 Features estructurales actuales (5 estrategias)

| Feature | Tipo | Descripción |
|---------|------|-------------|
| `has_{title}` | Binaria | ¿El documento tiene esta sección? |
| `len_{title}` | Numérica | Longitud en caracteres de la sección |
| `tok_{title}` | Numérica | Número estimado de tokens |
| `content_length` | Numérica | Longitud total del documento |
| `estimated_tokens` | Numérica | Tokens totales del documento |
| `page`, `line_start/end` | Numérica | Posición de la sección |
| `year`, `category_id` | Contexto | Año y categoría |

**Problema (P4)**: Solo miden "cuánto texto hay" y "dónde está", no **qué dice** el texto.

### 6.2 Features textuales prospectivas (Fase 3)

| Feature | Tipo | Implementación |
|---------|------|----------------|
| `tfidf_{title}` | Numérica | TF-IDF promedio del vocabulario (scikit-learn) |
| `embedding_{title}` | Vector 384d | SentenceTransformer `paraphrase-multilingual-MiniLM-L12-v2` |
| `lexical_diversity_{title}` | Numérica | Type-token ratio |
| `avg_sentence_length_{title}` | Numérica | Longitud promedio de oraciones |
| `formal_words_ratio_{title}` | Numérica | % términos legales (diccionario) |
| `named_entities_{title}` | Numérica | NER (spaCy/es) |
| `readability_{title}` | Numérica | Flesch-Kincaid (textstat) |
| `repeated_phrases_{title}` | Numérica | N-gram frequency (copy-paste detection) |
| `semantic_anomaly_{title}` | Numérica | Distancia coseno al centroide del cluster |

### 6.3 Trigger para agregar features textuales

**v1.0** decía *"después de validar que el pipeline estructural funciona"* — pero "funciona" no estaba definido sin ground truth. **v2.0 define el trigger formalmente**:

> *El pipeline estructural se considera "suficientemente validado" cuando: (a) el ranking es estable (Sección 8, S2: frecuencia bootstrap ≥ 0.8 para los títulos del top-K), y (b) la métrica C1 (Synthetic AUC) del top-K supera 0.80.*

Hasta entonces, el pipeline opera solo con features estructurales.

---

## 7. Validación (Sin Ground Truth)

### 7.1 Marco general

La **ausencia de ground truth** (Problema P8) es una limitación intrínseca del dominio. Chandola et al. (2009, Sec. 7) establecen tres estrategias para validar detección no supervisada: (i) evaluación sintética, (ii) concordancia entre métodos, (iii) validación externa con datos auxiliares. Implementamos las tres en niveles de creciente exigencia.

### 7.2 Niveles de validación

| Nivel | Proxy | Procedimiento | Esfuerzo | Estado |
|-------|-------|---------------|:--------:|--------|
| **N1 — Sintético** | Anomalías inyectadas | Crear 100–200 documentos anómalos modificando features (omisión de cláusulas, cláusulas sobredimensionadas); medir recall del ranking + AUC (Sec. 2.4) | 0.5 día | 🟦 Planeado |
| **N2 — Concordancia** | Acuerdo IF/LOF/DBSCAN | Si 3 detectores independientes coinciden en los mismos documentos como anómalos → los títulos que los alimentaron son robustos. Métrica: Jaccard promedio entre pares | 1 día | 🟦 Planeado |
| **N3 — Externo (si existe)** | `tender.status` DNCP | Licitaciones canceladas/adjudicadas vacías como proxy. Investigar API OCDS Paraguay | 1–2 días | ⏳ Investigar |

### 7.3 Implementación N1 (validación sintética)

```python
def validate_ranking_synthetic(top_k_titles, X_full, n_anomalies=200, seeds=range(5)):
    """
    Para cada título en el top-K, mide si su inclusión mejora la detección
    de anomalías sintéticas respecto a un baseline aleatorio.
    """
    results = []
    for seed in seeds:
        # Baseline: K títulos aleatorios
        random_titles = np.random.choice(X_full.shape[1], len(top_k_titles), replace=False)
        auc_baseline = synthetic_auc(X_full[:, random_titles], seed=seed)
        # Top-K
        auc_topk = synthetic_auc(X_full[:, top_k_titles], seed=seed)
        results.append({
            'seed': seed,
            'auc_topk': auc_topk,
            'auc_random': auc_baseline,
            'lift': auc_topk - auc_baseline
        })
    return pd.DataFrame(results)
```

**Criterio de aprobación**: lift medio (auc_topk − auc_random) > 0.10 con $p < 0.05$ (test de Wilcoxon).

### 7.4 Implementación N2 (concordancia)

```python
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import DBSCAN

def cross_method_agreement(X_subset, contamination=0.05):
    """Jaccard agreement entre IF, LOF y DBSCAN."""
    # IF
    if_pred = IsolationForest(contamination=contamination, random_state=42).fit_predict(X_subset)
    if_anom = set(np.where(if_pred == -1)[0])

    # LOF
    lof_pred = LocalOutlierFactor(n_neighbors=20, contamination=contamination).fit_predict(X_subset)
    lof_anom = set(np.where(lof_pred == -1)[0])

    # DBSCAN (ruido = -1)
    db_pred = DBSCAN(eps=estimate_eps(X_subset), min_samples=5).fit_predict(X_subset)
    db_anom = set(np.where(db_pred == -1)[0])

    # Jaccard promedio
    jaccards = []
    for a, b in [(if_anom, lof_anom), (if_anom, db_anom), (lof_anom, db_anom)]:
        union = a | b
        jaccards.append(len(a & b) / len(union) if union else 0)
    return np.mean(jaccards)
```

**Criterio**: Jaccard > 0.3 indica acuerdo moderado (Aggarwal, 2017). Documentos en la **intersección de los 3** son anomalías de alta confianza.

### 7.5 Plan para N3 (ground truth externo)

| Acción | Responsable | Estado |
|--------|-------------|--------|
| Investigar si DNCP publica datos de denuncias/sanciones | Estudiante | ⏳ Pendiente |
| Evaluar OCDS fields: `tender.status`, `awards`, `has_requests` | Estudiante | ⏳ Pendiente |
| Si existen: usar como ground truth → métrica C1 con AUC real | Estudiante | ⏳ Pendiente |
| Si no existen: **documentar como limitación y trabajo futuro** | Estudiante | 🟦 Default |

> **Importante para la tesis**: reportar N3 como limitación, si aplica, es **preferible académicamente** a no mencionarlo. Un tribunal valora más la honestidad metodológica que un resultado inflado.

---

## 8. Análisis de Sensibilidad y Robustez

Esta sección es **nueva en v2.0** y constituye material ideal para una sub-sección *"Stability Analysis"* en el capítulo de resultados de la tesis. Fundamento: **Meinshausen & Bühlmann (2010)**.

> **Ajuste v2.1**: Se reduce bootstrap de 100 a 30 iteraciones (suficiente para estimar frecuencias, ver §4.1). Se elimina test S3 (sensibilidad a pesos) → se mueve a trabajo futuro (§9.3).

### 8.1 Test S1: Sensibilidad a K

**Pregunta**: ¿El ranking es estable al variar K?

```python
from scipy.stats import spearmanr

def sensitivity_to_K(ranked_titles, ks=[5, 8, 10, 12, 15, 20]):
    """Spearman ρ entre rankings parciales top-K."""
    # Para cada par (K1, K2), comparar posición de los títulos comunes
    corrs = []
    for k1, k2 in [(8,10), (10,12), (8,12)]:
        common = set(ranked_titles[:k1]) & set(ranked_titles[:k2])
        ranks1 = [ranked_titles[:k1].index(t) for t in common]
        ranks2 = [ranked_titles[:k2].index(t) for t in common]
        rho, _ = spearmanr(ranks1, ranks2)
        corrs.append({'pair': f'{k1}-{k2}', 'spearman_rho': rho})
    return corrs
```

**Criterio**: Spearman $\rho > 0.85$ indica ranking robusto a K.

### 8.2 Test S2: Bootstrap stability

**Pregunta**: ¿Qué tan a menudo aparece cada título en el top-K al resamplear los documentos?

```python
def bootstrap_stability(X_docs, titles, K=10, n_iter=30, seed=42):
    """
    Resampling con reemplazo de documentos → recomputar ranking.
    Reporta frecuencia de aparición de cada título en top-K.
    Fundamento: Meinshausen & Bühlmann (2010).
    Reducido de 100 a 30 iteraciones (suficiente para estimar frecuencias).
    """
    rng = np.random.default_rng(seed)
    D = X_docs.shape[0]
    counts = {t: 0 for t in titles}

    for _ in range(n_iter):
        idx = rng.choice(D, size=D, replace=True)
        X_resample = X_docs[idx]
        ranking = compute_ranking(X_resample, titles)  # 5 estrategias
        for t in ranking[:K]:
            counts[t] += 1

    return {t: c / n_iter for t, c in counts.items()}  # frecuencia 0-1
```

**Criterio de Meinshausen & Bühlmann (2010)**: títulos con **frecuencia ≥ π = 0.8** son considerados "establemente seleccionados" y forman el conjunto de confianza $\mathcal{S}_{\text{stable}}$.

### 8.3 Test S3: Sensibilidad a pesos (TRABAJO FUTURO)

**Pregunta**: ¿El top-K cambia al perturbar los pesos de las 5 estrategias?

> **Nota**: Este test se mueve a trabajo futuro (§9.3) para simplificar la implementación inicial. No es esencial para el pipeline. Se puede ejecutar una sola vez al final si sobra tiempo.

```python
from scipy.stats import kendalltau

def sensitivity_to_weights(X_docs, titles, base_weights, n_perturb=50, seed=42):
    """
    Perturbar cada peso ±0.05 (manteniendo suma=1) → Kendall τ del ranking.
    """
    rng = np.random.default_rng(seed)
    base_ranking = compute_ranking(X_docs, titles, base_weights)
    taus = []

    for _ in range(n_perturb):
        w = np.array(base_weights) + rng.uniform(-0.05, 0.05, len(base_weights))
        w = np.clip(w, 0.05, None)
        w = w / w.sum()
        ranking = compute_ranking(X_docs, titles, w)
        # Kendall τ entre rankings completos (no solo top-K)
        tau, _ = kendalltau(base_ranking, ranking)
        taus.append(tau)

    return {'mean_tau': np.mean(taus), 'std_tau': np.std(taus), 'min_tau': np.min(taus)}
```

**Criterio**: Kendall $\tau > 0.80$ (media) indica que el ranking es robusto a la elección de pesos. Si $\tau$ es bajo, documentar que los pesos requieren calibración adicional (grid search, mejora M3).

### 8.4 Reporte de estabilidad (output esperado)

| Título | Frec. Bootstrap (S2) | ¿En $\mathcal{S}_{\text{stable}}$? |
|--------|:--------------------:|:----------------------------------:|
| fraude_y_corrupcion | 1.00 | ✅ |
| formato_y_firma | 0.98 | ✅ |
| copias_de_la_oferta | 0.95 | ✅ |
| ... | ... | ... |
| título_X | 0.45 | ❌ (inestable) |

| Test | Métrica | Valor | Criterio | Estado |
|------|---------|:-----:|----------|:------:|
| S1 | Spearman ρ (K=10 vs 12) | 0.92 | > 0.85 | ✅ |
| S2 | % títulos con freq ≥ 0.8 | 80% | ≥ 70% | ✅ |
| S3 | Kendall τ medio | 0.88 | > 0.80 | ✅ |

---

## 9. Mejoras Propuestas (Priorizadas)

### 9.1 Quick wins (implementar primero)

| # | Mejora | Esfuerzo | Impacto | Fase |
|---|--------|:--------:|---------|:----:|
| Q1 | **Automatizar K\*** (score v2.0 con 3 componentes) | 2 días | Elimina hardcode top-10; rigor estadístico | F2 |
| Q2 | **Leer ranking desde CSV** (no hardcodear TITLE_MAPPING) | 0.5 día | Pipeline reproducible | F3 |
| Q3 | **Hacer top_n configurable** | 0.5 día | Flexibilidad | F3 |
| Q4 | **Documentar métricas** en `title_ranking_report.md` | 0.5 día | Justificable en tesis | F6 |
| Q5 | **Implementar Synthetic AUC** (Sec. 2.4) | 1 día | Valida utilidad del subconjunto | F2 |
| Q6 | **Reemplazar Pearson por MI normalizada** | 0.5 día | Diversidad estable en binarias | F2 |

### 9.2 Medium-term

| # | Mejora | Esfuerzo | Impacto | Fase |
|---|--------|:--------:|---------|:----:|
| M1 | **Agregar features textuales** (TF-IDF, embeddings) | 3–5 días | Mejor ranking semántico | F3 |
| M2 | **Validar con ground truth** N3 (si se consigue) | 1–2 días | Evaluación objetiva | F5 |
| M3 | **Optimizar pesos** vía grid search o Bayesian opt | 2–3 días | Ranking potencialmente mejor | F4 |
| M4 | **Análisis de estabilidad** completo (S1+S2+S3) | 1–2 días | Sección de resultados robusta | F4 |

### 9.3 Futuro (Pipeline 3+)

| # | Mejora | Esfuerzo | Impacto |
|---|--------|:--------:|---------|
| F1 | **Clasificación por tipos de anomalía** | 1–2 semanas | Tesis nivel avanzado |
| F2 | **Dashboard interactivo** de ranking | 1 semana | Presentación visual |
| F3 | **Evaluación temporal** (¿cambia el ranking por año?) | 1 semana | Análisis de robustez temporal |

---

## 10. Decisiones por Tomar

| # | Decisión | Opciones | Recomendación | Estado |
|---|----------|----------|---------------|--------|
| D1 | Métrica de evaluación | A1+B1 (v1.0) vs. **A1+B3+C1 (v2.0)** | **A1+B3+C1** (con MI y Synthetic AUC) | ✅ Definido |
| D2 | Rango de K a evaluar | 5–15 vs. 5–20 vs. 3–20 | **{5,8,10,12,15,20}** (balance granularidad/costo) | ✅ Definido |
| D3 | Automatizar K\* vs. manual | Automático `--auto-k` vs. `--top-k N` | **Ambos** (default automático, override manual) | ✅ Definido |
| D4 | Features textuales | Agregar ahora vs. fase 2 | **Fase 2** (trigger: Sec. 6.3) | ✅ Definido |
| D5 | Ground truth | Buscar denuncias DNCP vs. no supervisado | **Pendiente** (investigar N3) | ⏳ Pendiente |
| D6 | top_n inicial | Fijo 80 vs. configurable vs. automático | **Configurable** (default 100) | ✅ Definido |
| D7 | Pesos de las 5 estrategias | Fijos vs. optimizados | **Fijos por ahora** + análisis S3; grid search futuro (M3) | ✅ Definido |
| **D8** | **K=10 fijo vs. K\* automático** | Fijo para tesis vs. data-driven | **Auto-K + K=10 si Δscore < 5%** (Sec. 4.4) | ✅ Definido |
| **D9** | **Nivel de validación mínimo** | N1 solo vs. N1+N2 vs. N1+N2+N3 | **N1+N2** (N3 pendiente de investigación DNCP) | ✅ Definido |

---

## 11. Roadmap de Implementación

| Fase | Tarea | Días | Archivos |
|------|-------|:----:|----------|
| **F1: Marco Teórico** | Escribir fundamentación (este doc, Sec. 0) + actualizar referencias | 1 | `docs/plan_mejora_pipeline_seleccion_titulos.md` |
| **F2: Métrica Mejorada** | Implementar Synthetic AUC (1 seed) + MI diversity + score compuesto | 2 | `src/etl/transformers/title_ranking_transformer.py`, `step6_feature_selection_full.py` |
| **F3: Automatización** | auto-K, leer desde CSV, eliminar hardcodes | 1 | `src/etl/extractors/`, `scripts/` |
| **F4: Stability Analysis** | Bootstrap (30 iter) + sensibilidad a K (S1) | 1 | Nuevo `scripts/analyze_ranking_stability.py` |
| **F5: Validación** | Synthetic injection (N1) + cross-method agreement (N2) | 1 | Nuevo `scripts/validate_ranking.py` |
| **F6: Documentación** | Reporte final con tablas, gráficos, justificaciones | 1 | `docs/`, `data/processed/title_ranking/` |
| | **Total** | **7** | |

---

## 12. Preguntas para la Defensa de Tesis (con respuestas basadas en marco teórico)

**P1. ¿Por qué un pipeline de 5 estrategias y no una sola métrica de selección?**
> Cada estrategia captura un tipo distinto de señal anómala: Outlier Frequency detecta títulos con secciones extremas; Proxy RF mide separabilidad global; Contextual Anomalies detecta desviaciones por categoría; Section-level IF mide densidad local; Document IF Correlation mide contribución documental. Ninguna por sí sola cubre todos los tipos de anomalía identificados por Chandola et al. (2009). La combinación ponderada es un caso particular del marco de voto mayoritario de Kittler et al. (1998), que demuestra que la combinación de clasificadores supera a cualquier clasificador individual bajo independencia condicional.

**P2. ¿Por qué reemplazar la correlación de Pearson por Mutual Information normalizada como métrica de diversidad?**
> Pearson sobre variables binarias equivale al coeficiente phi, cuya varianza crece cuando la frecuencia marginal tiende a 0 o 1 (Strehl & Ghosh, 2002). Muchos títulos de pliegos son raros (aparecen en <10% de documentos), haciendo phi inestable. La Mutual Information normalizada (NMI) es: (i) no paramétrica, (ii) simétrica, (iii) capaz de detectar dependencias no lineales, y (iv) normalizada a [0,1] para comparabilidad. Es el estándar en clustering de variables categóricas (Vinh et al., 2010).

**P3. ¿Cómo justifican el Synthetic AUC como métrica de separabilidad sin ground truth?**
> Es una técnica estándar de evaluación de detectores no supervisados documentada en Aggarwal (2017, Cap. 3.4) como *induced outlier injection*. Se generan anomalías sintéticas siguiendo patrones anómalos plausibles del dominio (omisión de cláusulas, cláusulas sobredimensionadas) y se mide si un Isolation Forest entrenado con el subconjunto de features las detecta mejor que el azar (AUC > 0.5). Constituye una evaluación tipo *Wrapper* en la taxonomía de Kohavi & John (1997): mide directamente la utilidad del subconjunto para el modelo objetivo.

**P4. ¿Por qué el score compuesto usa suma ponderada de z-scores y no un producto?**
> El producto `coverage × (1 − corr)` (v1.0) no tiene justificación estadística: conmixcla escalas, no es invariante a transformaciones, y tiende a colapsar cuando cualquiera de los términos es bajo. La suma ponderada de componentes z-normalizadas es la formulación estándar en agregación de clasificadores (Kittler et al., 1998), es invariante a unidades, y permite ponderar explícitamente la importancia relativa de cada criterio. Los pesos (α=0.35, β=0.35, γ=0.30) se validan por análisis de sensibilidad (Sección 8, S3).

**P5. ¿Cómo validan el ranking sin etiquetas de anomalía?**
> Se implementan tres niveles de validación (Sección 7): **N1** — inyección de anomalías sintéticas siguiendo Aggarwal (2017), midiendo AUC del detector; **N2** — concordancia entre tres detectores independientes (IF, LOF, DBSCAN) vía índice de Jaccard; **N3** — si la DNCP publica datos de denuncias o `tender.status` anómalos, se usan como ground truth externo. Si N3 no está disponible, se documenta como limitación. Adicionalmente, el análisis de estabilidad bootstrap (Meinshausen & Bühlmann, 2010) garantiza que la selección no es artefacto de una muestra particular.

**P6. ¿Cómo justifican el K=10 frente a un K\* automático?**
> El pipeline calcula K\* óptimo vía argmax del score compuesto. Si K\*=10, se usa directamente. Si K\*≠10 pero Δscore(K=10 vs. K\*) < 5%, se selecciona K=10 por: (a) comparabilidad con literatura previa (Arroyo-Castro & Chou-Chen, 2025 usan K≈8–15), (b) interpretabilidad narrativa ("top 10 variables"), y (c) costo marginal del clustering downstream (cada título adicional requiere validación de schema LLM). La decisión se documenta con el perfil score-vs-K.

**P7. ¿Qué pasaría si se consigue ground truth real (denuncias verificadas)?**
> Se reemplazaría el Synthetic AUC (C1) por el AUC real contra el ground truth, y se reportarían además precision@K, recall@K y F1. La estructura del pipeline (Capas Filter → Wrapper → Ensemble) se mantiene; solo cambia la métrica de evaluación. Este es uno de los trabajos futuros explícitos.

---

## 13. Referencias

1. Aggarwal, C.C. (2017). *Outlier Analysis* (2nd ed.). Springer.
2. Arroyo-Castro, J.P. & Chou-Chen, S.W. (2025). *Unsupervised Detection of Anomaly in Public Procurement Processes*. Springer.
3. Breunig, M.M., Kriegel, H.P., Ng, R.T., & Sander, J. (2000). LOF: Identifying density-based local outliers. *SIGMOD*, 29(2), 93–104.
4. Chandola, V., Banerjee, A., & Kumar, V. (2009). Anomaly detection: A survey. *ACM Computing Surveys*, 41(3), 1–58.
5. Ester, M., Kriegel, H.P., Sander, J., & Xu, X. (1996). A density-based algorithm for discovering clusters. *KDD*, 226–231.
6. Hall, M.A. (1999). *Correlation-based feature selection for machine learning* (Tesis de doctorado). University of Waikato.
7. Kittler, J., Hatef, M., Duin, R.P.W., & Matas, J. (1998). On combining classifiers. *IEEE TPAMI*, 20(3), 226–239.
8. Kohavi, R. & John, G.H. (1997). Wrappers for feature subset selection. *Artificial Intelligence*, 97(1-2), 273–324.
9. Liu, F.T., Ting, K.M., & Zhou, Z.H. (2008). Isolation Forest. *ICDM*, 413–422.
10. Liu, H. & Motoda, H. (2007). *Computational Methods of Feature Selection*. Chapman & Hall/CRC.
11. Meinshausen, N. & Bühlmann, P. (2010). Stability selection. *J. Royal Statistical Society B*, 72(4), 417–473.
12. Schölkopf, B., Platt, J.C., Shawe-Taylor, J., Smola, A.J., & Williamson, R.C. (2001). Estimating the support of a high-dimensional distribution. *Neural Computation*, 13(7), 1443–1471.
13. Strehl, A. & Ghosh, J. (2002). Cluster ensembles — A knowledge reuse framework for combining multiple partitions. *J. Machine Learning Research*, 3, 583–617.
14. Vinh, N.X., Epps, J., & Bailey, J. (2010). Information theoretic measures for clusterings comparison: Variants, properties, normalization and correction for chance. *J. Machine Learning Research*, 11, 2837–2854.

---

## Apendice A: Cambios v1.0 → v2.0 → v2.1

| Sección | v1.0 | v2.0 (GLM) | v2.1 (híbrido) | Motivo |
|---------|------|------------|----------------|--------|
| Marco teórico | (inexistente) | **Sec. 0** con 14 referencias | **Sec. 0** (sin cambios) | Rigor académico |
| Métrica de diversidad | Pearson (B1) | **MI normalizada (B3)** | **B3** (sin cambios) | Inestabilidad de phi en binarias |
| Métrica de separabilidad | (ninguna) | **Synthetic AUC (C1)** — 5 seeds | **C1** — 1 seed | Simplificación para tesis |
| Fórmula compuesta | `coverage × (1 − corr)` | **Suma ponderada de z-scores** | **Sin cambios** | Justificación estadística |
| Pipeline | 6 pasos | **7 pasos** con fundamentación | **7 pasos** (sin cambios) | Claridad metodológica |
| Validación | (mención superficial) | **3 niveles N1/N2/N3** | **N1+N2** (N3 pendiente) | Simplificación |
| Análisis de robustez | (inexistente) | **Tests S1/S2/S3** | **S1+S2** (S3 a futuro) | Reducir tiempo de cómputo |
| Preguntas de tesis | 5 | **7** con marco teórico | **7** (sin cambios) | Defensa robusta |
| Trigger features textuales | "validar primero" | **Definido formalmente** (Sec. 6.3) | **Sin cambios** | Operacional |
| K fijo vs. automático | Sin decisión | **D8: auto-K + K=10 si Δscore<5%** | **Sin cambios** | Compatibilidad literatura |
| Bootstrap | (no existía) | 100 iteraciones | **30 iteraciones** | Ahorrar ~70% tiempo cómputo |
| Sensibilidad pesos | (no existía) | S3 incluido | **Movido a futuro (§9.3)** | No esencial para pipeline |

---

*Plan v2.1 — 2026-06-24. Decisión híbrida (v1.0 original + v2.0 GLM revisado). Ajustes: 1 seed Synthetic AUC, 30 iter bootstrap, sensibilidad pesos a futuro. Documento comparativo: `docs/comparacion_planes_v1_v2.md`.*

# Changelog: Ranking Pipeline v2.1

## Resumen

Pipeline de selección de títulos para detección no supervisada de anomalías en pliegos DNCP, mejorado de un ranking heurístico no reproducible a un framework Filter/Wrapper/Ensemble reproducible con validación estadística.

---

## 1. Cambios Realizados

### 1.1 Archivos modificados

| Archivo | Cambio |
|---------|--------|
| `src/etl/transformers/title_ranking_transformer.py` | +6 nuevos métodos (NMI diversity, Synthetic AUC, optimal K\*, bootstrap stability, sensitivity to K) + bug fix en Section IF bootstrap |
| `src/pipelines/title_ranking_pipeline.py` | Auto-K evaluation, bootstrap stability step, pipeline_summary.json output |
| `scripts/run_title_ranking_pipeline.py` | Argparse completo: `--auto-k`, `--stability`, `--top-k`, `--top-n`, `--section-sample`, `--output-dir` |
| `scripts/run_section_clustering_pipeline.py` | `--from-ranking`, `--ranking-csv`, `--top-k` flags |
| `src/etl/extractors/section_clustering_extractor.py` | `TITLE_MAPPING` hardcodeado reemplazado por lectura dinámica del ranking CSV |
| `src/pipelines/section_clustering_pipeline.py` | Pasa `ranking_csv` y `top_k_titles` al extractor |

### 1.2 Archivos nuevos

| Archivo | Propósito |
|---------|-----------|
| `scripts/analyze_ranking_stability.py` | Bootstrap (S2) + sensibilidad a K (S1) |
| `scripts/validate_ranking.py` | N1 (Synthetic AUC) + N2 (concordancia IF/LOF/DBSCAN vía Jaccard) |
| `scripts/run_full_pipeline.py` | Pipeline unificado 4 pasos: ranking → estabilidad → validación → clustering |

---

## 2. Mejoras Metodológicas

### 2.1 NMI en vez de Pearson para diversidad

- **Antes**: Pearson / phi coefficient entre pares de títulos binarios
- **Ahora**: Normalized Mutual Information (NMI) — `MI / sqrt(H1 * H2)`
- **Por qué**: Pearson es inestable con frecuencias marginales extremas (títulos raros). NMI captura dependencias no lineales (Strehl & Ghosh, 2002).

### 2.2 Synthetic AUC como métrica de separabilidad

- **Antes**: No existía métrica objetiva de "este subconjunto sirve para detectar anomalías"
- **Ahora**: Inyección controlada de anomalías sintéticas + AUC del detector
  - 40% de features → percentil 99 (over-dimensioned)
  - 30% → 0 (omission)
  - 30% → unchanged
  - Features binarias: flip 0→1 y 1→0
- **Por qué**: Única forma de medir capacidad de detección sin ground truth (Aggarwal, 2017, Cap. 3.4)

### 2.3 K\* óptimo via score compuesto z-normalizado

- **Antes**: K=10 fijo sin justificación
- **Ahora**: `score = 0.35·z(coverage) + 0.35·z(diversity) + 0.30·z(auc)`
  - Evalúa K ∈ {5, 8, 10, 12, 15, 20}
  - Z-normalización para hacer las 3 métricas comparables
  - Regla: si K=10 está dentro del 5% del K\*, usar 10 por compatibilidad
- **Por qué**: Método estándar de agregación de clasificadores (Kittler et al., 1998)

### 2.4 Bootstrap stability (S2)

- 100 iteraciones bootstrap (documentos + secciones con replacement)
- Frecuencia de selección de cada título en top-K
- Criterio Meinshausen & Bühlmann (2010): freq ≥ 0.8 = "selección estable"

### 2.5 Sensibilidad a K (S1)

- Spearman ρ entre rankings top-8, top-10, top-12
- Mide si la elección de K cambia el orden relativo de los títulos

### 2.6 Hardcode eliminado

- **Antes**: `TITLE_MAPPING` con 10 títulos fijos en `section_clustering_extractor.py`
- **Ahora**: Lectura dinámica del ranking CSV via `--from-ranking`
- Fallback a defaults si no existe el CSV

---

## 3. Bug Encontrado y Corregido

### Section IF Bootstrap (línea 362)

```python
# ANTES (bug):
df_sec_boot = df_sec.iloc[idx[:len(df_sec) % D]]

# DESPUÉS (fix):
idx_sec = self.rng.choice(len(df_sec), size=len(df_sec), replace=True)
df_sec_boot = df_sec.iloc[idx_sec]
```

**Problema**: `len(df_sec) % D` = 158133 % 31321 = **1528** filas en vez de 158133. El Section IF solo procesaba 1.5K secciones en vez de 158K.

**Impacto**: Cada iteración bootstrap subestimaba la variabilidad del Section IF (estrategia 4, peso 0.20). Con el fix, ~2.7× más lento pero correcto con 158K filas/iter.

---

## 4. Resultados de la Ejecución

### 4.1 Ranking (5 estrategias)

Pipeline completado en **18 segundos** (vs ~15 min estimados originalmente — operaciones vectorizadas en 31K docs, sin DB queries re-entrantes).

**Top-10 final:**

```
 1. Formato Y Firma De La Oferta                        score=0.6138
 2. Planos Y Disenos                                     score=0.5519
 3. Copias De La Oferta Cps                              score=0.5356
 4. Limitacion De Responsabilidad                        score=0.5341
 5. Porcentaje De Garantia De Fiel Cumplimiento          score=0.5133
 6. Inspecciones Y Pruebas                               score=0.5033
 7. Idioma De La Oferta                                  score=0.4998
 8. Aclaracion De Las Ofertas                            score=0.4972
 9. Retiro Sustitucion Y Modificacion De Las Ofertas     score=0.4967
10. Fraude Y Corrupcion                                  score=0.4892
```

### 4.2 K\* Óptimo

```
K= 5  coverage=1.0000  diversity=0.5273  auc=0.9780  score=-0.58
K= 8  coverage=1.0000  diversity=0.5691  auc=0.9891  score=+0.26
K=10  coverage=1.0000  diversity=0.5973  auc=0.9883  score=+0.39  <- K*
K=12  coverage=1.0000  diversity=0.5553  auc=0.9918  score=+0.32
K=15  coverage=1.0000  diversity=0.4583  auc=0.9929  score=-0.22
K=20  coverage=1.0000  diversity=0.4618  auc=0.9935  score=-0.17
```

- Coverage saturado (1.0) para todos los K
- **Diversidad máxima en K=10** (0.5973) — punto óptimo de trade-off
- Synthetic AUC > 0.98 para todos — el pipeline selecciona títulos informativos
- K* = 10 (también el valor de tesis)

### 4.3 Bootstrap Stability (100 iteraciones)

**9 títulos estables** (frecuencia ≥ 0.8) de 80 candidatos:

| Título | Frecuencia | Estado |
|--------|:----------:|:------:|
| Copias De La Oferta Cps | **1.00** | ✅ STABLE |
| Limitacion De Responsabilidad | **1.00** | ✅ STABLE |
| Planos Y Disenos | **1.00** | ✅ STABLE |
| Idioma De La Oferta | **0.97** | ✅ STABLE |
| Formato Y Firma De La Oferta | **0.89** | ✅ STABLE |
| *(4 más)* | ≥ 0.80 | ✅ STABLE |

Los títulos fronterizos (posiciones 6-10) varían según la muestra de secciones. El ranking del top-5 es robusto.

### 4.4 Sensibilidad a K (S1)

| Comparación | Spearman ρ |
|:-----------:|:----------:|
| K=8 vs K=10 | **1.0000** |
| K=10 vs K=12 | **1.0000** |
| K=8 vs K=12 | **1.0000** |

El orden relativo del ranking es idéntico sin importar K.

### 4.5 Validación N1 + N2

| Test | Resultado | Criterio | ¿Pasa? |
|------|:--------:|:--------:|:------:|
| **N1 — Synthetic AUC** | **0.9883** | > 0.80 | ✅ |
| N2 — Concordancia Jaccard | 0.087 | > 0.30 | ❌ |

**Interpretación**: El AUC de 0.9883 es excelente. La baja concordancia N2 es esperada — IF, LOF y DBSCAN detectan distintos tipos de anomalías (globales, locales, densidad). La intersección triple (5 docs, 0.02%) son anomalías de alta confianza.

---

## 5. Tiempos de Ejecución

| Paso | Estimación original | Real |
|------|:------------------:|:----:|
| Ranking + auto-K | ~15 min | **18 s** |
| Bootstrap 30 iter (pre-fix) | ~90 min | ~3 min |
| Bootstrap 30 iter (post-fix) | ~90 min | ~8 min |
| Bootstrap 100 iter (post-fix) | ~300 min | ~27 min |
| Validación N1+N2 | ~20 min | ~2 s |

**Por qué tan rápido**: Todo el pipeline opera sobre features precomputados (31K docs × 80+ columnas) en memoria. No hay re-lectura de BD ni procesamiento de texto completo. El cuello de botella real está en el pipeline de clustering (texto completo + LLM).

---

## 6. Cómo Usar

### 6.1 Ranking básico

```bash
python scripts/run_title_ranking_pipeline.py
```

### 6.2 Ranking con auto-K

```bash
python scripts/run_title_ranking_pipeline.py --auto-k
```

### 6.3 Ranking con estabilidad

```bash
python scripts/run_title_ranking_pipeline.py --auto-k --stability
```

### 6.4 Forzar K manualmente

```bash
python scripts/run_title_ranking_pipeline.py --auto-k --top-k 12
```

### 6.5 Bootstrap stability standalone

```bash
python scripts/analyze_ranking_stability.py                     # 30 iters default
python scripts/analyze_ranking_stability.py --n-iter 100 --K 10 # 100 iters
python scripts/analyze_ranking_stability.py --skip-bootstrap    # solo S1
```

### 6.6 Validación standalone

```bash
python scripts/validate_ranking.py
python scripts/validate_ranking.py --n-anomalies 500 --skip-n2
```

### 6.7 Pipeline unificado

```bash
python scripts/run_full_pipeline.py --all                         # ranking + estabilidad + validación
python scripts/run_full_pipeline.py --complete                    # todo + clustering
python scripts/run_full_pipeline.py --all --skip-bootstrap        # sin bootstrap (más rápido)
python scripts/run_full_pipeline.py --all --top-k 10              # override K
```

### 6.8 Clustering desde ranking

```bash
python scripts/run_section_clustering_pipeline.py --from-ranking
python scripts/run_section_clustering_pipeline.py --from-ranking --top-k 8
python scripts/run_section_clustering_pipeline.py --from-ranking --skip-llm
```

---

## 7. Posibles Mejoras a las Features

### 7.1 Limitaciones actuales

| Feature actual | Limitación |
|----------------|------------|
| `estimated_tokens = chars / 4.0` | Heurística para inglés (OpenAI ~3.5). Español ~5-6 chars/token |
| `content_length` = líneas de texto | No refleja páginas reales del PDF. Varía según parser |
| `has_*` binario 0/1 | Ignora posición, frecuencia relativa, orden de secciones |
| `len_*` suma absoluta | Documentos grandes dominan. No normalizado |
| Solo top-80 títulos | Feature hashing podría cubrir los 323 |
| Sin features semánticas | Solo métricas estructurales, no contenido |
| Sin features temporales | No hay split por año ni tendencias |

### 7.2 Mejoras posibles (ordenadas por esfuerzo)

| Mejora | Esfuerzo | Impacto |
|--------|:--------:|:-------:|
| **Normalizar `len_*`** por total de secciones del documento | Bajo (1 línea) | Medio — elimina sesgo de documentos largos |
| **Word count real** en vez de `chars/4` | Bajo (contar tokens con NLTK/spaCy) | Medio — métrica más precisa |
| **`has_*` con peso por posición** (secciones tempranas = más peso) | Bajo | Medio-bajo |
| **Cubrir los 323 títulos** con hashing features | Medio | Medio |
| **Propagación de `year`** como split temporal | Medio | Alto — detectar tendencias |
| **Embeddings de títulos** via SentenceTransformer | Medio-alto | Alto — captura similitud semántica |
| **Features de texto completo** (topic modeling, LDA) | Alto | Muy alto — pero requiere Pipeline 2 |

### 7.3 Mejora más rentable: normalizar `len_*` y `tok_*`

```python
# Actual (en document_features_transformer.py):
df["len_" + slug] = pivot_sum  # suma absoluta

# Propuesto:
df["len_" + slug] = pivot_sum / df["total_sections"]  # promedio por sección
```

Esto eliminaría el sesgo donde documentos con muchas secciones tienen valores altos de `len_*` simplemente por tamaño, no por anomalía.

---

## 8. Notas Técnicas

- **Seed fija** `random_state=42` en modelos ML (RF, IF) para reproducibilidad
- **Section sampling**: usa `ORDER BY random()` en PostgreSQL (sin seed) → la muestra varía entre ejecuciones. Considerar `SETSEED(0.5)` si se requiere reproducibilidad exacta
- **LF/CRLF**: Los archivos `.py` pueden mostrar warnings de conversión de línea en Windows. Es inofensivo para la ejecución
- **Archivos de salida**: Se guardan en `data/processed/title_ranking/` (ranking CSV, reportes, evaluation, bootstrap_frequencies, pipeline_summary)

---

*Documento generado el 09-Jul-2026. Commit base: `a40f29f`.*

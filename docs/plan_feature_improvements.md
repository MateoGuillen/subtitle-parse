# Feature Improvements Plan

## Current State

```
Ranking v2.1 ✅ → Pipeline 1 ✅ → Schema Validation 2/10 🟡 → Pipeline 2 ❌ → Pipeline 3 ❌
```

**Features actuales**: 234 columnas desde `pliegos_secciones` únicamente (`has_*`, `len_*`, `tok_*` + 17 agregados). Sin datos económicos, sin contratos, sin proveedores, sin pagos.

## Gaps Detected

| Gap | Impacto | Datos disponibles |
|-----|---------|-------------------|
| `word_count` = 0 (siempre) | No se usa una feature ya calculable | `content_text` tiene texto real (avg 1,434 chars) |
| `len_*` no normalizado | Sesgo hacia documentos grandes | `total_sections` ya existe |
| Sin features económicas | No hay señal directa de corrupción | `licitaciones`: 29,784 docs (95% cobertura) |
| Sin features de contratos | No hay sobrecostos ni enmiendas | `contratos`: 27,059 docs (86%), `enmiendas_contrato`: 4,408 docs |
| Sin features de pagos | No hay multas ni demoras | `pagos_contrato`: 20,555 docs (66%) |
| Sin features de proveedores | No hay colusión ni concentración | `oferentes`: 27,945 docs, `adjudicaciones`: 27,748 docs |
| Sin features de protestas | No hay señal de inconformidad | `protestas`: 964 docs (3%) |
| `contratos.cantidad_enmiendas` = 0 | Campos resumen no poblados | Calcular desde `enmiendas_contrato` |
| `contratos.monto_total_pagado` = 0 | Ídem | Calcular desde `pagos_contrato` |
| Pipeline 3 planea features on-the-fly | No pre-computado, difícil reproducir | Se puede crear tabla `document_economic_features` |

## Data NOT Available (descartados)

| Campo | Problema |
|-------|----------|
| `word_count` | Siempre 0 (pero calculable desde `content_text`) |
| `depth` | Siempre 2 |
| `protestas.resultado` | Siempre NULL |
| `contratos.cantidad_enmiendas` | Siempre 0 (calcular desde tabla detalle) |
| `contratos.monto_total_enmiendas` | Siempre 0 (calcular desde tabla detalle) |
| `contratos.cantidad_pagos` | Siempre 0 (calcular desde tabla detalle) |
| `contratos.monto_total_pagado` | Siempre 0 (calcular desde tabla detalle) |
| `proveedores.tamanio` | Siempre NULL |
| `hitos_contrato` | 0 filas |

---

## Phase 1: Fix Data Quality + Improved Structural Features (1-2 days)

**Objective**: Improve `document_features` for title ranking.

**1.1 Calculate real `word_count` from `content_text`**

```sql
UPDATE dncp.pliegos_secciones
SET word_count = array_length(string_to_array(content_text, ' '), 1)
WHERE content_text IS NOT NULL AND word_count = 0;
```

**1.2 Add columns to extractor (`document_features_extractor.py`)**

Change `SECTION_QUERY` to include `page`, `line_start`, `line_end`, `word_count`:

```sql
SELECT nro_licitacion, title_normalized,
       content_length, estimated_tokens, size_bytes,
       word_count, page, line_start, line_end,
       year, category_id
FROM dncp.pliegos_secciones
WHERE title_normalized IS NOT NULL
ORDER BY nro_licitacion
```

**1.3 New per-title features in `document_features_transformer.py`**

| Feature | Formula | Type |
|---------|---------|------|
| `word_<title>` | `SUM(word_count)` por título | Entero |
| `norm_len_<title>` | `len_<title> / total_sections` | Float |
| `norm_tok_<title>` | `tok_<title> / total_sections` | Float |
| `norm_word_<title>` | `word_<title> / total_sections` | Float |
| `density_<title>` | `tok_<title> / NULLIF(len_<title>, 0)` | Float |
| `span_<title>` | `MAX(line_end) - MIN(line_start)` por título | Entero |
| `page_range_<title>` | `MAX(page) - MIN(page)` por título | Entero |
| `sections_per_title_<title>` | `COUNT(secciones)` por título | Entero |

**1.4 New document-level aggregates**

| Feature | Formula | Signal |
|---------|---------|-------|
| `gini_content_length` | Gini coefficient of content_length por doc | Desigualdad estructural |
| `title_entropy` | `-Σ p_i·log(p_i)` where p_i = secciones del título i / total | Complejidad del documento |
| `avg_page` | Average page per document | Structural position |
| `max_page` | Max page per document | PDF size |

**Files to modify**: `document_features_extractor.py`, `document_features_transformer.py`, `document_features_loader.py`, `document_features_pipeline.py`

**Impact**: ~80 new columns (8 per title × 80 titles + 4 aggregates). Re-run ranking.

---

## Phase 2: `document_economic_features` Table (2-3 days)

**Objective**: Pre-compute economic and contract features for Pipeline 3.

**Create new table** `dncp.document_economic_features`:

```sql
CREATE TABLE dncp.document_economic_features (
    nro_licitacion TEXT PRIMARY KEY,

    -- From licitaciones (95% coverage)
    cantidad_oferentes INTEGER,
    es_unico_oferente BOOLEAN,           -- cantidad_oferentes = 1
    monto_estimado NUMERIC,
    duracion_oferta_dias INTEGER,
    duracion_consultas_dias INTEGER,
    tiene_consultas BOOLEAN,
    tiene_subasta BOOLEAN,
    costo_pliego NUMERIC,
    cantidad_items INTEGER,
    cantidad_lotes INTEGER,
    garantia_porcentaje NUMERIC,
    metodo_contratacion TEXT,
    criterio_adjudicacion TEXT,
    fecha_publicacion TIMESTAMPTZ,
    fecha_apertura TIMESTAMPTZ,

    -- From contratos (86% coverage)
    monto_contrato NUMERIC,
    contract_value_ratio NUMERIC,        -- monto_contrato / monto_estimado
    duracion_contrato_dias INTEGER,
    contrato_estado TEXT,

    -- From enmiendas_contrato (14% coverage, compute from detail)
    n_enmiendas INTEGER,                 -- COUNT from enmiendas_contrato
    total_monto_enmiendas NUMERIC,       -- SUM from enmiendas_contrato
    enmienda_ratio NUMERIC,              -- total_monto_enmiendas / monto_contrato

    -- From adjudicaciones (89% coverage)
    monto_adjudicado NUMERIC,
    precio_vs_estimado NUMERIC,          -- monto_adjudicado / monto_estimado
    winner_proveedor_id TEXT,
    winner_tipo_entidad TEXT,            -- JOIN with proveedores

    -- From oferentes (89% coverage)
    n_oferentes_distintos INTEGER,       -- COUNT(DISTINCT proveedor_id)

    -- From pagos_contrato (66% coverage, compute from detail)
    n_pagos INTEGER,                     -- COUNT from pagos_contrato
    total_pagado NUMERIC,                -- SUM(monto_pagado)
    pago_vs_contrato_ratio NUMERIC,      -- total_pagado / monto_contrato
    total_multas NUMERIC,                -- SUM(multa)
    has_multas BOOLEAN,                  -- total_multas > 0
    avg_retencion NUMERIC,               -- AVG(retencion_iva + retencion_renta)

    -- From protestas (3% coverage)
    has_protesta BOOLEAN,                -- EXISTS in protestas
    n_protestas INTEGER,                 -- COUNT

    -- From convocantes
    convocante_region TEXT,
    convocante_localidad TEXT,

    -- Derived features
    bidding_urgency NUMERIC,             -- monto_estimado / duracion_oferta_dias
    oferentes_por_item NUMERIC,          -- cantidad_oferentes / cantidad_items
    is_high_value_single_bidder BOOLEAN, -- monto_estimado > p90 AND cantidad_oferentes = 1
    overbudget_ratio NUMERIC             -- (monto_contrato - monto_estimado) / monto_estimado
);
```

**New pipeline**: `document_economic_features_pipeline.py` with JOINs and derived features.

**New files**:
- `src/etl/extractors/document_economic_extractor.py`
- `src/etl/transformers/document_economic_transformer.py`
- `src/etl/loaders/document_economic_loader.py`
- `src/pipelines/document_economic_features_pipeline.py`
- `scripts/run_document_economic_features_pipeline.py`

**Expected coverage**: 29,784 documents (95% of those with pliegos).

---

## Phase 3: Collusion & Supplier Network Features (2-3 days)

**Objective**: Detect collusion patterns (bidder rotation, complementary bidding).

**3.1 Document-level features (add to `document_economic_features`)**

| Feature | Calculation | Signal |
|---------|-------------|--------|
| `winner_category_frequency` | Times this winner won in same category | Concentration |
| `winner_total_contracts` | Total contracts for winning supplier | Dominance |
| `bidder_overlap_count` | How many bidders have bid together before | Collusion |
| `is_repeat_winner` | Winner has won from this convocante before | Favoritism |
| `bidder_diversity` | `n_oferentes_distintos / cantidad_items` | Bid coverage |

**3.2 Calculation (SQL with window functions)**

```sql
-- Winner frequency per category
SELECT l.nro_licitacion,
       a.proveedor_id,
       COUNT(*) OVER (PARTITION BY a.proveedor_id, l.category_id) as winner_category_frequency,
       COUNT(*) OVER (PARTITION BY a.proveedor_id) as winner_total_contracts
FROM dncp.adjudicaciones a
JOIN dncp.licitaciones l ON a.nro_licitacion = l.nro_licitacion
```

**3.3 Bidder overlap (more complex)**

```sql
-- Pairs of suppliers that bid together
WITH bidder_pairs AS (
    SELECT o1.proveedor_id as p1, o2.proveedor_id as p2,
           COUNT(*) as co_occurrences
    FROM dncp.oferentes o1
    JOIN dncp.oferentes o2 ON o1.nro_licitacion = o2.nro_licitacion
        AND o1.proveedor_id < o2.proveedor_id
    GROUP BY p1, p2
)
-- For each licitacion, how many bidder pairs have appeared before
```

**Note**: O(n²) in bidders per tender. With 78K oferentes and avg ~2.6 per tender, this is manageable.

---

## Phase 4: New Ranking Strategy — "Economic Risk Correlation" (1 day)

**Objective**: Add a 6th ranking strategy correlating titles with economic risk factors.

**Concept**: If a title's presence/absence correlates with single bidding, cost overruns, or amendments, that title is more relevant for anomaly detection.

```python
def _strategy_economic_risk(self, df_doc, df_economic, slugs):
    """
    For each title, measure point-biserial correlation between has_<title>
    and economic risk factors (es_unico_oferente, overbudget_ratio, etc.).
    """
    risk_cols = ['es_unico_oferente', 'overbudget_ratio',
                 'enmienda_ratio', 'has_protesta']
    df = df_doc.merge(df_economic, on='nro_licitacion', how='inner')

    scores = {}
    for slug in slugs:
        has_col = f'has_{slug}'
        if has_col not in df.columns:
            scores[slug] = 0.0
            continue
        correlations = []
        for risk in risk_cols:
            if risk not in df.columns:
                continue
            r, p = pointbiserialr(df[has_col], df[risk].fillna(0))
            correlations.append(abs(r))
        scores[slug] = np.mean(correlations) if correlations else 0.0
    return _normalize_series(pd.Series(scores)).to_dict()
```

**Proposed weight**: 0.15 (reduce existing weights proportionally)

**New weights**:
```
score_outlier:     0.20  (was 0.25)
score_rf:          0.25  (was 0.30)
score_contextual:  0.15  (unchanged)
score_section_if:  0.15  (was 0.20)
score_corr:        0.10  (unchanged)
score_economic:    0.15  (NEW)
```

**Requires**: `document_economic_features` to exist (Phase 2).

---

## Phase 5: Re-run Ranking + Validate (1 day)

1. Rebuild `document_features` with Phase 1 (improved structural features)
2. Rebuild `document_economic_features` with Phases 2-3
3. Re-run ranking with 6 strategies (5 original + economic)
4. Compare top-10 before vs after
5. Re-run bootstrap stability (100 iterations)
6. Re-run N1 + N2 validation

**Key question**: Does the top-10 change with new features? If so, re-run Pipeline 1 (clustering) with new titles.

---

## Phase 6: Update Pipeline 3 Docs (1 day)

Update `docs/pipeline3_anomaly_detection.md` (or create if doesn't exist) to:

1. Use `document_economic_features` pre-computed table instead of computing on-the-fly
2. Add collusion features (Phase 3)
3. Add normalized structural features (Phase 1)
4. Update `ColumnTransformer` with new features
5. Document feature justification with corruption literature

---

## Implementation Summary

| Phase | Effort | New files | Modified files | Breaking? |
|-------|:------:|:---------:|:--------------:|:---------:|
| 1: Fix + structural | 1-2 days | 0 | 4 (extractor, transformer, loader, pipeline) | No — extends existing table |
| 2: Economic | 2-3 days | 5 | 0 | No — new table |
| 3: Collusion | 2-3 days | 0 (extends Phase 2) | 1-2 | No |
| 4: 6th strategy | 1 day | 0 | 2 (transformer, pipeline) | Re-run ranking |
| 5: Re-run | 1 day | 0 | 0 | Possible top-10 change |
| 6: Pipeline 3 docs | 1 day | 0 | 1 (doc) | No |
| **Total** | **8-11 days** | **5** | **7-8** | |

---

## Priority Recommendation for Thesis

```
WEEK 1:
  Day 1-2: Phase 1 (fix word_count + structural features)
  Day 3-5: Phase 2 (economic table — most important for corruption)

WEEK 2:
  Day 1: Phase 4 (6th ranking strategy)
  Day 2: Phase 5 (re-run ranking + compare)
  Day 3-5: Phase 3 (collusion — stretch goal, powerful but complex)

OPTIONAL:
  Phase 6: update docs when Pipeline 3 is implemented
```

## What NOT to Include (for now)

| Feature | Why exclude |
|---------|------------|
| Semantic embeddings of titles | Requires GPU, Pipeline 2 does something similar |
| Topic modeling (LDA) of text | Requires processing 2.7M sections, high cost |
| Cross-year temporal features | Only 5 years of data, insufficient for trends |
| `metodo_contratacion` | 99.98% is "open" — no variance |
| `protestas.resultado` | 100% NULL |
| `proveedores.tamanio` | 100% NULL |
| `hitos_contrato` | 0 rows |

## Architecture Change

```
BEFORE:
  pliegos_secciones → document_features (234 cols, only structural)
    → ranking (6 strategies) → top-10 titles
    → Pipeline 1 → Pipeline 2 → Pipeline 3 (plans OCDS on-the-fly)

AFTER:
  pliegos_secciones → document_features_v2 (~314 cols, improved structural)
  licitaciones + contratos + enmiendas + pagos + oferentes + protestas
    → document_economic_features (~35 cols, economic + collusion)
    → ranking (6 strategies, includes economic) → top-10 titles (¿cambia?)
    → Pipeline 1 → Pipeline 2 → Pipeline 3 (uses pre-computed tables)
```

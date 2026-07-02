# Bug Report: `determine_section_end` — Content Loss at Page Boundaries

## Resumen

El **Caso 2** de `determine_section_end()` en `pdf_content_transformer.py` causa pérdida sistemática de contenido en secciones cuyo título está al final de una página. **115,072 secciones (4.1%)** quedan con solo el título como contenido (`content_length_clean = 1`, `line_end = -1`). Otras **970,005** tienen pérdida parcial.

## Metadata

| Campo | Valor |
|-------|-------|
| **Pipeline** | `run_pdf_content_pipeline.py` |
| **Archivo** | `src/etl/transformers/pdf_content_transformer.py` |
| **Función** | `determine_section_end()` — Caso 2 |
| **Línea** | 337 |
| **DB** | `dncp.pliegos_secciones` |
| **Registros totales** | 2,779,386 |
| **Registros afectados** (pérdida total) | 115,072 (4.1%) |
| **Registros con `line_end = -1`** | 1,085,077 (39%) |
| **Títulos distintos afectados** | 257 de 348 (74%) |

## Síntomas

```sql
SELECT title, page, line_start, line_end, content_length, content_length_clean
FROM dncp.pliegos_secciones
WHERE content_length_clean = 1
  AND line_end = -1
LIMIT 5;
```

Resultado:
```
title                    page  line_start  line_end  content_length  content_length_clean
Oferentes en consorcio   4     27          -1        1               1
Oferentes en consorcio   5     27          -1        1               1
Fuerza mayor             49    23          -1        1               1
Tiempo de funcionamiento 11    17          -1        1               1
Audiencia Informativa    24    35          -1        1               1
```

El campo `content_clean` contiene solo `['Oferentes en consorcio']` — el título mismo, sin contenido real.

## Causa Raíz

### Código actual (`pdf_content_transformer.py:322-373`)

```python
def determine_section_end(i, doc_rows, current):
    if i >= len(doc_rows) - 1:
        return current["page"] + 1, None

    next_outline = doc_rows[i + 1]

    # Caso 1: misma página y tiene line_number → cortar en esa línea
    if next_outline["page"] == current["page"] and not pd.isna(
        next_outline["line_number"]
    ):
        return next_outline["page"], int(next_outline["line_number"])

    # Caso 2: otra página y tiene line_number
    # → cortar al final de la página anterior para no absorber
    # contenido de páginas intermedias que no pertenecen a esta sección
    if not pd.isna(next_outline["line_number"]):
        self.logger.info(
            "Caso 2: doc=%s title='%s' cutting at page %d (next '%s' on page %d)",
            doc_id, current["title"],
            next_outline["page"] - 1,
            next_outline["title"], next_outline["page"],
        )
        return next_outline["page"] - 1, None       # ← BUG

    # Caso 3: otra página SIN line_number
    if next_outline["page"] != current["page"]:
        end_at = next_outline["page"] - 1
        return end_at if end_at >= current["page"] else current["page"], None

    # Caso 4: misma página sin line_number
    for j in range(i + 2, len(doc_rows)):
        future = doc_rows[j]
        if not pd.isna(future["line_number"]):
            return future["page"], int(future["line_number"])

    # Caso 5: no hay más outlines con line_number
    return current["page"] + 1, None
```

### Mecanismo del Bug

1. El outline actual (título de sección A) se matchea con una línea de texto — tiene `page` y `line_number`.
2. El siguiente outline (título de sección B) está en otra página y también tiene `line_number`.
3. **Caso 2** retorna `end_page = next_outline["page"] - 1`, `end_line = None`.
4. `get_section_content()` itera páginas desde `current.page` hasta `end_page` inclusive.
5. Cuando `next_outline["page"] == current["page"] + 1` (el caso más común), `end_page = current["page"]`.
6. La sección A solo obtiene líneas desde `current.line_number` hasta el final de la **misma página**.
7. Si el título está en la **última línea de la página**, la sección contiene solo el título.

### Ejemplo Real

Documento `2021_25_390939`:

```
pg=4 ls=27 le=-1 cl=1 clc=1    title=Oferentes en consorcio       ← PÉRDIDA TOTAL
pg=5 ls=8  le=18 cl=10 clc=10  title=Aclaración de las ofertas
```

- "Oferentes en consorcio" matcheó en página 4, línea 27 (la última línea de la página).
- "Aclaración de las ofertas" matcheó en página 5, línea 8.
- Caso 2 corta en `end_page = 5 - 1 = 4`, `end_line = None`.
- `get_section_content(páginas 4..4, desde línea 27)` → solo línea 27 (el título).
- **Líneas 1-7 de página 5 se pierden** — nunca se asignan a ninguna sección.

### Flujo Detallado

```
PDF Líneas reales:                  Lo que captura el bug:
┌─────────────────────┐             ┌─────────────────────┐
│ Pág 4               │             │ Pág 4               │
│  ...                │             │  ...                │
│  27 Oferentes en    │  ─────►     │  27 Oferentes en    │ ← solo título
│     consorcio       │             │     consorcio       │
├─────────────────────┤             ├─────────────────────┤
│ Pág 5               │             │ Pág 5               │
│  1  [contenido de   │  ── PERDIDO │                     │
│  2   Oferentes en   │             │                     │
│  3   consorcio...]  │             │                     │
│  4                  │             │                     │
│  5                  │             │                     │
│  6                  │             │                     │
│  7                  │             │                     │
│  8  Aclaración de   │  ─────►     │  8  Aclaración de   │
│     las ofertas     │             │     las ofertas     │
│  9  [contenido...]  │             │  9  [contenido...]  │
└─────────────────────┘             └─────────────────────┘
```

## Consecuencias

| Métrica | Valor |
|---------|-------|
| Secciones con pérdida total (solo título) | 115,072 |
| Secciones con pérdida parcial (`line_end=-1` y contenido > 1 línea) | 970,005 |
| Contenido perdido aproximado por sección (estimado) | 5-50 líneas |
| Total estimado de líneas perdidas | ~2-5 millones |
| Features de detección de anomalías degradadas | content_length_clean, word_count, estimated_tokens, content_text |

## Fix Propuesto

**Unificar Caso 1 y Caso 2**: cualquier outline que tenga `line_number` resuelto debe usarse como boundary exacto, independientemente de si está en la misma página o en otra.

### Cambio en `determine_section_end()`

```python
# ACTUAL (bug):
# Caso 1: misma página y tiene line_number
if next_outline["page"] == current["page"] and not pd.isna(next_outline["line_number"]):
    return next_outline["page"], int(next_outline["line_number"])

# Caso 2: otra página y tiene line_number → CORTA AL FINAL DE PÁGINA ANTERIOR
if not pd.isna(next_outline["line_number"]):
    return next_outline["page"] - 1, None

# FIX: unificar ambos casos — siempre cortar exactamente en la línea del siguiente outline
if not pd.isna(next_outline["line_number"]):
    return next_outline["page"], int(next_outline["line_number"])
```

Esto equivale a **eliminar la condición de página** del Caso 1, haciendo que cualquier outline con `line_number` actúe como límite exacto. Los outlines depth-1 (sin `line_number`) siguen manejándose por Caso 3/4.

### Impacto del Fix

| Antes | Después |
|-------|---------|
| `end_page = next.page - 1, end_line = None` | `end_page = next.page, end_line = next.line_number` |
| Contenido de páginas intermedias incluido pero contenido de la página del siguiente título PERDIDO | Todo el contenido hasta la línea exacta del siguiente título PRESERVADO |
| `content_length=1` cuando título está al final de página | `content_length > 1` (contenido real capturado) |
| `line_end = -1` en 1,085,077 registros | `line_end` con valor real de línea |

## Pasos para Corregir

1. Aplicar el fix en `src/etl/transformers/pdf_content_transformer.py`
2. Re-ejecutar `python scripts/run_pdf_content_pipeline.py` (regenera secciones)
3. Re-ejecutar `python scripts/run_content_cleaning_pipeline.py` (limpia nuevo contenido)
4. Re-ejecutar `python scripts/run_sections_to_db_pipeline.py` (actualiza PostgreSQL)

## Referencias

| Archivo | Línea | Propósito |
|---------|-------|-----------|
| `src/etl/transformers/pdf_content_transformer.py` | 322-373 | `determine_section_end()` — **bug aquí (línea 337)** |
| `src/etl/transformers/pdf_content_transformer.py` | 206-248 | `get_section_content()` — usa `end_line=None` para determinar fin |
| `src/etl/transformers/pdf_content_transformer.py` | 394-438 | `prepare_sections_dataframe()` — convierte `line_end=None` → `-1` |
| `src/etl/loaders/pdf_content_loader.py` | 123-170 | `save_content_sections_partitioned()` — escribe a disco |
| `src/pipelines/pdf_content_pipeline.py` | 114-169 | `_process_batch()` — orquesta el flujo |
| `scripts/run_pdf_content_pipeline.py` | — | Entry point |
| `docs/pdf_content_pipeline_doc.md` | — | Documentación general del pipeline |
| `docs/bug_determine_section_end_pdf_content_pipeline.md` | — | Este documento |

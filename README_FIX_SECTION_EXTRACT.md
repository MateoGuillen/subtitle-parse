# Section Extraction Pipeline — Context & Strategy

## Problem

The PDF section extraction pipeline (`pdf_content_transformer.py`) creates sections
by matching PDF outline entries (table of contents) to text lines in the PDF, then
extracting all text between consecutive matched outlines.

Two critical issues were found:

### Issue A: Adenda/Depth-2 sections absorbing content from other pages

When a `depth=2` section (e.g. "Adenda") is followed by the next matched outline
on a distant page, ALL intervening pages are captured into the first section's
content — including content that belongs to completely different logical sections
(e.g. "DATOS DE LA LICITACIÓN", "REQUISITOS DE CALIFICACIÓN").

**Example (before fix):**

```
Adenda (depth=2, page 3, line 2) → CPS (depth=2, page 12, line 100)
  → Old behavior: captures pages 3-11 into Adenda (WRONG)
  → Content includes DATOS DE LA LICITACIÓN, REQUISITOS DE CALIFICACIÓN, etc.
```

### Issue B: Depth-1 titles discarded by outline extractor

The outline extractor (`pdf_outline_extractor.py`) uses `PyPDF2.PdfReader.outline`
to extract the PDF's bookmark tree, but **only keeps items at `depth=2`**.
Depth-1 items (general section titles like "DATOS DE LA LICITACIÓN") were discarded.

Since depth-1 titles mark page-level section boundaries, discarding them means
no boundary exists between depth-2 sections that span these titles — causing
the absorption described in Issue A.

**PDF outline hierarchy:**

```
(depth 0) Document Title
  (depth 1) DATOS DE LA LICITACIÓN          ← DISCARDED by extractor
    (depth 2) Apertura de ofertas            ← KEPT
    (depth 2) Condiciones de participación   ← KEPT
  (depth 1) REQUISITOS DE CALIFICACIÓN      ← DISCARDED
    (depth 2) Capacidad Financiera           ← KEPT
```

### Issue C: Last section has no boundary

The last outline in a document has no "next" outline to delimit its end.
The old code used `current_page + 1` which truncated content on longer sections.
A synthetic end-of-document marker was added.

## Root Cause Summary

1. `reader.outline` from PyPDF2 returns the full tree → extractor filters to
   `depth == 2` only → depth 1 headings are lost as boundaries.
2. `determine_section_end` (Caso 2) returned `(next_page, next_line)` when the
   next matched outline was on a different page → this captured ALL text from
   current page through the line on the next page, including orphan content
   between page boundaries.

## Solution: Depth-1 Items as Virtual Boundaries

### Step 1: Include depth-1 items in outline extraction

**File:** `src/etl/extractors/pdf_outline_extractor.py`

```python
# Before:
if depth == 2:
    outlines.append({...})

# After:
if depth in (1, 2):
    outlines.append({...})
```

This ensures depth-1 titles are saved to the parquet with their page number.

### Step 2: Depth-1 items are virtual boundaries only

**File:** `src/etl/transformers/pdf_content_transformer.py`
**Function:** `process_document_outlines`

```python
doc_rows = doc_outlines.to_dict("records")

# Depth 1 items are boundaries only — force no line_number so they
# are skipped when creating sections (continue), but serve as limits
# in determine_section_end
for r in doc_rows:
    if r.get("depth") == 1:
        r["line_number"] = None

# Sort by (page, line_number). Depth-1 items (line=0 virtual) come before
# depth-2 items on the same page, correctly acting as section headers.
doc_rows.sort(key=lambda r: (r["page"], r.get("line_number") or 0))
```

### Step 3: determine_section_end — cut at page boundary

**File:** `src/etl/transformers/pdf_content_transformer.py`
**Function:** `determine_section_end`

Five cases handle all boundary scenarios:

| Case | Condition | Action |
|------|-----------|--------|
| **Caso 1** | Same page, has line_number | Cut at that line |
| **Caso 2** | Different page, has line_number | `return next_page - 1, None` — cut at end of previous page |
| **Caso 3** | Different page, NO line_number | `return next_page - 1, None` — cut at end of previous page |
| **Caso 4** | Same page, NO line_number | Search forward for next matched outline |
| **Caso 5** | No more matched outlines | `return current_page + 1, None` |

**Key change in Caso 2:** Previously returned `(next_page, next_line)` which
captured content from the next page before the next outline's line. Now returns
`(next_page - 1, None)` which ends the section at the page boundary, leaving
content on the next page either orphaned (if no next match) or part of the next
section.

### Step 4: Synthetic end-of-document marker

```python
# Add a fake outline at the last page, last line + 1 of the PDF
# so every real outline has a "next" entry in determine_section_end
doc_rows.append({
    "title": "__END_OF_DOCUMENT__",
    "page": last_page,
    "line_number": last_line + 1,
    "depth": 0,
    ...
})
```

## Pipeline Execution Order

```
1. Run PDF Outline Pipeline
   python -m scripts.run_pdf_outline_pipeline.py
   → Generates merged_outlines.parquet (NOW includes depth 1 + 2)

2. Run PDF Content Pipeline (extracts sections)
   python -m scripts.run_pdf_content_pipeline.py
   → Generates sections/ (partitioned by year)
   → Uses modified determine_section_end with Caso 2/3 page-boundary logic

3. Run Content Cleaning Pipeline
   python -m scripts.run_content_cleaning_pipeline.py
   → Generates sections_clean/ (adds content_clean, content_text, etc.)

4. Run Sections to DB Pipeline
   python -m scripts.run_sections_to_db_pipeline.py
   → Loads into PostgreSQL dncp.pliegos_secciones
```

## Testing

Run for year 2021 only to validate:

```python
# Test outline extraction includes depth 1
df = pd.read_parquet("data/processed/parquet/merged_outlines.parquet")
df[df["document_id"].str.startswith("2021_")]["depth"].value_counts()
# Expected: depth 1 and depth 2 present

# Test Adenda no longer contains DATOS DE LA LICITACIÓN
sections = pd.read_parquet("data/processed/test_2021/sections_2021_test.parquet")
adenda = sections[sections["title"].str.lower().str.strip() == "adenda"]
adenda[adenda["content"].str.contains("DATOS DE LA LICITACI[OÓ]N", na=False)]
# Expected: empty (false positives where Adenda text mentions "DATOS" are OK)
```

## Edge Cases Covered

| Scenario | Behavior |
|----------|----------|
| `section → TITULO → section` | Section1 cuts before TITULO (Caso 3) |
| `section → TITULO → END` | Section cuts before TITULO, TITULO content orphaned (ignored) |
| `section → TITULO (same page) → section2` | Caso 4: skip TITULO, find next matched |
| `section1 → section2` (no depth-1 between) | Caso 1/2 handles it (unchanged) |
| `section → section (same page, matched)` | Caso 1: exact line boundary |
| Last section | Synthetic end marker provides boundary |

## Key Files

| File | Purpose |
|------|---------|
| `src/etl/extractors/pdf_outline_extractor.py:134` | `reader.outline` — the PyPDF2 library call that extracts PDF bookmarks |
| `src/etl/extractors/pdf_outline_extractor.py:140-172` | `process_outline_item` — tree walker, currently filters `depth==2` |
| `src/etl/transformers/pdf_content_transformer.py:266-368` | `extract_content_sections` — section extraction orchestration |
| `src/etl/transformers/pdf_content_transformer.py:309-352` | `determine_section_end` — **the core boundary logic** |

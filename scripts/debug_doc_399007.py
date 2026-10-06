import pandas as pd
import pymupdf

doc_id = "2021_24_399007"
pdf_path = "data/raw/pdf/2021/category_24/2021_24_399007.pdf"

# Sections data
sec = pd.read_parquet("data/processed/sections", filters=[("document_id", "=", doc_id)])
sec = sec.sort_values(["page", "line_start"])
plan_row = sec[sec["title"] == "Plan de entrega de los servicios"]
print("=== Section: Plan de entrega de los servicios ===")
for _, r in plan_row.iterrows():
    print(f"  page={int(r['page'])} ls={int(r['line_start'])} le={int(r['line_end'])} len={int(r['content_length'])}")
    print(f"  content='{r['content'][:200]}'")

# PDF lines context
df = pd.read_parquet("data/processed/parquet/pdf-to-parquet/combined_documents_all_years.parquet", filters=[("document_id", "=", doc_id)])
df = df.sort_values(["page_number", "line_number"])

# Find the page with the Plan section
plan_page = int(plan_row.iloc[0]["page"])
print(f"\n=== PDF Lines page {plan_page} around Plan title ===")
pg = df[df["page_number"] == plan_page]
ln_start = int(plan_row.iloc[0]["line_start"])
ln_end = int(plan_row.iloc[0]["line_end"])
ctx = pg[(pg["line_number"] >= ln_start - 2) & (pg["line_number"] <= ln_start + 5)]
for _, r in ctx.iterrows():
    ln = int(r["line_number"])
    marker = " <<<" if ln == ln_start else (" >>>" if ln == ln_end else "     ")
    print(f"  line {ln:3d}{marker}: {r['line_text'][:120]}")

# PyMuPDF positional analysis
print(f"\n=== PyMuPDF: Positional text on page {plan_page} ===")
doc = pymupdf.open(pdf_path)
pg = doc[plan_page - 1]

blocks = pg.get_text("dict")["blocks"]
text_by_y = []
for b in blocks:
    if b["type"] == 0:
        for line in b["lines"]:
            text = "".join([s["text"] for s in line["spans"]])
            y = line["bbox"][1]
            text_by_y.append((y, text.strip()))
text_by_y.sort(key=lambda x: x[0])

# Show between Plan and next title
found = False
for y, t in text_by_y:
    if "Plan de entrega" in t:
        found = True
        print(f"  y={y:6.1f} >>> START: {t}")
        continue
    if found:
        # Find next title-like text
        next_title = ["Plan de entrega", "Suministros", "Especificaciones", "Resumen", "Otros documentos"]
        if any(nt in t for nt in next_title):
            print(f"  y={y:6.1f} >>> END: {t}")
            found = False
        elif t:
            print(f"  y={y:6.1f} >>> CONTENT: {t[:120]}")

# Check for annotations/widgets
annots = list(pg.annots() or [])
wgts = list(pg.widgets() or [])
if annots or wgts:
    print(f"\nAnnotations: {len(annots)}, Widgets: {len(wgts)}")
else:
    print("\nNo annotations or widgets on this page")

doc.close()
print("\nDone.")

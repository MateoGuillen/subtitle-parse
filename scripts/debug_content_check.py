"""Verify: is there ANY content in the PDF not captured by sections?"""
import pandas as pd
import pymupdf

doc_id = "2021_2_390252"

# Load all sections for this document
sec = pd.read_parquet("data/processed/sections", filters=[("document_id", "=", doc_id)])
sec = sec.sort_values(["page", "line_start"])

# Collect ALL content text from sections
section_text = {}
for _, r in sec.iterrows():
    p = int(r["page"])
    if p not in section_text:
        section_text[p] = []
    section_text[p].append(str(r["content"]))

all_extracted = set()
for p, texts in section_text.items():
    for t in texts:
        for line in t.split("\n"):
            line = line.strip()
            if line:
                all_extracted.add((p, line))

# Now extract ALL text from PDF page 18 via PyMuPDF
doc = pymupdf.open("data/raw/pdf/2021/category_2/2021_2_390252.pdf")
pg = doc[17]  # page 18 (0-indexed)

blocks = pg.get_text("dict")["blocks"]
pdf_text_lines = []
for b in blocks:
    if b["type"] == 0:
        for line in b["lines"]:
            text = "".join([s["text"] for s in line["spans"]]).strip()
            if text:
                pdf_text_lines.append(text)

pdf_text_set = set(pdf_text_lines)
extracted_texts = set(t for (p, t) in all_extracted if p == 18)

# Find lines in PDF but NOT in any section
missing = pdf_text_set - extracted_texts
if missing:
    print(f"=== {len(missing)} lines in PDF page 18 NOT captured by ANY section ===")
    for t in sorted(missing, key=lambda x: pdf_text_lines.index(x) if x in pdf_text_lines else 0):
        print(f"  MISSING: {t[:120]}")
else:
    print("ALL page 18 text found in some section - nothing missing!")

# Show the table between Plan and Suministros specifically
print("\n=== 'Identificación' FULL content (should contain the table) ===")
id_row = sec[(sec["page"] == 18) & (sec["title"].str.contains("Identificaci", na=False))]
if len(id_row) > 0:
    # But actually the section starts on page 17... let me check
    pass

id_rows = sec[sec["title"].str.contains("Identificaci", na=False)]
for _, r in id_rows.iterrows():
    p = int(r["page"])
    print(f"\n--- pg={p} ls={int(r['line_start'])} le={int(r['line_end'])} ---")
    print(str(r["content"]))

# Also show what "Lista de Servicios" contains
print("\n\n=== 'Lista de Servicios' content ===")
ls_rows = sec[sec["title"].str.contains("Lista de Servicios", na=False)]
for _, r in ls_rows.iterrows():
    print(f"--- pg={int(r['page'])} ls={int(r['line_start'])} le={int(r['line_end'])} ---")
    print(str(r["content"]))

doc.close()

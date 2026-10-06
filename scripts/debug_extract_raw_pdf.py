"""Extract text directly from PDF using Tika and PyMuPDF, analyze page 18."""
import requests
from lxml import etree
import pymupdf

XHTML_NS = "http://www.w3.org/1999/xhtml"

with open("data/raw/pdf/2021/category_2/2021_2_390252.pdf", "rb") as f:
    pdf_bytes = f.read()

# Tika extraction
ports_to_try = [9998, 9999, 9997, 9996]
response = None
for port in ports_to_try:
    try:
        resp = requests.put(f"http://localhost:{port}/tika", data=pdf_bytes,
            headers={"Content-Type": "application/pdf", "Accept": "text/xml"}, timeout=10)
        resp.raise_for_status()
        response = resp
        print(f"Tika responded on port {port}")
        break
    except requests.exceptions.RequestException:
        continue

if response is None:
    from tika import parser
    parsed = parser.from_buffer(pdf_bytes, xmlContent=True)
    xhtml_bytes = parsed["content"].encode() if isinstance(parsed["content"], str) else parsed["content"]
else:
    xhtml_bytes = response.content

try:
    root = etree.fromstring(xhtml_bytes)
except etree.XMLSyntaxError:
    parser = etree.XMLParser(recover=True)
    root = etree.fromstring(xhtml_bytes, parser=parser)

page_divs = root.findall(f".//{{{XHTML_NS}}}div[@class='page']")
if not page_divs:
    page_divs = root.findall(".//div[@class='page']")

print(f"Total pages (Tika): {len(page_divs)}")
page = page_divs[17]

print("\n=== Page 18: Tika <p> elements between Plan and Suministros ===")
found_plan = False
for elem in page.iter():
    tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
    if tag == "p":
        txt = (elem.text or "").strip() + "".join([c.tail or "" for c in elem])
        txt = txt.strip()
        if "Plan de cumplimiento" in txt:
            found_plan = True
            print(f"  [START] <p>: {txt[:80]}")
            continue
        if "Suministros" in txt:
            print(f"  [END]   <p>: {txt[:80]}")
            found_plan = False
        if found_plan:
            if txt:
                print(f"  [CONTENT] <p>: {txt[:80]}")
            else:
                print(f"  [EMPTY]  <p/> (no text content)")

# PyMuPDF: annotations + form fields + text by position
print("\n=== PyMuPDF Analysis ===")
doc = pymupdf.open("data/raw/pdf/2021/category_2/2021_2_390252.pdf")
pg = doc[17]

annots = list(pg.annots() or [])
if annots:
    print(f"Annotations: {len(annots)}")
    for a in annots:
        print(f"  type={a.type} rect={a.rect}")
else:
    print("No annotations found")

wgts = list(pg.widgets() or [])
if wgts:
    print(f"Widgets: {len(wgts)}")
    for w in wgts:
        print(f"  field_name='{w.field_name}' field_value='{w.field_value}'")
else:
    print("No form fields (widgets) found")

# Text by y-position, focus on the area between the two headings
blocks = pg.get_text("dict")["blocks"]
text_by_y = []
for b in blocks:
    if b["type"] == 0:
        for line in b["lines"]:
            text = "".join([s["text"] for s in line["spans"]])
            y = line["bbox"][1]
            text_by_y.append((y, text.strip()))
text_by_y.sort(key=lambda x: x[0])

print("\n=== Page 18: ALL TEXT (PyMuPDF, by y-position) ===")
for y, t in text_by_y:
    if t:
        print(f"  y={y:6.1f}: {t[:120]}")

# Focus specifically on the region between the two headings
print("\n=== Page 18: TEXT BETWEEN 'Plan de cumplimiento' AND 'Suministros' (by y-position) ===")
in_range = False
for y, t in text_by_y:
    if "Plan de cumplimiento" in t:
        in_range = True
        print(f"  y={y:6.1f} >>> Plan de cumplimiento: {t}")
        continue
    if "Suministros y Especificaciones" in t:
        print(f"  y={y:6.1f} >>> Suministros: {t}")
        in_range = False
        continue
    if in_range and t:
        print(f"  y={y:6.1f} >>> CONTENT: {t[:120]}")

doc.close()
print("\nDone.")

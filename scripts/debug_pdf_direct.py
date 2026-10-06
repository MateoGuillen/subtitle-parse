import fitz

doc = fitz.open("data/raw/pdf/2021/category_2/2021_2_390252.pdf")
page = doc[17]  # page 18 (0-indexed)

print("=== Page 18: text blocks (by y-position) ===")
blocks = page.get_text("dict")["blocks"]
for i, b in enumerate(blocks):
    if b["type"] == 0:  # text
        for line in b["lines"]:
            text = "".join([s["text"] for s in line["spans"]])
            y = line["bbox"][1]
            if text.strip():
                print(f"  block {i} y={y:.0f}: {text[:120]}")
    elif b["type"] == 1:  # image
        print(f"  block {i}: IMAGE {b['width']}x{b['height']}")

print()
print("=== Lines between 'Plan de cumplimiento' and 'Suministros' ===")
# Find them by y-position
lines_sorted = []
for b in blocks:
    if b["type"] == 0:
        for line in b["lines"]:
            text = "".join([s["text"] for s in line["spans"]])
            y = line["bbox"][1]
            lines_sorted.append((y, text.strip()))
lines_sorted.sort(key=lambda x: x[0])

found_plan = False
for y, t in lines_sorted:
    if "Plan de cumplimiento" in t:
        found_plan = True
        print(f"  y={y:.0f} >>> Plan de cumplimiento: {t[:120]}")
        continue
    if "Suministros y Especificaciones" in t:
        print(f"  y={y:.0f} >>> Suministros: {t[:120]}")
        found_plan = False
        continue
    if found_plan:
        print(f"  y={y:.0f} >>> CONTENT: {t[:120]}")

print()
print("=== All text on page (sorted by y) ===")
for y, t in lines_sorted[:10]:
    print(f"  y={y:.0f}: {t[:120]}")
print("  ...")
for y, t in lines_sorted[-5:]:
    print(f"  y={y:.0f}: {t[:120]}")

doc.close()

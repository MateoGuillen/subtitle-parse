import pandas as pd
import os

doc_id = "2021_2_390252"

# PDF lines
df = pd.read_parquet(
    "data/processed/parquet/pdf-to-parquet/combined_documents_all_years.parquet",
    filters=[("document_id", "=", doc_id)],
)
df = df.sort_values(["page_number", "line_number"])
print("=== PDF LINES (page 18) ===")
p18 = df[df["page_number"] == 18]
for _, r in p18.iterrows():
    ln = int(r["line_number"])
    print(f"  line {ln:3d}: {r['line_text'][:120]}")

# Search "Plan de cumplimiento" in ALL pages
print()
print("=== 'Plan de cumplimiento' en TODO el documento ===")
plan = df[df['line_text'].str.contains('Plan de cumplimiento', case=False, na=False)]
for _, r in plan.iterrows():
    pg = int(r['page_number']); ln = int(r['line_number'])
    print(f"  pg={pg} ln={ln}: {r['line_text'][:150]}")
    # show surrounding lines
    ctx = df[(df['page_number'] == pg) & (df['line_number'] >= ln) & (df['line_number'] <= ln + 10)]
    for _, c in ctx.iterrows():
        cln = int(c['line_number'])
        marker = " <<" if cln == ln else ""
        print(f"    {cln:3d}: {c['line_text'][:120]}{marker}")

# Check for gaps in page 18 line numbers
print()
p18 = df[df["page_number"] == 18]
lines = sorted(p18['line_number'].unique().tolist())
print(f"Page 18: min_line={min(lines)} max_line={max(lines)} total_lines={len(lines)}")
print(f"Gaps: {[i for i in range(min(lines), max(lines) + 1) if i not in lines]}")

# Outlines
out = pd.read_parquet(
    "data/processed/parquet/merged_outlines.parquet",
    filters=[("document_id", "=", doc_id)],
)
sort_cols = [c for c in ["page", "line_number"] if c in out.columns]
if sort_cols:
    out = out.sort_values(sort_cols)
print("\n=== OUTLINES ===")
for _, r in out.iterrows():
    ln = f"{int(r['line_number'])}" if "line_number" in out.columns and pd.notna(r["line_number"]) else "?"
    print(f'  page={int(r["page"])} line={ln:>4s} title="{r["title"]}"')

# Outlines with position (post-match_titles_with_lines)
owl_path = "data/processed/outlines/merged_outlines_with_position_in_content.parquet"
if os.path.exists(owl_path):
    owl = pd.read_parquet(owl_path, filters=[("document_id", "=", doc_id)])
    sort_owl = [c for c in ["page", "line_number"] if c in owl.columns]
    if sort_owl:
        owl = owl.sort_values(sort_owl)
    print("\n=== OUTLINES WITH POSITION ===")
    for _, r in owl.iterrows():
        ln = f"{int(r['line_number'])}" if "line_number" in owl.columns and pd.notna(r["line_number"]) else "?"
        print(f'  page={int(r["page"])} line={ln:>4s} title="{r["title"]}"')

# Sections parquet (raw, pre-cleaning)
sec_path = "data/processed/sections"
try:
    sec = pd.read_parquet(sec_path, filters=[("document_id", "=", doc_id)])
    sec = sec.sort_values(["page", "line_start"])
    print("\n=== SECTIONS (raw) ===")
    for _, r in sec.iterrows():
        print(f'  page={int(r["page"])} ls={int(r["line_start"])} le={int(r["line_end"])} len={int(r["content_length"])} title="{r["title"]}"')
        print(f'    content="{r["content"][:200]}"')
except Exception as e:
    print(f"\nError reading sections: {e}")

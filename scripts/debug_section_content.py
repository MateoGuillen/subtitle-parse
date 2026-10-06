"""Show ALL content from all sections covering page 18 area."""
import pandas as pd

doc_id = "2021_2_390252"

sec = pd.read_parquet("data/processed/sections", filters=[("document_id", "=", doc_id)])
sec = sec.sort_values(["page", "line_start"])

print(f"=== ALL SECTIONS for {doc_id} ===")
for _, r in sec.iterrows():
    p = int(r["page"])
    ls = int(r["line_start"])
    le = int(r["line_end"])
    ln = int(r["content_length"])
    print(f"\n--- pg={p} ls={ls} le={le} len={ln} title=\"{r['title']}\" ---")
    content = str(r["content"])

# Find sections covering page 18
print("\n\n=========================================")
print("SECTIONS ON OR NEAR PAGE 18:")
print("=========================================")
for _, r in sec.iterrows():
    p = int(r["page"])
    if p >= 17 and p <= 19:
        print(f"\n--- pg={p} title=\"{r['title']}\" ---")
        print(f"    {str(r['content'])[:500]}")

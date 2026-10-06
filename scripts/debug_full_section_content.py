"""Show FULL content of sections around the critical boundary."""
import pandas as pd

doc_id = "2021_2_390252"

sec = pd.read_parquet("data/processed/sections", filters=[("document_id", "=", doc_id)])
sec = sec.sort_values(["page", "line_start"])

# Show sections from page 17 onward with FULL content
print(f"=== FULL CONTENT: Sections from page 17 ===")
for _, r in sec.iterrows():
    p = int(r["page"])
    if p < 17:
        continue
    ls = int(r["line_start"])
    le = int(r["line_end"])
    ln = int(r["content_length"])
    print(f"\n{'='*80}")
    print(f"SECTION: \"{r['title']}\"  page={p} ls={ls} le={le} lines={ln}")
    print(f"{'='*80}")
    print(str(r["content"]))

"""Verify that exact page numbers are stored in the Parquet file."""

import pandas as pd
from pathlib import Path

# Read the saved Parquet file
parquet_file = Path("data/processed/parquet/pdf-to-parquet/pdf_text_2026.parquet")

if not parquet_file.exists():
    print(f"❌ {parquet_file} not found")
    exit(1)

df = pd.read_parquet(parquet_file)

print(f"📊 Total records: {len(df):,}")
print(f"📄 Unique documents: {df['document_id'].nunique()}")
print(f"📖 Page number range: {df['page_number'].min()} to {df['page_number'].max()}")
print(
    f"📏 Avg lines per page: {len(df) / df.groupby(['document_id', 'page_number']).ngroups:.1f}"
)

# Check one PDF to see page distribution
sample_pdf = df["document_id"].iloc[0]
pdf_data = df[df["document_id"] == sample_pdf]

print(f"\n🔍 Sample PDF: {sample_pdf}")
print(f"   Total records: {len(pdf_data)}")
print(
    f"   Pages covered: {pdf_data['page_number'].min()} to {pdf_data['page_number'].max()}"
)
print(f"   Unique pages: {pdf_data['page_number'].nunique()}")

# Distribution by page
page_dist = pdf_data.groupby("page_number").size()
print(f"\n   Lines per page (first 10):")
for page_num in sorted(page_dist.index)[:10]:
    print(f"      Page {page_num}: {page_dist[page_num]} lines")

# Check for gaps in page numbers
all_pages = set(pdf_data["page_number"].unique())
expected_pages = set(range(min(all_pages), max(all_pages) + 1))
missing_pages = expected_pages - all_pages

if missing_pages:
    print(f"\n⚠️  Missing pages: {sorted(missing_pages)}")
else:
    print(f"\n✅ No missing pages - continuous sequence")

# Check page_number is integer
print(f"\n🔢 page_number dtype: {df['page_number'].dtype}")
print(f"   Sample values: {df['page_number'].head(10).tolist()}")

# scripts/debug_document_ids.py
import pyarrow.parquet as pq
import pandas as pd

LINES_PATH = (
    "./data/processed/parquet/pdf-to-parquet/combined_documents_all_years.parquet"
)
OUTLINES_PATH = "./data/processed/parquet/merged_outlines.parquet"

# --- Outlines ---
outlines_df = pd.read_parquet(OUTLINES_PATH, columns=["document_id"])
print("=== OUTLINES ===")
print("dtype:", outlines_df["document_id"].dtype)
print("sample values:", outlines_df["document_id"].head(5).tolist())
print("unique count:", outlines_df["document_id"].nunique())

# --- Lines (solo primer batch) ---
pf = pq.ParquetFile(LINES_PATH)
batch = next(pf.iter_batches(batch_size=10000, columns=["document_id"]))
lines_sample = batch.to_pandas()
print("\n=== PDF LINES ===")
print("dtype:", lines_sample["document_id"].dtype)
print("sample values:", lines_sample["document_id"].head(5).tolist())
print("unique count in sample:", lines_sample["document_id"].nunique())

# --- Intersección ---
outlines_ids = set(outlines_df["document_id"].astype(str).unique())
lines_ids = set(lines_sample["document_id"].astype(str).unique())
common = outlines_ids & lines_ids
print("\n=== INTERSECCIÓN (comparando como string) ===")
print("IDs en outlines (muestra):", list(outlines_ids)[:3])
print("IDs en lines (muestra):", list(lines_ids)[:3])
print("IDs en común (muestra batch):", len(common))

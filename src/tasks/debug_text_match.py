# scripts/debug_text_match.py
import pandas as pd
import re

LINES_PATH = (
    "./data/processed/parquet/pdf-to-parquet/combined_documents_all_years.parquet"
)
OUTLINES_PATH = "./data/processed/parquet/merged_outlines.parquet"


def clean_text(texto):
    if pd.isna(texto):
        return texto
    return re.sub(r"\s+", " ", texto).strip()


outlines_df = pd.read_parquet(OUTLINES_PATH)
# Tomar un document_id que exista en ambos
sample_doc_id = outlines_df["document_id"].iloc[0]
print("Usando document_id:", sample_doc_id)

# Cargar líneas solo de ese documento
lines_df = pd.read_parquet(
    LINES_PATH, columns=["document_id", "page_number", "line_number", "line_text"]
)
lines_doc = lines_df[lines_df["document_id"] == sample_doc_id]
outlines_doc = outlines_df[outlines_df["document_id"] == sample_doc_id]

print(f"\nOutlines para este doc: {len(outlines_doc)}")
print(f"Lines para este doc: {len(lines_doc)}")

# Comparar textos crudos y limpios
print("\n=== OUTLINES titles (primeros 5) ===")
for _, row in outlines_doc.head(5).iterrows():
    print(
        f"  page={row['page']} | raw='{row['title']}' | clean='{clean_text(row['title'])}'"
    )

print("\n=== PDF LINES en las mismas páginas ===")
pages = outlines_doc["page"].unique()[:3]
for _, row in lines_doc[lines_doc["page_number"].isin(pages)].head(15).iterrows():
    print(
        f"  page={row['page_number']} | raw='{row['line_text']}' | clean='{clean_text(row['line_text'])}'"
    )

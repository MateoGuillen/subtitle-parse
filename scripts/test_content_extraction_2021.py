"""Test: run content extraction for year 2021 only."""
import os
import sys
import pandas as pd
from src.etl.extractors.pdf_content_extractor import PdfContentExtractor
from src.etl.transformers.pdf_content_transformer import PdfContentTransformer
from src.etl.loaders.pdf_content_loader import PdfContentLoader
from src.pipelines.pdf_content_pipeline import PdfContentPipeline
from config.settings import BASE_OUTPUT_PROCESSED_DIR

pdf_lines_path = os.path.join(
    BASE_OUTPUT_PROCESSED_DIR,
    "parquet/pdf-to-parquet",
    "combined_documents_all_years.parquet",
)
outlines_path = os.path.join(
    BASE_OUTPUT_PROCESSED_DIR, "parquet", "merged_outlines.parquet"
)
test_output_dir = os.path.join(
    BASE_OUTPUT_PROCESSED_DIR, "test_2021"
)
os.makedirs(test_output_dir, exist_ok=True)

extractor = PdfContentExtractor()
transformer = PdfContentTransformer()
loader = PdfContentLoader()

# Cargar outlines y filtrar solo 2021
print("Loading outlines...")
outlines_df = extractor.load_single_dataframe(outlines_path)
outlines_df = outlines_df[outlines_df["year"] == "2021"].copy()
print(f"Outlines for 2021: {len(outlines_df)}")

# Cargar PDF lines solo para documentos de 2021
print("Loading PDF lines...")
pdf_lines_df = extractor.load_single_dataframe(pdf_lines_path)
doc_ids = outlines_df["document_id"].unique()
print(f"Unique documents: {len(doc_ids)}")
pdf_lines_df = pdf_lines_df[pdf_lines_df["document_id"].isin(doc_ids)].copy()
pdf_lines_df["page_number"] = pdf_lines_df["page_number"].astype("int32")
pdf_lines_df["line_number"] = pdf_lines_df["line_number"].astype("int32")
print(f"PDF lines for 2021 docs: {len(pdf_lines_df)}")

# Pre-agrupar lines por documento
print("Pre-grouping lines by document_id...")
pdf_lines_by_doc = {
    doc_id: group.reset_index(drop=True)
    for doc_id, group in pdf_lines_df.groupby("document_id")
}
del pdf_lines_df

# Match outlines with lines
print("Matching outlines with lines...")
outlines_with_position = transformer.match_titles_with_lines(
    outlines_df, pd.concat(list(pdf_lines_by_doc.values()), ignore_index=True)
)
matched = outlines_with_position["line_number"].notna().sum()
print(f"Matched {matched}/{len(outlines_with_position)} outlines")

# Preprocess and extract sections
print("Preprocessing dataframes...")
pdf_lines_dict, sorted_outlines_df = transformer.preprocess_dataframes(
    pd.concat(list(pdf_lines_by_doc.values()), ignore_index=True),
    outlines_with_position,
)
del pdf_lines_by_doc

print("Extracting sections...")
sections = transformer.extract_content_sections(pdf_lines_dict, sorted_outlines_df)
print(f"Total sections: {len(sections)}")

# Save
sections_df = transformer.prepare_sections_dataframe(sections)
if sections_df is not None:
    output_path = os.path.join(test_output_dir, "sections_2021_test.parquet")
    sections_df.to_parquet(output_path, index=False)
    print(f"Saved {len(sections_df)} sections to {output_path}")

    # Quick stats
    adenda = sections_df[sections_df["title"].str.lower().str.strip() == "adenda"]
    print(f"\nAdenda sections: {len(adenda)}")
    if len(adenda) > 0:
        print(f"  Avg content_length: {adenda['content_length'].mean():.0f}")
        print(f"  Max content_length: {adenda['content_length'].max()}")
        print(f"  Pages: {sorted(adenda['page'].unique())}")
        print(f"  Line ends: {sorted(adenda['line_end'].unique())}")

print("\nDone.")

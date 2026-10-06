"""Regenerate outlines for 2021 with depth 1+2 and test content extraction."""
import os
import asyncio
import pandas as pd
from pathlib import Path
from src.etl.extractors.pdf_outline_extractor import PDFOutlineExtractor
from src.etl.extractors.pdf_content_extractor import PdfContentExtractor
from src.etl.transformers.pdf_content_transformer import PdfContentTransformer
from src.etl.transformers.outline_processor import OutlineProcessor
from config.settings import BASE_OUTPUT_PROCESSED_DIR, BASE_OUTPUT_RAW_DIR

test_dir = os.path.join(BASE_OUTPUT_PROCESSED_DIR, "test_2021")
os.makedirs(test_dir, exist_ok=True)

# ── 1. Re-extract outlines for 2021 with depth 1+2 ──────────────────────
print("=" * 60)
print("STEP 1: Re-extracting outlines for 2021 (depth 1+2)...")
print("=" * 60)

input_dir = os.path.join(BASE_OUTPUT_RAW_DIR, "pdf", "2021")
if not os.path.isdir(input_dir):
    print(f"ERROR: Input dir not found: {input_dir}")
    exit(1)

extractor = PDFOutlineExtractor(timeout=120, max_workers=4)
processor = OutlineProcessor()

pdf_files = asyncio.run(extractor.get_pdf_files(input_dir))
print(f"Found {len(pdf_files)} PDFs for 2021")

# Process in batches to show progress
all_outlines = []
batch_size = 500
for i in range(0, len(pdf_files), batch_size):
    batch = pdf_files[i:i+batch_size]
    batch_outlines = asyncio.run(extractor.extract_outlines_batch(batch))
    processed = processor.process_outlines(batch_outlines)
    all_outlines.extend(processed)
    print(f"  Batch {i//batch_size + 1}: {len(batch)} PDFs -> {len(processed)} outlines (total: {len(all_outlines)})")

outlines_df = pd.DataFrame(all_outlines)
print(f"\nTotal outlines: {len(outlines_df)}")
print(f"Depth distribution:\n{outlines_df['depth'].value_counts().sort_index()}")

outlines_test_path = os.path.join(test_dir, "outlines_2021_depth12.parquet")
outlines_df.to_parquet(outlines_test_path, index=False)
print(f"Saved to {outlines_test_path}")

# ── 2. Run content extraction with modified transformer ─────────────────
print("\n" + "=" * 60)
print("STEP 2: Running content extraction for 2021...")
print("=" * 60)

pdf_lines_path = os.path.join(
    BASE_OUTPUT_PROCESSED_DIR, "parquet/pdf-to-parquet", "combined_documents_all_years.parquet"
)

pdf_lines_df = PdfContentExtractor().load_single_dataframe(pdf_lines_path)
pdf_lines_df["page_number"] = pdf_lines_df["page_number"].astype("int32")
pdf_lines_df["line_number"] = pdf_lines_df["line_number"].astype("int32")

# Filter by 2021 documents
doc_ids = outlines_df["document_id"].unique()
pdf_lines_df = pdf_lines_df[pdf_lines_df["document_id"].isin(doc_ids)].copy()
print(f"PDF lines for 2021: {len(pdf_lines_df)}")

# Pre-group
pdf_lines_by_doc = {
    doc_id: group.reset_index(drop=True)
    for doc_id, group in pdf_lines_df.groupby("document_id")
}
del pdf_lines_df

transformer = PdfContentTransformer()

# Match outlines with lines
all_lines = pd.concat(list(pdf_lines_by_doc.values()), ignore_index=True)
outlines_matched = transformer.match_titles_with_lines(outlines_df, all_lines)
matched = outlines_matched["line_number"].notna().sum()
print(f"Matched {matched}/{len(outlines_matched)} outlines")

# Preprocess and extract
pdf_lines_dict, sorted_outlines = transformer.preprocess_dataframes(all_lines, outlines_matched)
del all_lines, pdf_lines_by_doc

sections = transformer.extract_content_sections(pdf_lines_dict, sorted_outlines)
print(f"Total sections: {len(sections)}")

sections_df = transformer.prepare_sections_dataframe(sections)
if sections_df is not None:
    output_path = os.path.join(test_dir, "sections_2021_test.parquet")
    sections_df.to_parquet(output_path, index=False)
    print(f"Saved {len(sections_df)} sections to {output_path}")

    # ── 3. Verify results ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 3: Verifying Adenda sections...")
    print("=" * 60)
    
    adenda = sections_df[sections_df["title"].str.lower().str.strip() == "adenda"]
    print(f"Adenda sections: {len(adenda)}")
    if len(adenda) > 0:
        print(f"  Pages: {sorted(adenda['page'].unique())}")
        print(f"  Line ends: {sorted(adenda['line_end'].unique())}")
        print(f"  Max content_length: {adenda['content_length'].max()}")
        print(f"  Median content_length: {adenda['content_length'].median()}")
        
        # Check DATOS contamination
        datos_in_adenda = adenda[adenda["content"].str.contains("DATOS DE LA LICITACI[OÓ]N", na=False)]
        print(f"\n  Adenda with 'DATOS DE LA LICITACIÓN' in content: {len(datos_in_adenda)}")
        if len(datos_in_adenda) > 0:
            for _, row in datos_in_adenda.head(3).iterrows():
                lines = row["content"].split("\n")
                datos_line = next((i for i, l in enumerate(lines) if "DATOS" in l.upper()), -1)
                print(f"    {row['document_id']}: '{lines[datos_line][:80]}...' (line {datos_line} of {len(lines)})")
        else:
            print("  ✅ No DATOS contamination in Adenda!")

print("\nDone.")

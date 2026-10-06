"""Debug: analyze Adenda outlines - outline data only (no PDF lines needed)."""
import os
import pandas as pd
from src.etl.extractors.pdf_content_extractor import PdfContentExtractor
from config.settings import BASE_OUTPUT_PROCESSED_DIR

extractor = PdfContentExtractor()
outlines_path = os.path.join(BASE_OUTPUT_PROCESSED_DIR, 'parquet', 'merged_outlines.parquet')
outlines_df = extractor.load_single_dataframe(outlines_path)
outlines_df = outlines_df[outlines_df['year'] == '2021'].copy()

adenda_df = outlines_df[outlines_df['title'].str.lower().str.strip() == 'adenda']
print(f"Total Adenda outlines in 2021: {len(adenda_df)}")

# For each doc with Adenda, check: is Adenda the last outline?
# If not last: is next outline on same page or different page?
other_page = 0
same_page = 0
is_last = 0

# Adenda depth distribution
depth_counts = adenda_df['depth'].value_counts().sort_index()
print(f"\nAdenda depth distribution:")
for d, c in depth_counts.items():
    print(f"  depth={d}: {c}")

for doc_id in adenda_df['document_id']:
    doc_outlines = outlines_df[outlines_df['document_id'] == doc_id].sort_values('page')
    adenda_mask = doc_outlines['title'].str.lower().str.strip() == 'adenda'
    adenda_idx = doc_outlines[adenda_mask].index[0]
    adenda_pos = doc_outlines.index.get_loc(adenda_idx)
    
    if adenda_pos == len(doc_outlines) - 1:
        is_last += 1
    else:
        next_outline = doc_outlines.iloc[adenda_pos + 1]
        adenda_row = doc_outlines.loc[adenda_idx]
        if next_outline['page'] == adenda_row['page']:
            same_page += 1
        else:
            other_page += 1

print(f"\nAdenda position in document outline:")
print(f"  Is LAST outline: {is_last}")
print(f"  Next outline SAME page: {same_page}")
print(f"  Next outline OTHER page: {other_page}")

# Show examples for each category
print(f"\n--- Examples: Adenda is last outline ---")
examples = outlines_df[outlines_df['document_id'].isin(
    adenda_df.groupby('document_id').filter(lambda x: x[x['title'].str.lower().str.strip() == 'adenda'].index[0] == x.index[-1]).index
)]
for doc_id in examples['document_id'].unique()[:3]:
    doc_outlines = outlines_df[outlines_df['document_id'] == doc_id].sort_values('page')
    print(f"\n  {doc_id}:")
    for _, row in doc_outlines.iterrows():
        print(f"    page={row['page']} depth={row['depth']} \"{row['title']}\"")

print(f"\n--- Examples: next outline on other page ---")
for doc_id in adenda_df['document_id'].unique()[:5]:
    doc_outlines = outlines_df[outlines_df['document_id'] == doc_id].sort_values('page')
    adenda_mask = doc_outlines['title'].str.lower().str.strip() == 'adenda'
    adenda_idx = doc_outlines[adenda_mask].index[0]
    adenda_pos = doc_outlines.index.get_loc(adenda_idx)
    if adenda_pos < len(doc_outlines) - 1:
        adenda_row = doc_outlines.loc[adenda_idx]
        next_row = doc_outlines.iloc[adenda_pos + 1]
        if next_row['page'] != adenda_row['page']:
            print(f"\n  {doc_id}:")
            print(f"    Adenda:  page={adenda_row['page']} depth={adenda_row['depth']} \"{adenda_row['title']}\"")
            print(f"    Next:    page={next_row['page']} depth={next_row['depth']} \"{next_row['title']}\"")
            # Check all outlines after Adenda for this doc
            print(f"    All outlines after Adenda:")
            for _, row in doc_outlines.iloc[adenda_pos+1:].iterrows():
                print(f"      page={row['page']} depth={row['depth']} \"{row['title']}\"")

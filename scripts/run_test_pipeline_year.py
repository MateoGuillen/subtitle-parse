"""Test: run PDF content extraction pipeline for a single year."""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from src.pipelines.pdf_content_pipeline import PdfContentPipeline
from config.settings import BASE_OUTPUT_PROCESSED_DIR


def main():
    parser = argparse.ArgumentParser(description="Run PDF content pipeline for a single year (test)")
    parser.add_argument("--year", type=str, required=True, help="Year to process (e.g. 2021)")
    args = parser.parse_args()

    year = args.year

    pdf_lines_path = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR,
        "parquet/pdf-to-parquet",
        "combined_documents_all_years.parquet",
    )

    outlines_full_path = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "parquet", "merged_outlines.parquet"
    )

    test_dir = os.path.join(BASE_OUTPUT_PROCESSED_DIR, "test_2021_pipeline")
    filtered_outlines_path = os.path.join(test_dir, "outlines_2021.parquet")
    outlines_with_position_path = os.path.join(
        test_dir, "outlines", "merged_outlines_with_position_in_content.parquet"
    )
    content_sections_dir = os.path.join(test_dir, "sections")

    os.makedirs(os.path.join(test_dir, "outlines"), exist_ok=True)
    os.makedirs(content_sections_dir, exist_ok=True)

    print(f"[test] Filtering outlines to year={year}...")
    outlines_full = pd.read_parquet(outlines_full_path)
    if "year" not in outlines_full.columns:
        print("[test] ERROR: 'year' column not found in outlines parquet")
        sys.exit(1)

    outlines_filtered = outlines_full[outlines_full["year"] == year].reset_index(drop=True)
    n_docs = outlines_filtered["document_id"].nunique()
    n_rows = len(outlines_filtered)
    print(f"[test] Filtered: {n_rows} rows, {n_docs} unique documents for year {year}")

    if n_rows == 0:
        print(f"[test] No outlines found for year {year}, nothing to do.")
        sys.exit(0)

    outlines_filtered.to_parquet(filtered_outlines_path, compression="snappy")
    print(f"[test] Saved filtered outlines to {filtered_outlines_path}")

    config = {
        "pdf_lines_path": pdf_lines_path,
        "outlines_path": filtered_outlines_path,
        "outlines_with_position_in_content_path": outlines_with_position_path,
        "content_sections_path": os.path.join(content_sections_dir, "content_sections.parquet"),
        "content_sections_dir": content_sections_dir,
    }

    print(f"[test] Initializing pipeline for year {year}...")
    pipeline = PdfContentPipeline(config)
    pipeline.run()

    print(f"\n[test] Pipeline done for year {year}")
    print(f"[test] Output: {content_sections_dir}")

    if os.path.exists(filtered_outlines_path):
        os.remove(filtered_outlines_path)
        print(f"[test] Removed temp filtered outlines file")


if __name__ == "__main__":
    main()

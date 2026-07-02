"""Script to run the PDF content extraction pipeline."""

import argparse
import os
from src.pipelines.pdf_content_pipeline import PdfContentPipeline
from config.settings import BASE_OUTPUT_PROCESSED_DIR


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract PDF content sections from outlines and lines."
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years to process (e.g. 2021,2022). Default: all years.",
    )
    return parser.parse_args()


def main():
    """
    Main function to configure and run the PDF content extraction pipeline.

    This function sets up directory paths and filenames for input and output data.
    It ensures that the necessary directories exist. The function then initializes
    the PdfContentPipeline with the required parameters and executes the pipeline.

    The pipeline processes PDF outlines and PDF lines data, matches them, and extracts
    content for each section defined by the outlines.
    """

    args = parse_args()
    years = [int(y.strip()) for y in args.years.split(",")] if args.years else None

    pdf_lines_path = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR,
        "parquet/pdf-to-parquet",
        "combined_documents_all_years.parquet",
    )
    outlines_path = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "parquet", "merged_outlines.parquet"
    )
    outlines_with_position_in_content_path = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR,
        "outlines",
        "merged_outlines_with_position_in_content.parquet",
    )
    content_sections_path = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "sections", "content_sections.parquet"
    )

    # Ensure directories exist
    os.makedirs(os.path.dirname(outlines_with_position_in_content_path), exist_ok=True)
    os.makedirs(os.path.dirname(content_sections_path), exist_ok=True)

    # Configure pipeline
    config = {
        "pdf_lines_path": pdf_lines_path,
        "outlines_path": outlines_path,
        "outlines_with_position_in_content_path": outlines_with_position_in_content_path,
        "content_sections_path": content_sections_path,
        "content_sections_dir": os.path.join(
            BASE_OUTPUT_PROCESSED_DIR, "sections"
        ),
        "years": years,
    }

    # Initialize and run the pipeline
    pipeline = PdfContentPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()

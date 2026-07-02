"""Script to run the content cleaning pipeline."""

import argparse
import os
from src.pipelines.content_cleaning_pipeline import ContentCleaningPipeline
from config.settings import BASE_OUTPUT_PROCESSED_DIR


def parse_args():
    parser = argparse.ArgumentParser(
        description="Clean content text from extracted PDF sections."
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
    Configure and run the content cleaning pipeline.

    Input
    -----
    Partitioned parquet dataset produced by ``run_pdf_content_pipeline.py``::

        data/processed/sections/
          year=2019/part-0.parquet
          year=2020/part-0.parquet
          ...

    Output
    ------
    New partitioned parquet dataset with the same layout plus three extra
    columns (``content_clean``, ``content_text``, ``content_length_clean``)::

        data/processed/sections_clean/
          year=2019/part-0.parquet
          year=2020/part-0.parquet
          ...

    The raw ``sections/`` dataset is left untouched so the cleaning step
    can be re-run with different parameters without re-running extraction.
    """
    args = parse_args()
    years = [int(y.strip()) for y in args.years.split(",")] if args.years else None

    sections_dir = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "sections"
    )
    cleaned_sections_dir = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "sections_clean"
    )

    os.makedirs(cleaned_sections_dir, exist_ok=True)

    config = {
        "sections_dir":         sections_dir,
        "cleaned_sections_dir": cleaned_sections_dir,
        "years": years,
    }

    pipeline = ContentCleaningPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()

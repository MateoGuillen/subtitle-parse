"""Test: run content cleaning pipeline over test sections output."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipelines.content_cleaning_pipeline import ContentCleaningPipeline
from config.settings import BASE_OUTPUT_PROCESSED_DIR


def main():
    test_dir = os.path.join(BASE_OUTPUT_PROCESSED_DIR, "test_2021_pipeline")
    sections_dir = os.path.join(test_dir, "sections")
    cleaned_sections_dir = os.path.join(test_dir, "sections_clean")

    if not os.path.isdir(sections_dir):
        print(f"[test] ERROR: Input directory not found: {sections_dir}")
        print("[test] Run run_test_pipeline_year.py first.")
        sys.exit(1)

    os.makedirs(cleaned_sections_dir, exist_ok=True)

    print(f"[test] Cleaning sections from: {sections_dir}")
    print(f"[test] Output to: {cleaned_sections_dir}")

    config = {
        "sections_dir": sections_dir,
        "cleaned_sections_dir": cleaned_sections_dir,
    }

    pipeline = ContentCleaningPipeline(config)
    pipeline.run()

    print(f"\n[test] Content cleaning done.")
    print(f"[test] Now run: python scripts/validate_test_output.py")


if __name__ == "__main__":
    main()

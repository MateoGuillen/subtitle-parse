"""Validate test pipeline output: check content_length distribution and spot samples."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from config.settings import BASE_OUTPUT_PROCESSED_DIR


def main():
    test_dir = os.path.join(BASE_OUTPUT_PROCESSED_DIR, "test_2021_pipeline")
    cleaned_dir = os.path.join(test_dir, "sections_clean")

    if not os.path.isdir(cleaned_dir):
        print(f"[validate] ERROR: cleaned sections not found at {cleaned_dir}")
        print("[validate] Run run_test_pipeline_year.py and run_test_cleaning_year.py first.")
        sys.exit(1)

    # Read partitioned dataset year=YYYY/part-*.parquet manually
    # to avoid schema conflicts (year as string in schema vs int32 from directory name)
    entries = os.listdir(cleaned_dir)
    part_dirs = sorted([d for d in entries if d.startswith("year=")])
    if part_dirs:
        print(f"[validate] Reading partitioned dataset from {cleaned_dir} (years: {part_dirs})")
        chunks = []
        for pd_dir in part_dirs:
            year_path = os.path.join(cleaned_dir, pd_dir)
            for f in os.listdir(year_path):
                if f.endswith(".parquet"):
                    chunks.append(pd.read_parquet(os.path.join(year_path, f)))
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    else:
        files = [f for f in entries if f.endswith(".parquet")]
        if not files:
            print(f"[validate] No parquet files found in {cleaned_dir}")
            sys.exit(1)
        chunks = []
        for f in files:
            chunks.append(pd.read_parquet(os.path.join(cleaned_dir, f)))
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    if df.empty:
        print("[validate] Empty dataset, nothing to validate.")
        return

    total = len(df)
    print(f"\n{'='*60}")
    print("VALIDATION REPORT")
    print(f"{'='*60}")
    print(f"Total sections:  {total}")

    # 1. content_length_clean distribution
    cl1 = (df["content_length_clean"] == 1).sum()
    cl2_10 = ((df["content_length_clean"] >= 2) & (df["content_length_clean"] <= 10)).sum()
    cl_gt10 = (df["content_length_clean"] > 10).sum()
    print(f"  clc = 1:     {cl1:>7d}  ({cl1/total*100:5.1f}%)  <- BUG: should be near 0")
    print(f"  clc 2-10:    {cl2_10:>7d}  ({cl2_10/total*100:5.1f}%)")
    print(f"  clc > 10:    {cl_gt10:>7d}  ({cl_gt10/total*100:5.1f}%)")

    # 2. line_end = -1
    le_neg1 = (df["line_end"] == -1).sum()
    le_neg1_and_cl1 = ((df["line_end"] == -1) & (df["content_length_clean"] == 1)).sum()
    le_neg1_and_cl_gt1 = ((df["line_end"] == -1) & (df["content_length_clean"] > 1)).sum()
    print(f"  line_end=-1:                       {le_neg1:>7d}  ({le_neg1/total*100:5.1f}%)")
    print(f"    + content_length_clean=1:         {le_neg1_and_cl1:>7d}")
    print(f"    + content_length_clean > 1:       {le_neg1_and_cl_gt1:>7d}")

    # 3. Titles that previously had the bug
    problematic_titles = [
        "Oferentes en consorcio",
        "Idioma de la oferta",
        "Margen de preferencia local - CPS",
        "Composici\u00f3n de Precios",
        "Garant\u00edas: instrumentaci\u00f3n, plazos y ejecuci\u00f3n.",
        "Incoterms",
        "Copias de la oferta - CPS",
        "Fraude y Corrupci\u00f3n",
    ]
    for title in problematic_titles:
        sub = df[df["title"] == title]
        if sub.empty:
            continue
        total_t = len(sub)
        cl1_t = (sub["content_length_clean"] == 1).sum()
        pct = cl1_t / total_t * 100
        flag = " <<< BUG" if pct > 5 else ""
        print(f"  {title[:45]:45s}  total={total_t:>5d}  clc=1={cl1_t:>5d}  ({pct:5.1f}%){flag}")

    # 4. content_length distribution (raw, before cleaning)
    cl_dist = df["content_length"].value_counts().sort_index().head(15)
    for cl_val, cnt in cl_dist.items():
        print(f"  cl = {cl_val:>4d}:  {cnt:>7d}")

    # 5. Spot-check: first 3 sections with clc=1
    still_bad = df[df["content_length_clean"] == 1]
    if len(still_bad) > 0:
        print("\n  Spot-check: first 5 sections with clc=1")
        for _, r in still_bad.head(5).iterrows():
            text_preview = str(r.get("content_text", ""))[:120]
            print(f"  doc={r['document_id']}")
            print(f"  title={r['title'][:50]}")
            print(f"  page={r['page']} line_start={r['line_start']} line_end={r['line_end']}")
            print(f"  content_text='{text_preview}'")
            print()
    else:
        print("\n[OK] NO sections with content_length_clean=1 found!")

    # 6. Spot-check: first 3 sections of key titles (should have real content)
    print("\n  Spot-check: content_text for fixed titles")
    for title in ["Oferentes en consorcio", "Idioma de la oferta"]:
        sub = df[(df["title"] == title) & (df["content_length_clean"] > 1)]
        if sub.empty:
            print(f"  '{title}': no sections with content > 1 (check if present)")
            continue
        _, r = next(sub.iterrows())
        text_preview = str(r.get("content_text", ""))[:150]
        print(f"  title='{title}'")
        print(f"  doc={r['document_id']} pg={r['page']} ls={r['line_start']}")
        print(f"  content (first 150 chars): '{text_preview}'")
        print()

    # Summary verdict
    print(f"\n{'='*60}")
    if cl1 == 0:
        print("[PASS] No sections with content_length_clean=1")
    elif cl1 / total < 0.005:
        print(f"[PASS] Only {cl1/total*100:.2f}% with clc=1 (likely legitimate edge cases)")
    else:
        print(f"[FAIL] {cl1/total*100:.1f}% still have clc=1 (fix may be incomplete)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

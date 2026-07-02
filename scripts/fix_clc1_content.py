"""Fix clc=1 sections by re-extracting content with pdfplumber for specific problem docs.
Only updates the DB - does not touch parquet files.
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

import pdfplumber
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import DB_CONFIG

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def find_pdf(document_id):
    """Locate PDF file for a given document_id (format: YYYY_CAT_DOCID)."""
    parts = document_id.split("_")
    year = parts[0]
    cat = parts[1]
    pdf_dir = os.path.join(DATA_DIR, "raw", "pdf", year, f"category_{cat}")
    pdf_path = os.path.join(pdf_dir, f"{document_id}.pdf")
    if os.path.exists(pdf_path):
        return pdf_path
    logger.warning(f"PDF not found: {pdf_path}")
    return None


def extract_content_between(pdf_path, page_num, title_text, next_title_text=None):
    """Use pdfplumber to extract content between title_text and next_title_text on given page.

    pdfplumber sometimes doubles characters due to PDF letter-spacing.
    We handle this by de-doubling for matching but keeping original text.

    Args:
        pdf_path: Path to PDF file
        page_num: 1-indexed page number
        title_text: The title to find (start boundary)
        next_title_text: Optional next title (end boundary). If None, capture rest of page.

    Returns:
        str: extracted content between the titles
    """
    norm_title = _normalize(title_text)
    norm_title_ded = _dedouble(norm_title)
    norm_next = _normalize(next_title_text) if next_title_text else None
    norm_next_ded = _dedouble(norm_next) if norm_next else None

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_num - 1]
        full_text = page.extract_text() or ""

        lines = full_text.split("\n")
        result = []
        capture = False

        for line in lines:
            line_norm = _normalize(line.strip())
            line_ded = _dedouble(line_norm)

            if not capture:
                title_found = (
                    norm_title in line_norm
                    or norm_title in line_ded
                    or norm_title_ded in line_norm
                    or norm_title_ded in line_ded
                    or _fuzzy_match(norm_title, line_norm)
                )
                if title_found:
                    capture = True
                    # Skip the title line itself (it's just the section name)
                continue

            if norm_next:
                next_found = (
                    norm_next in line_norm
                    or norm_next in line_ded
                    or norm_next_ded in line_norm
                    or norm_next_ded in line_ded
                    or _fuzzy_match(norm_next, line_norm)
                )
                if next_found:
                    # The next title might be on the same line as content
                    # Try to split: keep content before the next title
                    for candidate in [line_norm, line_ded]:
                        idx = candidate.find(norm_next if norm_next in candidate else norm_next_ded)
                        if idx >= 0:
                            before = line.strip()[:idx]
                            if before.strip():
                                result.append(_dedouble(before.strip()))
                            break
                    break

            # De-double the content text for cleaner output
            cleaned = _dedouble(line.strip())
            if cleaned:
                result.append(cleaned)

    return "\n".join(result).strip()


def _normalize(text):
    """Normalize text for comparison - lowercase, collapse whitespace."""
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    text = text.strip()
    return text


def _dedouble(text):
    """Remove doubled characters caused by PDF letter-spacing artifacts."""
    # pdfplumber doubles chars when character-level positioning is used
    return re.sub(r"(.)\1+", r"\1", text)


def _fuzzy_match(needle, haystack):
    """Check if needle is approximately in haystack (handles doubled chars)."""
    if needle in haystack:
        return True
    # Try de-doubling haystack characters
    simplified = _dedouble(haystack)
    if needle in simplified:
        return True
    # Try de-doubling both
    if _dedouble(needle) in simplified:
        return True
    return False


def get_problem_docs(cur):
    """Get all clc=1 documents from DB."""
    cur.execute("""
        SELECT document_id, title, year, page
        FROM dncp.pliegos_secciones
        WHERE content_length_clean = 1
        ORDER BY year, document_id, page, line_start
    """)
    return cur.fetchall()


def get_surrounding_titles(cur, document_id, page, line_start):
    """Get the title before and after the problem section on the same page."""
    cur.execute("""
        SELECT title, line_start
        FROM dncp.pliegos_secciones
        WHERE document_id = %s AND page = %s
        ORDER BY line_start
    """, (document_id, page))
    titles_on_page = cur.fetchall()

    next_title = None
    for i, (t, ls) in enumerate(titles_on_page):
        if ls > line_start:
            next_title = t
            break

    return next_title


def process_document(cur, doc_id, title, year, page):
    """Process a single clc=1 document using pdfplumber."""
    pdf_path = find_pdf(doc_id)
    if not pdf_path:
        return False

    # Get next title on the same page for boundary
    # We use line_start from the problem record to find it
    cur.execute("""
        SELECT line_start FROM dncp.pliegos_secciones
        WHERE document_id = %s AND title = %s AND page = %s AND content_length_clean = 1
        LIMIT 1
    """, (doc_id, title, page))
    row = cur.fetchone()
    if not row:
        logger.warning(f"Cannot find problem record for {doc_id} / {title}")
        return False
    line_start = row[0]

    next_title = get_surrounding_titles(cur, doc_id, page, line_start)

    logger.info(f"Processing {doc_id} / '{title}' pg={page}, next_title='{next_title}'")

    content = extract_content_between(pdf_path, page, title, next_title)
    if not content:
        logger.warning(f"No content extracted for {doc_id} / '{title}'")
        return False

    logger.info(f"Extracted {len(content)} chars for '{title}'")
    logger.debug(f"Content: {content[:200]}...")

    # Split content into lines for multi-element text array
    lines = [l for l in content.split("\n") if l.strip()]
    if not lines:
        logger.warning(f"No content lines extracted for {doc_id} / '{title}'")
        return False

    # Build PG text array: {"line1","line2",...} with proper escaping
    def _escape_array_elem(s):
        return s.replace("\\", "\\\\").replace('"', '\\"')
    parts = [f'"{_escape_array_elem(l)}"' for l in lines]
    clean_array = "{" + ",".join(parts) + "}"
    cur.execute("""
        UPDATE dncp.pliegos_secciones
        SET content_clean = %s::text[],
            content_length_clean = array_length(%s::text[], 1)
        WHERE document_id = %s AND title = %s AND page = %s AND content_length_clean = 1
    """, (clean_array, clean_array, doc_id, title, page))

    logger.info(f"Updated {doc_id} / '{title}' in DB ({len(lines)} lines, {len(content)} chars)")
    return True


def main():
    parser = argparse.ArgumentParser(description="Fix clc=1 sections using pdfplumber")
    parser.add_argument("--dry-run", action="store_true", help="Don't update DB, just show what would be done")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    problem_docs = get_problem_docs(cur)
    logger.info(f"Found {len(problem_docs)} problem documents")

    successes = 0
    for doc_id, title, year, page in problem_docs:
        logger.info(f"\n{'='*60}")
        if args.dry_run:
            logger.info(f"[DRY RUN] Would process: {doc_id} / '{title}'")
        else:
            ok = process_document(cur, doc_id, title, year, page)
            if ok:
                successes += 1

    if not args.dry_run:
        conn.commit()
        logger.info(f"\nDone! Successfully fixed {successes}/{len(problem_docs)} documents")
    else:
        logger.info(f"\nDry run complete. Would process {len(problem_docs)} documents")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

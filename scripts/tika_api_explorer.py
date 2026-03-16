"""
Tika API Explorer - Test script to discover available metadata per endpoint.
Run: python -m scripts.tika_api_explorer --pdf D:/projects/subtitle-parse/data/raw/pdf/2021/category_1/2021_1_395227.pdf --port 9998
"""

import argparse
import json
import sys
import requests
from pathlib import Path
from typing import Any


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DEFAULT_PORT = 9998
SEPARATOR = "─" * 70


def get_base_url(port: int) -> str:
    return f"http://localhost:{port}"


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def print_section(title: str):
    print(f"/n{'═' * 70}")
    print(f"  {title}")
    print("═" * 70)


def print_sub(title: str):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


def put_request(
    url: str, pdf_bytes: bytes, accept: str, timeout: int = 60
) -> requests.Response:
    return requests.put(
        url,
        data=pdf_bytes,
        headers={"Content-Type": "application/pdf", "Accept": accept},
        timeout=timeout,
    )


def safe_json(response: requests.Response) -> Any:
    try:
        return response.json()
    except Exception:
        return None


def truncate(text: str, max_chars: int = 300) -> str:
    if len(text) > max_chars:
        return text[:max_chars] + f"... [+{len(text) - max_chars} chars]"
    return text


# ─────────────────────────────────────────────
# TEST 1: /tika → text/plain
# ─────────────────────────────────────────────
def test_tika_plain(base_url: str, pdf_bytes: bytes):
    print_section("TEST 1: PUT /tika  →  Accept: text/plain")
    url = f"{base_url}/tika"
    r = put_request(url, pdf_bytes, "text/plain")
    print(f"Status: {r.status_code}")
    text = r.content.decode("utf-8", errors="replace")
    print(f"Total chars: {len(text)}")
    print(f"\n--- First 500 chars ---\n{text[:500]}")

    # Check for common page separators
    print("\n--- Page separator analysis ---")
    print(f"  Form Feed \\f count  : {text.count(chr(12))}")
    print(f"  \\x0c count         : {text.count(chr(0x0c))}")
    print(f"  \\n\\n count         : {text.count(chr(10)*2)}")

    # Show raw bytes around first 1000 chars to detect hidden chars
    raw_sample = r.content[:1000]
    separators_found = [hex(b) for b in raw_sample if b < 32 and b not in (9, 10, 13)]
    print(f"  Control chars (excl tab/LF/CR) in first 1000 bytes: {separators_found}")


# ─────────────────────────────────────────────
# TEST 2: /tika → text/html
# ─────────────────────────────────────────────
def test_tika_html(base_url: str, pdf_bytes: bytes):
    print_section("TEST 2: PUT /tika  →  Accept: text/html")
    url = f"{base_url}/tika"
    r = put_request(url, pdf_bytes, "text/html")
    print(f"Status: {r.status_code}")
    html = r.content.decode("utf-8", errors="replace")
    print(f"Total chars: {len(html)}")
    print(f"\n--- First 1000 chars ---\n{html[:1000]}")

    # Check for page div markers
    import re

    page_divs = re.findall(
        r'<div[^>]*class=["\']?page["\']?[^>]*>', html, re.IGNORECASE
    )
    print(f"\n--- Page <div> markers found: {len(page_divs)} ---")
    for i, div in enumerate(page_divs[:5]):
        print(f"  [{i+1}] {div}")

    # Check for <p> tags (lines)
    paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, re.IGNORECASE | re.DOTALL)
    print(f"\n--- <p> tags found: {len(paragraphs)} ---")
    for i, p in enumerate(paragraphs[:10]):
        clean = re.sub(r"<[^>]+>", "", p).strip()
        if clean:
            print(f"  [{i+1}] {truncate(clean, 100)}")


# ─────────────────────────────────────────────
# TEST 3: /tika → text/xml (XHTML)
# ─────────────────────────────────────────────
def test_tika_xml(base_url: str, pdf_bytes: bytes):
    print_section("TEST 3: PUT /tika  →  Accept: text/xml")
    url = f"{base_url}/tika"
    r = put_request(url, pdf_bytes, "text/xml")
    print(f"Status: {r.status_code}")
    xml = r.content.decode("utf-8", errors="replace")
    print(f"Total chars: {len(xml)}")
    print(f"\n--- First 1500 chars ---\n{xml[:1500]}")

    # Look for page markers in XHTML
    import re

    # Tika XHTML often uses <div class="page"> per page
    pages = re.findall(r'<div class="page">', xml, re.IGNORECASE)
    print(f"\n--- <div class='page'> found: {len(pages)} ---")

    # Extract page content
    page_blocks = re.findall(
        r'<div class="page">(.*?)</div>', xml, re.IGNORECASE | re.DOTALL
    )
    print(f"\n--- Page content blocks: {len(page_blocks)} ---")
    for i, block in enumerate(page_blocks[:3]):
        clean = re.sub(r"<[^>]+>", "", block).strip()[:200]
        print(f"\n  [Page {i+1}] {clean}")


# ─────────────────────────────────────────────
# TEST 4: /meta → JSON (all metadata fields)
# ─────────────────────────────────────────────
def test_meta_json(base_url: str, pdf_bytes: bytes):
    print_section("TEST 4: PUT /meta  →  Accept: application/json")
    url = f"{base_url}/meta"
    r = put_request(url, pdf_bytes, "application/json")
    print(f"Status: {r.status_code}")
    data = safe_json(r)
    if data:
        print(f"\n--- All metadata fields ({len(data)} keys) ---")
        for key, value in sorted(data.items()):
            val_str = str(value)
            print(f"  {key:<40} : {truncate(val_str, 80)}")
    else:
        print(f"Raw response:\n{r.text[:500]}")


# ─────────────────────────────────────────────
# TEST 5: /rmeta → JSON (THE KEY TEST)
# ─────────────────────────────────────────────
def test_rmeta_json(base_url: str, pdf_bytes: bytes):
    print_section("TEST 5: PUT /rmeta  →  application/json  ★ KEY TEST ★")
    url = f"{base_url}/rmeta"
    r = put_request(url, pdf_bytes, "application/json")
    print(f"Status: {r.status_code}")
    data = safe_json(r)
    if not data:
        print(f"Could not parse JSON. Raw:\n{r.text[:500]}")
        return

    print(f"\nTotal objects in array: {len(data)}")

    for i, obj in enumerate(data):
        print_sub(f"Object [{i}]  —  {obj.get('resourceName', 'unnamed')}")

        # Show all keys
        print(f"  Keys available: {len(obj)} fields")

        # Highlight page-related fields
        page_fields = {
            k: v
            for k, v in obj.items()
            if any(
                kw in k.lower()
                for kw in ["page", "content", "text", "dc:", "xmp", "pdf"]
            )
        }
        print(f"\n  --- Page/Content-related fields ---")
        for k, v in sorted(page_fields.items()):
            print(f"    {k:<45} : {truncate(str(v), 80)}")

        # Show X-TIKA:content (the actual text)
        content = obj.get("X-TIKA:content", "")
        if content:
            print(f"\n  --- X-TIKA:content (first 300 chars) ---")
            print(f"    {truncate(content.strip(), 300)}")

        # Show ALL keys for first object
        if i == 0:
            print(f"\n  --- ALL keys (first object) ---")
            for k, v in sorted(obj.items()):
                if k != "X-TIKA:content":
                    print(f"    {k:<45} : {truncate(str(v), 80)}")


# ─────────────────────────────────────────────
# TEST 6: /rmeta/text → text handler
# ─────────────────────────────────────────────
def test_rmeta_text(base_url: str, pdf_bytes: bytes):
    print_section("TEST 6: PUT /rmeta/text  →  application/json")
    url = f"{base_url}/rmeta/text"
    r = put_request(url, pdf_bytes, "application/json")
    print(f"Status: {r.status_code}")
    data = safe_json(r)
    if not data:
        print(f"Raw:\n{r.text[:300]}")
        return

    print(f"Objects in array: {len(data)}")
    for i, obj in enumerate(data[:3]):
        content = obj.get("X-TIKA:content", "")
        print(
            f"\n  Object [{i}]: {obj.get('resourceName', '')} | chars: {len(content)}"
        )
        if content:
            print(f"    Preview: {truncate(content.strip(), 200)}")


# ─────────────────────────────────────────────
# TEST 7: Specific meta fields
# ─────────────────────────────────────────────
def test_meta_specific_fields(base_url: str, pdf_bytes: bytes):
    print_section("TEST 7: PUT /meta/{field}  →  specific PDF fields")
    fields_to_test = [
        "pdf:charsPerPage",
        "xmpTPg:NPages",
        "Page-Count",
        "pdf:docinfo:subject",
        "Content-Type",
    ]
    for field in fields_to_test:
        url = f"{base_url}/meta/{field}"
        try:
            r = put_request(url, pdf_bytes, "text/plain")
            print(
                f"  /meta/{field:<30} → [{r.status_code}] {truncate(r.text.strip(), 80)}"
            )
        except Exception as e:
            print(f"  /meta/{field:<30} → ERROR: {e}")


# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
def print_summary(base_url: str, pdf_bytes: bytes):
    print_section("SUMMARY: Recommended approach for exact page extraction")

    # Quick check of rmeta
    url = f"{base_url}/rmeta"
    try:
        r = put_request(url, pdf_bytes, "application/json")
        data = safe_json(r)
        if data and len(data) > 1:
            print("  ✅ /rmeta returns multiple objects → likely one per page")
            print(f"     Objects: {len(data)}")

            # Check if page numbers are embedded
            has_page_num = any(
                any(kw in k for kw in ["page", "Page"])
                for obj in data
                for k in obj.keys()
            )
            if has_page_num:
                print("  ✅ Page number metadata FOUND in /rmeta objects")
            else:
                print(
                    "  ⚠️  No explicit page number in /rmeta — need to use array index"
                )

            has_content = any("X-TIKA:content" in obj for obj in data)
            print(
                f"  {'✅' if has_content else '❌'} X-TIKA:content (text) {'present' if has_content else 'NOT present'} in /rmeta"
            )

        elif data and len(data) == 1:
            print("  ⚠️  /rmeta returns 1 object only → PDF treated as single document")
            print("     → Use /tika with text/html and parse <div class='page'>")
        else:
            print("  ❌ /rmeta returned empty or error")
    except Exception as e:
        print(f"  ERROR: {e}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Tika API Explorer")
    parser.add_argument("--pdf", required=True, help="Path to test PDF file")
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Tika server port"
    )
    parser.add_argument(
        "--tests",
        nargs="+",
        default=["all"],
        choices=[
            "all",
            "plain",
            "html",
            "xml",
            "meta",
            "rmeta",
            "rmeta_text",
            "fields",
            "summary",
        ],
        help="Which tests to run",
    )
    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        print(f"ERROR: PDF not found: {pdf_path}")
        sys.exit(1)

    pdf_bytes = pdf_path.read_bytes()
    base_url = get_base_url(args.port)

    print(f"\n{'█' * 70}")
    print(f"  TIKA API EXPLORER")
    print(f"  Server : {base_url}")
    print(f"  PDF    : {pdf_path.name}  ({len(pdf_bytes):,} bytes)")
    print(f"{'█' * 70}")

    # Check server is alive
    try:
        r = requests.get(f"{base_url}/version", timeout=5)
        print(f"\n  Server version: {r.text.strip()}")
    except Exception:
        print(f"\n  ❌ Cannot reach Tika at {base_url}")
        sys.exit(1)

    run_all = "all" in args.tests
    tests = args.tests

    if run_all or "plain" in tests:
        test_tika_plain(base_url, pdf_bytes)
    if run_all or "html" in tests:
        test_tika_html(base_url, pdf_bytes)
    if run_all or "xml" in tests:
        test_tika_xml(base_url, pdf_bytes)
    if run_all or "meta" in tests:
        test_meta_json(base_url, pdf_bytes)
    if run_all or "rmeta" in tests:
        test_rmeta_json(base_url, pdf_bytes)
    if run_all or "rmeta_text" in tests:
        test_rmeta_text(base_url, pdf_bytes)
    if run_all or "fields" in tests:
        test_meta_specific_fields(base_url, pdf_bytes)
    if run_all or "summary" in tests:
        print_summary(base_url, pdf_bytes)

    print(f"\n{'█' * 70}")
    print("  DONE")
    print(f"{'█' * 70}\n")


if __name__ == "__main__":
    main()

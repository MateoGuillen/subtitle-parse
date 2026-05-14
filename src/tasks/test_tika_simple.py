"""Simple test script to debug Tika text extraction."""

import io
import requests
from pathlib import Path
from PyPDF2 import PdfReader, PdfWriter
import time

# Pick one Tika server
TIKA_PORT = 9998
TIKA_URL = f"http://localhost:{TIKA_PORT}/tika"

# Find first PDF
pdf_dir = Path("data/raw/pdf/2025")
pdf_files = list(pdf_dir.glob("*.pdf"))

if not pdf_files:
    print(f"❌ No PDFs found in {pdf_dir}")
    exit(1)

pdf_file = pdf_files[0]
print(f"📄 Testing: {pdf_file.name}")

# Read PDF
pdf_bytes = pdf_file.read_bytes()
print(f"✓ Read {len(pdf_bytes):,} bytes")

# Get page count
pdf = PdfReader(io.BytesIO(pdf_bytes))
page_count = len(pdf.pages)
print(f"✓ PDF has {page_count} pages")

# Extract page 1 only
print(f"\n🔄 Extracting page 1 from {pdf_file.name}...")

writer = PdfWriter()
writer.add_page(pdf.pages[0])

temp_pdf = io.BytesIO()
writer.write(temp_pdf)
temp_pdf.seek(0)

# Send to Tika
print(f"📤 Sending to Tika Server on port {TIKA_PORT}...")
start_time = time.time()

try:
    response = requests.put(
        TIKA_URL,
        data=temp_pdf.getvalue(),
        headers={"Content-Type": "application/pdf"},
        timeout=30,
    )
    response.raise_for_status()

    elapsed = time.time() - start_time
    print(f"✓ Response in {elapsed:.2f}s (status: {response.status_code})")

    text = response.text[:500]  # First 500 chars
    print(f"\n📝 Extracted text preview:\n{text}\n...")

    # Count lines
    lines = response.text.split("\n")
    non_empty = [l for l in lines if l.strip()]
    print(f"\n✓ Total lines: {len(lines)}, Non-empty: {len(non_empty)}")

except requests.exceptions.RequestException as e:
    print(f"❌ Error: {e}")
    exit(1)

print("\n✅ Test successful!")

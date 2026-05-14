"""Test different Tika endpoints to see if they return page information."""

import requests
import json
from pathlib import Path
from xml.etree import ElementTree as ET

# Use first Tika server
TIKA_PORT = 9998
BASE_URL = f"http://localhost:{TIKA_PORT}"

# Find first PDF
pdf_dir = Path("data/raw/pdf/2025")
pdf_files = list(pdf_dir.glob("*.pdf"))
pdf_file = pdf_files[0]
pdf_bytes = pdf_file.read_bytes()

print(f"📄 Testing with: {pdf_file.name} ({len(pdf_bytes):,} bytes)\n")

# Test 1: PUT /tika with Accept: application/xml
print("=" * 80)
print("TEST 1: PUT /tika (Accept: text/xml) - Tika XML output")
print("=" * 80)
try:
    response = requests.put(
        f"{BASE_URL}/tika",
        data=pdf_bytes,
        headers={"Content-Type": "application/pdf", "Accept": "text/xml"},
        timeout=30,
    )
    xml_text = response.text[:2000]
    print(f"Status: {response.status_code}")
    print(f"Content preview:\n{xml_text}\n")
except Exception as e:
    print(f"❌ Error: {e}\n")

# Test 2: PUT /tika/{handler} with handler=json
print("=" * 80)
print("TEST 2: PUT /tika/json - JSON output")
print("=" * 80)
try:
    response = requests.put(
        f"{BASE_URL}/tika/json",
        data=pdf_bytes,
        headers={"Content-Type": "application/pdf"},
        timeout=30,
        json=None,  # Even though we're not sending JSON, the endpoint expects file in body
    )
    response.raise_for_status()
    try:
        data = response.json()
        print(f"Status: {response.status_code}")
        print(f"JSON keys: {list(data.keys()) if isinstance(data, dict) else 'List'}")
        if isinstance(data, dict):
            for key in list(data.keys())[:5]:  # First 5 keys
                val = data[key]
                if isinstance(val, str):
                    print(f"  {key}: {val[:100]}...")
                elif isinstance(val, list):
                    print(f"  {key}: List[{len(val)} items]")
                else:
                    print(f"  {key}: {type(val).__name__}")
    except:
        print(f"Status: {response.status_code}")
        print(f"Response preview:\n{response.text[:1000]}\n")
except Exception as e:
    print(f"❌ Error: {e}\n")

# Test 3: PUT /rmeta/json - Recursive metadata as JSON
print("=" * 80)
print("TEST 3: PUT /rmeta/json - Recursive metadata (JSON)")
print("=" * 80)
try:
    response = requests.put(
        f"{BASE_URL}/rmeta/json",
        data=pdf_bytes,
        headers={"Content-Type": "application/pdf"},
        timeout=30,
    )
    response.raise_for_status()
    try:
        data = response.json()
        print(f"Status: {response.status_code}")
        print(f"Type: {type(data).__name__}")
        if isinstance(data, list) and len(data) > 0:
            print(f"List with {len(data)} items")
            first_item = data[0]
            print(f"First item keys: {list(first_item.keys())}")
            # Look for page info
            for key in [
                "page",
                "Page",
                "page_number",
                "X-TIKA:content_handler",
                "resourceName",
            ]:
                if key in first_item:
                    print(f"  Found: {key} = {first_item[key]}")
        elif isinstance(data, dict):
            print(f"Dict keys: {list(data.keys())[:10]}")
    except:
        print(f"Status: {response.status_code}")
        print(f"Response preview:\n{response.text[:1000]}\n")
except Exception as e:
    print(f"❌ Error: {e}\n")

# Test 4: PUT /meta with detailed metadata
print("=" * 80)
print("TEST 4: PUT /meta - Metadata extraction (CSV format)")
print("=" * 80)
try:
    response = requests.put(
        f"{BASE_URL}/meta",
        data=pdf_bytes,
        headers={"Content-Type": "application/pdf", "Accept": "text/csv"},
        timeout=30,
    )
    response.raise_for_status()
    print(f"Status: {response.status_code}")
    print(f"CSV preview:\n{response.text[:500]}\n")
except Exception as e:
    print(f"❌ Error: {e}\n")

# Test 5: Try processing with XML and look for page tags
print("=" * 80)
print("TEST 5: PUT /tika (Accept: text/xml) - Parse for page structure")
print("=" * 80)
try:
    response = requests.put(
        f"{BASE_URL}/tika",
        data=pdf_bytes,
        headers={"Content-Type": "application/pdf", "Accept": "text/xml"},
        timeout=30,
    )
    response.raise_for_status()

    # Try to parse XML
    try:
        root = ET.fromstring(response.text)
        print(f"Status: {response.status_code}")
        print(f"Root tag: {root.tag}")

        # Look for page-related elements
        for elem in root.iter():
            if "page" in elem.tag.lower() or "p" == elem.tag:
                print(f"Found element: {elem.tag} with attribs: {elem.attrib}")
                break

        # Count different tags
        tags = {}
        for elem in root.iter():
            tags[elem.tag] = tags.get(elem.tag, 0) + 1
        print(f"\nTag frequency (top 10):")
        for tag, count in sorted(tags.items(), key=lambda x: -x[1])[:10]:
            print(f"  {tag}: {count}")

    except ET.ParseError as pe:
        print(f"XML parse error: {pe}")
        print(f"Raw text preview:\n{response.text[:1000]}")

except Exception as e:
    print(f"❌ Error: {e}\n")

print("\n" + "=" * 80)
print("✅ Endpoint testing complete!")
print("=" * 80)

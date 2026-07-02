import pandas as pd
from src.etl.extractors.pdf_content_extractor import PdfContentExtractor
from src.etl.transformers.pdf_content_transformer import PdfContentTransformer
from config.settings import BASE_OUTPUT_PROCESSED_DIR
import os

df = pd.read_parquet('./data/processed/test_2021/sections_2021_test.parquet')
datos = df[df['content'].str.contains('DATOS DE LA LICITACI[OÓ]N', na=False)]

extractor = PdfContentExtractor()
pdf_lines_path = os.path.join(BASE_OUTPUT_PROCESSED_DIR, 'parquet/pdf-to-parquet', 'combined_documents_all_years.parquet')

print(f"Sections with DATOS in content: {len(datos)}")
print(f"All are Adenda: {(datos['title'].str.lower().str.strip() == 'adenda').all()}")

# Check one example
row = datos.iloc[0]
doc_id = row['document_id']
lines = row['content'].split('\n')
print(f"\nExample: {doc_id}")
print(f"  page={row['page']} line_start={row['line_start']} line_end={row['line_end']}")
print(f"  content_length={row['content_length']}")

# Find DATOS line index in content
for i, line in enumerate(lines):
    if 'DATOS DE LA LICITACI' in line.upper():
        print(f"  DATOS appears at content line {i}: {line[:80]}")
        break

# Check where DATOS would be on the page
# The content starts at line_start on the page, so DATOS at content line i
# means it's at page line: line_start + i
print(f"  DATOS at PDF page line: {row['line_start'] + i}")
print(f"  Section starts at page {row['page']} line {row['line_start']}")
print(f"  DATOS likely on same page or next page?")

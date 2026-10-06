#!/usr/bin/env python3
import os
from dotenv import load_dotenv

load_dotenv()

import psycopg2

conn = psycopg2.connect(
    user="postgres",
    password=os.getenv("DB_PASSWORD"),
    host="localhost",
    port=5433,
    database="dncp"
)

cursor = conn.cursor()

cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'dncp'
      AND table_name = 'document_features_v2'
      AND (column_name LIKE '%pct%' OR column_name LIKE '%ratio%' OR column_name LIKE '%dias%' OR column_name LIKE '%num%' OR column_name LIKE '%anos%' OR column_name LIKE '%garantia%')
      AND column_name NOT LIKE '%_conf%'
      AND column_name NOT LIKE '%_source%'
    ORDER BY column_name
""")

cols = [row[0] for row in cursor.fetchall()]
print(f"Total columns with key terms: {len(cols)}")

cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'dncp'
      AND table_name = 'document_features_v2'
      AND column_name = 'ratio_liquidez_boost'
""")
result = cursor.fetchone()
if result:
    print(f"✓ ratio_liquidez_boost found: {result[0]}")
else:
    print(f"✗ ratio_liquidez_boost NOT found")

conn.close()
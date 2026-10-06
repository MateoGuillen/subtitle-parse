import sys
sys.path.insert(0, r'D:\projects\subtitle-parse')
from config.settings import DB_CONFIG
import psycopg2
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'dncp'
      AND table_name = 'document_economic_features'
    ORDER BY ordinal_position
""")
for r in cur.fetchall():
    print('{:45s} {}'.format(r[0], r[1]))
conn.close()

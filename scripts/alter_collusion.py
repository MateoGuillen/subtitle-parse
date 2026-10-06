import sys
sys.path.insert(0, r'D:\projects\subtitle-parse')
from config.settings import DB_CONFIG
import psycopg2

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

statements = [
    "ALTER TABLE dncp.document_economic_features ADD COLUMN IF NOT EXISTS winner_category_frequency INTEGER DEFAULT 0",
    "ALTER TABLE dncp.document_economic_features ADD COLUMN IF NOT EXISTS winner_total_contracts INTEGER DEFAULT 0",
    "ALTER TABLE dncp.document_economic_features ADD COLUMN IF NOT EXISTS is_repeat_winner BOOLEAN DEFAULT FALSE",
    "ALTER TABLE dncp.document_economic_features ADD COLUMN IF NOT EXISTS bidder_diversity DOUBLE PRECISION DEFAULT 0.0",
]
for s in statements:
    cur.execute(s)
    print('OK:', s[:60])

cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'dncp'
      AND table_name = 'document_economic_features'
    ORDER BY ordinal_position
""")
for r in cur.fetchall():
    print('  {:45s} {}'.format(r[0], r[1]))

conn.commit()
conn.close()

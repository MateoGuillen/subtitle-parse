import sys; sys.path.insert(0, 'D:\\projects\\subtitle-parse')
from config.settings import DB_CONFIG
import psycopg2
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='dncp' AND table_name='document_economic_features' ORDER BY ordinal_position")
for r in cur.fetchall():
    print(f'{r[0]:45s} {r[1]}')
conn.close()

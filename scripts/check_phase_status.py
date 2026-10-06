import sys; sys.path.insert(0, r'D:\projects\subtitle-parse')
from config.settings import DB_CONFIG
import psycopg2
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM dncp.document_economic_features')
print('econ rows:', str(cur.fetchone()[0]))
cur.execute('SELECT COUNT(*) FROM dncp.document_features')
print('df rows:', str(cur.fetchone()[0]))
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='dncp' AND table_name='document_features' AND column_name LIKE 'gini_%'")
r = cur.fetchone()
print('has gini:', 'yes' if r else 'no')
conn.close()

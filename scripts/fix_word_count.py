import sys; sys.path.insert(0, r'D:\projects\subtitle-parse')
from config.settings import DB_CONFIG
import psycopg2
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("UPDATE dncp.pliegos_secciones SET word_count = array_length(string_to_array(content_text, ' '), 1) WHERE content_text IS NOT NULL AND word_count = 0")
print('Updated ' + str(cur.rowcount) + ' rows')
conn.commit()
cur.execute("SELECT MIN(word_count), AVG(word_count), MAX(word_count), COUNT(*) FROM dncp.pliegos_secciones WHERE word_count > 0")
r = cur.fetchone()
print('word_count stats (non-zero): min=' + str(r[0]) + ' avg=' + str(round(r[1],1)) + ' max=' + str(r[2]) + ' count=' + str(r[3]))
cur.execute("SELECT COUNT(*) FROM dncp.pliegos_secciones WHERE word_count = 0")
print('still zero: ' + str(cur.fetchone()[0]))
conn.close()

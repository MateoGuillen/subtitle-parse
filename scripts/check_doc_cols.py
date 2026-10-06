import sys; sys.path.insert(0, r'D:\projects\subtitle-parse')
import pandas as pd
from sqlalchemy import create_engine
from config.settings import DB_CONFIG
user = DB_CONFIG['user']
pw = DB_CONFIG['password']
host = DB_CONFIG['host']
port = DB_CONFIG['port']
db = DB_CONFIG['database']
conn_str = f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}'
df = pd.read_sql('SELECT * FROM dncp.document_features LIMIT 1', create_engine(conn_str))
print(list(df.columns[:5]))

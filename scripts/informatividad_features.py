"""Informatividad de TODAS las features (3 tablas): nulls, distintos, frac modal, entropía -> data/processed/llm_benchmark/informatividad_features.csv"""
import sys, math, psycopg2, pandas as pd
sys.path.insert(0,'.')
from config.settings import DB_CONFIG
cfg=dict(DB_CONFIG); cfg['dbname']=cfg.pop('database')
conn=psycopg2.connect(**cfg, options='-c default_transaction_read_only=on -c statement_timeout=600000'); cur=conn.cursor()
rows=[]
for tbl in ['document_features','document_economic_features','document_features_v3']:
    cur.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='dncp' AND table_name=%s ORDER BY ordinal_position",(tbl,))
    cols=[(c,t) for c,t in cur.fetchall() if c not in ('nro_licitacion','year','category_id','llm_model') and not c.endswith(('_conf','_source','_status'))]
    cur.execute(f"SELECT count(*) FROM dncp.{tbl}"); n=cur.fetchone()[0]
    for c,t in cols:
        if t in ('text','character varying'):
            cur.execute(f'SELECT count(*) FILTER (WHERE "{c}" IS NULL), count(DISTINCT "{c}") FROM dncp.{tbl}'); nn,nd=cur.fetchone()
            cur.execute(f'SELECT count(*) FROM dncp.{tbl} GROUP BY "{c}" ORDER BY 1 DESC LIMIT 1'); top=cur.fetchone()[0]
            rows.append((tbl,c,'text',n,nn,nd,top/n,None)); continue
        cur.execute(f'SELECT "{c}", count(*) FROM dncp.{tbl} WHERE "{c}" IS NOT NULL GROUP BY 1')
        d=dict(cur.fetchall()); k=sum(d.values())
        if not k: rows.append((tbl,c,t,n,n,0,None,None)); continue
        top=max(d.values()); ent=-sum((v/k)*math.log2(v/k) for v in d.values())
        rows.append((tbl,c,t,n,n-k,len(d),top/k,round(ent,3)))
df=pd.DataFrame(rows,columns=['tabla','col','tipo','n','nulls','distintos','frac_modal','entropia'])
df['cobertura']=1-df.nulls/df.n
df.to_csv('data/processed/llm_benchmark/informatividad_features.csv',index=False)
print(df.tabla.value_counts().to_dict())
for tbl,g in df.groupby('tabla'):
    print(f"\n== {tbl}: {len(g)} cols | constantes(frac_modal>=0.99 o distintos<=1): {int(((g.frac_modal>=0.99)|(g.distintos<=1)).sum())} | cobertura<10%: {int((g.cobertura<0.10).sum())} | informativas (entropía>=1 y cobertura>=0.5): {int(((g.entropia>=1)&(g.cobertura>=0.5)).sum())}")

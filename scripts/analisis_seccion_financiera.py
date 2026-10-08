"""Análisis de las secciones "capacidad financiera" y "reajuste" (dump) + ratios B1/B2.
Solo lectura. Salidas en el directorio del argumento 1 (por defecto /tmp)."""
import sys, re, hashlib, collections
import pandas as pd, psycopg2
sys.path.insert(0, ".")
from config.settings import DB_CONFIG
S = (sys.argv[1] if len(sys.argv) > 1 else "/tmp") + "/"

# ===== fin =====
cfg=dict(DB_CONFIG); cfg['dbname']=cfg.pop('database')
cur=psycopg2.connect(**cfg, options='-c default_transaction_read_only=on').cursor()
for title,out in [('capacidad financiera','fin'),('reajuste','rea')]:
    cur.execute("SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones WHERE title_normalized=%s AND content_text IS NOT NULL",(title,))
    rows=cur.fetchall(); d={}; c=collections.Counter()
    for nro,t in rows:
        n=re.sub(r"\s+"," ",t).strip(); h=hashlib.sha256(n.lower().encode()).hexdigest(); c[h]+=1; d.setdefault(h,(str(nro),n))
    df=pd.DataFrame([(h,n,t,c[h]) for h,(n,t) in d.items()],columns=['h','nro','texto','n_docs']); df.to_pickle(S+out+'.pkl')
    print(f"\n===== {title}: {len(rows)} secciones, {len(df)} textos")
    for r in df.sort_values('n_docs',ascending=False).head(3).itertuples(): print(f"\n[{r.n_docs} docs]",r.texto[:1300])

# ===== fin2 =====
df=pd.read_pickle(S+'fin.pkl')
df['seg']=df.texto.str.replace(r"^.*?se considerar[aá]n los siguientes [ií]ndices\s*:?","",regex=True,case=False)
df['na']=df.seg.str.fullmatch(r"\W*no aplica\W*",case=False)
N=r"\[?\(?\s*(\d+(?:[.,]\d+)?)\s*\)?\]?"
LIQ=re.compile(r"liquidez[^.;]{0,160}?(?:mayor|superior|m[ií]nim|≥|>=|no (?:deber[aá] ser )?menor|igual a)[^0-9\[]{0,30}"+N,re.I)
END=re.compile(r"(?:endeudamiento|solvencia|pasivo total\s*/\s*activo total)[^.;]{0,160}?(?:mayor|menor|inferior|superior|m[aá]xim|≤|<=|igual a)[^0-9\[]{0,30}"+N,re.I)
def vals(rx,s): return [float(m.group(1).replace(',','.')) for m in rx.finditer(s)]
df['liq']=df.seg.map(lambda s:vals(LIQ,s)); df['end']=df.seg.map(lambda s:vals(END,s))
df['liq_ment']=df.seg.str.contains('liquidez',case=False); df['end_ment']=df.seg.str.contains(r'endeudamiento|solvencia',case=False)
df['placeholder']=df.seg.str.contains(r"\[indicar|\[\s*\]|haga clic|xx",case=False)
nn=df[~df.na]; D=lambda m:int(m.n_docs.sum())
print('docs',D(df),'NoAplica',D(df[df.na]),'| menciona liquidez',D(nn[nn.liq_ment]),'con valor',D(nn[nn.liq.str.len()>0]),'| endeud',D(nn[nn.end_ment]),'con valor',D(nn[nn.end.str.len()>0]),'| placeholder',D(nn[nn.placeholder]))
for c,mod in [('liq',1.0),('end',0.8)]:
    v=nn[nn[c].str.len()>0]; s=v[c].map(lambda l:tuple(sorted(set(l))))
    vc=v.assign(k=s).groupby('k').n_docs.sum().sort_values(ascending=False)
    print(f'\n{c} combos (docs):',vc.head(14).to_dict())
    print(f'{c}: un valor={D(v[s.map(len)==1])} varios={D(v[s.map(len)>1])}  no-estándar (algún valor != {mod}) = {D(v[s.map(lambda k:any(x!=mod for x in k))])}')
# otros índices
for k,rx in [('rentabilidad','rentabilidad'),('eficiencia',r'eficiencia|ingreso\s*/\s*egreso'),('capital de trabajo','capital de trabajo'),('patrimonio neto','patrimonio'),('facturación/volumen','facturaci[oó]n|volumen de negocio|ventas|ingresos? (promedio|anual|bruto)'),('línea de crédito','l[ií]nea de cr[eé]dito|carta de cr[eé]dito|solvencia bancaria')]:
    print(f"  índice {k:22s} docs={D(nn[nn.seg.str.contains(rx,case=False,regex=True)])}")
df.to_pickle(S+'fin2.pkl')

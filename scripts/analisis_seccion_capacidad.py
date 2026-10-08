"""Análisis de la sección "capacidad tecnica" para las fichas A5/A6 (años y porcentajes).
Solo lectura. Salidas en el directorio del argumento 1 (por defecto /tmp)."""
import sys, re, hashlib, collections
import pandas as pd, psycopg2
sys.path.insert(0, ".")
from config.settings import DB_CONFIG
S = (sys.argv[1] if len(sys.argv) > 1 else "/tmp") + "/"

# ===== cap =====
cfg=dict(DB_CONFIG); cfg['dbname']=cfg.pop('database')
cur=psycopg2.connect(**cfg, options='-c default_transaction_read_only=on').cursor()
cur.execute("""SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones
 WHERE title_normalized='capacidad tecnica' AND content_text IS NOT NULL""")
rows=cur.fetchall(); d={}; c=collections.Counter()
for nro,t in rows:
    n=re.sub(r"\s+"," ",t).strip(); h=hashlib.sha256(n.lower().encode()).hexdigest(); c[h]+=1; d.setdefault(h,(str(nro),n))
df=pd.DataFrame([(h,n,t,c[h]) for h,(n,t) in d.items()],columns=['h','nro','texto','n_docs'])
df.to_pickle(S+'cap.pkl')
print(len(rows),'secciones',len(df),'textos'); print(df.texto.str.len().describe().round())
for r in df.sort_values('n_docs',ascending=False).head(4).itertuples(): print(f"\n[{r.n_docs} docs]",r.texto[:900])

# ===== cap2 =====
df=pd.read_pickle(S+'cap.pkl')
df['na']=df.texto.str.contains(r":\s*no aplica\W*$",case=False,regex=True)
W={'un':1,'uno':1,'dos':2,'tres':3,'cuatro':4,'cinco':5,'seis':6,'siete':7,'ocho':8,'nueve':9,'diez':10,'quince':15,'veinte':20}
NW="|".join(sorted(W,key=len,reverse=True))
YR=re.compile(rf"\(?\s*(\d{{1,2}})\s*\)?\s*(?:\(\s*[a-zá-ú ]+\s*\)\s*)?a[ñn]os?\b|\b({NW})\s*(?:\(\s*\d+\s*\)\s*)?a[ñn]os?\b",re.I)
def cls_y(pre,post):
    p=pre[-90:].lower(); q=post[:120].lower(); w=p+' '+q
    if re.search(r"[uú]ltimos?\s*$|en los [uú]ltimos|durante los [uú]ltimos|de los [uú]ltimos|[uú]ltimos\s*\(?$",p): return 'ventana_ultimos'
    if re.search(r"antig[uü]edad|fabricaci|modelo|veh[ií]culo|equipo|vida [uú]til|garant[ií]a",w) and not re.search('experiencia',w): 
        return 'antig_empresa' if re.search(r"empresa|firma|oferente|rubro|constitu|en el mercado|existencia",w) else 'trampa_bien'
    if re.search(r"constitu|existencia|funcionamiento|en el mercado|inscripci",w) and not re.search('experiencia',w): return 'antig_empresa'
    if re.search("experiencia",w):
        if re.search(r"profesional|t[eé]cnico|ingenier|personal|responsable|director|residente|coordinador|especialista|licenciad|arquitect|m[eé]dic|chofer|operador|supervisor|t[ií]tulo|egresad",w): return 'exp_personal'
        return 'exp_empresa'
    if re.search(r"edad|mayor de",w): return 'trampa_edad'
    return 'otro'
rows=[]
for r in df[~df.na].itertuples():
    t=r.texto
    for m in YR.finditer(t):
        v=int(m.group(1)) if m.group(1) else W[m.group(2).lower()]
        rows.append((r.h,r.n_docs,v,cls_y(t[max(0,m.start()-200):m.start()],t[m.end():m.end()+150]),t[max(0,m.start()-170):m.end()+110]))
Y=pd.DataFrame(rows,columns=['h','n_docs','v','cat','ctx']); Y.to_pickle(S+'cap_y.pkl')
D=lambda s:int(df[df.h.isin(s)].n_docs.sum())
tot=int(df.n_docs.sum()); print('docs',tot,'No Aplica',int(df[df.na].n_docs.sum()))
print('docs con alguna mención "N años":',D(Y.h.unique()))
print(Y.groupby('cat').apply(lambda g:pd.Series({'menciones':len(g),'textos':g.h.nunique(),'docs':D(g.h.unique())})).sort_values('docs',ascending=False))
for c in ['exp_empresa','exp_personal','ventana_ultimos','antig_empresa']:
    g=Y[Y.cat==c].groupby('h').v.max(); print(c,'max por texto, docs:',df.set_index('h').loc[g.index].assign(v=g.values).groupby('v').n_docs.sum().sort_values(ascending=False).head(10).to_dict())
both=set(Y[Y.cat=='exp_empresa'].h)&set(Y[Y.cat=='exp_personal'].h); print('textos con exp empresa Y personal:',len(both),'docs',D(both))
mult=Y[Y.cat=='exp_empresa'].groupby('h').v.nunique(); print('textos con >1 valor exp_empresa:',(mult>1).sum())

# ===== cap3 =====
df=pd.read_pickle(S+'cap.pkl'); df['na']=df.texto.str.contains(r":\s*no aplica\W*$",case=False,regex=True)
P=re.compile(r"(\d{1,3}(?:[.,]\d+)?)\s*(?:\)\s*)?(?:%|por\s*ciento)",re.I)
def cls(pre,post):
    w=(pre[-160:]+' '+post[:120]).lower()
    if re.search(r"consorci|l[ií]der|integrante|socio",w): return 'consorcio'
    if re.search(r"anticipo",w): return 'anticipo'
    if re.search(r"0[,.]4|contribuci|retenci",w): return 'retencion'
    if re.search(r"garant[ií]a",w): return 'garantia'
    if re.search(r"monto|valor|oferta|lote|facturaci|importe|adjudicad|contrat(o|ado)|presupuesto|total ofertado",w):
        if re.search(r"referencia|experiencia|antecedente|factura|contrato[s]? (similar|ejecutad|cumplid)|facturaci|acredit|demostrar|similares",w): return 'ref_monto'
        return 'monto_otro'
    if re.search(r"cumplimiento|avance|ejecuci",w): return 'cumplimiento/avance'
    return 'otro'
rows=[]
for r in df[~df.na].itertuples():
    t=r.texto
    for m in P.finditer(t):
        v=float(m.group(1).replace(',','.')); rows.append((r.h,r.n_docs,v,cls(t[max(0,m.start()-220):m.start()],t[m.end():m.end()+150]),t[max(0,m.start()-200):m.end()+120]))
X=pd.DataFrame(rows,columns=['h','n_docs','v','cat','ctx']); X.to_pickle(S+'cap_p.pkl')
D=lambda s:int(df[df.h.isin(s)].n_docs.sum())
print('docs con algún %:',D(X.h.unique()))
print(X.groupby('cat').apply(lambda g:pd.Series({'menciones':len(g),'textos':g.h.nunique(),'docs':D(g.h.unique())})).sort_values('docs',ascending=False))
g=X[X.cat=='ref_monto']; per=g.groupby('h').v.agg(lambda s:tuple(sorted(set(s))))
print('ref_monto valores (docs):',df.set_index('h').loc[per.index].assign(v=per.values).groupby('v').n_docs.sum().sort_values(ascending=False).head(12).to_dict())
for c,k in [('ref_monto',7),('monto_otro',4),('otro',4),('cumplimiento/avance',2)]:
    print(f"\n######## {c}")
    for t in X[X.cat==c].drop_duplicates('h').sample(min(k,(X.cat==c).sum()),random_state=3).ctx: print("  •",re.sub(r'\s+',' ',t)[:300])

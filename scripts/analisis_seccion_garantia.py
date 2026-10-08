"""Análisis de la sección "periodo de validez de la garantia de los bienes" para la ficha A2.
Solo lectura. Salidas en el directorio del argumento 1 (por defecto /tmp)."""
import sys, re, hashlib, collections
import pandas as pd, psycopg2
sys.path.insert(0, ".")
from config.settings import DB_CONFIG
S = (sys.argv[1] if len(sys.argv) > 1 else "/tmp") + "/"

# ===== val =====
cfg=dict(DB_CONFIG); cfg['dbname']=cfg.pop('database')
cur=psycopg2.connect(**cfg, options='-c default_transaction_read_only=on').cursor()
cur.execute("""SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones
 WHERE title_normalized='periodo de validez de la garantia de los bienes' AND content_text IS NOT NULL""")
rows=cur.fetchall(); d={}; c=collections.Counter()
for nro,t in rows:
    n=re.sub(r"\s+"," ",t).strip(); h=hashlib.sha256(n.lower().encode()).hexdigest(); c[h]+=1; d.setdefault(h,(str(nro),n))
df=pd.DataFrame([(h,n,t,c[h]) for h,(n,t) in d.items()],columns=['h','nro','texto','n_docs'])
df.to_pickle(S+'val.pkl')
print(len(rows),'secciones',len(df),'textos'); print(df.texto.str.len().describe().round())
top=df.sort_values('n_docs',ascending=False).head(6)
for r in top.itertuples(): print(f"\n[{r.n_docs} docs]",r.texto[:700])

# ===== val2 =====
df=pd.read_pickle(S+'val.pkl')
A=re.compile(r"ser[aá] el siguiente\s*:?",re.I)
df['seg']=df.texto.map(lambda t:(m:=A.search(t)) and t[m.end():].strip())
print('sin ancla:',df.seg.isna().sum())
df['seg']=df.seg.fillna(df.texto)
WN={'un':1,'una':1,'uno':1,'dos':2,'tres':3,'cuatro':4,'cinco':5,'seis':6,'siete':7,'ocho':8,'nueve':9,'diez':10,'once':11,'doce':12,'dieciocho':18,'veinticuatro':24,'treinta':30,'treinta y seis':36,'sesenta':60,'noventa':90,'ciento ochenta':180,'trescientos sesenta y cinco':365}
NW="|".join(sorted(WN,key=len,reverse=True))
U=r"(d[ií]as?|mes(?:es)?|a[ñn]os?)"
RX=re.compile(rf"(?:\b({NW})\s*)?\(?\s*(\d{{1,4}})\s*\)?\s*(?:\(\s*(?:{NW})\s*\)\s*)?{U}|\b({NW})\s*{U}",re.I)
def conv(n,u):
    u=u.lower()
    if u.startswith('d'): return n
    if u.startswith('m'): return {1:30,2:61,3:91,6:182,12:365,18:548,24:730,36:1095,48:1460,60:1825}.get(n,round(n*30.4167))
    return {1:365,2:730,3:1095,4:1460,5:1825}.get(n,n*365)
def vals(s):
    out=[]
    for m in RX.finditer(s):
        if m.group(2): n=int(m.group(2)); u=m.group(3)
        else: n=WN[m.group(4).lower()]; u=m.group(5)
        ctx=s[max(0,m.start()-80):m.end()+60]
        out.append((conv(n,u),n,u,ctx))
    return out
def cls(s):
    l=s.lower()
    if re.fullmatch(r"\W*no aplica\W*",l) or l.strip() in ('','-','.',',','n/a'): return 'no_aplica'
    v=vals(s)
    if not v:
        if re.search(r"especificaci|suministros requeridos|ficha t[eé]cnica|eett|pliego|apartado|anexo|plantilla|sicp",l): return 'remite_eett'
        if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}|de 20\d\d|31 de diciembre",l): return 'fecha_fija'
        if re.search(r"km|kil[oó]metro|horas",l): return 'km_horas'
        if re.search(r"vida [uú]til",l): return 'vida_util'
        if re.search(r"no aplica",l): return 'no_aplica'
        return 'texto_sin_numero'
    ds=sorted(set(x[0] for x in v))
    return 'un_valor' if len(ds)==1 else 'varios_valores'
df['cls']=df.seg.map(cls); df['vals']=df.seg.map(lambda s: sorted(set(x[0] for x in vals(s))))
g=df.groupby('cls').agg(textos=('h','size'),docs=('n_docs','sum')).sort_values('docs',ascending=False)
g['%docs']=(g.docs/g.docs.sum()*100).round(1); print(g)
one=df[df.cls=='un_valor']; vc=one.assign(v=one.vals.str[0]).groupby('v').n_docs.sum().sort_values(ascending=False)
print('\nun_valor -> días (docs):',vc.head(15).to_dict(),' distintos:',len(vc))
df.to_pickle(S+'val2.pkl')

"""Análisis de la sección "solicitud de pago de anticipo" para las fichas A3/A4.
Solo lectura. Salidas en el directorio del argumento 1 (por defecto /tmp)."""
import sys, re, hashlib, collections
import pandas as pd, psycopg2
sys.path.insert(0, ".")
from config.settings import DB_CONFIG
S = (sys.argv[1] if len(sys.argv) > 1 else "/tmp") + "/"

# ===== ant =====
cfg=dict(DB_CONFIG); cfg['dbname']=cfg.pop('database')
cur=psycopg2.connect(**cfg, options='-c default_transaction_read_only=on').cursor()
cur.execute("""SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones
 WHERE title_normalized='solicitud de pago de anticipo' AND content_text IS NOT NULL""")
rows=cur.fetchall(); d={}; c=collections.Counter()
for nro,t in rows:
    n=re.sub(r"\s+"," ",t).strip(); h=hashlib.sha256(n.lower().encode()).hexdigest(); c[h]+=1; d.setdefault(h,(str(nro),n))
df=pd.DataFrame([(h,n,t,c[h]) for h,(n,t) in d.items()],columns=['h','nro','texto','n_docs'])
df.to_pickle(S+'ant.pkl')
print(len(rows),'secciones',len(df),'textos'); print(df.texto.str.len().describe().round())
for r in df.sort_values('n_docs',ascending=False).head(5).itertuples(): print(f"\n[{r.n_docs} docs]",r.texto[:1500])

# ===== ant2 =====
df=pd.read_pickle(S+'ant.pkl')
A=re.compile(r"(se solicitar[aá] el anticipo ser[aá] \(en d[ií]as corridos\) de|pago de anticipo son como se establecen a continuaci[oó]n)\s*:?",re.I)
B=re.compile(r"\d\.\s*El anticipo es la suma de dinero",re.I)
def seg(t):
    a=None
    for a in A.finditer(t): pass   # última ancla de encabezado
    st=a.end() if a else 0
    b=B.search(t,st); return t[st:b.start() if b else len(t)].strip()
df['seg']=df.texto.map(seg); df['boiler']=df.texto.map(lambda t:(m:=B.search(t)) and t[m.start():])
W={'un':1,'uno':1,'dos':2,'tres':3,'cuatro':4,'cinco':5,'seis':6,'siete':7,'ocho':8,'nueve':9,'diez':10,'quince':15,'veinte':20,'treinta':30,'sesenta':60}
NW="|".join(sorted(W,key=len,reverse=True))
NUM=re.compile(rf"\(?\s*(\d{{1,3}})\s*\)?\s*(?:\(\s*[a-zá-ú ]+\s*\)\s*)?d[ií]as|\b({NW})\s*(?:\(\s*\d+\s*\)\s*)?d[ií]as",re.I)
def num(s):
    m=NUM.search(s or ''); 
    if not m: return None
    return int(m.group(1)) if m.group(1) else W[m.group(2).lower()]
SOL=re.compile(r"plazo m[aá]ximo para la presentaci[oó]n de la solicitud de pago de anticipo\s*(es\s*de|:)?",re.I)
PAG=re.compile(r"plazo o fecha en la cual se abonar[aá] al contratista el monto del anticipo[^:]{0,80}:",re.I)
def after(rx,s,k=220):
    m=rx.search(s); return None if not m else s[m.end():m.end()+k]
df['na']=df.seg.str.fullmatch(r"\W*no aplica\W*",case=False)
df['sol_txt']=df.seg.map(lambda s:after(SOL,s)); df['pag_txt']=df.seg.map(lambda s:after(PAG,s))
# valor = primer N días dentro de los primeros ~120 caracteres tras el ancla, sin cruzar al siguiente ítem
cut=lambda x: re.split(r"\s(?:\d{1,2}[.)-]|[a-h]\))\s",x)[0] if isinstance(x,str) else None
df['sol_v']=df.sol_txt.map(lambda x: num(cut(x)) if x else None); df['pag_v']=df.pag_txt.map(lambda x: num(cut(x)) if x else None)
D=lambda m: int(df[m].n_docs.sum())
tot=D(df.n_docs>0)
print('docs',tot,'| No Aplica',D(df.na),f"({D(df.na)/tot:.1%})")
nn=~df.na
print('\n-- SOLICITUD (docs no-NoAplica =',D(nn),')')
print('  ancla presente + número:',D(nn&df.sol_txt.notna()&df.sol_v.notna()),' ancla sin número:',D(nn&df.sol_txt.notna()&df.sol_v.isna()),' sin ancla:',D(nn&df.sol_txt.isna()))
print('\n-- PAGO (abonará)')
print('  ancla presente + número:',D(nn&df.pag_txt.notna()&df.pag_v.notna()),' ancla sin número:',D(nn&df.pag_txt.notna()&df.pag_v.isna()),' sin ancla:',D(nn&df.pag_txt.isna()))
for c in ['sol_v','pag_v']:
    vc=df[nn&df[c].notna()].groupby(c).n_docs.sum().sort_values(ascending=False); print(f'\n{c} valores (docs):',vc.head(12).to_dict(),'distintos',len(vc))
# boilerplate: ¿es constante? ¿qué días contiene?
bc=df.boiler.dropna().map(lambda b: tuple(sorted(set(re.findall(r'\((\d+)\)\s*d[ií]as|(\d+)\s*d[ií]as',b)))))
print('\nboilerplate textos distintos:',df.boiler.dropna().nunique(),' combos de días en boilerplate:',collections.Counter(bc).most_common(3))
df.to_pickle(S+'ant2.pkl')

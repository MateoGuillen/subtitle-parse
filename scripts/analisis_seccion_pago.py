"""Análisis de la sección "formas y condiciones de pago" para la ficha A1 (especificacion_campos.md).
Paso 1: volcado de textos distintos; paso 2: tramo propio de la entidad; paso 3: clasificación de cada "N días".
Solo lectura. Salidas en un directorio temporal (argumento 1, por defecto /tmp)."""

import sys, re, hashlib, collections, random
import pandas as pd, psycopg2
sys.path.insert(0, ".")
from config.settings import DB_CONFIG
S = (sys.argv[1] if len(sys.argv) > 1 else "/tmp") + "/"
# ===== pago_dump =====
cfg=dict(DB_CONFIG); cfg['dbname']=cfg.pop('database')
conn=psycopg2.connect(**cfg, options='-c default_transaction_read_only=on')
cur=conn.cursor()
cur.execute("""SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones
 WHERE title_normalized='formas y condiciones de pago' AND content_text IS NOT NULL""")
rows=cur.fetchall()
d={}
cnt={}
for nro,t in rows:
    n=re.sub(r"\s+"," ",t).strip()
    h=hashlib.sha256(n.lower().encode()).hexdigest()
    cnt[h]=cnt.get(h,0)+1
    d.setdefault(h,(str(nro),n))
df=pd.DataFrame([(h,n,t,cnt[h]) for h,(n,t) in d.items()],columns=['h','nro','texto','n_docs'])
out=S+'pago.pkl'
df.to_pickle(out); print(len(rows),'secciones;',len(df),'textos distintos ->',out)

# ===== pago_seg =====
df=pd.read_pickle(S+'pago.pkl')
A=re.compile(r"otras formas y condiciones de pago[^:]{0,80}:",re.I)
B=re.compile(r"(\d\.\s*)?la contratante efectuar[aá] los pagos",re.I)
STD=re.compile(r"sin exceder\s+\w+\s*\(\s*(\d+)\s*\)\s*d[ií]as",re.I)
def seg(t):
    a=A.search(t); b=B.search(t, a.end() if a else 0)
    s=t[a.end():b.start()] if a and b else t[a.end():] if a else None
    return s
df['ancla_a']=df.texto.str.contains(A); df['ancla_b']=df.texto.str.contains(B)
df['std']=df.texto.map(lambda t: (m:=STD.search(t)) and int(m.group(1)))
df['seg']=df.texto.map(seg)
df['seg_len']=df.seg.fillna('').str.strip().str.len()
W=lambda d: int(d.n_docs.sum())
print('textos',len(df),'docs',W(df))
print('con ancla "Otras formas":',df.ancla_a.sum(),'| con cláusula estándar:',df.ancla_b.sum())
print('valor estándar:',df['std'].value_counts(dropna=False).to_dict())
print('tramo específico vacío (<15 chars):',(df.seg_len<15).sum(),'textos /',W(df[df.seg_len<15]),'docs')
print('tramo con "d[ií]as":', df.seg.fillna('').str.contains(r'd[ií]as',case=False).sum())
df.to_pickle(S+'pago_seg.pkl')

# ===== pago_cls =====
df=pd.read_pickle(S+'pago_seg.pkl')
W={'uno':1,'dos':2,'tres':3,'cuatro':4,'cinco':5,'seis':6,'siete':7,'ocho':8,'nueve':9,'diez':10,'doce':12,'quince':15,'veinte':20,'treinta':30,'cuarenta':40,'cuarenta y cinco':45,'sesenta':60,'noventa':90,'ciento veinte':120,'ciento ochenta':180}
NUM=re.compile(r"(?:(\d{1,3})|\b("+"|".join(sorted(W,key=len,reverse=True))+r"))\s*(?:\(\s*[\w ]+\s*\)\s*)?(?:\)\s*)?d[ií]as",re.I)
# "treinta (30) días" -> captura 30 por el dígito: buscar primero dígito entre paréntesis
NUM2=re.compile(r"(?:\b(?:"+"|".join(W)+r")\s*)?\(\s*(\d{1,3})\s*\)\s*d[ií]as|(\d{1,3})\s*(?:\([^)]{0,25}\)\s*)?d[ií]as|\b("+"|".join(sorted(W,key=len,reverse=True))+r")\s+d[ií]as",re.I)
CAT=[
 ('frecuencia', r"cada\s+\S+\s*(\(\d+\)\s*)?d[ií]as|mensual|quincenal|semanal|por mes|bimestral"),
 ('no_pago:aceptacion', r"acept(a|ar|aci[oó]n)\s+o\s+rechaz|rechaz"),
 ('no_pago:recibo', r"recibo"),
 ('no_pago:presentar_factura', r"(present(ar|aci[oó]n de)|remitir|entregar)\s+(la\s+)?(s\s+)?factura[s]?\s+(hasta|dentro|en un plazo|a m[aá]s tardar|los primeros|antes)|hasta el d[ií]a\s+\d+"),
 ('no_pago:retencion', r"retenci[oó]n|tesoro|ingreso[s]? del|dncp"),
 ('no_pago:garantia', r"garant[ií]a"),
 ('no_pago:entrega', r"plazo de entrega|entregar los bienes|ejecuci[oó]n del servicio"),
]
PAGO=r"pag[oa]|abon|cancel|efectiv|desembols|acredit|transfer|liquidaci|honrar"
ORIG=[('factura',r"factura"),('solicitud',r"solicitud|pedido de pago|nota de pago"),('recepcion',r"recepci[oó]n|recibid|entrega|acta|conformidad"),('certificado',r"certificado|certificaci[oó]n|informe|planilla|avance"),('aprobacion',r"aprobaci[oó]n|aprobad|aceptad|visto bueno|presentaci[oó]n de (los )?documentos")]
rows=[]
for r in df.itertuples():
    s=r.seg if isinstance(r.seg,str) else ''
    for m in NUM2.finditer(s):
        v=m.group(1) or m.group(2) or m.group(3)
        v=int(v) if v.isdigit() else W[v.lower()]
        ctx=s[max(0,m.start()-160):m.end()+140]
        near=s[max(0,m.start()-160):m.end()+60]
        cat=next((c for c,p in CAT if re.search(p,near,re.I)),None)
        if cat is None: cat='pago' if re.search(PAGO,near,re.I) else 'sin_verbo_pago'
        orig=next((o for o,p in ORIG if re.search(p,s[m.end():m.end()+140],re.I)),'otro/no dice')
        rows.append((r.h,r.n_docs,v,cat,orig,ctx))
M=pd.DataFrame(rows,columns=['h','n_docs','v','cat','orig','ctx'])
M.to_pickle(S+'pago_menc.pkl')
print('menciones',len(M),'en',M.h.nunique(),'textos')
print(M.groupby('cat').agg(menciones=('v','size'),textos=('h','nunique'),docs=('n_docs','sum')).sort_values('menciones',ascending=False))
P=M[M.cat=='pago']
print('\nORIGEN del plazo (cat=pago):'); print(P.groupby('orig').agg(textos=('h','nunique'),docs=('n_docs','sum')).sort_values('textos',ascending=False))
print('\nValores pago:',P.v.value_counts().head(15).to_dict())
per=P.groupby('h').v.agg(lambda x: tuple(sorted(set(x))))
print('\ntextos con 1 valor pago:',(per.map(len)==1).sum(),' con >=2 valores distintos:',(per.map(len)>=2).sum())
print('combinaciones frecuentes:',collections.Counter(per[per.map(len)>=2]).most_common(10))
print('\ntextos con plazo de pago >60:',P[P.v>60].h.nunique(), P[P.v>60].v.value_counts().to_dict())

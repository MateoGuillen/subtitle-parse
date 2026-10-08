"""Clasificación de fórmulas de reajuste (B3). Requiere haber corrido analisis_seccion_financiera.py (genera rea.pkl).
Solo lectura. Salidas en el directorio del argumento 1 (por defecto /tmp)."""
import sys, re, hashlib, collections
import pandas as pd, psycopg2
sys.path.insert(0, ".")
from config.settings import DB_CONFIG
S = (sys.argv[1] if len(sys.argv) > 1 else "/tmp") + "/"

# ===== rea2 =====
df=pd.read_pickle(S+'rea.pkl'); D=lambda m:int(m.n_docs.sum())
t=df.texto.str.lower()
df['no_sujeto']=t.str.contains(r"no estar[aá] sujeto a reajuste|no (se )?(aplica|aplicar[aá]|habr[aá]|corresponde)[^.]{0,40}reajuste|sin reajuste|no ser[aá] reajustad|: no aplica\W*$")
df['tiene_formula']=t.str.contains(r"f[oó]rmula") & t.str.contains(r"\w+\s*=\s*\w")
def tipo(s):
    s=s.lower()
    if re.search(r"ipc\s*1|ipc1|ipc\s*\(?\s*1",s) and not re.search(r"s\s*1\s*/\s*s\s*0|so\b|salario|jornal|combustible|i\.?i\.?b|0[.,][1-9]\d?\s*[x*×]",s): return 'Pr=P×IPC1/IPC0 (3)'
    if re.search(r"i\.?\s*i\.?\s*b\.?\s*c\.?\s*p|iibcp",s): return 'A=P×IIBCP (2)'
    if re.search(r"salario|jornal|\bs\s*1\b|\bs1\b|sm1|sm0",s) and re.search(r"ipc",s): return 'salario+IPC (polinómica corta)'
    if re.search(r"0[.,]\d+\s*[x*×]?\s*\(?\s*\w+\s*1\s*/|coeficiente|ponderaci",s): return 'polinómica con coeficientes'
    if re.search(r"combustible|gasoil|petropar|precio del litro",s): return 'combustible'
    if re.search(r"d[oó]lar|tipo de cambio|cotizaci",s): return 'tipo de cambio'
    return 'otra/sin clasificar'
df['tipo']=[('no sujeto' if n else (tipo(s) if f else 'sin fórmula (prosa)')) for s,n,f in zip(df.texto,df.no_sujeto,df.tiene_formula)]
g=df.groupby('tipo').agg(textos=('h','size'),docs=('n_docs','sum')).sort_values('docs',ascending=False); g['%']=(g.docs/g.docs.sum()*100).round(1); print(g)
def show(k,n):
    sub=df[df.tipo==k]; print(f"\n######## {k}")
    for s in sub.sample(min(n,len(sub)),random_state=1).texto:
        m=re.search(r"f[oó]rmula",s,re.I); i=m.start() if m else 0; print("  •",re.sub(r'\s+',' ',s[i:i+420]))
for k,n in [('polinómica con coeficientes',4),('salario+IPC (polinómica corta)',3),('otra/sin clasificar',6),('sin fórmula (prosa)',4),('combustible',2),('tipo de cambio',2)]: show(k,n)
df.to_pickle(S+'rea2.pkl')

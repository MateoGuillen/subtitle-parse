"""Análisis de la estructura del pago en "formas y condiciones de pago" para las fichas A9/A10
(pago plurianual o diferido y pago por hitos). Solo lectura.

Paso 1: tramo propio de la entidad (entre "Otras formas y condiciones de pago…:" y la cláusula estándar).
Paso 2: subpatrones candidatos de cada variable, con conteo de docs y textos distintos.
Paso 3: validación externa contra dncp.pagos_contrato (pagos reales): años con pagos y cantidad de pagos.
Salidas en un directorio temporal (argumento 1, por defecto /tmp): muestras por subpatrón para lectura.
"""
import collections
import random
import re
import sys

import pandas as pd
import psycopg2

sys.path.insert(0, ".")
from config.settings import DB_CONFIG  # noqa: E402

OUT = (sys.argv[1] if len(sys.argv) > 1 else "/tmp") + "/"
cfg = dict(DB_CONFIG)
cfg["dbname"] = cfg.pop("database")
cur = psycopg2.connect(**cfg, options="-c default_transaction_read_only=on").cursor()
cur.execute("""SELECT s.nro_licitacion::text, COALESCE(extract(year FROM l.fecha_publicacion)::int, l.year), string_agg(s.content_text, E'\n' ORDER BY s.line_start)
 FROM dncp.pliegos_secciones s JOIN dncp.licitaciones l ON l.nro_licitacion::text = s.nro_licitacion::text
 WHERE s.title_normalized='formas y condiciones de pago' AND s.content_text IS NOT NULL GROUP BY 1, 2""")  # año = publicación del llamado
rows = cur.fetchall()
A = re.compile(r"otras formas y condiciones de pago[^:]{0,80}:", re.I)
B = re.compile(r"(\d\.\s*)?la contratante efectuar[aá] los pagos", re.I)


def seg(t):
    t = re.sub(r"\s+", " ", t)
    a = A.search(t)
    if not a:
        return None
    b = B.search(t, a.end())
    return t[a.end():b.start() if b else len(t)]


YEAR = r"(?:20\d\d)"
SUB = {
    # ---- plurianual / diferido
    "P_plurianual": r"plurianual",
    "P_condicional": r"(en\s+caso\s+de\s+(que\s+)?|si\s+)(la\s+contrataci[oó]n|el\s+(llamado|contrato|proceso))\s+"
                     r"(sea|fuera|fuese|resultare?)\s+plurianual",
    "P_ejercicio_siguiente": r"ejercicios?\s+(fiscal(es)?\s+)?(siguientes?|pr[oó]ximos?|futuros?|posteriores?)|"
                             r"a[nñ]o\s+(fiscal\s+)?(siguiente|posterior|pr[oó]ximo)",
    "P_saldo_presupuesto": r"saldo[^.]{0,160}(presupuesto|ejercicio|a[nñ]o)",
    "P_supeditado_presupuesto": r"supeditad[oa]s?\s+a\s+la\s+aprobaci[oó]n\s+del\s+presupuesto",
    "P_anio_explicito": r"(ejercicio\s+(fiscal\s+)?|presupuesto\s+(general\s+)?(de\s+gastos\s+)?(de\s+la\s+naci[oó]n\s+)?"
                        r"(para\s+el\s+)?(del?\s+)?(ejercicio\s+)?(fiscal\s+)?|dentro\s+del\s+a[nñ]o\s+|"
                        r"a\s+finales\s+de\s+\w+\s+(de\s+)?|en\s+el\s+a[nñ]o\s+)(" + YEAR + ")",
    # ---- hitos / cuotas
    "H_ordinal_pago": r"\b(primer|segund|tercer|cuart)[oa]?\s+(pago|cuota|desembolso|tramo|hito)\b",
    "H_n_cuotas": r"\ben\s+(\d+|dos|tres|cuatro|cinco|seis|doce)\s*(\(\w+\)\s*)?(cuotas|pagos|desembolsos|tramos|etapas)",
    "H_pct_monto": r"(\d{1,3}(?:[.,]\d+)?)\s*\)?\s*(%|por\s*ciento)\s*(\))?\s*(del|de\s+la)\s+(valor|monto|total|precio|"
                   r"contrato|importe)",
    "H_saldo_pct": r"(saldo|restante|pago\s+final)[^.]{0,40}\d{1,3}\s*\)?\s*(%|por\s*ciento)",
    "H_fis_template": r"con\s+anterioridad\s+al\s+primer\s+pago",
}
SUB = {k: re.compile(v, re.I) for k, v in SUB.items()}
TRAP_PCT = re.compile(r"multa|retenci|contribuci|garant|anticipo|penalidad|descuento|mora|seguro|tasa", re.I)

recs = []
hits = collections.defaultdict(list)
for nro, year, t in rows:
    s = seg(t)
    if s is None or len(s.strip()) < 15:
        continue
    r = {"nro": nro, "year": year}
    for k, rx in SUB.items():
        ms = list(rx.finditer(s))
        if k == "H_pct_monto":  # % de pago, no multa/retención/garantía/anticipo pegados al número
            ms = [m for m in ms if not TRAP_PCT.search(s[max(0, m.start() - 90):m.end() + 10])]
        if k == "P_anio_explicito":
            ms = [m for m in ms if int(m.group(m.lastindex)) > year]  # año POSTERIOR al del llamado
        r[k] = len(ms)
        if ms:
            m = ms[0]
            hits[k].append((nro, year, s[max(0, m.start() - 200):m.end() + 200]))
    recs.append(r)
df = pd.DataFrame(recs)
n = len(df)
print(f"docs con tramo propio no vacío: {n}")
for k in SUB:
    print(f"  {k:26s} docs={int((df[k] > 0).sum()):6d}  {100 * (df[k] > 0).mean():5.1f}%")

# ---- variables candidatas (primera versión, a ajustar con la lectura)
df["plurianual"] = ((df.P_plurianual > df.P_condicional) | (df.P_ejercicio_siguiente > 0) |
                    (df.P_anio_explicito > 0) | ((df.P_saldo_presupuesto > 0) & (df.P_supeditado_presupuesto > 0)))
df["hitos"] = (df.H_ordinal_pago > 0) | (df.H_n_cuotas > 0) | (df.H_pct_monto >= 2) | (df.H_saldo_pct > 0)
df["hitos"] &= ~((df.H_fis_template > 0) & (df.H_ordinal_pago <= df.H_fis_template) & (df.H_pct_monto < 2)
                 & (df.H_n_cuotas == 0) & (df.H_saldo_pct == 0))
print(f"\nplurianual={int(df.plurianual.sum())} ({100 * df.plurianual.mean():.1f}%)  "
      f"hitos={int(df.hitos.sum())} ({100 * df.hitos.mean():.1f}%)")

# ---- validación externa: pagos reales (demora observada = fecha_pago - fecha_factura)
cur.execute("""SELECT p.nro_licitacion, count(*) / count(DISTINCT p.contrato_id)::float,
   percentile_cont(0.5) WITHIN GROUP (ORDER BY p.fecha_pago::date - p.fecha_factura::date),
   avg((p.fecha_pago::date - p.fecha_factura::date > 60)::int),
   max(extract(year FROM p.fecha_pago)) - min(extract(year FROM c.fecha_firma)),
   max(c.duracion_dias)
 FROM dncp.pagos_contrato p JOIN dncp.contratos c USING (contrato_id)
 WHERE p.fecha_factura IS NOT NULL AND p.fecha_pago >= p.fecha_factura GROUP BY 1""")
pg = pd.DataFrame(cur.fetchall(), columns=["nro", "pagos_por_contrato", "demora_mediana", "pct_pagos_gt60",
                                           "anios_pago_tras_firma", "duracion_contrato"])
m = df.merge(pg, on="nro", how="inner")
print(f"\ncon pagos reales: {len(m)} docs")
for v in ("plurianual", "hitos"):
    g = m.groupby(v).agg(docs=("nro", "size"), pagos_por_contrato=("pagos_por_contrato", "median"),
                         demora_mediana=("demora_mediana", "median"), pct_pagos_gt60=("pct_pagos_gt60", "mean"),
                         paga_2_anios_tras_firma=("anios_pago_tras_firma", lambda x: (x >= 2).mean()),
                         duracion_contrato=("duracion_contrato", "median"))
    print(f"\n{v}:\n{g.round(3).to_string()}")
df.to_pickle(OUT + "pago_estructura.pkl")
random.seed(7)
with open(OUT + "pago_estructura_muestras.txt", "w") as f:
    for k, lst in hits.items():
        f.write(f"\n######## {k} ({len(lst)})\n")
        for nro, year, e in random.sample(lst, min(25, len(lst))):
            f.write(f"- {nro} [{year}] | {e}\n")
print("muestras ->", OUT + "pago_estructura_muestras.txt")

# -*- coding: utf-8 -*-
"""
Anotador pseudo-GT: aplica el extraction_hint de cada variable como regla de anotacion
(yo, DeepSeek, actuo como anotador: las reglas son mi criterio). Marca confianza y casos REVISAR.
Salida: data/processed/alolab/gold_standard_pliego_nuevo_8variables_pseudoGT.csv
"""
import csv, re, collections, pathlib

SRC = r'D:\projects\subtitle-parse\data\processed\alolab\gold_standard_pliego_nuevo_8variables.csv'
OUT = r'D:\projects\subtitle-parse\data\processed\alolab\gold_standard_pliego_nuevo_8variables_pseudoGT.csv'

def norm(s):
    s = (s or '').lower().replace('\x00', '')
    # normalizar tildes para regex sin acentos
    for a, b in [('á','a'),('é','e'),('í','i'),('ó','o'),('ú','u'),('ü','u'),('ñ','n'),('’',"'"),('‘',"'"),('“','"'),('”','"')]:
        s = s.replace(a, b)
    return re.sub(r'\s+', ' ', s)

# ---------- utilidades ----------
def after_colon(t):
    # toma el valor que sigue a "por: |" o "sera de: |" en el primer bloque tabular
    m = re.search(r'(?:por|sera de)\s*:\s*\|\s*([^|]+?)\s*\|', t, re.I)
    return m.group(1).strip() if m else None

def first_int(t):
    m = re.search(r'(\d{1,4})', t)
    return m.group(1) if m else None

def strip_ctx(s):
    return re.sub(r'\s+', ' ', s).strip()

# ---------- anotadores por variable ----------
def anotar_N1(r):
    t = r['text']; low = norm(t)
    # valor en el bloque "por: | N" o "sera de: | N" o "sera de: N"
    m = re.search(r'(?:por|sera de)\s*:\s*\|\s*(\d{1,3})', low)
    if not m:
        m = re.search(r'(?:por|sera de)\s*:\s*(\d{1,3})', low)
    if not m:
        m = re.search(r'validas?\s*\(en dias[^)]*\)\s*por\s*:\s*(\d{1,3})', low)
    if not m:
        return None, "REVISAR: no se hallo patron validez", 0.5
    v = int(m.group(1))
    if not (1 <= v <= 365):
        return None, f"REVISAR: valor fuera de rango {v}", 0.5
    return v, "patron 'por: N' en bloque de validez", 0.98

def anotar_N2(r):
    t = r['text']; low = norm(t)
    m = re.search(r'validez[^|]*sera de\s*:\s*\|\s*(\d{1,3})', low)
    if not m:
        m = re.search(r'validez[^|]*sera de\s*:\s*(\d{1,3})', low)
    if not m:
        return None, "REVISAR: no se hallo plazo garantia", 0.5
    v = int(m.group(1))
    if not (1 <= v <= 365):
        return None, f"REVISAR: fuera de rango {v}", 0.5
    return v, "patron 'validez ... sera de: N'", 0.98

def anotar_N3(r):
    t = r['text']; low = norm(t)
    # plazo de entrega: buscar "entrega... dentro de los N dias", "plazo de entrega de N dias", "N dias ... entrega"
    patrones = [
        r'(?:entrega|entregarse|entrega de los bienes)[^.]{0,200}?(?:dentro de los|plazo de|sera de|en un plazo de)\s*(\d{1,3})\s*(?:dias|días)',
        r'(?:plazo de entrega|plazo para la entrega|entrega se realizara|la entrega sera)\s*(?:de|en)?\s*(\d{1,3})\s*(?:dias|días)',
        r'(?:dentro de los|no mayor a|en un maximo de|hasta)\s*(\d{1,3})\s*(?:dias|días)[^.]{0,80}?entrega',
    ]
    for p in patrones:
        m = re.search(p, low)
        if m:
            v = int(m.group(1))
            if 1 <= v <= 365:
                return v, f"plazo entrega {v} dias (regex)", 0.9
    # si no, buscar "No Aplica" -> NULL
    if 'no aplica' in low or 'conforme a especificaciones tecnicas' in low:
        return None, "seccion sin plazo (No Aplica)", 0.85
    return None, "REVISAR: no se hallo plazo de entrega claro", 0.5

def anotar_N4(r):
    t = r['text']; low = norm(t)
    # visita tecnica: 1 si hay fecha/lugar/hora/indicaciones, 0 si "No Aplica"
    if re.search(r'visita\s+al\s+sitio', low) and ('no aplica' in low.split('visita')[0] or low.strip().endswith('no aplica')):
        return 0, "'No Aplica' -> sin visita", 0.9
    if re.search(r'fecha\s*:|lugar\s*:|hora\s*:', low):
        return 1, "visita configurada (fecha/lugar/hora)", 0.95
    if 'no aplica' in low:
        return 0, "'No Aplica' -> sin visita", 0.9
    return None, "REVISAR: ambiguo", 0.5

def anotar_N5(r):
    t = r['text']; low = norm(t)
    if re.search(r'combinacion de varios criterios|uno o la combinacion|calidad.*precio|mas ventajosa|puntaje|meat|criterios multiples|costo.*evaluado', low):
        if re.search(r'mas baja|menor precio|precio mas bajo', low):
            # si menciona ambos, priorizar combinacion (MEAT) si es explicito
            if re.search(r'combinacion de varios criterios|uno o la combinacion', low):
                return 'calidad_precio', "combinacion de criterios (art.52 Ley 7021)", 0.9
            return 'precio_mas_bajo', "menor precio (menciona combinacion pero predomina menor precio)", 0.6
        return 'calidad_precio', "criterios multiples/MEAT", 0.9
    if re.search(r'costo.*evaluado|costo integral', low):
        return 'costo_evaluado', "costo evaluado", 0.9
    if re.search(r'mas baja|menor precio|precio mas bajo|evaluada como la mas baja', low):
        return 'precio_mas_bajo', "oferta mas baja", 0.98
    return None, "REVISAR: criterio no identificado", 0.5

def anotar_N6(r):
    t = r['text']; low = norm(t)
    # multa en jornales: "2 (dos) jornales", "N jornales minimos diarios"
    m = re.search(r'(\d{1,2})\s*\(([a-z]+)\)\s*jornales?', low)
    if not m:
        m = re.search(r'(\d{1,2})\s*jornales?\s*(?:minimos?|diarios?)?', low)
    if m:
        v = int(m.group(1))
        if 1 <= v <= 100:
            return v, f"multa en {v} jornales", 0.95
    if 'jornal' not in low:
        return None, "sin jornales en la seccion", 0.8
    return None, "REVISAR: jornal presente pero sin numero claro", 0.5

def anotar_N7(r):
    t = r['text']; low = norm(t)
    elim = re.search(r'(seran? desechadas?|seran? descalificadas?|no seran? evaluadas?|sera desestimada|seran? rechazadas?|causales? de descalificacion|requisitos? eliminatorios?)', low)
    if elim:
        return 1, f"lenguaje eliminatorio: '{strip_ctx(elim.group(1))}'", 0.95
    return 0, "sin lenguaje eliminatorio explicito", 0.85

def anotar_N8(r):
    t = r['text']; low = norm(t)
    # "sera de: | N%" o "Se permitira hasta el N%" o "N%" ; "No Aplica" -> NULL
    if re.search(r'no aplica', low):
        return None, "'No Aplica' -> subcontratacion no permitida", 0.9
    m = re.search(r'(?:sera de|permitira|permitido|hasta el|sera el)\s*[^:]*?(\d{1,3})\s*%', low)
    if not m:
        m = re.search(r'(\d{1,3})\s*%', low)
    if m:
        v = float(m.group(1))
        if 0 < v <= 100:
            return v, f"subcontratacion {v}%", 0.95
    # texto no-NUMERICO que no es No Aplica (ej. sistema no preve) -> NULL con nota
    return None, "REVISAR: texto sin % (no es No Aplica)", 0.6

ANNOT = {
    'N1': anotar_N1, 'N2': anotar_N2, 'N3': anotar_N3, 'N4': anotar_N4,
    'N5': anotar_N5, 'N6': anotar_N6, 'N7': anotar_N7, 'N8': anotar_N8,
}

def main():
    with open(SRC, encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    out_rows = []
    stats = collections.defaultdict(lambda: collections.Counter())
    for r in rows:
        v = r['variable']
        val, nota, conf = ANNOT[v](r)
        r = dict(r)
        r['ground_truth'] = '' if val is None else str(val)
        r['nota'] = nota
        r['confianza'] = str(conf)
        r['es_revisar'] = '1' if ('REVISAR' in nota) else '0'
        stats[v]['total'] += 1
        if val is None:
            stats[v]['NULL'] += 1
        else:
            stats[v][str(val)] += 1
        out_rows.append(r)

    fieldnames = list(out_rows[0].keys())
    with open(OUT, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    print(f"Escrito {len(out_rows)} filas -> {OUT}")
    for v in ['N1','N2','N3','N4','N5','N6','N7','N8']:
        c = stats[v]
        n_revisar = sum(1 for r in out_rows if r['variable']==v and r['es_revisar']=='1')
        # top valores
        top = c.most_common(8)
        print(f"{v}: total={c['total']} NULL={c['NULL']} REVISAR={n_revisar} valores={top}")

if __name__ == '__main__':
    main()

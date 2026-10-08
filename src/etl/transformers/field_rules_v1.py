"""Reglas deterministas y features nuevas de la especificación de campos v1.0 (08-oct-2026).

Fuente de verdad: docs/especificacion_campos.md (cada ficha aprobada por el usuario tras leer la
sección completa en la BD; evidencia en scripts/analisis_seccion_*.py). Este módulo reemplaza,
para el pipeline nuevo, a las reglas de la sesión 24 (que siguen en extraction_postprocess.py
porque el parser legado congelado las usa para reproducir resultados publicados).

Contrato de las reglas de valor (RULES_V1):
  - número  -> la regla decide el valor (source='rule')
  - None    -> la regla dictamina AUSENCIA ("No Aplica", remite a otro documento, sin número)
  - NO_RULE -> la regla no sabe (hay candidatos que no clasifica): decide el LLM (residual)
Ratios (B1/B2) son totalitarios: nunca devuelven NO_RULE.

section_features(slug, text) devuelve las columnas nuevas (regex/flags) de una sección.
"""
import re

NO_RULE = object()

# ------------------------------------------------------------------ normalización y números
_UNITS = {"cero": 0, "un": 1, "uno": 1, "una": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5,
          "seis": 6, "siete": 7, "ocho": 8, "nueve": 9, "diez": 10, "once": 11, "doce": 12,
          "trece": 13, "catorce": 14, "quince": 15, "dieciseis": 16, "dieciséis": 16,
          "diecisiete": 17, "dieciocho": 18, "diecinueve": 19, "veinte": 20, "veintiuno": 21,
          "veintidos": 22, "veintidós": 22, "veintitres": 23, "veintitrés": 23,
          "veinticuatro": 24, "veinticinco": 25, "veintiseis": 26, "veintiséis": 26,
          "veintisiete": 27, "veintiocho": 28, "veintinueve": 29}
_TENS = {"treinta": 30, "cuarenta": 40, "cincuenta": 50, "sesenta": 60, "setenta": 70,
         "ochenta": 80, "noventa": 90}
_HUNDREDS = {"cien": 100, "ciento": 100, "doscientos": 200, "trescientos": 300,
             "cuatrocientos": 400, "quinientos": 500}
_MIL = {"mil": 1000}
_NUMWORD = "|".join(sorted(list(_UNITS) + list(_TENS) + list(_HUNDREDS) + list(_MIL), key=len, reverse=True))


def words_to_int(s):
    """'treinta y seis' -> 36, 'ciento veinte' -> 120, 'trescientos sesenta y cinco' -> 365."""
    toks = [t for t in re.split(r"[\s-]+", (s or "").lower().strip()) if t and t != "y"]
    if not toks:
        return None
    total = 0
    for t in toks:
        if t in _MIL:
            total = (total or 1) * 1000
        elif t in _HUNDREDS:
            total += _HUNDREDS[t]
        elif t in _TENS:
            total += _TENS[t]
        elif t in _UNITS:
            total += _UNITS[t]
        else:
            return None
    return total


def normalize(text):
    """Espacios colapsados; 'D Í A S' -> 'DÍAS'; tilde combinante 'di ́as' -> 'días';
    '10 (diez) corridos' -> '10 (diez) días corridos'."""
    t = re.sub(r"\s+", " ", (text or "").replace('"', " ")).strip()
    t = re.sub(r"(\d+)\s*\(\s*(a[ñn]os?|mes(?:es)?|d[ií]as?)\s*\)", r"\1 \2", t, flags=re.I)
    t = re.sub(r"d[ií]\s?[́́]\s?as", "días", t, flags=re.I)
    t = re.sub(r"\b([A-ZÁÉÍÓÚÑ](?:\s[A-ZÁÉÍÓÚÑ]){3,})\b", lambda m: m.group(1).replace(" ", ""), t)
    t = re.sub(r"\b(\d)\s(\d)\b(?=\s*[(\[])", r"\1\2", t)  # OCR: "1 0 ( DIEZ )"
    t = re.sub(r"(\d{1,3}\s*(?:\([^)]{0,25}\)|\[[^\]]{0,8}\])?)\s+(corridos|calendarios?|h[aá]biles)\b",
               r"\1 días \2", t, flags=re.I)
    return t


def num_before(s, pos):
    """Número que precede inmediatamente a `pos` (donde empieza la unidad).

    Orden: '(30)' gana sobre la palabra ('sesenta (30)' -> 30); '[05]'; '30 (treinta)';
    '(diez)'; '30'; 'treinta y seis'."""
    base = max(0, pos - 60)
    w = s[base:pos].rstrip()
    m = re.search(r"(?<![\d.,])(\d{1,2})\.(\d{3})\s*(?:\([^)]*\))?$", w)
    if m:
        return int(m.group(1) + m.group(2)), base + m.start()
    for rx in (r"\(\s*(\d{1,4})\s*\)$", r"\[\s*(\d{1,4})\s*\]$",
               r"(\d{1,4})\s*\(\s*[a-záéíóúñ ]+\s*\)$", r"\(\s*(\d{1,4})\s+[a-záéíóúñ ]+\)$",
               r"(?<![\d.,/])(\d{1,4})$"):
        m = re.search(rx, w, re.I)
        if m:
            return int(m.group(1)), base + m.start()
    m = re.search(r"\(\s*((?:%s)(?:\s+y\s+(?:%s))?(?:\s+(?:%s))?)\s*\)$" % (_NUMWORD, _NUMWORD, _NUMWORD), w, re.I)
    if m:
        return words_to_int(m.group(1)), base + m.start()
    m = re.search(r"(?<![a-záéíóúñ])((?:%s)(?:\s+(?:y\s+)?(?:%s)){0,4})$" % (_NUMWORD, _NUMWORD), w, re.I)
    if m:
        v = words_to_int(m.group(1))
        if v is not None:
            return v, base + m.start()
    return None, None


_BOUND = re.compile(r"(?<=[.;])\s+(?=[A-ZÁÉÍÓÚÑ0-9¿\-•*(])|\s+(?=(?:\d{1,2}|[a-hA-H])[.)-]\s)")


def sentence_span(s, pos):
    """(inicio, fin) de la oración que contiene `pos`."""
    a = 0
    for m in _BOUND.finditer(s, 0, pos):
        a = m.end()
    m = _BOUND.search(s, pos)
    return a, (m.start() if m else len(s))


def _has_text(text):
    """Texto vacío o placeholder (",", "-") = sin insumo: la regla dictamina ausencia."""
    return bool(re.search(r"\w{2,}", text or ""))


def _is_no_aplica(seg):
    return bool(re.fullmatch(r"[\W_]*(?:no\s+aplica|n/?a)[\W_]*", seg or "", re.I))


# sin re.I a propósito: tras "DÍAS" puede venir texto en mayúsculas pegado por el OCR ("DÍASCORRIDOS")
_DAYS = re.compile(r"\b[dD][iíIÍ][aA](?:[sS])?(?![a-záéíóúñ])")
_DUR = re.compile(r"\b(d[ií]as?|mes(?:es)?|a[ñn]os?)\b", re.I)


def _day_mentions(s):
    """[(valor, inicio_numero, fin_unidad)] de cada 'N días' de s."""
    out = []
    for m in _DAYS.finditer(s):
        v, st = num_before(s, m.start())
        if v is not None and v > 0:
            out.append((v, st, m.end()))
    return out


# ------------------------------------------------------------------ A1 dias_pago_maximo
_PAGO_STD = re.compile(r"la\s+contratante\s+efectuar[aá]\s+los\s+pagos", re.I)
_PAGO_STD_END = re.compile(r"posteriores\s+a\s+su\s+presentaci[oó]n\.?", re.I)
_SIN_EXCEDER = re.compile(r"sin\s+exceder\s+(?:los\s+)?(.{0,30}?)\s*d[ií]as", re.I)
_PAY_KW = re.compile(r"pag|abon|efectu|cancel|desembols|transfer|liquidaci|honrar|\ba\s+plazos?\b|"
                     r"plazo\s+(?:de|para\s+el)\s+pago|ser[aá]\s+(?:realizad|efectivizad)", re.I)


def _pago_trap(s, st, en):
    """Motivo por el que el 'N días' en [st, en) NO es el plazo de pago (None si lo es).

    Las trampas se buscan en una ventana local (no en toda la oración): una oración
    puede traer el plazo de pago y, más adelante, una retención o un recibo."""
    a, b = sentence_span(s, st)
    pre = s[max(a, st - 110):st]
    near = pre + s[en:min(b, en + 40)]
    far = pre + s[en:min(b, en + 80)]
    if re.search(r"cada\s*(?:\(\s*)?$", pre[-12:], re.I):
        return "frecuencia"
    if re.search(r"acept(?:a|ar|ad[ao]|aci[oó]n)\s+o\s+(?:no\s+|rechaz)|rechaz|expedir[aá]\s+sobre|se\s+expedir|"
                 r"aprobaci[oó]n\s+o\s+rechazo|plazo\s+de\s+verificaci", near, re.I):
        return "aceptacion"
    if re.search(r"recibo|acreditaci[oó]n\s+(?:de|del)\s+(?:pago|los\s+recursos)", far, re.I):
        return "recibo"
    if re.search(r"(?:primer|segund|tercer|cuart)[oa]?\s+(?:solicitud|pago|cuota|desembolso)|"
                 r"(?:al|del)\s+(?:primer|segundo|tercer)\s+pago", s[en:min(b, en + 80)], re.I):
        return "cuota"
    if re.search(r"FI[SP]\)?\s+deber[aá]n?\s+ser\s+completad|obtenci[oó]n\s+del\s+c[oó]digo\s+de\s+contrataci|"
                 r"emplaz", far, re.I):
        return "fis"
    if re.search(r"deber[aá]n?\s+(?:presentar|entregar|remitir)|ser\s+(?:entregad|presentad)[oa]s?|"
                 r"primeros\s*$|hasta\s+el\s+d[ií]a", pre, re.I):
        return "presentar"
    if re.search(r"retenci[oó]n|retendr|tesoro|contribuci[oó]n|\bdncp\b", near, re.I):
        return "retencion"
    if re.search(r"garant[ií]a|acta\s+de\s+recepci[oó]n[^.]{0,40}emitid|ser[aá]\s+emitid", near, re.I):
        return "otro_tramite"
    sent = s[a:b]
    if not (_PAY_KW.search(sent) or re.search(r"factura|solicitud", sent, re.I)):
        return "sin_pago"
    return None


def _pago_candidates(text):
    s = normalize(text)
    std_val = None
    m = _PAGO_STD.search(s)
    if m:
        e = _PAGO_STD_END.search(s, m.end())
        end = e.end() if e and e.start() - m.end() < 600 else min(len(s), m.end() + 400)
        sx = _SIN_EXCEDER.search(s, m.start(), end)
        if sx:
            std_val = _parse_std(sx)
        s = s[:m.start()] + " " * (end - m.start()) + s[end:]
    cands = []
    for v, st, en in _day_mentions(s):
        if v > 400:
            continue
        if _pago_trap(s, st, en):
            continue
        a, b = sentence_span(s, st)
        cands.append((v, st, en, bool(re.search(r"sin\s+exceder", s[a:b], re.I)), s))
    if std_val is None:
        sx = _SIN_EXCEDER.search(s)
        if sx:
            std_val = _parse_std(sx)
    return cands, std_val


def _parse_std(m):
    frag = m.group(0)
    v, _ = num_before(frag, _DAYS.search(frag).start())
    return v


def dias_pago_maximo(text):
    if not _has_text(text):
        return None
    cands, std_val = _pago_candidates(text)
    own = [c for c in cands if not c[3]] or cands
    if own:
        return float(max(c[0] for c in own))
    if std_val:
        return float(std_val)
    return NO_RULE


def _pago_flags(text):
    cands, std_val = _pago_candidates(text)
    own = [c for c in cands if not c[3]] or cands
    habiles = None
    if own:
        best = max(own, key=lambda c: c[0])
        s = best[4]
        habiles = int(bool(re.match(r"\s*h[aá]biles", s[best[2]:best[2] + 12], re.I)))
    elif std_val:
        habiles = 0
    s = normalize(text)
    plan = int(bool(re.search(r"plan\s+de\s+caja|plan\s+financiero|disponibilidad\s+(?:financiera|presupuestaria)|"
                              r"transferencia\s+de\s+(?:fondos|recursos)", s, re.I)))
    acept = int(bool(re.search(r"acept(?:aci[oó]n|ada)\s+o\s+(?:el\s+)?rechaz[^.]{0,120}?d[ií]as", s, re.I)))
    cert = int(bool(re.search(r"certificado\s+laboral", s, re.I)))
    return {"pago_en_dias_habiles": habiles, "pago_sujeto_plan_caja": plan,
            "tiene_clausula_aceptacion_factura": acept, "exige_certificado_laboral": cert}


# ------------------------------------------------------------------ A2 dias_validez_garantia
_MONTHS = {1: 30, 2: 61, 3: 91, 6: 182, 12: 365, 18: 548, 24: 730, 36: 1095, 48: 1460, 60: 1825}
_G_TRAP = re.compile(r"reposici|\brepon|reemplaz|sustitu|vencimient|caducid|antig[uü]edad|a[ñn]o\s+de\s+fabricaci|"
                     r"fabricaci[oó]n\s+no\s+mayor|garant[ií]a\s+de\s+(?:fiel\s+)?cumplimiento|"
                     r"renova|plazo\s+de\s+entrega|capacitaci|vida\s+[uú]til|indisponibil", re.I)
# soporte/horario: solo pegado al número y en la dirección correcta ("7 días de la semana",
# "soporte en 5 días"); no anula "12 meses. Incluye: Soporte…" ni "5 años con soporte 24 horas"
_G_TRAP_POST = re.compile(r"^\W*(?:de\s+la\s+)?(?:semana|semanal)|^[^.]{0,20}(?:24\s*horas|7\s*x\s*24|24\s*x\s*7)", re.I)
_G_TRAP_PRE = re.compile(r"(?:soporte|tiempo\s+de\s+respuesta|atenci[oó]n)[^.]{0,25}$", re.I)
_G_REMITE = re.compile(r"especificaci|suministros?\s+requerid|ficha\s+t[eé]cnica|\beett\b|plantilla|\bsicp\b|"
                       r"seg[uú]n\s+(?:lo\s+)?(?:establecid|estipulad|indicad|detallad)|vigencia\s+del\s+contrato|"
                       r"duraci[oó]n\s+del\s+contrato|plazo\s+de\s+prestaci", re.I)


def _to_days(n, unit):
    u = unit.lower()
    if u.startswith("d"):
        return n
    if u.startswith("m"):
        return _MONTHS.get(n, round(n * 30.4167))
    return n * 365


def _validez_values(text):
    s = normalize(text)
    anchors = list(re.finditer(r"ser[aá]\s+el\s+siguiente\s*:?", s, re.I))
    seg = s[anchors[-1].end():].strip() if anchors else s
    if not seg or _is_no_aplica(seg):
        return None, seg, 0
    # la sección habla del vencimiento de productos o de la garantía de cumplimiento, no de la del bien
    if (re.search(r"vencimient", seg[:200], re.I) and not re.search(r"garant[ií]a", seg, re.I)) or \
            re.match(r"\W*(?:la\s+)?garant[ií]a\s+de\s+(?:fiel\s+)?cumplimiento", seg, re.I):
        return None, seg, 0
    found, trapped = [], 0
    for m in _DUR.finditer(seg):
        v, st = num_before(seg, m.start())
        if v is None or v <= 0 or v > 3650:
            continue
        a, b = sentence_span(seg, st)
        local = seg[max(a, st - 90):min(b, m.end() + 60)]
        if _G_TRAP.search(local) or _G_TRAP_POST.search(seg[m.end():min(b, m.end() + 25)]) or \
                _G_TRAP_PRE.search(seg[max(a, st - 30):st]):
            trapped += 1
            continue
        found.append((st, m.end(), _to_days(v, m.group(1)), m.group(1).lower().startswith("d")))
    # "6 meses (180 días)": duraciones adyacentes = la misma; mandan los días
    found.sort()
    keep = []
    for f in found:
        if keep and f[0] - keep[-1][1] < 40 and (f[3] or keep[-1][3]):
            if f[3] and not keep[-1][3]:
                keep[-1] = f
            continue
        keep.append(f)
    return [k[2] for k in keep], seg, trapped


def dias_validez_garantia(text):
    if not _has_text(text):
        return None
    vals, seg, trapped = _validez_values(text)
    if vals is None:
        return None
    if vals:
        return float(max(vals))
    if trapped:  # solo había plazos de otros conceptos (vencimiento, reposición…)
        return None
    if _G_REMITE.search(seg) or not re.search(r"\d|" + _NUMWORD, seg, re.I):
        return None
    if not _DUR.search(seg):
        return None
    return NO_RULE


# ------------------------------------------------------------------ A3/A4 anticipo
_ANT_BOILER = re.compile(r"\d\.\s*El\s+anticipo\s+es\s+la\s+suma\s+de\s+dinero", re.I)
_ANT_HEAD = re.compile(r"(?:se\s+solicitar[aá]\s+el\s+anticipo\s+ser[aá]\s+\(en\s+d[ií]as\s+corridos\)\s+de|"
                       r"pago\s+de\s+anticipo\s+son\s+como\s+se\s+establecen\s+a\s+continuaci[oó]n)\s*:?", re.I)
_SOL_ANCHOR = re.compile(r"plazo\s+m[aá]ximo\s+para\s+la\s+presentaci[oó]n\s+de\s+la\s+solicitud\s+de\s+pago\s+"
                         r"de(?:l)?\s+anticipo\s*(?:es\s*de|:)?", re.I)
_PAG_ANCHOR = re.compile(r"plazo\s+o\s+fecha\s+en\s+la\s+cual\s+se\s+(?:abonar|otorgar|pagar|desembolsar)[aá]\s+al\s+(?:contratista|proveedor)"
                         r"\s+el\s+monto\s+del\s+anticipo[^:]{0,90}:", re.I)
_ITEM_END = re.compile(r"\s(?:[a-hA-H]\)|\d{1,2}[.)-])\s|\b(?:direcci[oó]n|horario\s+de\s+atenci|"
                       r"forma\s+de\s+amortizaci|oficina\s+y/o)", re.I)
_ANT_TRAP = re.compile(r"rechaz|devoluci|devuelt|liberad|aceptaci[oó]n\s+o\s+no|amortiz|\bhoras\b", re.I)
_PAG_VERB = re.compile(r"se\s+abonar|abonad|abonar[aá]|se\s+otorgar[aá]\s+al\s+(?:contratista|proveedor)\s+el\s+monto|ser[aá]\s+(?:efectuad|pagad|desembolsad|transferid)|"
                       r"a\s+ser\s+(?:efectuad|abonad|pagad)|desembols|efectuar[aá]\s+el\s+pago|"
                       r"pago\s+del\s+anticipo\s+se\s+(?:realizar|efectuar)", re.I)
_SOL_VERB = re.compile(r"solicit|deber[aá]n?\s+(?:ser\s+)?present|presentar[aá]", re.I)
_DISPON = re.compile(r"disponibilidad|plan\s+de\s+caja|plan\s+financiero|transferencia|ministerio|tesoro|"
                     r"verificaci", re.I)


def _anticipo_seg(text, with_head=False):
    """Tramo de la entidad (sin el texto legal fijo). with_head: además, si el tramo sigue al
    encabezado "El plazo dentro del cual se solicitará el anticipo será (en días corridos) de:",
    que es en sí mismo el ancla del plazo de SOLICITUD."""
    s = normalize(text)
    b = _ANT_BOILER.search(s)
    if b:
        s = s[:b.start()]
    heads = list(_ANT_HEAD.finditer(s))
    seg = s[heads[-1].end():].strip() if heads else s.strip()
    sol_head = bool(heads) and bool(re.search(r"se\s+solicitar[aá]\s+el\s+anticipo", heads[-1].group(0), re.I))
    return (seg, sol_head) if with_head else seg


def _clause(seg, anchor, maxlen):
    m = anchor.search(seg)
    if not m:
        return None
    rest = seg[m.end():m.end() + maxlen]
    e = _ITEM_END.search(rest, 3)
    return rest[:e.start()] if e else rest


def _first_days(clause, trap=True):
    for v, st, en in _day_mentions(clause):
        if trap:
            a, b = sentence_span(clause, st)
            if _ANT_TRAP.search(clause[max(a, st - 80):min(b, en + 40)]):
                continue
        return float(v)
    return None


def _anticipo_free(seg, sol_head=False):
    """Clasifica cada 'N días' del texto libre en ('pago'|'solicitud'|'sin_clasificar', valor).

    sol_head: el tramo sigue al encabezado de solicitud; un plazo en su primera oración (sin verbo
    de pago antes) es el plazo de solicitud ("…será (en días corridos) de: 20 (veinte) días")."""
    out = []
    for v, st, en in _day_mentions(seg):
        a, b = sentence_span(seg, st)
        sent = seg[a:b]
        if _ANT_TRAP.search(seg[max(a, st - 80):min(b, en + 40)]):
            continue
        if sol_head and a == 0 and not _PAG_VERB.search(seg[:st]):
            out.append(("solicitud", float(v)))
            continue
        if not re.search(r"anticip|solicit", sent, re.I):
            continue
        if _PAG_VERB.search(seg[a:st]):
            out.append(("pago", float(v)))
        elif _SOL_VERB.search(sent) and not _PAG_VERB.search(seg[st:b]):
            out.append(("solicitud", float(v)))
        else:
            out.append(("sin_clasificar", float(v)))
    return out


def _anticipo(text, kind):
    if not _has_text(text):
        return None
    seg, sol_head = _anticipo_seg(text, with_head=True)
    if not seg or _is_no_aplica(seg):
        return None
    anchor, maxlen = (_SOL_ANCHOR, 250) if kind == "solicitud" else (_PAG_ANCHOR, 700)
    cl = _clause(seg, anchor, maxlen)
    if cl is not None:
        v = _first_days(cl, trap=(kind == "pago"))
        if v is not None:
            return v
        if kind == "pago":  # ancla de pago presente sin número = el pliego no fija el plazo
            return None
    ms = _anticipo_free(seg, sol_head)
    free = [v for k, v in ms if k == kind]
    if free:
        return free[0]
    # solo plazos del otro concepto (p.ej. el pliego fija la solicitud pero no el pago) -> ausencia
    if not any(k == "sin_clasificar" for k, _ in ms):
        return None
    return NO_RULE


def dias_solicitud_anticipo(text):
    return _anticipo(text, "solicitud")


def dias_pago_anticipo(text):
    return _anticipo(text, "pago")


def _anticipo_flags(text):
    seg = _anticipo_seg(text or "")
    if not seg or _is_no_aplica(seg):
        return {"anticipo_sujeto_disponibilidad": None}
    pago = dias_pago_anticipo(text)
    cl = _clause(seg, _PAG_ANCHOR, 700)
    zona = cl if cl is not None else seg
    flag = int(pago is None and bool(_DISPON.search(zona)))
    return {"anticipo_sujeto_disponibilidad": flag}


# ------------------------------------------------------------------ A5 anos_experiencia_minima_tecnica
_CT_HEADER = re.compile(r"^.{0,320}?(?:requisitos\s+de\s+capacidad\s+t[eé]cnica|siguientes\s+[ií]ndices)\s*:", re.I)


def _strip_ct_header(text):
    s = normalize(text)
    return _CT_HEADER.sub("", s, count=1).strip()

_YEARS = re.compile(r"\ba[ñn]os?\b", re.I)
_EXP_KW = re.compile(r"experien?cia|antig[uü]edad|existencia\s+legal|ejercicio\s+(?:de\s+la\s+)?profesi|"
                     r"expedici[oó]n\s+del\s+t[ií]tulo|trayectoria|en\s+el\s+rubro|en\s+el\s+mercado|"
                     r"desempe[ñn]ad|dedicad", re.I)
_EXP_TRAP = re.compile(r"repuesto|garant|vigencia|antelaci|registro\s+sanitario|rescisi|sanci[oó]n|\bedad\b|"
                       r"modelo|a[ñn]os?\s+de\s+fabricaci|fabricaci[oó]n\s+no\s+mayor|veh[ií]cul|vida\s+[uú]til|"
                       r"no\s+poseer|no\s+haber|incumplimient|vencimient|ejercicios?\s+fiscal|balance|"
                       r"provisi[oó]n\s+de|"
                       r"menor(?:es)?\s+de\s+\d+\s+a[ñn]os|hijos?", re.I)
# trampas que solo valen pegadas al número: escala de puntaje, duración de una carrera
_EXP_TRAP_NEAR = re.compile(r"puntos?\b|puntaje|carrera|per[ií]odo\s+de|mantenimiento\s+preventivo", re.I)
_PERSONAL = re.compile(r"profesional|t[eé]cnic[oa]s?\b|ingenier|\bpersonal\b|responsable|director|residente|"
                       r"coordinador|especialista|licenciad|arquitect|m[eé]dic|chofer|operador|supervisor|"
                       r"t[ií]tulo|egresad|funcionari|plantel|gerente|jefe|consultor|programador|desarrollador|"
                       r"analista|abogad|contador|instructor|capacitador|carpinter|pintor|alba[ñn]il|electricist|"
                       r"plomer|mec[aá]nic|obrer|operari|guardia|vigilante|enfermer|cociner|personales|"
                       r"trabajador|emplead|integrantes\s+del\s+equipo", re.I)


def _exp_mentions(text):
    s = _strip_ct_header(text)
    out = []
    for m in _YEARS.finditer(s):
        v, st = num_before(s, m.start())
        if v is None or v < 1 or v > 50:
            continue
        a, b = sentence_span(s, st)
        sent = s[a:b]
        pre = s[max(a, st - 40):st]
        local = s[max(a, st - 80):min(b, m.end() + 70)]
        if re.search(r"[uú]ltim[oa]s?\s*(?:\(\s*)?$|\bmejores\s*$", pre, re.I):
            out.append(("ventana", v))
        elif _EXP_TRAP.search(local) or _EXP_TRAP_NEAR.search(s[max(a, st - 30):min(b, m.end() + 20)]):
            out.append(("trampa", v))
        elif _EXP_KW.search(sent):
            near = s[max(a, st - 140):min(b, m.end() + 60)]
            out.append(("personal" if _PERSONAL.search(near) else "empresa", v))
        else:
            out.append(("sin_clasificar", v))
    return out


def anos_experiencia_minima_tecnica(text):
    if not _has_text(text):
        return None
    if _is_no_aplica(_strip_ct_header(text)):
        return None
    ms = _exp_mentions(text)
    emp = [v for k, v in ms if k == "empresa"]
    per = [v for k, v in ms if k == "personal"]
    if emp:
        return float(max(emp))
    if per:
        return float(max(per))
    if any(k == "sin_clasificar" for k, _ in ms):
        return NO_RULE
    return None


# ------------------------------------------------------------------ A6 pct_monto_referencia_tecnica
_PCT = re.compile(r"(\d{1,3}(?:[.,]\d+)?)\s*\)?\s*(?:%|por\s*ciento)", re.I)
_PCT_TRAP = re.compile(r"anticipo|retenci|contribuci|garant|entreg(?:a|ar)\s+(?:hasta\s+)?(?:el\s+)?\d|"
                       r"capacidad\s+de\s+entrega|precio|animales|pureza|germinaci|vigor|de\s+los\s+requisitos|"
                       r"requisitos\s+m[ií]nimos|carnet|alcohol|hipoclorito|concentraci|humedad|grasa|prote[ií]na|"
                       # otros conceptos sobre el monto: solvencia/capacidad económica, seguros, multas, stock
                       r"solvencia|capacidad\s+econ[oó]mica|capital\s+de\s+trabajo|disponibilidad|cobertura|"
                       r"asegurad|reasegur|p[oó]liza|multa|stock", re.I)
# reparto de consorcio / porcentaje de personal: solo si está pegado al número
_PCT_TRAP_NEAR = re.compile(r"consorci|l[ií]der|integrante|socio|restante|\bpersonal(?:es)?\b|trabajadores|"
                            r"emplead", re.I)
_PCT_MONTO = re.compile(r"monto|valor|importe|presupuesto|ofertad|\blote\b|referencial|adjudicad", re.I)
_PCT_REF = re.compile(r"factur|contrato|referencia|experiencia|antecedent|acredit|sumatoria|ventas|volumen", re.I)


def _pct_candidates(text):
    """([(valor, es_sumatoria)] genuinos, n_inciertos, hubo_algún_%) del texto de capacidad técnica."""
    s = _strip_ct_header(text)
    ms = list(_PCT.finditer(s))
    good, unsure = [], 0
    for m in ms:
        v = float(m.group(1).replace(",", "."))
        if not 0 < v <= 100:
            continue
        a, b = sentence_span(s, m.start())
        sent = s[a:b]
        local = s[max(a, m.start() - 150):min(b, m.end() + 120)]
        if _PCT_TRAP.search(local) or _PCT_TRAP_NEAR.search(s[max(a, m.start() - 50):min(b, m.end() + 40)]):
            continue
        if _PCT_MONTO.search(local) and _PCT_REF.search(sent):
            good.append((v, bool(re.search(r"sumatoria|\bsuma\b|total\s+de\s+(?:las|los)", local, re.I))))
        elif _PCT_MONTO.search(local):
            unsure += 1
    return good, unsure, bool(ms)


def pct_monto_referencia_tecnica(text):
    if not _has_text(text):
        return None
    good, unsure, any_pct = _pct_candidates(text)
    if good:
        tot = [v for v, is_sum in good if is_sum]
        return float(max(tot) if tot else max(v for v, _ in good))
    return NO_RULE if unsure else None


# ------------------------------------------------------------------ A8 pct_respaldo_financiero_monto (nueva, 08-oct)
# Exigencia de respaldo financiero proporcional al monto: línea de crédito, capital operativo
# (activo corriente - pasivo corriente), solvencia/capacidad económica o disponibilidad de fondos
# >= N% del monto. Se lee de las secciones de capacidad financiera/técnica y sus requisitos documentales.
SOLVENCIA_TITLES = (
    "capacidad financiera", "capacidad tecnica", "experiencia requerida",
    "requisitos documentales para la evaluacion de la capacidad financiera",
    "requisitos documentales para evaluar el criterio de capacidad financiera",
    "requisitos documentales para evaluar el criterio de capacidad tecnica",
    "requisito documental para evaluar la capacidad tecnica",
    "requisitos documentales para la evaluacion de la experiencia",
)
_SOLV_KW = re.compile(r"l[ií]nea\s+de\s+cr[eé]dito|capital\s+(?:operativo|de\s+trabajo)|"
                      r"activo\s+corriente\s*[-–]\s*pasivo\s+corriente|solvencia(?:\s+econ[oó]mica)?|"
                      r"capacidad\s+econ[oó]mica|disponibilidad\s+(?:en\s+efectivo|financiera|de\s+fondos|del)|"
                      r"saldo\s+(?:bancario|disponible)|extracto\s+bancario", re.I)
_SOLV_TRAP = re.compile(r"anticipo|presupuestaria|margen\s+de\s+solvencia|coeficiente\s+de\s+solvencia|"
                        r"solvencia\s+en\s+la\s+prestaci|facturaci|volumen|ventas|referencias?|experiencia|"
                        r"garant[ií]a|multa|retenci|consorci|l[ií]der", re.I)


def pct_respaldo_financiero_monto(text):
    """% del monto que el oferente debe demostrar como respaldo financiero (máximo); None si no exige."""
    if not _has_text(text):
        return None
    s = normalize(text)
    vals = []
    for m in _PCT.finditer(s):
        v = float(m.group(1).replace(",", "."))
        if not 0 < v <= 200:
            continue
        a, b = sentence_span(s, m.start())
        pre = s[max(a, m.start() - 200):m.start()]
        post = s[m.end():min(b, m.end() + 70)]
        if not (_SOLV_KW.search(pre) and _PCT_MONTO.search(post)):
            continue
        if _SOLV_TRAP.search(s[max(a, m.start() - 120):min(b, m.end() + 40)]):
            continue
        vals.append(v)
    return float(max(vals)) if vals else None


# ------------------------------------------------------------------ B1/B2 ratios (totalitarios)
def _ratio_norm(text):
    t = normalize(text)
    t = re.sub(r"\[\s*([\d\s,.]+?)\s*\]", lambda m: "[" + re.sub(r"\s+", "", m.group(1)) + "]", t)
    t = re.sub(r"(\d),\s(\d{2})(?!\d)", r"\1,\2", t)
    t = re.sub(r"=\s*[óo]\s*(?=\d)", "igual o mayor a ", t)
    t = re.sub(r"igual\s+o\s+m\s*a\s*y\s*o\s*r\s*(a|que)", r"igual o mayor \1", t, flags=re.I)
    return t


_NUMR = r"(?:a\s+)?\[?\(?\s*(\d{1,2}(?:[.,]\d{1,3})?|uno|dos|cero\s+coma\s+ochenta)\s*\)?\]?"
_RWORDS = {"uno": 1.0, "dos": 2.0, "cero coma ochenta": 0.8}


def _rnum(g):
    g = g.lower()
    return _RWORDS[g] if g in _RWORDS else float(g.replace(",", "."))


def ratio_liquidez_minimo(text):
    t = _ratio_norm(text)
    for m in re.finditer(r"liquidez", t, re.I):
        ctx = t[m.start():m.start() + 160]
        cut = re.search(r"endeudamiento|solvencia|rentabilidad|eficiencia|pasivo\s+total", ctx[8:], re.I)
        if cut:
            ctx = ctx[:8 + cut.start()]
        m2 = re.search(r"(?:igual\s+o\s+mayor\s+(?:que|a)|mayor\s+o\s+igual\s+(?:que|a)|>=|≥|"
                       r"mayor\s+(?:que|a)|igual\s+a|deber[aá]\s+ser|m[ií]nimo\s+(?:de)?|no\s+(?:deber[aá]\s+ser\s+)?menor\s+(?:que|a))"
                       r"\s*:?\s*" + _NUMR, ctx, re.I)
        if m2:
            v = _rnum(m2.group(1))
            if v > 0:
                return v
    return None


def ratio_endeudamiento_maximo(text):
    t = _ratio_norm(text)
    for m in re.finditer(r"endeudamiento|coeficiente\s+de\s+solvencia|solvencia", t, re.I):
        if re.search(r"margen\s+de\s*$", t[max(0, m.start() - 12):m.start()], re.I):
            continue
        ctx = t[m.start():m.start() + 200]
        cut = re.search(r"liquidez|rentabilidad|eficiencia", ctx[10:], re.I)
        if cut:
            ctx = ctx[:10 + cut.start()]
        inverse = bool(re.search(r"activo\s+total\s*/\s*pasivo\s+total", ctx, re.I))
        rx = (r"(?:igual\s+o\s+mayor\s+(?:que|a)|mayor\s+o\s+igual\s+(?:que|a)|>=|≥|mayor\s+(?:que|a)|m[ií]nimo\s+(?:de)?)"
              if inverse else
              r"(?:no\s+deber[aá]\s+ser\s+mayor\s+(?:que|a)|igual\s+o\s+menor\s+(?:que|a)|menor\s+o\s+igual\s+(?:que|a)|"
              r"menor\s+(?:que|a)|<=|≤|m[aá]ximo\s+(?:de|igual\s+a)?|no\s+exceder\s+(?:de)?|"
              r"(?:deber[aá]\s+ser\s+)?(?:mayor\s+o\s+igual|igual\s+o\s+mayor)\s+(?:que|a))")
        m2 = re.search(rx + r"\s*:?\s*" + _NUMR, ctx, re.I)
        if m2:
            v = _rnum(m2.group(1))
            if v <= 0:
                continue
            return round(1 / v, 2) if inverse else v
    return None


# ------------------------------------------------------------------ B3 tipo_indice_reajuste + C
REAJUSTE_TIPOS = ("polinomica", "tipo_cambio", "combustible", "salario", "iibcp", "ipc",
                  "prosa_sin_formula", "no_sujeto")
_REA_KW = [
    # sin "insumos": aparece también en prosa ("justificando… materia prima, insumos, salario mínimo")
    ("polinomica", r"coeficientes?\s+de\s+incidencia|polin[oó]mic|\bA\s*k\b|Σ|cemento|asfalt|\bacero\b|hierro|ligante"),
    ("tipo_cambio", r"d[oó]lar|tipo\s+de\s+cambio|cotizaci[oó]n\s+de\s+(?:la\s+)?moneda"),
    ("combustible", r"combustible|gas\s?oil|nafta|di[eé]sel|petropar|precio\s+del\s+litro"),
    ("salario", r"salario\s+m[ií]nimo|jornal"),
    ("iibcp", r"i\.?\s*i\.?\s*b\.?\s*c\.?\s*p|iibcp|[ií]ndice\s+(?:de\s+precios\s+)?de\s+(?:la\s+)?construcci"),
    ("ipc", r"\bipc|[ií]ndice\s+de\s+precios?\s+(?:al|del)\s+consum"),
]
_UMBRAL = re.compile(r"(?:variaci[oó]n|incremento|aumento)[^.%]{0,140}?(?:igual\s+o\s+mayor|mayor\s+o\s+igual|"
                     r"superior|mayor)\s*(?:al?|del?)?[^0-9%]{0,40}?\(?\s*(\d{1,2}(?:[.,]\d+)?)\s*\)?\s*"
                     r"(?:%|por\s*ciento)", re.I)


def tipo_indice_reajuste(text):
    s = normalize(text)
    m = re.search(r"ser[aá]n\s+los\s+siguientes\s*:?", s, re.I)
    seg = s[m.end():] if m else s
    if not seg.strip():
        return None
    if _is_no_aplica(seg) or re.search(
            r"no\s+estar[aá]\s+sujet[oa]\s+a\s+reajuste|no\s+(?:se\s+)?(?:aplica|aplicar[aá]|habr[aá]|corresponde)"
            r"[^.]{0,40}reajuste|sin\s+reajuste|no\s+ser[aá]n?\s+reajustad", seg, re.I):
        return "no_sujeto"
    for tipo, rx in _REA_KW:
        if re.search(rx, seg, re.I):
            return tipo
    return "prosa_sin_formula"


def _reajuste_flags(text):
    us = {float(m.group(1).replace(",", ".")) for m in _UMBRAL.finditer(normalize(text))}
    return {"tipo_indice_reajuste": tipo_indice_reajuste(text),
            "umbral_reajuste_no_estandar": None if not us else int(any(u != 15 for u in us))}


def tiene_clausula_fuerza_mayor(text):
    return int(bool(re.search(r"siete\s*\(\s*7\s*\)\s*d[ií]as|7\s*\(\s*siete\s*\)\s*d[ií]as|\bsiete\s+d[ií]as|"
                              r"\b7\s*(?:\([^)]*\))?\s*d[ií]as", normalize(text), re.I)))


# ------------------------------------------------------------------ API
RULES_V1 = {
    "dias_pago_maximo": dias_pago_maximo,
    "dias_validez_garantia": dias_validez_garantia,
    "dias_pago_anticipo": dias_pago_anticipo,
    "dias_solicitud_anticipo": dias_solicitud_anticipo,
    "anos_experiencia_minima_tecnica": anos_experiencia_minima_tecnica,
    "pct_monto_referencia_tecnica": pct_monto_referencia_tecnica,
    "ratio_liquidez_minimo": ratio_liquidez_minimo,
    "ratio_endeudamiento_maximo": ratio_endeudamiento_maximo,
}
TOTALITARIAN_V1 = {"ratio_liquidez_minimo", "ratio_endeudamiento_maximo"}

# Columnas nuevas por sección (slug del schema) -> tipo SQL
NEW_COLUMNS = {
    "formas_y_condiciones_de_pago": {"pago_en_dias_habiles": "SMALLINT", "pago_sujeto_plan_caja": "SMALLINT",
                                     "tiene_clausula_aceptacion_factura": "SMALLINT",
                                     "exige_certificado_laboral": "SMALLINT"},
    "periodo_de_validez_de_la_garantia_de_los_bienes": {"garantia_varia_por_item": "SMALLINT"},
    "solicitud_de_pago_de_anticipo": {"anticipo_sujeto_disponibilidad": "SMALLINT"},
    "capacidad_financiera": {"liquidez_no_estandar": "SMALLINT", "endeudamiento_no_estandar": "SMALLINT"},
    "reajuste": {"tipo_indice_reajuste": "TEXT", "umbral_reajuste_no_estandar": "SMALLINT"},
    "fuerza_mayor": {"tiene_clausula_fuerza_mayor": "SMALLINT"},
}
# Valor cuando el documento no tiene la sección (None = desconocido)
NO_SECTION_DEFAULT = {"tiene_clausula_fuerza_mayor": 0, "tiene_clausula_aceptacion_factura": 0}


def section_features(slug, text):
    """Columnas nuevas (regex/flags) de la especificación v1.0 para una sección."""
    if slug == "formas_y_condiciones_de_pago":
        return _pago_flags(text)
    if slug == "periodo_de_validez_de_la_garantia_de_los_bienes":
        vals, _, _ = _validez_values(text or "")
        return {"garantia_varia_por_item": None if not vals else int(len(set(vals)) > 1)}
    if slug == "solicitud_de_pago_de_anticipo":
        return _anticipo_flags(text)
    if slug == "capacidad_financiera":
        liq, end = ratio_liquidez_minimo(text), ratio_endeudamiento_maximo(text)
        return {"liquidez_no_estandar": None if liq is None else int(abs(liq - 1.0) > 1e-9),
                "endeudamiento_no_estandar": None if end is None else int(abs(end - 0.8) > 1e-9)}
    if slug == "reajuste":
        return _reajuste_flags(text)
    if slug == "fuerza_mayor":
        return {"tiene_clausula_fuerza_mayor": tiene_clausula_fuerza_mayor(text)}
    return {}

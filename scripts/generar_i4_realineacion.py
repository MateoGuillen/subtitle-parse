"""I4 — Propuesta de re-alineación del GT de agosto (y del pseudo-GT Opus) con la especificación v1.0.

Para cada ítem de referencia donde la regla spec-v1 difiere del valor anotado, propone el valor de la
regla y CLASIFICA la diferencia según la decisión de ficha que la explica (docs/especificacion_campos.md).
Si ninguna decisión la explica, queda como 'revisar' (sin propuesta automática: lo decide el usuario).
Los residuales que la regla deja al LLM no se proponen (el GT se mantiene).

Salida (no modifica ningún GT):
  data/processed/llm_benchmark/planillas/i4_realineacion.csv   (una fila por diferencia; columnas
      aprobar / valor_final / nota_usuario vacías para el usuario)
  docs/i4_realineacion_gt.md                                  (resumen por campo y decisión, ejemplos)
Uso: /root/vllm-env/bin/python scripts/generar_i4_realineacion.py
"""
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.etl.transformers import field_rules_v1 as fr  # noqa: E402

GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
PS = ROOT / "data/processed/llm_benchmark/pseudo_gt_opus"
OUT_CSV = ROOT / "data/processed/llm_benchmark/planillas/i4_realineacion.csv"
OUT_MD = ROOT / "docs/i4_realineacion_gt.md"

DECISIONES = {
    "A1-trampa": "A1: el GT tomó un plazo que la ficha define como trampa (no es plazo de pago)",
    "A1-plazo-obra": "A1: plazo propio de obra ('Plazo de pago: N días desde la aprobación del certificado') > estándar 60",
    "A1-plazo-propio": "A1: plazo propio de la entidad > estándar 60 (el GT tomó el estándar)",
    "A1-mayor": "A1: dos plazos de pago genuinos → el mayor",
    "A2-mayor-365": "A2: plazos > 365 días se toman tal cual (el GT aplicaba un rango 1-365)",
    "A2-por-item": "A2: plazos distintos por lote/ítem → el máximo + flag garantia_varia_por_item",
    "A2-trampa": "A2: la sección habla de vencimiento/garantía de cumplimiento/reposición, no de la garantía del bien",
    "A3A4-trampa": "A3/A4: el GT tomó el plazo del otro concepto (solicitud vs pago) o del texto legal fijo",
    "A5-antiguedad": "A5: la antigüedad en el rubro / existencia legal cuenta como experiencia de la empresa",
    "A5-personal": "A5: experiencia del personal ('ejercicio de la profesión', 'desde el título', antigüedad en el plantel)",
    "A5-maximo": "A5: empresa > personal y, dentro del nivel, el máximo",
    "A5-trampa": "A5: el GT tomó años que no son experiencia (ventana 'últimos N años', puntaje, edad, repuestos…)",
    "A6-solvencia": "A6→A8: 'solvencia/capacidad económica del N% del monto' pasa a la variable nueva pct_respaldo_financiero_monto",
    "A6-sumatoria": "A6: con alternativas, el porcentaje de la sumatoria",
    "A6-trampa": "A6: el % no es de referencias (consorcio, stock, repuestos, personal, póliza, multa…)",
    "A6-maximo": "A6: varios % de referencias genuinos (no alternativos) → el máximo",
    "A6-omitido": "A6: el texto fija un % de referencias sobre el monto (facturas/ventas) que el GT no tomó",
    "B2-invertido": "B2: ratio activo/pasivo invertido → 1/x",
    "revisar": "sin decisión de ficha que la explique: decide el usuario",
}


def _f(v):
    try:
        return None if v is None or pd.isna(v) else float(v)
    except (TypeError, ValueError):
        return None


def _eq(a, b):
    return (a is None and b is None) or (a is not None and b is not None and abs(a - b) < 1e-6)


def _snip(s, i, j, w=170):
    return re.sub(r"\s+", " ", s[max(0, i - w):j + 90]).strip()


def _find_val(mentions, val):
    """Primera mención (v, st, en) con valor val."""
    return next((m for m in mentions if val is not None and abs(m[0] - val) < 1e-6), None)


# ----------------------------------------------------------------------------- clasificadores
def cls_pago(text, gt, v1):
    s = fr.normalize(text)
    ms = fr._day_mentions(s)
    g = _find_val(ms, gt)
    if g is not None:
        why = fr._pago_trap(s, g[1], g[2])
        if why:
            return "A1-trampa", f"{why}: …{_snip(s, g[1], g[2])}"
    if v1 is not None:
        m = _find_val(ms, v1)
        if m is not None:
            ctx = _snip(s, m[1], m[2])
            if re.search(r"plazo\s+de\s+pago\s*:", s[max(0, m[1] - 40):m[1]], re.I):
                return "A1-plazo-obra", ctx
            if gt == 60:
                return "A1-plazo-propio", ctx
            if gt is not None and v1 > gt and g is not None and fr._pago_trap(s, g[1], g[2]) is None:
                return "A1-mayor", ctx
            return "revisar", ctx
    return "revisar", _snip(s, 0, 0, 0)[:400]


def cls_validez(text, gt, v1):
    vals, seg, trapped = fr._validez_values(text)
    ctx = re.sub(r"\s+", " ", seg or "")[:420]
    if v1 is None and gt is not None:
        return ("A2-trampa" if trapped or re.search(r"vencimient|cumplimiento", seg or "", re.I) else "revisar"), ctx
    if gt is None and vals:
        if len(set(vals)) > 1:
            return "A2-por-item", ctx
        if v1 > 365:
            return "A2-mayor-365", ctx
    return "revisar", ctx


def cls_anticipo(text, gt, v1, field):
    seg = fr._anticipo_seg(text)
    ms = fr._day_mentions(seg)
    g = _find_val(ms, gt)
    other = [v for k, v in fr._anticipo_free(seg) if k not in (("pago", "sin_clasificar") if field.endswith("pago_anticipo") else ("solicitud", "sin_clasificar"))]
    ctx = _snip(seg, g[1], g[2]) if g else re.sub(r"\s+", " ", seg)[:420]
    if g is None and gt is not None and re.search(rf"\b{int(gt)}\b|\(\s*{int(gt)}\s*\)", fr.normalize(text)):
        return "A3A4-trampa", "valor del GT solo en el texto legal fijo: " + ctx
    if gt is not None and gt in other:
        return "A3A4-trampa", ctx
    return "revisar", ctx


def cls_exp(text, gt, v1):
    s = fr._strip_ct_header(text)
    hits = []
    for m in fr._YEARS.finditer(s):
        v, st = fr.num_before(s, m.start())
        if v is not None:
            a, b = fr.sentence_span(s, st)
            hits.append((v, st, m.end(), s[a:b]))
    if gt is None and v1 is not None:
        h = next((x for x in hits if x[0] == v1), None)
        if h:
            ctx = _snip(s, h[1], h[2])
            if re.search(r"antig[uü]edad|existencia\s+legal|en\s+el\s+rubro|en\s+el\s+mercado", h[3], re.I):
                return ("A5-personal" if fr._PERSONAL.search(h[3]) else "A5-antiguedad"), ctx
            if re.search(r"ejercicio|t[ií]tulo|plantel", h[3], re.I):
                return "A5-personal", ctx
        return "revisar", ctx if h else s[:400]
    if gt is not None and v1 is not None:
        h = next((x for x in hits if x[0] == v1), None)
        return "A5-maximo", _snip(s, h[1], h[2]) if h else s[:400]
    if gt is not None and v1 is None:
        h = next((x for x in hits if x[0] == gt), None)
        if h:
            return "A5-trampa", _snip(s, h[1], h[2])
    return "revisar", s[:400]


def cls_pct(text, gt, v1):
    s = fr._strip_ct_header(text)
    g = next((m for m in fr._PCT.finditer(s) if gt is not None and abs(float(m.group(1).replace(",", ".")) - gt) < 1e-6), None)
    ctx = _snip(s, g.start(), g.end()) if g else s[:400]
    good = [v for v, _ in fr._pct_candidates(text)[0]]
    if g is not None and not (v1 is not None and gt in good):
        near = s[max(0, g.start() - 200):g.end() + 60]
        if fr.pct_respaldo_financiero_monto(near) is not None or re.search(r"solvencia|capacidad\s+econ", near, re.I):
            return "A6-solvencia", ctx
        if v1 is not None and re.search(r"al\s+menos\s+una\s+referencia", s, re.I):
            return "A6-sumatoria", ctx
        if fr._PCT_TRAP.search(near[-260:]) or fr._PCT_TRAP_NEAR.search(s[max(0, g.start() - 50):g.end() + 40]):
            return "A6-trampa", ctx
    good = [v for v, _ in fr._pct_candidates(text)[0]]
    vm = next((m for m in fr._PCT.finditer(s) if v1 is not None and abs(float(m.group(1).replace(",", ".")) - v1) < 1e-6), None)
    vctx = _snip(s, vm.start(), vm.end()) if vm else ctx
    if gt is None and v1 is not None:
        return "A6-omitido", vctx
    if gt is not None and v1 is not None and gt in good:
        alt = re.search(r"al\s+menos\s+una\s+referencia", s, re.I)
        return ("A6-sumatoria" if alt else "A6-maximo"), vctx
    return "revisar", ctx


def cls_ratio(text, gt, v1, field):
    t = fr.normalize(text)
    if field == "ratio_endeudamiento_maximo" and re.search(r"activo\s+total\s*/\s*pasivo\s+total", t, re.I):
        return "B2-invertido", t[:400]
    return "revisar", t[:400]


def classify(field, text, gt, v1):
    if field == "dias_pago_maximo":
        return cls_pago(text, gt, v1)
    if field == "dias_validez_garantia":
        return cls_validez(text, gt, v1)
    if field in ("dias_pago_anticipo", "dias_solicitud_anticipo"):
        return cls_anticipo(text, gt, v1, field)
    if field == "anos_experiencia_minima_tecnica":
        return cls_exp(text, gt, v1)
    if field == "pct_monto_referencia_tecnica":
        return cls_pct(text, gt, v1)
    return cls_ratio(text, gt, v1, field)


# ----------------------------------------------------------------------------- referencias
def iter_refs():
    for p in sorted(GT_DIR.glob("*.csv")):
        if p.name.startswith("errores_"):
            continue
        g = pd.read_csv(p)
        f = g.campo.iloc[0]
        if f not in fr.RULES_V1:
            continue
        for r in g.itertuples():
            yield ("gt_agosto", f"{f}|{r.nro_licitacion}", f, str(r.nro_licitacion), _f(r.ground_truth),
                   str(r.text) if pd.notna(r.text) else "", getattr(r, "nota", ""))
    muestra = pd.read_csv(PS / "muestra.csv")
    ann = json.loads((PS / "anotaciones.json").read_text(encoding="utf-8"))
    for r in muestra.itertuples():
        a = ann.get(str(r.id))
        if r.campo not in fr.RULES_V1 or a is None or a[1] == "ambiguo":
            continue
        yield ("pseudo_opus", str(r.id), r.campo, str(r.nro_licitacion), _f(a[0]), str(r.texto), a[2])


def main():
    rows = []
    totals = Counter()
    for ref, rid, f, nro, gt, text, nota in iter_refs():
        totals[(ref, f)] += 1
        v = fr.RULES_V1[f](text)
        if v is fr.NO_RULE:
            continue  # residual LLM: el GT se mantiene
        v1 = _f(v)
        if _eq(v1, gt):
            continue
        dec, ev = classify(f, text, gt, v1)
        rows.append({"referencia": ref, "id": rid, "campo": f, "nro_licitacion": nro,
                     "valor_actual": gt, "valor_propuesto": v1 if dec != "revisar" else None,
                     "valor_regla_v1": v1, "decision": dec, "motivo": DECISIONES[dec],
                     "evidencia": ev[:600], "nota_anotador_original": str(nota)[:250],
                     "aprobar": "", "valor_final": "", "nota_usuario": ""})
    df = pd.DataFrame(rows)
    order = {"gt_agosto": 0, "pseudo_opus": 1}
    df = df.sort_values(["referencia", "campo", "decision"], key=lambda c: c.map(order) if c.name == "referencia" else c)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)

    # ---- resumen
    L = ["# I4 — Re-alineación del GT con la especificación v1.0 (propuesta, 08-oct-2026)", "",
         "> Generado por `scripts/generar_i4_realineacion.py`. **Nada se modificó**: el GT de agosto y el pseudo-GT",
         "> Opus siguen intactos hasta que el usuario apruebe. Planilla para revisar/aprobar:",
         f"> `{OUT_CSV.relative_to(ROOT)}` (columnas `aprobar` = s/n, `valor_final` si corrige, `nota_usuario`).",
         "> Los residuales que la regla deja al LLM no se proponen (su valor GT se mantiene).", "",
         "## Resumen por campo", "",
         "| Campo | Ref. | Ítems | Difieren | Con decisión de ficha | A revisar |", "|---|---|---|---|---|---|"]
    for (ref, f), n in sorted(totals.items(), key=lambda x: (order[x[0][0]], x[0][1])):
        d = df[(df.referencia == ref) & (df.campo == f)]
        L.append(f"| {f} | {ref} | {n} | {len(d)} | {int((d.decision != 'revisar').sum())} | {int((d.decision == 'revisar').sum())} |")
    L += ["", "## Por decisión (con un ejemplo cada una)", ""]
    for dec, g in df.groupby("decision", sort=False):
        L.append(f"### `{dec}` — {len(g)} ítems")
        L.append(f"{DECISIONES[dec]}. Por referencia: " +
                 ", ".join(f"{k}={v}" for k, v in g.referencia.value_counts().items()) + ".")
        ex = g.iloc[0]
        L.append(f"- Ej. `{ex.campo}` nro {ex.nro_licitacion}: actual **{ex.valor_actual}** → propuesto "
                 f"**{ex.valor_propuesto}** · …{ex.evidencia[:300]}")
        L.append("")
    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(df.groupby(["referencia", "decision"]).size().to_string())
    print(f"\n{len(df)} diferencias -> {OUT_CSV.relative_to(ROOT)} | resumen -> {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

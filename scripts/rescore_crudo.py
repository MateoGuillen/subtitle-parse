"""F1 — Re-score del benchmark sobre SALIDA CRUDA (CPU, sin GPU). Plan de corrección §F1.

Auditoría sesión 23 (H2.1): `parse_response()` aplica overrides deterministas antes de
mirar la respuesta del LLM, así que las métricas publicadas ("crudo+postproceso")
miden en buena parte las reglas. Este script recalcula, desde los JSON ya guardados
(que conservan `llm_raw`), tres métricas por experimento × modelo × campo:

  crudo     número literal emitido por el modelo (regex estricto ^-?\\d+([.,]\\d+)?$ o NULL),
            sin reglas ni conversión de unidades (validez en meses cuenta como fallo:
            se reporta también el agregado "dias sin validez")
  post      lo publicado: valor tras `extraction_postprocess.parse_response` (guardado)
  null_bl   baseline "modelo que siempre responde NULL" pasado por el MISMO postproceso

Experimentos: C3 (288 humanos), C5-P2 sobre 288 (humanos), Fase 5 (867 pseudo),
C4 P1/P2 y C5 P2/P3/P4 (150 pseudo, con base = Fase 5).
Estadística: Wilson IC95, McNemar exacto pareado (Bonferroni α=0.0167 para 3 pares),
diferencia pareada con IC95, y diferencia mínima detectable (poder 0.8) para pct.

Salidas: data/processed/llm_benchmark/F1_rescore_crudo.csv (largo),
         F1_mcnemar.csv, F1_rescore_crudo.json (resumen + verificaciones).
Uso: /root/vllm-env/bin/python scripts/rescore_crudo.py
"""
import json
import math
import re
import sys
from math import comb
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.etl.transformers.extraction_postprocess import parse_response  # noqa: E402

B = ROOT / "data/processed/llm_benchmark"
RES = B / "multimodel_results"
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
MODELS = {"FP8": "Llama_31_8B-FP8", "AWQ": "Llama_31_8B-AWQ", "QWEN": "Qwen3-8B-FP8"}
DIAS = ["dias_validez_garantia", "anos_experiencia_minima_tecnica",
        "dias_pago_anticipo", "dias_solicitud_anticipo"]
PCT = "pct_monto_referencia_tecnica"
GT_FILES = {f: GT_DIR / f"{f}_GT.csv" for f in DIAS + [PCT]}
GT_FILES["anos_experiencia_minima_tecnica"] = GT_DIR / "anos_experiencia_minima_tecnica_GT (1).csv"
RAW_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")
ALPHA_BONF = 0.05 / 3


# ---------------------------------------------------------------- utilidades
def to_num(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    try:
        return float(str(v).strip().replace(",", "."))
    except ValueError:
        return None


def raw_num(raw):
    s = (raw or "").strip()
    if not RAW_RE.match(s):
        return None
    return float(s.replace(",", "."))


def eq(a, b, field):
    """Misma igualdad que c3_rerun_top3_288.eq (pct tolera escala x100)."""
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    if field.startswith("pct_"):
        return abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6
    return False


def wilson(k, n, z=1.96):
    if n == 0:
        return (float("nan"), float("nan"))
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def mcnemar(x, y):
    """x, y: listas booleanas pareadas. Devuelve b (x ok, y no), c, p exacta 2 colas."""
    b = sum(1 for a, c_ in zip(x, y) if a and not c_)
    c = sum(1 for a, c_ in zip(x, y) if c_ and not a)
    n = b + c
    p = 1.0 if n == 0 else min(1.0, 2 * sum(comb(n, i) for i in range(min(b, c) + 1)) / 2 ** n)
    return b, c, p


def paired_diff_ci(x, y, z=1.96):
    n = len(x)
    b, c, _ = mcnemar(x, y)
    d = (b - c) / n
    se = math.sqrt(max(b + c - (b - c) ** 2 / n, 0)) / n
    return d, d - z * se, d + z * se


def mde_paired(n, disc_rate, alpha=ALPHA_BONF, power=0.8):
    """Diferencia mínima detectable (aprox. normal) en McNemar con tasa de discordancia dada."""
    from statistics import NormalDist
    za = NormalDist().inv_cdf(1 - alpha / 2)
    zb = NormalDist().inv_cdf(power)
    return (za + zb) * math.sqrt(disc_rate / n)


# ---------------------------------------------------------------- cargas
def gt_texts():
    out = {}
    for f, p in GT_FILES.items():
        g = pd.read_csv(p).drop_duplicates("nro_licitacion")
        for r in g.itertuples():
            out[(str(int(float(r.nro_licitacion))), f)] = str(r.text)
    return out


def pseudo_versions():
    v2 = json.loads((B / "pseudo900.json").read_text(encoding="utf-8"))["samples"]
    v1 = json.loads((B / "pseudo900_v1_867.json").read_text(encoding="utf-8"))["samples"]
    idx = lambda s: {(x["nro"], x["field"]): x for x in s}  # noqa: E731
    return {"v1": idx(v1), "v2": idx(v2)}


def pick_pseudo(items, versions, key_val="value", key_agree="agree"):
    """Elige la versión de pseudo que reproduce los flags `agree` guardados."""
    best = None
    for name, ps in versions.items():
        hits = n = 0
        for it in items:
            s = ps.get((it["nro"], it["field"]))
            if s is None:
                continue
            n += 1
            lv = to_num(it[key_val])
            ag = lv is not None and abs(lv - s["pseudo"]) < 1e-9
            hits += ag == bool(it[key_agree])
        rate = hits / n if n else 0
        if best is None or rate > best[1]:
            best = (name, rate, n)
    return best


# ---------------------------------------------------------------- núcleo
ROWS, MCN, CHECKS = [], [], {}


def score(exp, model, field, n, k, metric):
    lo, hi = wilson(k, n)
    ROWS.append(dict(experimento=exp, modelo=model, campo=field, metrica=metric,
                     n=n, k=k, p=round(k / n, 4) if n else None,
                     ic_lo=round(lo, 4), ic_hi=round(hi, 4)))


def add_group(exp, model, items, ref_key, text_of, fields_groups):
    """items: dicts con nro, field, raw, post (valor guardado), ref (gt/pseudo)."""
    per = {}
    for it in items:
        txt = text_of.get((it["nro"], it["field"]))
        null_v = to_num(parse_response("NULL", field=it["field"], text=txt)) if txt else None
        rec = dict(crudo=eq(raw_num(it["raw"]), it["ref"], it["field"]),
                   post=eq(to_num(it["post"]), it["ref"], it["field"]),
                   null_bl=(eq(null_v, it["ref"], it["field"]) if txt else None))
        per.setdefault(it["field"], []).append((it, rec))
    for gname, fields in fields_groups.items():
        recs = [r for f in fields for _, r in per.get(f, [])]
        if not recs:
            continue
        for m in ("crudo", "post", "null_bl"):
            vals = [r[m] for r in recs if r[m] is not None]
            score(exp, model, gname, len(vals), sum(vals), m)
    return per


def load_c3():
    texts = gt_texts()
    out = {}
    for tag, name in MODELS.items():
        rs = json.loads((B / f"C3_{name}.json").read_text(encoding="utf-8"))["results"]
        out[tag] = [dict(nro=r["nro"], field=r["field"], raw=r["llm_raw"],
                         post=r["llm_value"], ref=r["gt"]) for r in rs]
    return out, texts


def heldout_keys():
    h = pd.read_csv(B / "heldout50_frozen.csv")
    return {(str(int(float(n))), c) for n, c in zip(h.nro_licitacion, h.campo)}


def run_c3():
    data, texts = load_c3()
    groups = {**{f: [f] for f in DIAS + [PCT]}, "DIAS_239": DIAS,
              "DIAS_sin_validez_189": [f for f in DIAS if f != "dias_validez_garantia"]}
    hk = heldout_keys()
    per_model = {}
    for tag, items in data.items():
        per = add_group("C3_288_humano", tag, items, "gt", texts, groups)
        held = [it for it in items if (it["nro"], it["field"]) in hk]
        add_group("C3_heldout50", tag, held, "gt", texts, {"DIAS_heldout": DIAS})
        per_model[tag] = per
    # McNemar sobre crudo y post, por grupo
    for gname, fields in groups.items():
        for metric in ("crudo", "post"):
            vecs = {}
            for tag, per in per_model.items():
                keyed = {(it["nro"], it["field"]): r[metric]
                         for f in fields for it, r in per.get(f, [])}
                vecs[tag] = keyed
            keys = sorted(set.intersection(*(set(v) for v in vecs.values())))
            for a, b_ in (("FP8", "AWQ"), ("FP8", "QWEN"), ("AWQ", "QWEN")):
                x = [vecs[a][k] for k in keys]
                y = [vecs[b_][k] for k in keys]
                bb, cc, p = mcnemar(x, y)
                d, lo, hi = paired_diff_ci(x, y)
                MCN.append(dict(experimento="C3_288_humano", grupo=gname, metrica=metric,
                                par=f"{a}-{b_}", n=len(keys), ok_a=sum(x), ok_b=sum(y),
                                b=bb, c=cc, p_exacta=p, signif_bonf=p < ALPHA_BONF,
                                dif_a_menos_b=round(d, 4), dif_ic_lo=round(lo, 4),
                                dif_ic_hi=round(hi, 4)))
    # MDE en pct (pares FP8-AWQ, post): reemplaza "indistinguibles"
    x = [r["post"] for _, r in per_model["FP8"][PCT]]
    y = [r["post"] for _, r in per_model["AWQ"][PCT]]
    bb, cc, _ = mcnemar(x, y)
    n = len(x)
    CHECKS["pct_mde_FP8_AWQ"] = dict(n=n, discordantes=bb + cc,
                                     mde=round(mde_paired(n, (bb + cc) / n), 3))
    return data, texts


def run_c5_p2_288(texts):
    for tag, name in MODELS.items():
        d = json.loads((B / f"C5_replica_{name}.json").read_text(encoding="utf-8"))
        c3 = {(r["nro"], r["field"]): r["gt"] for r in
              json.loads((B / f"C3_{name}.json").read_text(encoding="utf-8"))["results"]}
        items = [dict(nro=r["nro"], field=r["field"], raw=r["raw"], post=r["value"],
                      ref=c3[(r["nro"], r["field"])]) for r in d["P2_288"]["items"]]
        add_group("C5_P2_288_humano", tag, items, "gt", texts,
                  {"DIAS_239": DIAS, PCT: [PCT]})


def run_fase5(versions):
    ident = {}
    for tag, name in MODELS.items():
        for ver, path in (("v1", RES / "fase5_v1" / f"Fase5_{name}.json"),
                          ("v2", RES / f"Fase5_{name}.json")):
            rs = json.loads(path.read_text(encoding="utf-8"))["results"]
            ps = versions[ver]
            texts = {k: v["text"] for k, v in ps.items()}
            items = [dict(nro=r["nro"], field=r["field"], raw=r["llm_raw"],
                          post=r["llm_value"], ref=r["pseudo"]) for r in rs]
            fields = sorted({it["field"] for it in items})
            per = add_group(f"Fase5_867_{ver}", tag, items, "pseudo", texts,
                            {"TOTAL": fields, "sin_pct": [f for f in fields if f != PCT],
                             PCT: [PCT]})
            for f, lst in per.items():
                for it, r in lst:
                    ident.setdefault(ver, {}).setdefault((it["nro"], f), {})[tag] = r
    for ver, d in ident.items():
        out = {}
        for metric in ("post", "crudo"):
            same_ok_no_pct = sum(1 for (nro, f), r in d.items() if f != PCT and len(r) == 3
                                 and all(x[metric] for x in r.values()))
            out[f"todos_aciertan_sin_pct_{metric}"] = same_ok_no_pct
        CHECKS[f"fase5_{ver}_identicos"] = out


def run_150(versions):
    """C4 (P1/P2) y C5 (P2/P3/P4) sobre los 150 ítems fijos, más su base (Fase 5)."""
    for tag, name in MODELS.items():
        c4 = json.loads((B / f"C4_parapfrase_{name}.json").read_text(encoding="utf-8"))
        c5 = json.loads((B / f"C5_replica_{name}.json").read_text(encoding="utf-8"))
        variants = {"C4_P1": c4["P1_sin_hint"], "C4_P2": c4["P2_reescrito"],
                    "C5_P2": c5["variants"]["P2"]["items"],
                    "C5_P3": c5["variants"]["P3"]["items"],
                    "C5_P4": c5["variants"]["P4"]["items"]}
        for vname, items in variants.items():
            ag_key = "agree" if "agree" in items[0] else "agree_pseudo"
            ver, rate, n = pick_pseudo(items, versions, key_agree=ag_key)
            CHECKS.setdefault("pseudo_version_150", {})[f"{tag}_{vname}"] = (ver, round(rate, 3))
            ps = versions[ver]
            texts = {k: v["text"] for k, v in ps.items()}
            its = [dict(nro=r["nro"], field=r["field"], raw=r["raw"], post=r["value"],
                        ref=ps[(r["nro"], r["field"])]["pseudo"]) for r in items
                   if (r["nro"], r["field"]) in ps]
            fields = sorted({it["field"] for it in its})
            add_group(f"150_{vname}", tag, its, "pseudo", texts,
                      {"TOTAL": fields, "sin_pct": [f for f in fields if f != PCT],
                       PCT: [PCT]})
            if vname == "C4_P1":  # base sobre los mismos 150, con la misma versión de pseudo
                keys = {(r["nro"], r["field"]) for r in items}
                base_path = (RES / "fase5_v1" / f"Fase5_{name}.json") if ver == "v1" \
                    else RES / f"Fase5_{name}.json"
                rs = json.loads(base_path.read_text(encoding="utf-8"))["results"]
                bits = [dict(nro=r["nro"], field=r["field"], raw=r["llm_raw"],
                             post=r["llm_value"], ref=ps[(r["nro"], r["field"])]["pseudo"])
                        for r in rs if (r["nro"], r["field"]) in keys]
                add_group("150_base", tag, bits, "pseudo", texts,
                          {"TOTAL": fields, "sin_pct": [f for f in fields if f != PCT],
                           PCT: [PCT]})


def main():
    versions = pseudo_versions()
    _, texts = run_c3()
    run_c5_p2_288(texts)
    run_fase5(versions)
    run_150(versions)
    df = pd.DataFrame(ROWS)
    mc = pd.DataFrame(MCN)
    df.to_csv(B / "F1_rescore_crudo.csv", index=False)
    mc.to_csv(B / "F1_mcnemar.csv", index=False)

    def get(exp, model, grp, metric):
        r = df[(df.experimento == exp) & (df.modelo == model) & (df.campo == grp)
               & (df.metrica == metric)]
        return None if r.empty else f"{int(r.k.iloc[0])}/{int(r.n.iloc[0])}"

    summary = {
        "verificaciones_auditoria": {
            "C3_DIAS_null_baseline": {m: get("C3_288_humano", m, "DIAS_239", "null_bl") for m in MODELS},
            "C3_heldout_null_baseline": {m: get("C3_heldout50", m, "DIAS_heldout", "null_bl") for m in MODELS},
            "C3_DIAS_crudo": {m: get("C3_288_humano", m, "DIAS_239", "crudo") for m in MODELS},
            "C3_DIAS_post": {m: get("C3_288_humano", m, "DIAS_239", "post") for m in MODELS},
            "C3_pct_post": {m: get("C3_288_humano", m, PCT, "post") for m in MODELS},
            "C3_pct_null_baseline": {m: get("C3_288_humano", m, PCT, "null_bl") for m in MODELS},
            "Fase5_v2_post": {m: get("Fase5_867_v2", m, "TOTAL", "post") for m in MODELS},
        },
        "checks": CHECKS,
    }
    (B / "F1_rescore_crudo.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False),
                                             encoding="utf-8")
    print(json.dumps(summary, indent=1, ensure_ascii=False))
    print(f"\n{len(df)} filas -> F1_rescore_crudo.csv | {len(mc)} filas -> F1_mcnemar.csv")


if __name__ == "__main__":
    main()

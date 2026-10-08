"""I7 — Evalúa el GT fuera de muestra: regla, LLM crudo y sistema (rule_first) de cada modelo contra la referencia.

Referencias (planillas/): i7_gt_opus.csv (Opus a ciegas, columnas id, valor[, nota]) y, cuando exista,
i7_gt_usuario.csv (id, valor_anotador). Con las dos: κ usuario-Opus y la referencia pasa a ser la del usuario.
Predicciones: data/processed/llm_benchmark/I7_<modelo>.json (scripts/benchmark_i6.py --model <m> --i7).
Precisión por campo y estrato. La muestra sobre-representa a propósito los residuales difíciles, así que la
precisión del campo en PRODUCCIÓN se estima ponderando cada estrato por su tamaño en la base (pob_estrato):
p = Σ W_h p_h, IC 95 % estratificado con p̃_h = (k+2)/(n+4) (Agresti-Caffo) para no dar varianza 0 a 10/10.
Criterio de promote (plan I7 / sesión 24) sobre ese LCB del sistema: ≥ 0,85 PROMOTE, < 0,70 HOLD, intermedio
REVISAR. Los ítems con nota DUDA se informan aparte.
Salida: tabla en pantalla + data/processed/llm_benchmark/I7_resumen.csv
Uso: /root/vllm-env/bin/python scripts/evaluar_i7.py
"""
import json
import math
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from benchmark_i6 import MODELS_I6, _n  # noqa: E402
import ablacion_gt as ab  # noqa: E402
from evaluar_calibracion_v2 import kappa, val  # noqa: E402

LB = ROOT / "data/processed/llm_benchmark"
P = LB / "planillas"
SHORT = {"gemma-4-12b-qat-w4a16": "g12", "qwen3-14b-awq": "q14", "qwen3-8b-fp8": "q8", "gemma-4-26b-a4b-gguf": "g26"}


def wilson(k, n, z=1.96):
    if n == 0:
        return float("nan"), float("nan")
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return c - h, c + h


def strat(df, col_ok):
    """Precisión ponderada por tamaño de estrato y su IC 95 % (normal estratificada, Agresti-Caffo)."""
    gs = [(g.pob_estrato.iloc[0], int(g[col_ok].sum()), len(g)) for _, g in df.groupby("estrato")]
    tot = sum(N for N, _, _ in gs)
    p = sum(N / tot * k / n for N, k, n in gs)
    var = sum((N / tot) ** 2 * ((k + 2) / (n + 4)) * (1 - (k + 2) / (n + 4)) / (n + 4) for N, k, n in gs)
    return p, max(0.0, p - 1.96 * math.sqrt(var)), min(1.0, p + 1.96 * math.sqrt(var))


def num(x):
    v = val(x)
    return None if v == "NULL" else float(v)


def main():
    key = pd.read_csv(P / "i7_gt_clave.csv", dtype={"id": str, "nro_licitacion": str})
    op = pd.read_csv(P / "i7_gt_opus.csv", dtype={"id": str})
    df = key.merge(op[["id", "valor", "nota"]].rename(columns={"valor": "opus", "nota": "nota_opus"}), on="id")
    up = P / "i7_gt_usuario.csv"
    if up.exists():
        us = pd.read_csv(up, dtype={"id": str})
        df = df.merge(us[["id", "valor_anotador"]].rename(columns={"valor_anotador": "usuario"}), on="id", how="left")
        print(f"κ usuario-Opus global: {kappa(list(df.usuario.map(val)), list(df.opus.map(val))):.3f}")
        for f, g in df.groupby("campo"):
            print(f"  {f:34s} κ={kappa(list(g.usuario.map(val)), list(g.opus.map(val))):.3f}")
        df["ref"] = df.usuario.map(num)
        ref_name = "usuario"
    else:
        df["ref"] = df.opus.map(num)
        ref_name = "opus"
    df["duda"] = df.nota_opus.fillna("").str.upper().str.contains("DUDA")
    models = [m for m in MODELS_I6 if (LB / f"I7_{m}.json").exists()]
    for m in models:
        its = {it["id"]: it for it in json.loads((LB / f"I7_{m}.json").read_text(encoding="utf-8"))["items"]}
        df[f"llm_{SHORT[m]}"] = [its[i]["llm"] for i in df.id]
        df[f"sis_{SHORT[m]}"] = [its[i]["sistema"] for i in df.id]
    df["regla"] = df.valor_regla.where(df.estado_regla != "llm")

    def ok(col, g):
        return sum(ab.eq(_n(a), _n(b), f) for a, b, f in zip(g[col], g["ref"], g["campo"]))

    rows = []
    for (f, st), g in list(df.groupby(["campo", "estrato"])) + [((f, "TOTAL"), g) for f, g in df.groupby("campo")]:
        r = dict(campo=f, estrato=st, n=len(g), duda=int(g.duda.sum()))
        nr = g[g.estado_regla != "llm"]
        r["regla"] = f"{ok('regla', nr)}/{len(nr)}" if len(nr) else ""
        for m in models:
            s = SHORT[m]
            r[f"llm_{s}"] = f"{ok(f'llm_{s}', g)}/{len(g)}"
            r[f"sis_{s}"] = f"{ok(f'sis_{s}', g)}/{len(g)}"
        if st == "TOTAL":
            for m in models:
                s = SHORT[m]
                g = g.assign(_ok=[ab.eq(_n(a), _n(b), f) for a, b in zip(g[f"sis_{s}"], g["ref"])])
                pw, lo, hi = strat(g, "_ok")
                r[f"prod_{s}"] = f"{pw:.3f} [{lo:.3f}]"
                if m == models[0]:
                    r["veredicto"] = "PROMOTE" if lo >= 0.85 else "HOLD" if lo < 0.70 else "REVISAR"
        rows.append(r)
    res = pd.DataFrame(rows).sort_values(["campo", "estrato"])
    res.to_csv(LB / "I7_resumen.csv", index=False)
    print(f"\nReferencia: {ref_name}. prod_<m> = precisión del sistema ponderada a producción [LCB]; veredicto con {models[0] if models else '-'}")
    with pd.option_context("display.width", 250, "display.max_rows", 200, "display.max_columns", 50):
        print(res.to_string(index=False))
        # agregado: residuales (lo que decide el LLM) y regla fuera de muestra
        resid = df[df.estado_regla == "llm"]
        rule = df[df.estado_regla != "llm"]
        print(f"\nRegla fuera de muestra: {ok('regla', rule)}/{len(rule)} "
              f"(sin DUDA {ok('regla', rule[~rule.duda])}/{(~rule.duda).sum()})")
        for m in models:
            s = SHORT[m]
            k = ok(f"llm_{s}", resid)
            lo, hi = wilson(k, len(resid))
            print(f"LLM {m:24s} residuales {k}/{len(resid)} = {k / len(resid):.3f} [{lo:.3f}, {hi:.3f}]"
                  f" · discrepa {ok(f'llm_{s}', resid[resid.estrato == 'residual_discrepa'])}"
                  f"/{(resid.estrato == 'residual_discrepa').sum()}"
                  f" · sistema total {ok(f'sis_{s}', df)}/{len(df)}")
    df.to_csv(P / "i7_gt_resultado.csv", index=False)


if __name__ == "__main__":
    main()

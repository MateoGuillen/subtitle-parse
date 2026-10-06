"""Valida una planilla de aceptación v2 auditada (plan F5; auditoría H3.3, H3.4).

Lee la planilla CIEGA auditada + su CLAVE y compara el valor escrito por el auditor con
el valor del sistema (el auditor nunca vio el del sistema).

Regla de decisión por campo (unidad = plantilla/texto distinto):
  LCB95 Wilson >= 0.85  -> PROMOTE
  punto        <  0.70  -> HOLD
  resto                 -> JUICIO (casos listados uno por uno)
Cambios vs v1: sin `otro`=0,5 (no es un conteo binomial). `estado`:
  (vacío)  el auditor escribió `valor_auditor`
  no_fija  el pliego no fija el valor -> correcto si el sistema dio NULL
  ambiguo  cuenta como INCORRECTO en el criterio principal; se reporta también
           la sensibilidad excluyendo ambiguos.
Se reporta además la precisión ponderada por filas cubiertas (cuánto del universo
representa cada plantilla) y el desglose por fuente (rule / llm).
Con --segundo <planilla del 2º anotador> calcula acuerdo y κ de Cohen (H3.4).

Uso:
  /root/vllm-env/bin/python scripts/validar_aceptacion.py aceptacion_v2_<tag>.csv [--segundo otra.csv]
"""
import argparse
import math
import sys
from pathlib import Path

import pandas as pd

PLANILLAS = Path(__file__).resolve().parents[1] / "data/processed/llm_benchmark/planillas"


def wilson(k, n, z=1.96):
    if n == 0:
        return (float("nan"), float("nan"))
    p, d = k / n, 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def num(v):
    try:
        x = float(str(v).strip().replace(",", "."))
        return None if math.isnan(x) else x
    except ValueError:
        return None


def veredicto(row):
    """True/False/None(ambiguo) para un ítem auditado."""
    est = str(row.get("estado", "")).strip().lower()
    sist = num(row["valor_sistema"])
    if est == "ambiguo":
        return None
    if est == "no_fija":
        return sist is None
    aud = num(row["valor_auditor"])
    if aud is None:
        raise ValueError(f"ítem {row['id']}: sin valor_auditor ni estado válido")
    if sist is None:
        return False
    if abs(aud - sist) < 1e-9:
        return True
    return row["campo"].startswith("pct_") and (abs(aud * 100 - sist) < 1e-6
                                                or abs(aud - sist * 100) < 1e-6)


def load(name):
    sheet = pd.read_csv(PLANILLAS / name, dtype=str).fillna("")
    key = pd.read_csv(PLANILLAS / name.replace(".csv", "_clave.csv"), dtype={"id": str})
    return sheet.merge(key.drop(columns=["campo"]), on="id", how="left")


def kappa(a, b):
    """κ de Cohen sobre la etiqueta normalizada (valor | no_fija | ambiguo)."""
    labs = lambda r: (str(r["estado"]).strip().lower() or  # noqa: E731
                      f"v{num(r['valor_auditor'])}")
    la, lb = a.apply(labs, axis=1), b.apply(labs, axis=1)
    n = len(la)
    po = sum(x == y for x, y in zip(la, lb)) / n
    cats = set(la) | set(lb)
    pe = sum((sum(la == c) / n) * (sum(lb == c) / n) for c in cats)
    return po, (po - pe) / (1 - pe) if pe < 1 else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("planilla")
    ap.add_argument("--segundo", default="")
    a = ap.parse_args()
    df = load(a.planilla)
    pend = ((df["valor_auditor"].str.strip() == "") & (df["estado"].str.strip() == "")).sum()
    if pend:
        print(f"pendiente de auditar: {pend}/{len(df)} ítems — nada que validar todavía")
        return
    df["ok"] = df.apply(veredicto, axis=1)
    print(f"{'campo':34s} {'n':>4s} {'prec':>6s} {'IC95':>13s} {'decisión':8s} "
          f"{'pond.filas':>10s} {'sin_amb':>8s}  por fuente")
    for campo, g in df.groupby("campo"):
        n = len(g)
        k = int((g["ok"] == True).sum())  # noqa: E712
        lo, hi = wilson(k, n)
        p = k / n
        dec = "PROMOTE" if lo >= 0.85 else ("HOLD" if p < 0.70 else "JUICIO")
        w = g["filas_cubiertas"].astype(float)
        pw = float((w * (g["ok"] == True)).sum() / w.sum())  # noqa: E712
        ga = g[g["ok"].notna()]
        p_na = (ga["ok"] == True).mean() if len(ga) else float("nan")  # noqa: E712
        fuentes = {s: f"{int((gg['ok'] == True).sum())}/{len(gg)}"  # noqa: E712
                   for s, gg in g.groupby("source")}
        censo = " CENSO" if str(g["censo"].iloc[0]) == "True" else ""
        print(f"{campo:34s} {n:4d} {p:6.1%} [{lo:.2f},{hi:.2f}] {dec:8s} {pw:10.1%} "
              f"{p_na:8.1%}  {fuentes}{censo}")
        for _, r in g[g["ok"] != True].head(6).iterrows():  # noqa: E712
            print(f"    x id={r['id']} nro={r['nro_licitacion']} sistema={r['valor_sistema']} "
                  f"auditor={r['valor_auditor'] or r['estado']} :: {str(r['notas'])[:70]}")
    if a.segundo:
        b = load(a.segundo).set_index("id").loc[df["id"]].reset_index()
        po, k = kappa(df, b)
        print(f"\nIAA: acuerdo observado {po:.1%} | κ de Cohen {k:.3f} (n={len(df)})")


if __name__ == "__main__":
    sys.exit(main())

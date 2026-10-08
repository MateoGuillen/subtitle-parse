"""I5 — Evalúa la calibración v2: acuerdo usuario-Opus (κ de Cohen) y de cada anotador con la regla spec-v1.

Entradas (planillas/): calibracion_v2_clave.csv, calibracion_v2_opus.csv y la anotación del usuario
(calibracion_v2_usuario.csv: columnas id, valor_anotador[, nota]; convertir el xlsx a csv si hace falta).
Sin la del usuario, informa solo Opus vs regla.
κ por campo sobre categorías = valor exacto (NULL es una categoría). Criterio del plan: κ < 0,8 → se corrige
la ficha del campo. La regla se recalcula con el código vigente; los residuales (NO_RULE) no cuentan en el acuerdo con la regla.
Salida: tabla por campo + lista de desacuerdos con id; CSV en planillas/calibracion_v2_resultado.csv.
Uso: /root/vllm-env/bin/python scripts/evaluar_calibracion_v2.py [ruta_usuario.csv]
"""
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.etl.transformers import field_rules_v1 as fr  # noqa: E402
P = ROOT / "data/processed/llm_benchmark/planillas"


def val(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "NULL"
    s = str(x).strip().replace(",", ".")
    if s == "" or s.upper() in ("NULL", "NONE", "NAN", "NA"):
        return "NULL"
    try:
        return f"{float(s):g}"
    except ValueError:
        return s


def kappa(a, b):
    n = len(a)
    if n == 0:
        return float("nan")
    po = sum(x == y for x, y in zip(a, b)) / n
    ca, cb = Counter(a), Counter(b)
    pe = sum(ca[k] * cb[k] for k in set(ca) | set(cb)) / n / n
    return 1.0 if pe == 1 else (po - pe) / (1 - pe)


def main():
    key = pd.read_csv(P / "calibracion_v2_clave.csv", dtype={"id": str})
    op = pd.read_csv(P / "calibracion_v2_opus.csv", dtype={"id": str})
    # la regla VIGENTE sobre el mismo texto que vio el anotador (la clave guarda la del muestreo)
    tx = pd.read_csv(P / "calibracion_v2.csv", dtype={"id": str}).set_index("id").texto
    cur = {i: fr.RULES_V1[f](tx[i]) for i, f in zip(key.id, key.campo)}
    key["valor_regla"] = [None if (v is fr.NO_RULE or v is None) else v for v in cur.values()]
    key["estado_regla"] = ["llm" if v is fr.NO_RULE else "ausente" if v is None else "valor" for v in cur.values()]
    df = key.merge(op[["id", "valor"]].rename(columns={"valor": "opus"}), on="id", how="left")
    upath = Path(sys.argv[1]) if len(sys.argv) > 1 else P / "calibracion_v2_usuario.csv"
    has_user = upath.exists()
    if has_user:
        us = pd.read_csv(upath, dtype={"id": str})
        df = df.merge(us[["id", "valor_anotador"]].rename(columns={"valor_anotador": "usuario"}), on="id", how="left")
    for c in ["valor_regla", "opus"] + (["usuario"] if has_user else []):
        df[c + "_v"] = df[c].map(val)
    df.loc[df.estado_regla == "llm", "valor_regla_v"] = "RESIDUAL"
    rows = []
    for f, g in df.groupby("campo"):
        r = {"campo": f, "n": len(g)}
        nr = g[g.valor_regla_v != "RESIDUAL"]
        r["opus=regla"] = f"{(nr.opus_v == nr.valor_regla_v).sum()}/{len(nr)}"
        if has_user:
            r["usuario=opus"] = f"{(g.usuario_v == g.opus_v).sum()}/{len(g)}"
            r["kappa_usuario_opus"] = round(kappa(list(g.usuario_v), list(g.opus_v)), 3)
            r["usuario=regla"] = f"{(nr.usuario_v == nr.valor_regla_v).sum()}/{len(nr)}"
        rows.append(r)
    res = pd.DataFrame(rows)
    print(res.to_string(index=False))
    if has_user:
        print(f"\nκ global usuario-Opus: {kappa(list(df.usuario_v), list(df.opus_v)):.3f} "
              "(criterio: κ < 0,8 en un campo → corregir su ficha)")
    cols = ["id", "campo", "estrato", "valor_regla_v", "opus_v"] + (["usuario_v"] if has_user else [])
    dis = df[(df.opus_v != df.valor_regla_v) | (has_user and (df.get("usuario_v") != df.opus_v))][cols]
    print(f"\nDesacuerdos ({len(dis)}):\n{dis.to_string(index=False)}")
    df.to_csv(P / "calibracion_v2_resultado.csv", index=False)


if __name__ == "__main__":
    main()
